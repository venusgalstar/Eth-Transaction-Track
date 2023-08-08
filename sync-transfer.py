from web3 import Web3
from web3.middleware import geth_poa_middleware
import time
import sqlite3
import os

TOKEN_ABI = '[{	"inputs": [],	"name": "decimals",	"outputs": [{"internalType": "uint8","name": "","type": "uint8"}	],	"stateMutability": "view",	"type": "function"},{	"inputs": [],	"name": "name",	"outputs": [{"internalType": "string","name": "","type": "string"}	],	"stateMutability": "view",	"type": "function"},{	"inputs": [],	"name": "symbol",	"outputs": [{"internalType": "string","name": "","type": "string"}	],	"stateMutability": "view",	"type": "function"},{	"inputs": [],	"name": "totalSupply",	"outputs": [{"internalType": "uint256","name": "","type": "uint256"}	],	"stateMutability": "view",	"type": "function"}]'
# Set global environment variables
confirmationBlocks = "1"
nodeUrl = "/media/blockchain/execution/geth/geth.ipc"
# nodeUrl = "http://localhost:8545"
pollingPeriod = 1
startBlock = 17231623
dbName = "transfer.db"

# Connect to Ethereum node
if nodeUrl.startswith("http"):
    web3 = Web3(Web3.HTTPProvider(nodeUrl))  # "http://publicnode:8545"
elif nodeUrl.startswith("ws"):
    web3 = Web3(Web3.WebsocketProvider(nodeUrl))  # "ws://publicnode:8546"
else:
    web3 = Web3(Web3.IPCProvider(nodeUrl))  # "/home/geth/.ethereum/geth.ipc"

web3.middleware_onion.inject(geth_poa_middleware, layer=0)

# Create sqlite database


def create_database():
    conn = sqlite3.connect(dbName)
    c = conn.cursor()

    c.execute('''
        CREATE TABLE transfer (
            blockNumber INTEGER,
            fromAddress TEXT,
            toAddress TEXT,
            amount TEXT,
            address TEXT,
            transactionHash TEXT
        )
    ''')

    c.execute('''
        CREATE TABLE token (
            name TEXT,
            symbol TEXT,
            address TEXT,
            decimal INTEGER
        )
    ''')

    conn.commit()
    conn.close()

# Insert transfer data into sqlite database
def insert_transfer(data):
    conn = sqlite3.connect(dbName)
    c = conn.cursor()

    c.execute('''
        INSERT INTO transfer VALUES (
            :blockNumber, :fromAddress, :toAddress, :amount, :address, :transactionHash
        )
    ''', data)

    conn.commit()
    conn.close()

# Create database if it does not exist
if not os.path.exists(dbName):
    create_database()

def handle_event(event, c):
    
    if len(event['data']) < len("0x000000000000000000000000"):
        amount = 0
    else : amount = int(event['data'], 16)

    try:
        data = {
            'blockNumber': event['blockNumber'],
            'fromAddress': event['topics'][1].hex().replace("0x000000000000000000000000","0x"),
            'toAddress': event['topics'][2].hex().replace("0x000000000000000000000000","0x"),
            'amount': str(amount),
            'address': event['address'],
            'transactionHash': event['transactionHash'].hex(),
        }

        c.execute('''
            INSERT INTO transfer VALUES (
                :blockNumber, :fromAddress, :toAddress, :amount, :address, :transactionHash
            )
        ''', data)

    except:
        return
    
    c.execute("SELECT address FROM token WHERE address = ?", (event['address'],))
    token = c.fetchone()

    if token is None:

        contract = web3.eth.contract(address = event['address'], abi = TOKEN_ABI)
        
        try:
            name = contract.functions.name().call()
            symbol = contract.functions.symbol().call()
        except:
            return

        try:
            decimal = contract.functions.decimals().call()
        except:
            decimal = 18

        c.execute('''
            INSERT INTO token VALUES (
                :name, :symbol, :address, :decimal
            )
        ''', {'name':name, 'symbol':symbol, 'address':event['address'], 'decimal':decimal})
    
    
def log_loop(event_filter):
    conn = sqlite3.connect(dbName)
    c = conn.cursor()

    for event in event_filter.get_all_entries():
        handle_event(event, c)
    
    conn.commit()
    conn.close()

# Fetch all of new (not in index) Ethereum blocks and add transactions to index
max_block_id = startBlock

while True:

    endblock = int(web3.eth.blockNumber) - int(confirmationBlocks)

    transfer_event_topic = web3.keccak(text="Transfer(address,address,uint256)").hex()

    checkingBlock = max_block_id + 100

    if checkingBlock > endblock:
        checkingBlock = endblock

    print(max_block_id)

    if max_block_id < endblock :
        log_filter = web3.eth.filter({
            "fromBlock": max_block_id,
            "toBlock": checkingBlock,
            "topics": [transfer_event_topic]
        })
        log_loop(log_filter)
        max_block_id = checkingBlock + 1


    # time.sleep(pollingPeriod)
