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
pollingPeriod = 5
startBlock = 17230000
dbName = "transactions.db"

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
            name TEXT,
            symbol TEXT,
            address TEXT,
            transactionHash TEXT
        )
    ''')

    c.execute('''             
        CREATE TABLE swap (
            blockNumber INTEGER,
            fromAddress TEXT,
            pairAddress TEXT,
            amountIn TEXT,
            amountOut TEXT,
            inTokenName TEXT,
            inTokenSymbol TEXT,              
            outTokenName TEXT,
            outTokenSymbol TEXT,
            inTokenAddress TEXT,
            outTokenAddress TEXT,
            transactionHash TEXT
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
            :blockNumber, :fromAddress, :toAddress, :amount, :name, :symbol, :address,
            :transactionHash
        )
    ''', data)

    conn.commit()
    conn.close()

# Insert swap data into sqlite database


def insert_swap(data):
    conn = sqlite3.connect('transactions.db')
    c = conn.cursor()

    c.execute('''
        INSERT INTO swap VALUES (
            :blockNumber, :fromAddress, :pairAddress, :amountIn, :amountOut, 
            :inTokenName, :inTokenNymbol, :outTokenName, :outTokenNymbol,
            :inTokenAddress, :outTokenAddress, :transactionHash
        )
    ''', data)

    conn.commit()
    conn.close()


# Create database if it does not exist
if not os.path.exists(dbName):
    create_database()

def handle_event(event):
    print(event['address'])
    contract = web3.eth.contract(address = event['address'], abi = TOKEN_ABI)
    name = contract.functions.name().call()
    symbol = contract.functions.symbol().call()

    data = {
        'blockNumber': event['blockNumber'],
        'fromAddress': event['topics'][1].hex().replace("0x000000000000000000000000","0x"),
        'toAddress': event['topics'][2].hex().replace("0x000000000000000000000000","0x"),
        'amount': str(int(event['data'],16)),
        'name': name,
        'symbol': symbol,
        'address': event['address'],
        'transactionHash': event['transactionHash'].hex(),
    }

    insert_transfer(data)


def log_loop(event_filter):
    for event in event_filter.get_all_entries():
        handle_event(event)


# Fetch all of new (not in index) Ethereum blocks and add transactions to index
while True:

    conn = sqlite3.connect(dbName)
    c = conn.cursor()
    c.execute("SELECT MAX(blockNumber) FROM transfer")
    max_block_id = c.fetchone()
    max_block_id = max_block_id[0]
    conn.close()

    if max_block_id is None:
        max_block_id = startBlock
    elif max_block_id < startBlock:
        max_block_id = startBlock

    endblock = int(web3.eth.blockNumber) - int(confirmationBlocks)
    checkingBlock = max_block_id + 1 
    transfer_event_topic = web3.keccak(text="Transfer(address,address,uint256)").hex()
    swap_event_topic = web3.keccak(text="Transfer(address,address,uint256)").hex()

    if checkingBlock > endblock:
        checkingBlock = endblock

    if max_block_id < endblock :
        log_filter = web3.eth.filter({
            "fromBlock": max_block_id,
            "toBlock": checkingBlock,
            "topics": [transfer_event_topic]
        })
        log_loop(log_filter)


    # for blockHeight in range(max_block_id, endblock):
    #     block = web3.eth.getBlock(blockHeight, True)
    #     if len(block.transactions) > 0:
    #         insertTxsFromBlock(block)
    #         print('Block ' + str(blockHeight) + ' with ' +
    #               str(len(block.transactions)) + ' transactions is processed')
    #     else:
    #         print('Block ' + str(blockHeight) +
    #               ' does not contain transactions')

    time.sleep(pollingPeriod)
