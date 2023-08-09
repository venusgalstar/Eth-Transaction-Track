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
startBlock = 17230000
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
            transactionHash TEXT,
            logIndex INTEGER
        )
    ''')

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
        sender = event['topics'][1].hex().replace("0x000000000000000000000000","0x")
    except:
        sender = "0x0000000000000000000000000000000000000000"

    try:
        receiver = event['topics'][2].hex().replace("0x000000000000000000000000","0x")
    except:
        receiver = "0x0000000000000000000000000000000000000000"

    accounts = {
        "0x1daD947dD181fAa6c751ec14e2683e0A8fE2bf8c".lower(),
        "0xc17b1e62eAEf2805F664ed44972FCc7E6647474A".lower(),
        "0xCD76BD589A81E978014F237C5063c80335490Ae0".lower(),
        "0x788425510Bf225b75580804E2441339E17e1a6a5".lower(),
    }

    if not(sender in accounts) and not(receiver in accounts):
        return

    # print(event['transactionHash'].hex())

    try:
        data = {
            'blockNumber': event['blockNumber'],
            'fromAddress': sender,
            'toAddress': receiver,
            'amount': str(amount),
            'address': event['address'],
            'transactionHash': event['transactionHash'].hex(),
            'logIndex': event['logIndex'],
        }

        c.execute('''
            INSERT INTO transfer VALUES (
                :blockNumber, :fromAddress, :toAddress, :amount, :address, :transactionHash, :logIndex
            )
        ''', data)

    except:
        return
    
def log_loop(event_filter):
    # try:

        conn = sqlite3.connect(dbName)
        c = conn.cursor()

        for event in event_filter.get_all_entries():
            handle_event(event, c)
        
        conn.commit()
        conn.close()
    # except:
    #     return

# Fetch all of new (not in index) Ethereum blocks and add transactions to index
max_block_id = startBlock

while True:

    endblock = int(web3.eth.blockNumber) - int(confirmationBlocks)

    transfer_event_topic = web3.keccak(text="Transfer(address,address,uint256)").hex()

    checkingBlock = max_block_id + 1000

    if checkingBlock > endblock:
        checkingBlock = endblock

    print(max_block_id)

    if max_block_id < endblock :
        # log_filter = web3.eth.filter({
        #     "fromBlock": max_block_id,
        #     "toBlock": checkingBlock,
        #     "topics": [transfer_event_topic]
        # })
        # log_loop(log_filter)
        log_filter = web3.eth.filter({
            "fromBlock": max_block_id,
            "toBlock": checkingBlock,
            "topics": [
                [transfer_event_topic],
                [
                    '0x000000000000000000000000788425510bf225b75580804e2441339e17e1a6a5', 
                    '0x000000000000000000000000cd76bd589a81e978014f237c5063c80335490ae0', 
                    '0x000000000000000000000000c17b1e62eaef2805f664ed44972fcc7e6647474a', 
                    '0x0000000000000000000000001dad947dd181faa6c751ec14e2683e0a8fe2bf8c'
                ],
            ]
        })
        log_loop(log_filter)
        log_filter = web3.eth.filter({
            "fromBlock": max_block_id,
            "toBlock": checkingBlock,
            "topics": [
                [transfer_event_topic],
                [],
                [
                    '0x000000000000000000000000788425510bf225b75580804e2441339e17e1a6a5', 
                    '0x000000000000000000000000cd76bd589a81e978014f237c5063c80335490ae0', 
                    '0x000000000000000000000000c17b1e62eaef2805f664ed44972fcc7e6647474a', 
                    '0x0000000000000000000000001dad947dd181faa6c751ec14e2683e0a8fe2bf8c'
                ],
            ]
        })
        log_loop(log_filter)
        max_block_id = checkingBlock + 1


    # time.sleep(pollingPeriod)
