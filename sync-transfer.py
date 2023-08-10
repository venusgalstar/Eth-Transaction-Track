from web3 import Web3
from web3.middleware import geth_poa_middleware
import time
import sqlite3
import os
from env import startBlock
from env import confirmationBlocks
from env import nodeUrl
from env import pollingPeriod
from env import topics

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
            logIndex INTEGER,
            unique (transactionHash, logIndex)
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

    sender = event['topics'][1].hex().replace("0x000000000000000000000000","0x")
    receiver = event['topics'][2].hex().replace("0x000000000000000000000000","0x")

    if not(event['topics'][1].hex() in topics) and not(event['topics'][2].hex() in topics):
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
            INSERT or IGNORE INTO transfer VALUES (
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
        log_filter = web3.eth.filter({
            "fromBlock": max_block_id,
            "toBlock": checkingBlock,
            "topics": [
                [transfer_event_topic],
                topics
            ]
        })
        log_loop(log_filter)
        log_filter = web3.eth.filter({
            "fromBlock": max_block_id,
            "toBlock": checkingBlock,
            "topics": [
                [transfer_event_topic],
                [],
                topics
            ]
        })
        log_loop(log_filter)
        max_block_id = checkingBlock + 1
    else:
        time.sleep(pollingPeriod)
