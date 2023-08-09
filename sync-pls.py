from web3 import Web3
from web3.middleware import geth_poa_middleware
import time
import sqlite3
import os

# Set global environment variables
confirmationBlocks = "1"
nodeUrl = "/media/blockchain/execution/geth/geth.ipc"
# nodeUrl = "http://localhost:8545"
pollingPeriod = 5
startBlock = 17230000
dbName = "transactions.db"

# Connect to Ethereum node
if nodeUrl.startswith("http"):
    web3 = Web3(Web3.HTTPProvider(nodeUrl)) # "http://publicnode:8545"
elif nodeUrl.startswith("ws"):
    web3 = Web3(Web3.WebsocketProvider(nodeUrl)) # "ws://publicnode:8546"
else:
    web3 = Web3(Web3.IPCProvider(nodeUrl)) # "/home/geth/.ethereum/geth.ipc"

web3.middleware_onion.inject(geth_poa_middleware, layer=0)

# Create sqlite database
def create_database():
    conn = sqlite3.connect(dbName)
    c = conn.cursor()

    c.execute('''
        CREATE TABLE transactions (
            blockNumber INTEGER,
            fromAddress TEXT,
            gas TEXT,
            gasPrice TEXT,
            toAddress TEXT,
            value TEXT,
            gasUsed TEXT,
            transactionHash TEXT,
        )
    ''')

    conn.commit()
    conn.close()

# Insert data into sqlite database
def insert_data(data):
    conn = sqlite3.connect('transactions.db')
    c = conn.cursor()

    c.execute('''
        INSERT INTO transactions VALUES (
            :blockNumber, :fromAddress, :gas, :gasPrice,
            :toAddress, :value,:gasUsed, :transactionHash
        )
    ''', data)

    conn.commit()
    conn.close()

# Create database if it does not exist
if not os.path.exists(dbName):
    create_database()

# Convert public key to address
def publicKeyToAddress(public_key):
    public_key_bytes = bytes.fromhex(public_key)
    hashed_public_key = Web3.keccak(public_key_bytes)
    address_bytes = hashed_public_key[-20:]
    address = address_bytes.hex()
    return address

# Adds all transactions from Ethereum block
def insertTxsFromBlock(block):
    blockid = block['number']
    for txNumber in range(0, len(block.transactions)):
        trans = block.transactions[txNumber]

        # filter transactions
        # txinfo = trans['input']
        txfrom = trans['from']
        txto = trans['to']

        # inputinfo contains address
        accounts = {
            '0x788425510bf225b75580804e2441339e17e1a6a5', 
            '0xcd76bd589a81e978014f237c5063c80335490ae0', 
            '0xc17b1e62eaef2805f664ed44972fcc7e6647474a', 
            '0x1dad947dd181faa6c751ec14e2683e0a8fe2bf8c'
        }

        if not(txfrom in accounts) and not(txto in accounts):
            continue

        # Perform receipt and strip operations lateron
        transReceipt = web3.eth.getTransactionReceipt(trans['hash'])

        data = {
            'blockNumber': blockid,
            'fromAddress': trans['from'],
            'gas': trans['gas'],
            'gasPrice': trans['gasPrice'],
            'toAddress': trans['to'],
            'value': str(trans['value']),
            'gasUsed': transReceipt['gasUsed'],
            'transactionHash': transReceipt['transactionHash'].hex(),
        }

        insert_data(data)


# Fetch all of new (not in index) Ethereum blocks and add transactions to index
max_block_id = startBlock

while True:

    endblock = int(web3.eth.blockNumber) - int(confirmationBlocks)

    for blockHeight in range(max_block_id, endblock):
        block = web3.eth.getBlock(blockHeight, True)
        if len(block.transactions) > 0:
            insertTxsFromBlock(block)
            print('Block ' + str(blockHeight) + ' with ' + str(len(block.transactions)) + ' transactions is processed')
        else:
            print('Block ' + str(blockHeight) + ' does not contain transactions')

    time.sleep(pollingPeriod)
