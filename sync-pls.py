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
            transactionHash TEXT
        )
    ''')

    conn.commit()
    conn.close()

# Create database if it does not exist
if not os.path.exists(dbName):
    create_database()

# Adds all transactions from Ethereum block
def insertTxsFromBlock(block, c):

    blockid = block['number']
    # inputinfo contains address
    accounts = {
        "0x1daD947dD181fAa6c751ec14e2683e0A8fE2bf8c",
        "0xc17b1e62eAEf2805F664ed44972FCc7E6647474A",
        "0xCD76BD589A81E978014F237C5063c80335490Ae0",
        "0x788425510Bf225b75580804E2441339E17e1a6a5",
    }
    
    for txNumber in range(0, len(block.transactions)):
        trans = block.transactions[txNumber]
        txfrom = trans['from']
        txto = trans['to']

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

        c.execute('''
            INSERT INTO transactions VALUES (
                :blockNumber, :fromAddress, :gas, :gasPrice,
                :toAddress, :value,:gasUsed, :transactionHash
            )
        ''', data)


# Fetch all of new (not in index) Ethereum blocks and add transactions to index
max_block_id = startBlock

while True:

    conn = sqlite3.connect(dbName)
    c = conn.cursor()

    endblock = int(web3.eth.blockNumber) - int(confirmationBlocks)

    for blockHeight in range(max_block_id, endblock):        
            
        
        block = web3.eth.getBlock(blockHeight, True)
        if len(block.transactions) > 0:
            insertTxsFromBlock(block, c)

        if (blockHeight - max_block_id) % 1000 == 0 :
            print(blockHeight)
            conn.commit()
    
    max_block_id = endblock
    
    conn.commit()
    conn.close()

    time.sleep(pollingPeriod)
