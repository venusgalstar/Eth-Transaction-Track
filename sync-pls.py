from env import web3
from env import accounts
from env import startBlock
from env import confirmationBlocks

import time
import sqlite3
import os

dbName = "transactions.db"

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
            unique (transactionHash)
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
    
    for txNumber in range(0, len(block.transactions)):
        trans = block.transactions[txNumber]
        txfrom = trans['from'].lower()
        txto = trans['to'].lower()

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
            INSERT or IGNORE INTO transactions VALUES (
                :blockNumber, :fromAddress, :gas, :gasPrice,
                :toAddress, :value,:gasUsed, :transactionHash
            )
        ''', data)


# Fetch all of new (not in index) Ethereum blocks and add transactions to index
max_block_id = startBlock

# while True:
print("Starting pls syncing")

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
print("Ended pls syncing")
    # time.sleep(pollingPeriod)
