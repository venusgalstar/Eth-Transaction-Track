
from env import web3
from env import accounts
from env import startBlock
from env import endBlock
from env import confirmationBlocks
from tqdm import tqdm
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

        try:
            txfrom = trans['from'].lower()
        except:
            txfrom = trans['from']

        try:
            txto = trans['to'].lower()
        except:
            txto = trans['to']

        if not(txfrom in accounts) and not(txto in accounts):
            continue

        # Perform receipt and strip operations lateron
        transReceipt = web3.eth.getTransactionReceipt(trans['hash'])
        print(transReceipt)

        data = {
            'blockNumber': blockid,
            'fromAddress': txfrom,
            'gas': trans['gas'],
            'gasPrice': trans['gasPrice'],
            'toAddress': txto,
            'value': str(trans['value']),
            'gasUsed': str(transReceipt['gasUsed'] * transReceipt['effectiveGasPrice']),
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
endblock = endBlock

conn = sqlite3.connect(dbName)
c = conn.cursor()

print("Starting pls transfer syncing " + str(endblock - max_block_id) + " blocks, final block number is " + str(endblock))
pbar = tqdm(total = endblock - max_block_id)

for blockHeight in range(max_block_id, endblock):            
    block = web3.eth.getBlock(blockHeight, True)
    if len(block.transactions) > 0:
        insertTxsFromBlock(block, c)

    if (blockHeight - max_block_id) % 1000 == 0 :
        conn.commit()
    pbar.update(1)

conn.commit()
conn.close()
pbar.close()

