from env import web3
import time
import sqlite3
import os
from env import startBlock
from env import confirmationBlocks
from env import topics
from tqdm import tqdm

dbName = "wrap.db"
depositTopic = web3.keccak(text="Deposit(address,uint256)").hex()
withdrawTopic = web3.keccak(text="Withdrawal(address,uint256)").hex()

print(depositTopic)
print(withdrawTopic)

# Create sqlite database
def create_database():
    conn = sqlite3.connect(dbName)
    c = conn.cursor()

    c.execute('''
        CREATE TABLE wrap (
            blockNumber INTEGER,
            fromAddress TEXT,
            amount TEXT,
            type TEXT,
            transactionHash TEXT,
            unique (transactionHash)
        )
    ''')

    conn.commit()
    conn.close()

# Create database if it does not exist
if not os.path.exists(dbName):
    create_database()

def handle_event(event, c):
    
    print(event)

    if len(event['data']) < len("0x000000000000000000000000"):
        amount = 0
    else : amount = int(event['data'], 16)

    sender = event['topics'][1].hex().replace("0x000000000000000000000000","0x")
    
    if depositTopic == event['topics'][0]:
        type = "Deposit"
    else:
        type = "Withdraw"

    try:
        data = {
            'blockNumber': event['blockNumber'],
            'fromAddress': sender,
            'amount': str(amount),
            'type': type,
            'transactionHash': event['transactionHash'].hex(),
        }

        c.execute('''
            INSERT or IGNORE INTO wrap VALUES (
                :blockNumber, :fromAddress, :amount, :type, :transactionHash
            )
        ''', data)

    except:
        return
    
def log_loop(event_filter):
    conn = sqlite3.connect(dbName)
    c = conn.cursor()

    for event in event_filter.get_all_entries():
        handle_event(event, c)
    
    conn.commit()
    conn.close()

# Fetch all of new (not in index) Ethereum blocks and add transactions to index
max_block_id = startBlock
endblock = int(web3.eth.blockNumber) - int(confirmationBlocks)


print("Starting transfer syncing " + str(endblock - max_block_id) + " blocks, final block number is " + str(endblock))

pbar = tqdm(total = endblock - max_block_id)

while max_block_id < endblock :

    checkingBlock = max_block_id + 1000

    if checkingBlock > endblock:
        checkingBlock = endblock
    
    # print(max_block_id)

    log_filter = web3.eth.filter({
        "fromBlock": max_block_id,
        "toBlock": checkingBlock,
        "topics": [
            [
                depositTopic,
                withdrawTopic
            ],
            topics
        ]
    })
    log_loop(log_filter)
    
    max_block_id = checkingBlock + 1
    pbar.update(1001)

pbar.close()
