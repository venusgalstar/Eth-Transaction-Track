from env import web3
from env import PAIR_ABI
from env import startBlock
from env import confirmationBlocks
from env import topics
import time
from tqdm import tqdm
import sqlite3
import os

dbName = "swap.db"

# Create sqlite database
def create_database():
    conn = sqlite3.connect(dbName)
    c = conn.cursor()

    c.execute('''             
        CREATE TABLE swap (
            blockNumber INTEGER,
            fromAddress TEXT,
            pairAddress TEXT,
            amountIn TEXT,
            amountOut TEXT,
            inTokenAddress TEXT,
            outTokenAddress TEXT,
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

def handle_swap_event(event, c):


    # try:
    pair_contract = web3.eth.contract(address = event['address'], abi = PAIR_ABI)
    token0 = pair_contract.functions.token0().call()
    token1 = pair_contract.functions.token1().call()

    amount0In = str(int(event['data'][2:66], 16))
    amount1In = str(int(event['data'][66:130], 16))
    amount0Out = str(int(event['data'][130:194], 16))
    amount1Out = str(int(event['data'][194:258], 16))
    
    amountIn = 0
    amountOut = 0

    if amount0In == '0' and amount1Out =='0':
        amountIn = amount1In
        amountOut = amount0Out
        inToken = token1
        outToken = token0
    else:
        amountIn = amount0In
        amountOut = amount1Out
        inToken = token0
        outToken = token1

    data = {
        'blockNumber': event['blockNumber'],
        'fromAddress': event['topics'][2].hex().replace("0x000000000000000000000000","0x").lower(),
        'pairAddress': event['address'].lower(),
        'amountIn': amountIn,
        'amountOut': amountOut,
        'inTokenAddress': inToken,
        'outTokenAddress': outToken,
        'transactionHash': event['transactionHash'].hex(),
        'logIndex': event['logIndex']
    }

    c.execute('''
        INSERT or IGNORE INTO swap VALUES (
            :blockNumber, :fromAddress, :pairAddress, :amountIn, :amountOut, 
            :inTokenAddress, :outTokenAddress, :transactionHash, :logIndex
        )
    ''', data)

def swap_loop(event_filter):
    conn = sqlite3.connect(dbName)
    c = conn.cursor()

    for event in event_filter.get_all_entries():
        handle_swap_event(event, c)
    
    conn.commit()
    conn.close()

# Fetch all of new (not in index) Ethereum blocks and add transactions to index
max_block_id = startBlock
endblock = int(web3.eth.blockNumber) - int(confirmationBlocks)
swap_event_topic = web3.keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)").hex()

print("Starting swap syncing " + str(endblock - max_block_id) + " blocks, final block number is " + str(endblock))

pbar = tqdm(total = endblock - max_block_id)

while max_block_id < endblock :

    # TODO: Again logging using tqdm or something similar; proper instead of debugging print for yourself

    checkingBlock = max_block_id + 1000

    if checkingBlock > endblock:
        checkingBlock = endblock

    swap_filter = web3.eth.filter({
        "fromBlock": max_block_id,
        "toBlock": checkingBlock,
        "topics": [
            [swap_event_topic],
            [],
            topics
        ]
    })
    swap_loop(swap_filter)
    max_block_id = checkingBlock + 1
    pbar.update(1001)

pbar.close()