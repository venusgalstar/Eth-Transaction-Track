from web3 import Web3
from web3.middleware import geth_poa_middleware
import time
import sqlite3
import os

TOKEN_ABI = '[{	"inputs": [],	"name": "decimals",	"outputs": [{"internalType": "uint8","name": "","type": "uint8"}	],	"stateMutability": "view",	"type": "function"},{	"inputs": [],	"name": "name",	"outputs": [{"internalType": "string","name": "","type": "string"}	],	"stateMutability": "view",	"type": "function"},{	"inputs": [],	"name": "symbol",	"outputs": [{"internalType": "string","name": "","type": "string"}	],	"stateMutability": "view",	"type": "function"},{	"inputs": [],	"name": "totalSupply",	"outputs": [{"internalType": "uint256","name": "","type": "uint256"}	],	"stateMutability": "view",	"type": "function"}]'
PAIR_ABI ='[{"type": "function","stateMutability": "view","outputs": [{"type": "uint8","name": "","internalType": "uint8"}],"name": "decimals","inputs": [],"constant": true},{"type": "function","stateMutability": "view","outputs": [{"type": "address","name": "","internalType": "address"}],"name": "factory","inputs": [],"constant": true},{"type": "function","stateMutability": "view","outputs": [{"type": "uint112","name": "_reserve0","internalType": "uint112"},{"type": "uint112","name": "_reserve1","internalType": "uint112"},{"type": "uint32","name": "_blockTimestampLast","internalType": "uint32"}],"name": "getReserves","inputs": [],"constant": true},{"type": "function","stateMutability": "view","outputs": [{"type": "string","name": "","internalType": "string"}],"name": "name","inputs": [],"constant": true},{"type": "function","stateMutability": "view","outputs": [{"type": "string","name": "","internalType": "string"}],"name": "symbol","inputs": [],"constant": true},{"type": "function","stateMutability": "view","outputs": [{"type": "address","name": "","internalType": "address"}],"name": "token0","inputs": [],"constant": true},{"type": "function","stateMutability": "view","outputs": [{"type": "address","name": "","internalType": "address"}],"name": "token1","inputs": [],"constant": true}]'

# Set global environment variables
confirmationBlocks = "1"
nodeUrl = "/media/blockchain/execution/geth/geth.ipc"
# nodeUrl = "http://localhost:8545"
pollingPeriod = 1
startBlock = 17230000
dbName = "swap.db"

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

    amount0In = str(int(event['data'][2:65], 16))
    amount1In = str(int(event['data'][66:129], 16))
    amount0Out = str(int(event['data'][130:193], 16))
    amount1Out = str(int(event['data'][194:257], 16))

    amountIn = 0
    amountOut = 0

    print(event['transactionHash'].hex())

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
        'fromAddress': event['topics'][2].hex().replace("0x000000000000000000000000","0x"),
        'pairAddress': event['address'],
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

while True:

    print(max_block_id)

    endblock = int(web3.eth.blockNumber) - int(confirmationBlocks)
    swap_event_topic = web3.keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)").hex()
    checkingBlock = max_block_id + 1000

    if checkingBlock > endblock:
        checkingBlock = endblock

    if max_block_id < endblock :
        swap_filter = web3.eth.filter({
            "fromBlock": max_block_id,
            "toBlock": checkingBlock,
            "topics": [
                [swap_event_topic],
                [],
                [
                    '0x000000000000000000000000788425510bf225b75580804e2441339e17e1a6a5', 
                    '0x000000000000000000000000cd76bd589a81e978014f237c5063c80335490ae0', 
                    '0x000000000000000000000000c17b1e62eaef2805f664ed44972fcc7e6647474a', 
                    '0x0000000000000000000000001dad947dd181faa6c751ec14e2683e0a8fe2bf8c'
                ]
            ]
        })
        swap_loop(swap_filter)
        max_block_id = checkingBlock + 1
    else:
        time.sleep(pollingPeriod)
