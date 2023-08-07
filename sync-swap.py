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
            inTokenName TEXT,
            inTokenSymbol TEXT,              
            outTokenName TEXT,
            outTokenSymbol TEXT,
            inTokenAddress TEXT,
            outTokenAddress TEXT,
            inTokenDecimal INTEGER,
            outTokenDecimal INTEGER,
            transactionHash TEXT
        )
    ''')

    conn.commit()
    conn.close()

# Insert swap data into sqlite database
def insert_swap(data):
    conn = sqlite3.connect(dbName)
    c = conn.cursor()

    c.execute('''
        INSERT INTO swap VALUES (
            :blockNumber, :fromAddress, :pairAddress, :amountIn, :amountOut, 
            :inTokenName, :inTokenSymbol, :outTokenName, :outTokenSymbol,
            :inTokenAddress, :outTokenAddress, :inTokenDecimal, :outTokenDecimal,
            :transactionHash
        )
    ''', data)

    conn.commit()
    conn.close()


# Create database if it does not exist
if not os.path.exists(dbName):
    create_database()

def handle_swap_event(event):

    print(event['transactionHash'].hex())
    pair_contract = web3.eth.contract(address = event['address'], abi = PAIR_ABI)
    token0 = pair_contract.functions.token0().call()
    token1 = pair_contract.functions.token1().call()

    token0_contract = web3.eth.contract(address = token0, abi = TOKEN_ABI)
    token1_contract = web3.eth.contract(address = token1, abi = TOKEN_ABI)

    token0Name = token0_contract.functions.name().call()
    token0Symbol = token0_contract.functions.symbol().call()
    
    try:
        token0Decimal = token0_contract.functions.decimals().call()
    except:
        token0Decimal = 18

    token1Name = token1_contract.functions.name().call()
    token1Symbol = token1_contract.functions.symbol().call()

    try:
        token1Decimal = token1_contract.functions.decimals().call()
    except:
        token1Decimal = 18

    amount0In = str(int(event['data'][2:65], 16))
    amount1In = str(int(event['data'][66:129], 16))
    amount0Out = str(int(event['data'][130:193], 16))
    amount1Out = str(int(event['data'][194:257], 16))

    amountIn = 0
    amountOut = 0

    inTokenName = ''
    inTokenSymbol = ''
    outTokenName = ''
    outTokenSymbol = ''

    inToken = ''
    outToken = ''

    inTokenDecimal = 0
    outTokenDecimal = 0

    if amount0In == '0' and amount1Out =='0':
        amountIn = amount1In
        amountOut = amount0Out
        inTokenName = token1Name
        inTokenSymbol = token1Symbol
        outTokenName = token0Name
        outTokenSymbol = token0Symbol
        inToken = token1
        outToken = token0
        inTokenDecimal = token1Decimal
        outTokenDecimal = token0Decimal
    else:
        amountIn = amount0In
        amountOut = amount1Out
        inTokenName = token0Name
        inTokenSymbol = token0Symbol
        outTokenName = token1Name
        outTokenSymbol = token1Symbol
        inToken = token0
        outToken = token1
        inTokenDecimal = token0Decimal
        outTokenDecimal = token1Decimal

    data = {
        'blockNumber': event['blockNumber'],
        'fromAddress': event['topics'][2].hex().replace("0x000000000000000000000000","0x"),
        'pairAddress': event['address'],
        'amountIn': amountIn,
        'amountOut': amountOut,
        'inTokenName': inTokenName,
        'inTokenSymbol': inTokenSymbol,
        'outTokenName': outTokenName,
        'outTokenSymbol': outTokenSymbol,
        'inTokenAddress': inToken,
        'outTokenAddress': outToken,
        'inTokenDecimal': inTokenDecimal,
        'outTokenDecimal': outTokenDecimal,
        'transactionHash': event['transactionHash'].hex(),
    }

    insert_swap(data)

def swap_loop(event_filter):
    for event in event_filter.get_all_entries():
        handle_swap_event(event)

# Fetch all of new (not in index) Ethereum blocks and add transactions to index
while True:

    conn = sqlite3.connect(dbName)
    c = conn.cursor()
    c.execute("SELECT MAX(blockNumber) FROM swap")
    max_block_id = 17386422 #c.fetchone()
    # max_block_id = max_block_id[0]
    conn.close()

    print(max_block_id)

    if max_block_id is None:
        max_block_id = startBlock
    elif max_block_id < startBlock:
        max_block_id = startBlock

    endblock = int(web3.eth.blockNumber) - int(confirmationBlocks)
    checkingBlock = max_block_id + 1 
    swap_event_topic = web3.keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)").hex()

    if checkingBlock > endblock:
        checkingBlock = endblock

    if max_block_id < endblock :

        swap_filter = web3.eth.filter({
            "fromBlock": max_block_id,
            "toBlock": checkingBlock,
            "topics": [swap_event_topic]
        })
        swap_loop(swap_filter)

    time.sleep(pollingPeriod)
