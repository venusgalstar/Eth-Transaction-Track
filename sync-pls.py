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
            time INTEGER,
            blockHash TEXT,
            blockNumber INTEGER,
            fromAddress TEXT,
            gas TEXT,
            gasPrice TEXT,
            hash TEXT,
            input TEXT,
            nonce INTEGER,
            toAddress TEXT,
            transactionIndex INTEGER,
            value TEXT,
            type TEXT,
            contractAddress TEXT,
            cumulativeGasUsed TEXT,
            effectiveGasPrice TEXT,
            gasUsed TEXT,
            status INTEGER,
            transactionHash TEXT PRIMARY KEY,
            contractTo TEXT,
            contractValue TEXT
        );
        CREATE TABLE transfer (
            time INTEGER,
            fromAddress TEXT,
            toAddress TEXT,
            amount TEXT,
            name TEXT,
            symbol TEXT,
            address TEXT,
            transactionHash TEXT PRIMARY KEY,
        );
              
        CREATE TABLE swap (
            time INTEGER,
            fromAddress TEXT,
            pairAddress TEXT,
            amountIn TEXT,
            amountOut TEXT,
            inTokenName TEXT,
            inTokenNymbol TEXT,              
            outTokenName TEXT,
            outTokenNymbol TEXT,
            inTokenAddress TEXT,
            outTokenAddress TEXT,
            transactionHash TEXT PRIMARY KEY,
        );
    ''')

    conn.commit()
    conn.close()

# Insert data into sqlite database
def insert_data(data):
    conn = sqlite3.connect('transactions.db')
    c = conn.cursor()

    c.execute('''
        INSERT INTO transactions VALUES (
            :time, :blockHash, :blockNumber, :fromAddress, :gas, :gasPrice,
            :hash, :input, :nonce, :toAddress,
            :transactionIndex, :value, :type, :contractAddress,
            :cumulativeGasUsed, :effectiveGasPrice, :gasUsed, :status,
            :transactionHash, :contractTo, :contractValue
        )
    ''', data)

    conn.commit()
    conn.close()

# Insert transfer data into sqlite database
def insert_transfer(data):
    conn = sqlite3.connect('transactions.db')
    c = conn.cursor()

    c.execute('''
        INSERT INTO transfer VALUES (
            :time, :fromAddress, :toAddress, :amount, :name, :sybmol, :address,
            :transactionHash,
        )
    ''', data)

    conn.commit()
    conn.close()

# Insert swap data into sqlite database
def insert_swap(data):
    conn = sqlite3.connect('transactions.db')
    c = conn.cursor()

    c.execute('''
        INSERT INTO swap VALUES (
            :time, :fromAddress, :pairAddress, :amountIn, :amountOut, 
            :inTokenName, :inTokenNymbol, :outTokenName, :outTokenNymbol,
            :inTokenAddress, :outTokenAddress, :transactionHash,
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
    time = block['timestamp']
    for txNumber in range(0, len(block.transactions)):
        trans = block.transactions[txNumber]

        # filter transactions
        # txinfo = trans['input']
        txfrom = trans['from']
        txto = trans['to']

        # inputinfo contains address
        accounts = {
            "0xc17b1e62eAEf2805F664ed44972FCc7E6647474A",
            "0x788425510Bf225b75580804E2441339E17e1a6a5"
        }

        if not(txfrom in accounts) and not(txto in accounts):
            continue
        else:
            print("match found")

        # Perform receipt and strip operations lateron
        transReceipt = web3.eth.getTransactionReceipt(trans['hash'])

        data = {
            'time': time,
            'blockHash': trans['hash'].hex(),
            'blockNumber': blockid,
            'fromAddress': trans['from'],
            'gas': trans['gas'],
            'gasPrice': trans['gasPrice'],
            'hash': trans['hash'].hex(),
            'input': trans['input'],
            'nonce': trans['nonce'],
            'toAddress': trans['to'],
            'transactionIndex': trans['transactionIndex'],
            'value': str(trans['value']),
            'type': trans['type'],
            'contractAddress': transReceipt['contractAddress'],
            'cumulativeGasUsed': transReceipt['cumulativeGasUsed'],
            'effectiveGasPrice': transReceipt['effectiveGasPrice'],
            'gasUsed': transReceipt['gasUsed'],
            'status': bool(transReceipt['status']),
            'transactionHash': transReceipt['transactionHash'].hex(),
            'contractTo': '',
            'contractValue': ''
        }

        # try:
        insert_data(data)
        # except Exception as e:
        #     print(str(e))
        #     print(data)
        #     pass


# Fetch all of new (not in index) Ethereum blocks and add transactions to index
while True:

    conn = sqlite3.connect(dbName)
    c = conn.cursor()
    c.execute("SELECT MAX(blockNumber) FROM transactions")
    max_block_id = c.fetchone()
    max_block_id = max_block_id[0]

    if max_block_id is None:
        max_block_id = startBlock
    elif max_block_id < startBlock:
        max_block_id = startBlock

    endblock = int(web3.eth.blockNumber) - int(confirmationBlocks)

    for blockHeight in range(max_block_id, endblock):
        block = web3.eth.getBlock(blockHeight, True)
        if len(block.transactions) > 0:
            insertTxsFromBlock(block)
            print('Block ' + str(blockHeight) + ' with ' + str(len(block.transactions)) + ' transactions is processed')
        else:
            print('Block ' + str(blockHeight) + ' does not contain transactions')

    time.sleep(pollingPeriod)
