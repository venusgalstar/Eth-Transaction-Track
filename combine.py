from web3 import Web3
from web3.middleware import geth_poa_middleware
import sqlite3
import os
from env import TOKEN_ABI
from env import nodeUrl

combineDBName = "account.db"

# Connect to Ethereum node
if nodeUrl.startswith("http"):
    web3 = Web3(Web3.HTTPProvider(nodeUrl))  # "http://publicnode:8545"
elif nodeUrl.startswith("ws"):
    web3 = Web3(Web3.WebsocketProvider(nodeUrl))  # "ws://publicnode:8546"
else:
    web3 = Web3(Web3.IPCProvider(nodeUrl))  # "/home/geth/.ethereum/geth.ipc"

web3.middleware_onion.inject(geth_poa_middleware, layer=0)

def create_databse():
    connectionCombine = sqlite3.connect(combineDBName)
    combineQ = connectionCombine.cursor()

    # Copy transfer
    combineQ.execute('''
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

    # combineQ.execute('''
    #     CREATE INDEX address_index on transfer (address)
    # ''')

    # Copy swap
    combineQ.execute('''
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

    # combineQ.execute('''
    #     CREATE INDEX address_index1 on swap (inTokenAddress)
    # ''')

    # combineQ.execute('''
    #     CREATE INDEX address_index2 on swap (outTokenAddress)
    # ''')

    # Copy swap
    combineQ.execute('''
        CREATE TABLE token (
            address TEXT,
            name TEXT,
            symbol TEXT,
            decimal TEXT,
            unique (address)
        )
    ''')

    # Copy swap
    combineQ.execute('''
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

    connectionCombine.commit()
    connectionCombine.close()


def combine_database():

    connectionCombine = sqlite3.connect(combineDBName)
    combineQ = connectionCombine.cursor()

    # Copy transfer
    combineQ.execute('''
        ATTACH DATABASE "transfer.db" AS transfer
    ''')

    combineQ.execute('''
        INSERT or IGNORE INTO transfer SELECT * FROM transfer.transfer
    ''')

    connectionCombine.commit()

    # Copy swap
    combineQ.execute('''
        ATTACH DATABASE "swap.db" AS swap
    ''')

    combineQ.execute('''
        INSERT or IGNORE INTO swap SELECT * FROM swap.swap
    ''')

    connectionCombine.commit()   

    # Copy transaction
    combineQ.execute('''
        ATTACH DATABASE "transactions.db" AS transactions
    ''')

    combineQ.execute('''
        INSERT or IGNORE INTO transactions SELECT * FROM transactions.transactions
    ''')

    connectionCombine.commit() 

    connectionCombine.close()

def sync_token():
    connectionCombine = sqlite3.connect(combineDBName)
    combineQ = connectionCombine.cursor()
    
    combineQ.execute('''
        SELECT address FROM transfer
        EXCEPT
        SELECT address from token
    ''')
    tokenList = combineQ.fetchall()

    for token in tokenList:
        try:
            print(token[0])
            tokenContract = web3.eth.contract(address = token[0], abi = TOKEN_ABI)
            name = tokenContract.functions.name().call()
            symbol = tokenContract.functions.symbol().call()
            decimal = tokenContract.functions.decimals().call()

            combineQ.execute('''
                INSERT INTO token VALUES (
                    :address, :name, :symbol, :decimal
                )
            ''', {'address': token[0], 'name':name, 'symbol':symbol,  'decimal':decimal})
            connectionCombine.commit()
        except:
            continue

    print(len(tokenList))

    connectionCombine.commit()   
    connectionCombine.close()

print("Started combining")
if not os.path.exists(combineDBName):
    create_databse()

combine_database()

sync_token()

print("Ended combining")