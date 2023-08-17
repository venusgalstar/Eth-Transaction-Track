from env import web3
import sqlite3
import os
from env import TOKEN_ABI
from tqdm import tqdm

combineDBName = "account.db"

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

    # Copy wrap
    combineQ.execute('''
        CREATE TABLE wrap (
            blockNumber INTEGER,
            fromAddress TEXT,
            amount TEXT,
            type TEXT,
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

    # Copy transaction
    combineQ.execute('''
        ATTACH DATABASE "wrap.db" AS wrap
    ''')

    combineQ.execute('''
        INSERT or IGNORE INTO wrap SELECT * FROM wrap.wrap
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

    pbar = tqdm(total = len(tokenList))

    for token in tokenList:
        pbar.update(1)
        try:
            tokenContract = web3.eth.contract(address = token[0], abi = TOKEN_ABI)
            
            try:
                name = tokenContract.functions.name().call()
            except:
                name = "None"
            
            try:
                symbol = tokenContract.functions.symbol().call()
            except:
                symbol = "None"

            try:
                decimal = tokenContract.functions.decimals().call()
            except:
                decimal = 18

            combineQ.execute('''
                INSERT INTO token VALUES (
                    :address, :name, :symbol, :decimal
                )
            ''', {'address': token[0], 'name':name, 'symbol':symbol,  'decimal':decimal})
            connectionCombine.commit()
        except:
            continue
    pbar.close()

    connectionCombine.commit()   
    connectionCombine.close()

print("Started combining")
if not os.path.exists(combineDBName):
    create_databse()

combine_database()

sync_token()

print("Ended combining")