import sqlite3
import os

combineDBName = "account.db"
transferDB = "transfer.db"
swapDB = "swap.db"
transactionDB = "transactions.db"

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

    connectionCombine.close()

if not os.path.exists(combineDBName):
    create_databse()

combine_database()
