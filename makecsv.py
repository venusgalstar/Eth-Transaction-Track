import sqlite3
import os
from env import web3
import csv
from tqdm import tqdm
from datetime import datetime

combineDBName = "account.db"

connectionCombine = sqlite3.connect(combineDBName)
combineQ = connectionCombine.cursor()

combineQ.execute('''
        select count(transactionHash) from transfer
        union
        select count(transactionHash) from swap
        union
        select count(transactionHash) from transactions
    ''')
countList = combineQ.fetchall()
transactionCount = countList[0] + countList[1] + countList[2]

pbar = tqdm(total = len(transactionCount))

def division(numberS, decimalS):
    lenS = len(numberS)
    decS = len(decimalS)
    return numberS[:lenS - decS] + '.' + numberS[ lenS - decS :]

with open('result.csv', 'w', newline='') as file:
    writer = csv.writer(file)
     
    writer.writerow([
        "Tx-ID",
        "Address",
        "Type", 
        "Buy Amount", 
        "Buy Currency",
        "Sell Amount", 
        "Sell Currency",
        "Fee",
        "Fee Currency",
        "Exchange",
        "Trade-Group",
        "Comment",
        "Date"
    ])

    combineQ.execute('''
        select t3.blockNumber, t3.fromAddress, t3.amountIn, t3.amountOut,
            t3.symbol as inTokenName, t3.decimal as inDecimal,
            t3.transactionHash,
            t4.symbol as outTokenName, t4.decimal as outDecimal
        from 
        (
            select t1.blockNumber, t1.fromAddress, t1.amountIn, t1.amountOut, 
                t1.inTokenAddress, t1.outTokenAddress, t2.name, t2.symbol, 
                t2.decimal, t1.transactionHash
            from swap as t1
            left join token as t2
            on t1.inTokenAddress = t2.address
        ) as t3
        left join token as t4
        on t3.outTokenAddress = t4.address
    ''')

    swapTransactions = combineQ.fetchall()

    for swap in swapTransactions:
        pbar.update(1)

        block = web3.eth.getBlock(swap[0], True)
        
        writer.writerow([
            swap[6], #"Tx-ID",
            swap[1], #"Address",
            "Trade", #"Type", 
            division(swap[2], swap[5]), # int(swap[2], 10) / pow(10, swap[5]), # "Buy Amount", 
            swap[4], #"Buy Currency",
            division(swap[3], swap[8]), # int(swap[3], 10) / pow(10, swap[8]), # "Sell Amount", 
            swap[7], #"Sell Currency",
            0, #"Fee",
            "PLS", #"Fee Currency",
            "PulseChain Transaction", #"Exchange",
            "", # "Trade-Group",
            "", # "Comment",
            datetime.fromtimestamp(block['timestamp']), "Date"
        ])
        
connectionCombine.close()