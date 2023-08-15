import sqlite3
import os
from env import web3
from env import accounts
import csv
from tqdm import tqdm
from datetime import datetime

combineDBName = "account.db"

connectionCombine = sqlite3.connect(combineDBName)
combineQ = connectionCombine.cursor()

combineQ.execute('''
        select count(transactionHash) from transfer
        union
        select count(transactionHash) from transactions
        union
        select count(transactionHash) from wrap
    ''')
countList = combineQ.fetchall()
transactionCount = countList[0] + countList[1]

pbar = tqdm(total = len(transactionCount))

def division(numberS, decimalS):
    lenS = int(numberS, 10)
    decS = int(decimalS, 10)
    # return numberS[:lenS - decS] + '.' + numberS[ lenS - decS :]
    return lenS/pow(10, decS)

with open('result.csv', 'w', newline='') as file:
    writer = csv.writer(file)
     
    writer.writerow([
        "Tx-ID",
        "Sender",
        "Receiver",
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

    transfer_event_topic = web3.keccak(text="Transfer(address,address,uint256)").hex()

    combineQ.execute('''
        select * from transactions
    ''')

    transactions = combineQ.fetchall()

    for trans in transactions:

        pbar.update(1)
        row = []
        row.append(trans[7]) # hash
        row.append(trans[1].lower()) # sender
        row.append(trans[4].lower()) # receiver
        row.append("")
        row.append(0)
        row.append("")
        row.append(0)
        row.append("")
        row.append(division(trans[6], "18")) # fee
        row.append("PLS") # Fee currency
        row.append("PulseChain Transaction")  # Exchange
        row.append("")
        row.append("")
        block = web3.eth.getBlock(trans[0], True)
        row.append(datetime.fromtimestamp(block['timestamp']))

        transHex = web3.eth.get_transaction(trans[7])

        if transHex.input == '0x':
            if not(row[1] in accounts):
                row[3] = "Income" # Type
                row[4] = division(trans[5], "18") # value
                row[5] = "PLS"
            else:
                row[3] = "Withdraw" # Type
                row[6] = division(trans[5], "18") # value
                row[7] = "PLS"
        elif transHex.input[:5] != transfer_event_topic[:5]:
            row[3] = "Other Fee"
            row[6] = division(trans[5], "18") # value
            row[7] = "PLS"

        writer.writerow(row)

    combineQ.execute('''
        select t1.*, t2.symbol, t2.decimal
        from transfer as t1
        left join token as t2
        on t1.address = t2.address
    ''')

    transfers = combineQ.fetchall()
    
    for trans in transfers:

        pbar.update(1)
        row = []
        row.append(trans[5]) # hash
        row.append(trans[1].lower()) # sender
        row.append(trans[2].lower()) # receiver
        row.append("")
        row.append(0)
        row.append("")
        row.append(0)
        row.append("")
        row.append(0) # fee
        row.append("PLS") # Fee currency
        row.append("PulseChain Transaction")  # Exchange
        row.append("")
        row.append("")
        block = web3.eth.getBlock(trans[0], True)
        row.append(datetime.fromtimestamp(block['timestamp']))

        if not(row[1] in accounts):
            row[3] = "Income" # Type
            row[4] = division(trans[3], trans[8]) # value
            row[5] = trans[7]
        else:
            row[3] = "Withdraw" # Type
            row[6] = division(trans[3], trans[8]) # value
            row[7] = trans[7]

        writer.writerow(row)

    combineQ.execute('''
        select * from wrap
    ''')

    swaps = combineQ.fetchall()
    
    for trans in swaps:
        
        pbar.update(1)
        row = []
        row.append(trans[4]) # hash
        row.append(trans[1].lower()) # sender
        row.append(trans[1].lower()) # receiver
        row.append("")
        row.append(0)
        row.append("")
        row.append(0)
        row.append("")
        row.append(0) # fee
        row.append("PLS") # Fee currency
        row.append("PulseChain Transaction")  # Exchange
        row.append("")
        row.append("")
        block = web3.eth.getBlock(trans[0], True)
        row.append(datetime.fromtimestamp(block['timestamp']))

        if trans[3] == "Withdraw":
            row[3] = "Swap" # Type
            row[4] = division(trans[2], "18") # value
            row[5] = "PLS"
            row[6] = division(trans[2], "18") # value
            row[7] = "WPLS"
        else:
            row[3] = "Swap" # Type
            row[4] = division(trans[2], "18") # value
            row[5] = "WPLS"
            row[6] = division(trans[2], "18") # value
            row[7] = "PLS"

        writer.writerow(row)
        
connectionCombine.close()