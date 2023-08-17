import sqlite3
import os
from env import web3
from env import accounts
from env import CSVAccount
import csv
from tqdm import tqdm
from datetime import datetime
import pandas as pd

combineDBName = "account.db"

connectionCombine = sqlite3.connect(combineDBName)
combineQ = connectionCombine.cursor()

combineQ.execute('''
        select count(transactionHash) from transfer where fromAddress = ? or toAddress = ?
        union all
        select count(transactionHash) from transactions where fromAddress = ?
        union all
        select count(transactionHash) from wrap where fromAddress = ?
    ''', (CSVAccount, CSVAccount, CSVAccount, CSVAccount))
countList = combineQ.fetchall()
transactionCount = countList[0] + countList[1] + countList[2]

pbar = tqdm(total = len(transactionCount))

def division(numberS, decimalS):
    lenS = int(numberS, 10)
    decS = int(decimalS, 10)
    res = lenS/pow(10, decS)
    # return numberS[:lenS - decS] + '.' + numberS[ lenS - decS :]
    return "%.18f" % res
    # return lenS/pow(10, decS)

with open(CSVAccount+'result_test.csv', 'w', newline='') as file:
    writer = csv.writer(file)
     
    writer.writerow([
        "Tx-ID",
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
        select t1.*, t2.transactionHash as flag, t2.fromAddress as checker
        from
        (
            select * from transactions 
            where fromAddress = ?) as t1
        left join swap as t2 
        on t1.transactionHash = t2.transactionHash 
        where flag IS NULL OR checker != ?
    ''',(CSVAccount,CSVAccount))

    transactions = combineQ.fetchall()

    for trans in transactions:

        pbar.update(1)
        row = []
        row.append(trans[7]) # hash
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
            if trans[1] != CSVAccount:
                row[1] = "Income" # Type
                row[2] = division(trans[5], "18") # value
                row[3] = "PLS"
            else:
                row[1] = "Withdraw" # Type
                row[4] = division(trans[5], "18") # value
                row[5] = "PLS"
        elif transHex.input[:5] != transfer_event_topic[:5]:
            row[1] = "Other Fee"
            row[4] = division(trans[5], "18") # value
            row[5] = "PLS"

        writer.writerow(row)

    combineQ.execute('''
        select t1.*, t2.symbol, t2.decimal, t3.transactionHash as flag, t3.fromAddress as checker
        from 
        (
            select * from transfer 
            where fromAddress = ? 
            or toAddress = ?
        )
        as t1
        left join token as t2
        on t1.address = t2.address
        left join swap as t3
        on t1.transactionHash = t3.transactionHash
        where flag IS NULL or checker != ?
    ''',(CSVAccount, CSVAccount,CSVAccount))

    transfers = combineQ.fetchall()
    
    for trans in transfers:

        pbar.update(1)
        row = []
        row.append(trans[5]) # hash
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

        if trans[1] != CSVAccount:
            row[1] = "Income" # Type
            row[2] = division(trans[3], trans[8]) # value
            row[3] = trans[7]
        else:
            row[1] = "Withdraw" # Type
            row[4] = division(trans[3], trans[8]) # value
            row[5] = trans[7]

        writer.writerow(row)

    combineQ.execute('''
        select * from wrap where fromAddress = ?
    ''',(CSVAccount,))

    swaps = combineQ.fetchall()
    
    for trans in swaps:
        
        pbar.update(1)
        row = []
        row.append(trans[4]) # hash
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
            row[1] = "Trade" # Type
            row[2] = division(trans[2], "18") # value
            row[3] = "PLS"
            row[4] = division(trans[2], "18") # value
            row[5] = "WPLS"
        else:
            row[1] = "Trade" # Type
            row[2] = division(trans[2], "18") # value
            row[3] = "WPLS"
            row[4] = division(trans[2], "18") # value
            row[5] = "PLS"

        writer.writerow(row)

    combineQ.execute('''
        select 
        t1.*, t2.symbol as inSymbol, t2.decimal as inDecimal,
        t3.symbol as outSymbol, t3.decimal as outDecimal,
        t4.symbol as tInSymbol, t4.decimal as tInDecimal
        from
        (
            select t1.*, t2.address as tInTokenAddress, t2.amount as tInAmount, t3.value as plsValue
            from
            (select * from swap 
            where fromAddress = ?) as t1
            left join 
            (select * from transfer
            where fromAddress = ?) as t2
            on t1.transactionHash  = t2.transactionHash
            left join
            (select * from transactions
            where fromAddress = ?) as t3
            on t1.transactionHash = t3.transactionHash
        ) as t1
        left join token as t2
        on t1.inTokenAddress = t2.address
        left join token as t3
        on t1.outTokenAddress = t3.address
        left join token as t4
        on t1.tInTokenAddress = t4.address
    ''',(CSVAccount,CSVAccount,CSVAccount))

    swaps = combineQ.fetchall()

    # print(swaps)
    
    for trans in swaps:
        
        pbar.update(1)
        row = []
        row.append(trans[7]) # hash
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

        row[1] = "Trade" # Type

        if trans[11] is not None and trans[11] != "0":
            row[4] = division(trans[11], "18")
            row[5] = "PLS"
        elif trans[9] is not None and trans[9] != "0":
            row[4] = division(trans[10], trans[17])
            row[5] = trans[16]
        else:
            row[4] = division(trans[3], trans[13]) # value
            row[5] = trans[12]
        row[2] = division(trans[4], trans[15]) # value
        row[3] = trans[14]

        writer.writerow(row)
        
connectionCombine.close()

df = pd.read_csv(CSVAccount+"result_test.csv", low_memory=False)
sorted_df = df.sort_values(by=["Date"], ascending=False)
sorted_df.to_csv(CSVAccount+'result.csv', float_format='%.18f', index=False)