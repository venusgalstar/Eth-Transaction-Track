import sqlite3
import os
from env import web3
from env import startBlock
from env import endBlock
from env import CSVAccount
import csv
from tqdm import tqdm
from datetime import datetime
import pandas as pd
import pandas as pd

combineDBName = "account.db"

connectionCombine = sqlite3.connect(combineDBName)
combineQ = connectionCombine.cursor()

# For TDQM, we should get count of transactionHash we should inspect
# Each select has where clause because of blockNumber Range
combineQ.execute('''
        select count(transactionHash) from transfer where (fromAddress = ? or toAddress = ?) and blockNumber >= ? and blockNumber <= ? 
        union all
        select count(transactionHash) from transactions where fromAddress = ? and blockNumber >= ? and blockNumber <= ?
        union all
        select count(transactionHash) from wrap where fromAddress = ? and blockNumber >= ? and blockNumber <= ?
    ''', (CSVAccount, CSVAccount, startBlock, endBlock, CSVAccount, startBlock, endBlock, CSVAccount, startBlock, endBlock,))
countList = combineQ.fetchall()

# Sum up for 3 type of transactionHash Count to determine range for tqdm
transactionCount = countList[0][0] + countList[1][0] + countList[2][0]

pbar = tqdm(total = transactionCount)

# For precise float result from token amount, pls value, etc...

def division(numberS, decimalS):
    lenS = int(numberS, 10)
    decS = int(decimalS, 10)
    res = lenS/pow(10, decS)

    # To write precise decimals of float to CSV, it should be formatted as follows
    return "%.18f" % res

# Opening pre-result csv file, it will contain un-sorted result. All data for same file should be overwirted.

with open(CSVAccount + '_' + str(startBlock) +'_'+ str(endBlock) + 'test.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    
    # Write CSV Header
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

    # Fetching Sending Transactioin except swap and wrap transaction
    combineQ.execute('''
        select 
            t1.*, t2.transactionHash as flag, t2.fromAddress as checker, 
            t3.transactionHash as flag1, t3.fromAddress as checker1
        from
        (
            select * from transactions 
            where fromAddress = ? or toAddress = ?) as t1
        left join swap as t2 
        on t1.transactionHash = t2.transactionHash 
        left join wrap as t3
        on t1.transactionHash = t3.transactionHash
        where (flag IS NULL OR checker != ?) and (flag1 IS NULL or checker !=?) and t1.blockNumber >=? and t1.blockNumber <= ?
    ''',(CSVAccount,CSVAccount,CSVAccount,CSVAccount, startBlock,endBlock,))

    transactions = combineQ.fetchall()

    # Adding to CSV, if it has msg.value it will be "withdraw", if not it will be "other fee"
    for trans in transactions:

        # Update progress for tqdm
        pbar.update(1)

        # Create a array for row data to csv and filled with default values
        row = []
        row.append(trans[7]) # hash
        row.append("")
        row.append("")
        row.append("")
        row.append("")
        row.append("")
        row.append("") # fee
        row.append("PLS4") # Fee currency
        row.append("PLS Transaction")  # Exchange
        row.append("") # Trade Platfrom
        row.append("") # Comment
        block = web3.eth.getBlock(trans[0], True)
        row.append(datetime.fromtimestamp(block['timestamp']))

        transHex = web3.eth.get_transaction(trans[7])

        # if transaction has msg.value, it shoulbe "sell amount", if not, gasUsed will be "sell amount"
        if trans[4] == CSVAccount:
            row[1] = "Income" # Type
            row[2] = division(trans[5], "18") # value
            row[3] = "PLS4"
            row[7] = ""
        elif trans[5] != "0":
            row[1] = "Withdraw" # Type
            row[4] = division(trans[5], "18") # value
            row[5] = "PLS4"
            row[6] = division(trans[6], "18")
        else:
            row[1] = "Other Fee" # Type
            row[4] = division(trans[6], "18") # value
            row[5] = "PLS4"

        writer.writerow(row)

    # Fetching erc20 transfer event except direct transfer calling, because it was captured above.
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
        where (flag IS NULL or checker != ?) and t1.blockNumber >=? and t1.blockNumber <=?
    ''',(CSVAccount, CSVAccount,CSVAccount,startBlock,endBlock,))

    transfers = combineQ.fetchall()
    
    # Adding row from transfer events to csv
    for trans in transfers:

        pbar.update(1)
        row = []
        row.append(trans[5]) # hash
        row.append("")
        row.append("")
        row.append("")
        row.append("")
        row.append("")
        row.append("") # fee
        row.append("PLS4") # Fee currency
        row.append("PLS Transaction")  # Exchange
        row.append("")
        row.append("")
        block = web3.eth.getBlock(trans[0], True)
        row.append(datetime.fromtimestamp(block['timestamp']))

        # If sender is account we care, then this transaction will be "withdraw", if not it will be "income"
        if trans[1] != CSVAccount:
            row[1] = "Income" # Type
            row[2] = division(trans[3], trans[8]) # value
            row[3] = trans[7]
            row[7] = ""
        else:
            row[1] = "Withdraw" # Type
            row[4] = division(trans[3], trans[8]) # value
            row[5] = trans[7]

        writer.writerow(row)

    # Fetching PLS->WPLS or WPLS->PLS event
    combineQ.execute('''
        select t1.*, t2.gasUsed
        from 
            (select * from wrap where fromAddress = ?) as t1
        left join
            (select * from transactions where fromAddress = ?) as t2
        on
        t1.transactionHash = t2.transactionHash
        where t1.blockNumber >=? and t1.blockNumber <=?
    ''',(CSVAccount,CSVAccount,startBlock, endBlock,))

    swaps = combineQ.fetchall()
    
    # Adding row from wrap or deposit event to csv
    for trans in swaps:
        
        pbar.update(1)
        row = []
        row.append(trans[4]) # hash
        row.append("")
        row.append("")
        row.append("")
        row.append("")
        row.append("")

        if trans[5] is not None and trans[5] != "0":
            row.append(division(trans[5], "18")) # fee
        else:
            row.append("") # fee

        row.append("PLS4") # Fee currency
        row.append("PLS Transaction")  # Exchange
        row.append("")
        row.append("")
        block = web3.eth.getBlock(trans[0], True)
        row.append(datetime.fromtimestamp(block['timestamp']))

        row[1] = "Trade" # Type
        
        # In this case, transaction is "Trade", but we should distinguish PLS->WPLS or WPLS->PLS
        if trans[3] == "Withdraw":
            row[2] = division(trans[2], "18") # value
            row[3] = "PLS4"
            row[4] = division(trans[2], "18") # value
            row[5] = "WPLS"
        else:
            row[2] = division(trans[2], "18") # value
            row[3] = "WPLS"
            row[4] = division(trans[2], "18") # value
            row[5] = "PLS4"

        writer.writerow(row)

    # Fetching swapping transaction, to check input currency, it is joined with pls transaction and token transfer
    combineQ.execute('''
        select 
        t1.*, t2.symbol as inSymbol, t2.decimal as inDecimal,
        t3.symbol as outSymbol, t3.decimal as outDecimal,
        t4.symbol as tInSymbol, t4.decimal as tInDecimal
        from
        (
            select t1.*, t2.address as tInTokenAddress, t2.amount as tInAmount, t3.value as plsValue, t3.gasUsed
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
        where blockNumber >=? and blockNumber <=?
    ''',(CSVAccount,CSVAccount,CSVAccount,startBlock,endBlock,))

    swaps = combineQ.fetchall()

    # Adding row from swap event to csv    
    for trans in swaps:
        
        pbar.update(1)
        row = []
        row.append(trans[7]) # hash
        row.append("")
        row.append("")
        row.append("")
        row.append("")
        row.append("")
        row.append("") # fee
        row.append("PLS4") # Fee currency
        row.append("PLS Transaction")  # Exchange
        row.append("")
        row.append("")
        block = web3.eth.getBlock(trans[0], True)
        row.append(datetime.fromtimestamp(block['timestamp']))

        # In this case, transaction is "trade", 
        # if transaction has msg.value, sell currency is pls
        # if transaction has token tranfser, sell currency is token
        # other case, sell currency come from swap event
        row[1] = "Trade" # Type

        print(trans[12])

        if trans[11] is not None and trans[11] != "0":
            row[4] = division(trans[11], "18")
            row[5] = "PLS4"
        elif trans[9] is not None and trans[9] != "0":
            row[4] = division(trans[10], trans[18])
            row[5] = trans[17]
        else:
            row[4] = division(trans[3], trans[14]) # value
            row[5] = trans[13]

        if trans[12] is not None and trans[12] != "0":
            row[6] = division(trans[12], "18")

        row[2] = division(trans[4], trans[16]) # value
        row[3] = trans[15]

        writer.writerow(row)
        
connectionCombine.close()

# Read test file and sort it by Date
df = pd.read_csv(CSVAccount + '_' + str(startBlock) +'_'+ str(endBlock) + 'test.csv', low_memory=False)
sorted_df = df.sort_values(by=["Date"], ascending=False)
sorted_df.to_csv(CSVAccount + '_' + str(startBlock) +'_'+ str(endBlock) + 'result.csv', float_format='%.18f', index=False)