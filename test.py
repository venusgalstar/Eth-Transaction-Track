# import sqlite3

# # address = "0x7361A0E33F717BaF49cd946f5B748E6AA81cC6Fb"
# transactionHash = "0x957ea400a43c8def6d459051e9f3850dc8344ec607be6a2d3883aba75a708832"

# def get_all_data_by_hash():
#     conn = sqlite3.connect("account.db")
#     c = conn.cursor()

#     # try: 
#     # Use the SELECT statement to get all transactions from the specified address
#     c.execute("SELECT * FROM transfer WHERE transactionHash = ?", (transactionHash,))

#     # Fetch all the rows - each row represents a transaction
#     transactions = c.fetchall()
#     # except:
#     #     print("There is no ")

#     print("erc20 transfer log for this hash")
#     for transaction in transactions:
#         print(transaction)
#         print("\n")

#     c.execute("SELECT * FROM swap WHERE transactionHash = ?", (transactionHash,))

#     print("erc20 swap log for this hash")
#     # Fetch all the rows - each row represents a transaction
#     transactions = c.fetchall()

#     for transaction in transactions:
#         print(transaction)
#         print("\n")

#     c.execute("SELECT * FROM transactions WHERE transactionHash = ?", (transactionHash,))

#     print("all log for this hash")
#     # Fetch all the rows - each row represents a transaction
#     transactions = c.fetchall()

#     for transaction in transactions:
#         print(transaction)
#         print("\n")

#     # Close the connection
#     conn.close()


from env import web3
from env import accounts
from env import startBlock
from env import confirmationBlocks
from tqdm import tqdm
import time
import sqlite3
import os

dbName = "transactions.db"

# Adds all transactions from Ethereum block
def updateTransaction(trans, c):

    # Perform receipt and strip operations lateron
    transReceipt = web3.eth.getTransactionReceipt(trans[0])
    print(transReceipt)

    data = {
        'gasUsed': transReceipt['gasUsed'] * transReceipt['effectiveGasPrice'],
    }

    c.execute('''
        UPDATE transactions SET gasUsed = ? WHERE transactionHash = ?
    ''', (str(transReceipt['gasUsed'] * transReceipt['effectiveGasPrice']), trans[0]))


# Fetch all of new (not in index) Ethereum blocks and add transactions to index


conn = sqlite3.connect(dbName)
c = conn.cursor()

c.execute('''
            select transactionHash from transactions
        ''')
transList = c.fetchall()

print("Starting pls transfer syncing " + str(len(transList)) + " blocks, final block number is ")
pbar = tqdm(total = len(transList))

for trans in transList:            
    updateTransaction(trans, c)
    pbar.update(1)
    # break

# conn.commit()

# c.execute('''
#             select * from transactions where transactionHash = '0x00000d19ba117cf6c8c14560b7dd99cb436ab1a31882fa1a3a00b0b8c180aeae'
#         ''')

# print(c.fetchall())

conn.commit()
conn.close()
pbar.close()



    

# get_all_data_by_hash()
# # 17,270,687

# import pandas as pd


# df = pd.DataFrame({
#     'Name': ['Alice', 'Bob', 'Charlie'],
#     'Age': [25, 30, 35],
#     'Salary': [2500.50, 3000.75, 0.000000000000000001]
# })

# df.to_csv('employees.csv', index=False, float_format='%.6f')