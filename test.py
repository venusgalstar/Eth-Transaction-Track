import sqlite3

address = "0x7361A0E33F717BaF49cd946f5B748E6AA81cC6Fb"
transactionHash = ""

def get_all_data_by_hash():
    conn = sqlite3.connect("account.db")
    c = conn.cursor()

    # try: 
    # Use the SELECT statement to get all transactions from the specified address
    c.execute("SELECT * FROM transfer WHERE transactionHash = ?", (transactionHash,))

    # Fetch all the rows - each row represents a transaction
    transactions = c.fetchall()
    # except:
    #     print("There is no ")

    print("erc20 transfer log for this hash")
    for transaction in transactions:
        print(transaction)
        print("\n")

    c.execute("SELECT * FROM swap WHERE transactionHash = ?", (transactionHash,))

    print("erc20 swap log for this hash")
    # Fetch all the rows - each row represents a transaction
    transactions = c.fetchall()

    for transaction in transactions:
        print(transaction)
        print("\n")

    c.execute("SELECT * FROM transactions WHERE transactionHash = ?", (transactionHash,))

    print("all log for this hash")
    # Fetch all the rows - each row represents a transaction
    transactions = c.fetchall()

    for transaction in transactions:
        print(transaction)
        print("\n")

    # Close the connection
    conn.close()

    

get_all_data_by_hash()
# 17,270,687