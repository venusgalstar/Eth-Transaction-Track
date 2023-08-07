import sqlite3

address = "0x7361A0E33F717BaF49cd946f5B748E6AA81cC6Fb"
dbName = "transfer.db"

def get_transfer_from_address(address):
    conn = sqlite3.connect(dbName)
    c = conn.cursor()

    # Use the SELECT statement to get all transactions from the specified address
    c.execute("SELECT * FROM transfer WHERE fromAddress = ?", (address,))

    # Fetch all the rows - each row represents a transaction
    transactions = c.fetchall()

    # Close the connection
    conn.close()

    return transactions

def get_swap_from_address(address):
    conn = sqlite3.connect(dbName)
    c = conn.cursor()

    # Use the SELECT statement to get all transactions from the specified address
    c.execute("SELECT * FROM swap WHERE fromAddress = ?", (address,))

    # Fetch all the rows - each row represents a transaction
    transactions = c.fetchall()

    # Close the connection
    conn.close()

    return transactions

print(get_transfer_from_address(address))
print(get_swap_from_address(address))
# 17,270,687