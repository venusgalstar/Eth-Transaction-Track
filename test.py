import sqlite3

dbName = "transactions.db"

def get_transactions_from_address():
    conn = sqlite3.connect(dbName)
    c = conn.cursor()

    # Use the SELECT statement to get all transactions from the specified address
    c.execute("SELECT * FROM transfer")

    # Fetch all the rows - each row represents a transaction
    transactions = c.fetchall()

    # Close the connection
    conn.close()

    return transactions

print(get_transactions_from_address())
# 17,270,687