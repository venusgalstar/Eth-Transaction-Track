import sqlite3

dbName = "transactions.db"

def get_transactions_from_address(address):
    conn = sqlite3.connect(dbName)
    c = conn.cursor()

    # Use the SELECT statement to get all transactions from the specified address
    c.execute("SELECT * FROM transactions WHERE fromAddress = ?", (address,))

    # Fetch all the rows - each row represents a transaction
    transactions = c.fetchall()

    # Close the connection
    conn.close()

    return transactions

print(get_transactions_from_address("0xc17b1e62eAEf2805F664ed44972FCc7E6647474A"))
# 17,270,687