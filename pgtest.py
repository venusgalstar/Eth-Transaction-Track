import psycopg2

try:
    print("Trying to connect to 'index' database…")
    conn = psycopg2.connect(database="index") # user="postgres"
    print(conn)
    conn.autocommit = True
    print("Connected to the 'index' database")
except:
    print("Unable to connect to 'index' database", conn)
