import pandas as pd
import sqlite3

# Load data from CSV files
spreadsheet0_data = pd.read_csv("data/shipping_data_0.csv")
spreadsheet1_data = pd.read_csv("data/shipping_data_1.csv")
spreadsheet2_data = pd.read_csv("data/shipping_data_2.csv")

# Connect to the SQLite database
conn = sqlite3.connect('walmart_database.db')
cursor = conn.cursor()

# Create tables if they don't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS shipping_data_0 (
        origin_warehouse TEXT,
        destination_store TEXT,
        product TEXT,
        on_time TEXT,
        product_quantity INTEGER,
        driver_identifier TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS shipping_data_1 (
        shipment_identifier TEXT,
        product TEXT,
        on_time TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS shipping_data_2 (
        shipment_identifier TEXT,
        origin_warehouse TEXT,
        destination_store TEXT,
        driver_identifier TEXT
    )
''')

# Insert data from spreadsheet0_data
spreadsheet0_data.to_sql('shipping_data_0', conn, if_exists='replace', index=False)

# Transform and insert data from spreadsheet1_data
for index, row in spreadsheet1_data.iterrows():
    cursor.execute(
        """
        INSERT INTO shipping_data_1 (shipment_identifier, product, on_time)
        VALUES (?, ?, ?)
        """,
        (row['shipment_identifier'], row['product'], row['on_time'])
    )

# Transform and insert data from spreadsheet2_data
for index, row in spreadsheet2_data.iterrows():
    cursor.execute(
        """
        INSERT INTO shipping_data_2 (shipment_identifier, origin_warehouse, destination_store, driver_identifier)
        VALUES (?, ?, ?, ?)
        """,
        (row['shipment_identifier'], row['origin_warehouse'], row['destination_store'], row['driver_identifier'])
    )

# Commit changes and close the connection
conn.commit()
conn.close()
