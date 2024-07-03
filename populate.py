import pandas as pd
import sqlite3

# Define the file paths
db_file = 'shipping_data.db'
spreadsheet0 = '/Users/asherkirshtein/Desktop/Walm/forage-walmart-task-4/data/shipping_data_0.csv'
spreadsheet1 = '/Users/asherkirshtein/Desktop/Walm/forage-walmart-task-4/data/shipping_data_1.csv'
spreadsheet2 = '/Users/asherkirshtein/Desktop/Walm/forage-walmart-task-4/data/shipping_data_2.csv'

# Read the data from the spreadsheets
df0 = pd.read_csv(spreadsheet0)
df1 = pd.read_csv(spreadsheet1)
df2 = pd.read_csv(spreadsheet2)

# Print columns to debug
print("Columns in spreadsheet 1:", df1.columns)
print("Columns in spreadsheet 2:", df2.columns)

# Merge data from spreadsheet 1 and spreadsheet 2 on 'shipment_identifier'
merged_df = pd.merge(df1, df2, on='shipment_identifier')

# Connect to the SQLite database
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Create the shipments table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS shipments (
        shipment_id TEXT,
        product_name TEXT,
        quantity INTEGER,
        origin TEXT,
        destination TEXT
    )
''')

# Insert data from spreadsheet 0 into the database
df0.to_sql('table0', conn, if_exists='append', index=False)

# Process each row in the merged dataframe
for index, row in merged_df.iterrows():
    # Extract the relevant data
    shipment_id = row['shipment_identifier']
    product_name = row['product']
    quantity = row['product_quantity'] if 'product_quantity' in row else 1  # Defaulting quantity to 1 if not found
    origin = row['origin_warehouse']
    destination = row['destination_store']

    # Insert the data into the database
    cursor.execute('''
        INSERT INTO shipments (shipment_id, product_name, quantity, origin, destination)
        VALUES (?, ?, ?, ?, ?)
    ''', (shipment_id, product_name, quantity, origin, destination))

# Commit the transaction and close the connection
conn.commit()
conn.close()
