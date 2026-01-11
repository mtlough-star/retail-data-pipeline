"""
Script to extract data from CSV and load into Snowflake raw table
This handles the EL (Extract-Load) part of our ELT pipeline
"""

import pandas as pd
import snowflake.connector
from datetime import datetime
import sys
import os

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import SNOWFLAKE_CONFIG, CSV_FILE_PATH, BATCH_SIZE


def create_snowflake_connection():
    """
    Establishes connection to Snowflake using credentials from config
    Returns: snowflake connection object
    """
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG['user'],
        password=SNOWFLAKE_CONFIG['password'],
        account=SNOWFLAKE_CONFIG['account'],
        warehouse=SNOWFLAKE_CONFIG['warehouse'],
        database=SNOWFLAKE_CONFIG['database'],
        schema=SNOWFLAKE_CONFIG['schema'],
        role=SNOWFLAKE_CONFIG['role']
    )
    print("✓ Connected to Snowflake successfully")
    return conn


def extract_csv_data(file_path):
    """
    Reads CSV file into pandas DataFrame with proper data types
    Args:
        file_path: path to CSV file
    Returns: pandas DataFrame
    """
    print(f"Reading CSV file from {file_path}...")
    
    # Read CSV with explicit date parsing
    df = pd.read_csv(
        file_path,
        encoding='ISO-8859-1',  # Common encoding for retail data
        dtype={
            'InvoiceNo': str,
            'StockCode': str,
            'Description': str,
            'Quantity': int,
            'UnitPrice': float,
            'CustomerID': str,
            'Country': str
        },
        parse_dates=['InvoiceDate']
    )
    
    # Handle missing values
    df['CustomerID'] = df['CustomerID'].fillna('UNKNOWN')
    df['Description'] = df['Description'].fillna('NO DESCRIPTION')
    
    print(f"✓ Loaded {len(df)} rows from CSV")
    return df


def load_to_snowflake(conn, df):
    """
    Loads DataFrame into Snowflake raw table using batch inserts
    Args:
        conn: Snowflake connection object
        df: pandas DataFrame to load
    """
    cursor = conn.cursor()
    
    # Truncate table before loading (for fresh load)
    print("Truncating existing data in RAW.ONLINE_RETAIL...")
    cursor.execute("TRUNCATE TABLE RETAIL_DW.RAW.ONLINE_RETAIL")
    
    # Prepare insert statement
    insert_query = """
        INSERT INTO RETAIL_DW.RAW.ONLINE_RETAIL 
        (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, 
         UnitPrice, CustomerID, Country)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Insert in batches for better performance
    total_rows = len(df)
    rows_inserted = 0
    
    print(f"Loading {total_rows} rows to Snowflake in batches of {BATCH_SIZE}...")
    
    for i in range(0, total_rows, BATCH_SIZE):
        batch = df.iloc[i:i + BATCH_SIZE]
        
        # Convert timestamps to strings for Snowflake
        batch_copy = batch.copy()
        batch_copy['InvoiceDate'] = batch_copy['InvoiceDate'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(row) for row in batch_copy.values]
        
        # Execute batch insert
        cursor.executemany(insert_query, data_tuples)
        
        rows_inserted += len(batch)
        print(f"  Inserted {rows_inserted}/{total_rows} rows ({rows_inserted/total_rows*100:.1f}%)")
    
    conn.commit()
    print(f"✓ Successfully loaded {rows_inserted} rows to Snowflake")
    
    cursor.close()


def main():
    """
    Main execution function that orchestrates the ETL process
    """
    try:
        start_time = datetime.now()
        print(f"\n{'='*60}")
        print(f"Starting CSV to Snowflake Load Process")
        print(f"Started at: {start_time}")
        print(f"{'='*60}\n")
        
        # Step 1: Extract data from CSV
        df = extract_csv_data(CSV_FILE_PATH)
        
        # Step 2: Connect to Snowflake
        conn = create_snowflake_connection()
        
        # Step 3: Load data to Snowflake
        load_to_snowflake(conn, df)
        
        # Clean up
        conn.close()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"\n{'='*60}")
        print(f"✓ Load process completed successfully!")
        print(f"Duration: {duration:.2f} seconds")
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"\n✗ Error occurred: {str(e)}")
        raise


if __name__ == "__main__":
    main()