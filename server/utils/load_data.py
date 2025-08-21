import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import time
from datetime import datetime

load_dotenv()
DB_URL = os.getenv("POSTGRES_URI")
print(DB_URL)
engine = create_engine(DB_URL)

CHUNK_SIZE = 50000  # process 50k rows at a time

def convert_time_columns(df, time_columns):
    """Convert time string columns to time objects"""
    for col in time_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format='%H:%M:%S').dt.time
    return df

def load_store_status():
    """Load store status data"""
    csv_path = "server/data/store_status.csv"
    start_time = time.time()
    total_rows = 0
    print(f"[INFO] Starting store_status load from {csv_path}")

    try:
        for idx, chunk in enumerate(pd.read_csv(csv_path, chunksize=CHUNK_SIZE)):
            rows_in_chunk = len(chunk)
            total_rows += rows_in_chunk

            # Convert timestamp_utc to datetime
            if 'timestamp_utc' in chunk.columns:
                chunk['timestamp_utc'] = pd.to_datetime(chunk['timestamp_utc'])

            # Convert status string to integer
            if 'status' in chunk.columns:
                chunk['status'] = chunk['status'].str.lower().map({'active': 1, 'inactive': 0})

            print(f"[INFO] Processing store_status chunk {idx+1} with {rows_in_chunk} rows...")
            chunk.to_sql("store_status_log", con=engine, if_exists="append", index=False)
            print(f"[INFO] Inserted store_status chunk {idx+1} into database.")

        elapsed = time.time() - start_time
        print(f"[SUCCESS] Finished loading store_status {total_rows} rows in {elapsed:.2f} seconds.")

    except Exception as e:
        print("[ERROR] Failed to load store_status data:", str(e))


def load_business_hours():
    """Load business hours data with time conversion"""
    csv_path = "server/data/menu_hours.csv"
    start_time = time.time()
    total_rows = 0
    print(f"[INFO] Starting business_hours load from {csv_path}")

    try:
        for idx, chunk in enumerate(pd.read_csv(csv_path, chunksize=CHUNK_SIZE)):
            rows_in_chunk = len(chunk)
            total_rows += rows_in_chunk

            # Convert time string columns to time objects
            chunk = convert_time_columns(chunk, ['start_time_local', 'end_time_local'])
            
            # Rename dayOfWeek to day_of_week to match your model
            if 'dayOfWeek' in chunk.columns:
                chunk = chunk.rename(columns={'dayOfWeek': 'day_of_week'})

            print(f"[INFO] Processing business_hours chunk {idx+1} with {rows_in_chunk} rows...")
            chunk.to_sql("business_hours", con=engine, if_exists="append", index=False)
            print(f"[INFO] Inserted business_hours chunk {idx+1} into database.")

        elapsed = time.time() - start_time
        print(f"[SUCCESS] Finished loading business_hours {total_rows} rows in {elapsed:.2f} seconds.")

    except Exception as e:
        print("[ERROR] Failed to load business_hours data:", str(e))

def load_stores():
    """Load store/timezone data"""
    csv_path = "server/data/timezones.csv"
    start_time = time.time()
    total_rows = 0
    print(f"[INFO] Starting stores load from {csv_path}")

    try:
        for idx, chunk in enumerate(pd.read_csv(csv_path, chunksize=CHUNK_SIZE)):
            rows_in_chunk = len(chunk)
            total_rows += rows_in_chunk

            print(f"[INFO] Processing stores chunk {idx+1} with {rows_in_chunk} rows...")
            chunk.to_sql("store", con=engine, if_exists="append", index=False)
            print(f"[INFO] Inserted stores chunk {idx+1} into database.")

        elapsed = time.time() - start_time
        print(f"[SUCCESS] Finished loading stores {total_rows} rows in {elapsed:.2f} seconds.")

    except Exception as e:
        print("[ERROR] Failed to load stores data:", str(e))

def load_all_data():
    """Load all CSV files"""
    print("[INFO] Starting complete data load process...")
    
    # Load in order: stores first (foreign key dependency)
    load_stores()
    load_business_hours()  # This handles the time conversion
    load_store_status()
    
    print("[SUCCESS] All data loading completed!")

if __name__ == "__main__":
    load_all_data()