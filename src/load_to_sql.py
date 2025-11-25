import sqlite3
import pandas as pd
import os

DB_PATH = "database/weather.db"
SILVER_DIR = "data/silver"

import sqlite3
import pandas as pd
import os

DB_PATH = "database/weather.db"
SILVER_DIR = "data/silver"

def get_latest_silver_file():
    """Return the most recent CSV file from the silver folder."""
    files = [
        f for f in os.listdir(SILVER_DIR)
        if f.endswith(".csv")
    ]
    if not files:
        raise FileNotFoundError("No CSV files found in data/silver. Run your cleaning step first.")
    
    # Sort by name or time – here by name, assuming date is in filename
    files.sort()
    latest = files[-1]
    return os.path.join(SILVER_DIR, latest)
def create_connection():
    os.makedirs("database", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    return conn
def create_table(conn):
    create_sql = """
    CREATE TABLE IF NOT EXISTS weather (
        timestamp TEXT,
        temp REAL,
        city TEXT
    );
    """
    cursor = conn.cursor()
    cursor.execute(create_sql)
    conn.commit()
def load_csv_to_sql(conn, csv_path):
    print(f"Loading data from {csv_path} into SQLite...")
    df = pd.read_csv(csv_path)
    # Optional: print first few rows
    print(df.head())
    df.to_sql("weather", conn, if_exists="append", index=False)
    print("✅ Data loaded into 'weather' table.")
def quick_check(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM weather;")
    count = cursor.fetchone()[0]
    print(f"🔎 Rows in weather table: {count}")
    cursor.execute("""
        SELECT timestamp, temp, city
        FROM weather
        ORDER BY timestamp DESC
        LIMIT 5;
    """)
    rows = cursor.fetchall()
    print("Last 5 records:")
    for row in rows:
        print(row)
def main():
    print("Connecting to SQLite database...")
    conn = create_connection()
    try:
        create_table(conn)
        csv_path = get_latest_silver_file()
        load_csv_to_sql(conn, csv_path)
        quick_check(conn)
    finally:
        conn.close()
        print("Connection closed.")
if __name__ == "__main__":
    main()