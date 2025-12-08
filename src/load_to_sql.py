import sqlite3
import pandas as pd
import os
from datetime import datetime

from logger import get_logger
from config_loader import load_config
logger = get_logger(__name__)
config = load_config()
paths_cfg = config["paths"]
DB_PATH = paths_cfg["database_path"]
SILVER_DIR = paths_cfg["silver_dir"]

def get_latest_silver_file():
    """Return the most recent CSV file from the silver folder."""
    if not os.path.exists(SILVER_DIR):
        logger.error("Silver directory does not exist: %s", SILVER_DIR)
        raise FileNotFoundError(f"No silver directory found: {SILVER_DIR}")

    files = [f for f in os.listdir(SILVER_DIR) if f.endswith(".csv")]
    if not files:
        logger.warning("No CSV files found in silver directory: %s", SILVER_DIR)
        raise FileNotFoundError("No CSV files found in data/silver. Run your cleaning step first.")

    files.sort()
    latest = files[-1]
    latest_path = os.path.join(SILVER_DIR, latest)
    logger.info("Latest silver file determined: %s", latest_path)
    return latest_path

def create_connection(db_path=DB_PATH):
    os.makedirs(os.path.dirname(db_path) or "database", exist_ok=True)
    logger.info("Creating SQLite connection to %s", db_path)
    conn = sqlite3.connect(db_path)
    return conn

def create_table(conn):
    cursor = conn.cursor()

    # 1. Create table if it does NOT exist
    create_sql = """
    CREATE TABLE IF NOT EXISTS weather (
        timestamp TEXT,
        temp REAL,
        city TEXT,
        run_date TEXT
    );
    """
    cursor.execute(create_sql)
    conn.commit()

    # 2. Check existing columns in case the table already existed without run_date
    cursor.execute("PRAGMA table_info(weather);")
    columns = [col[1] for col in cursor.fetchall()]  # column names

    # 3. Add run_date if missing
    if "run_date" not in columns:
        logger.info("run_date column missing. Adding it now...")
        cursor.execute("ALTER TABLE weather ADD COLUMN run_date TEXT;")
        conn.commit()
        logger.info("Added run_date column to weather table")

    else:
        logger.info("run_date column already exists in weather table")
        
def load_csv_to_sql(conn, csv_path):
    logger.info("Loading data from CSV into SQLite: %s", csv_path)

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        logger.exception("Failed to read CSV %s", csv_path)
        raise

    if df.empty:
        logger.warning("CSV file is empty: %s", csv_path)
        return 0

    # Extract run_date from filename (clean_2025-01-23.csv)
    run_date_from_filename = os.path.basename(csv_path).replace("clean_", "").replace(".csv", "")
    df["run_date"] = run_date_from_filename  # Add run_date into the dataframe

    try:
        df.to_sql("weather", conn, if_exists="append", index=False)
        rows_loaded = len(df)
        logger.info(
            "Successfully loaded %d rows from %s into weather table (run_date = %s)",
            rows_loaded,
            csv_path,
            run_date_from_filename
        )
        return rows_loaded

    except Exception:
        logger.exception("Failed to write dataframe to SQL")
        raise

def quick_check(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM weather;")
        count = cursor.fetchone()[0]
        logger.info("Rows in weather table: %d", count)

        cursor.execute("""
            SELECT timestamp, temp, city, run_date
            FROM weather
            ORDER BY timestamp DESC
            LIMIT 5;
        """)
        rows = cursor.fetchall()
        logger.info("Last 5 records retrieved from weather table")
        return count, rows
    except Exception:
        logger.exception("Quick check query failed")
        return None, []

def main():
    logger.info("SQL load job started")

    try:
        csv_path = get_latest_silver_file()
    except FileNotFoundError as e:
        logger.error("Aborting SQL load job: %s", e)
        return

    conn = None
    try:
        conn = create_connection()
        create_table(conn)

        rows_inserted = load_csv_to_sql(conn, csv_path)
        count, preview = quick_check(conn)

        logger.info(
            "SQL load job finished successfully. Rows inserted in last run: %s",
            rows_inserted
        )

        print(f"Loaded {rows_inserted} rows from {csv_path}")
        print(f"Total rows in weather table: {count}")

        if preview:
            print("\nLast 5 records:")
            for r in preview:
                print(r)

    except Exception as e:
        logger.exception("SQL load job failed with exception: %s", e)

    finally:
        if conn:
            conn.close()
            logger.info("SQLite connection closed")

if __name__ == "__main__":
    main()