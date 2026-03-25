import boto3
import os
import sqlite3
import logging
import pandas as pd

# ─── CONFIGURATION ────────────────────────────────────────────────────────────
BUCKET_NAME = "cm2606-coursework-holidays"        # ← Same bucket as step1 and step2
REGION = "us-east-1"

# Folder paths (relative to this script inside src/)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PROCESSED_DIR = os.path.join(BASE_DIR, "processed")
WAREHOUSE_DIR = os.path.join(BASE_DIR, "warehouse")

CLEANED_FILE = os.path.join(PROCESSED_DIR, "holidays_cleaned.csv")
SUMMARY_FILE = os.path.join(PROCESSED_DIR, "holidays_summary.csv")
DB_FILE = os.path.join(WAREHOUSE_DIR, "holidays_dw.db")

# S3 key for the database file
S3_DB_KEY = "warehouse/holidays_dw.db"
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def ensure_folders_exist():
    """Creates the warehouse/ folder if it doesn't exist."""
    os.makedirs(WAREHOUSE_DIR, exist_ok=True)
    log.info(f"Folder ready: {WAREHOUSE_DIR}")


def read_processed_files():
    """Reads both processed CSV files into DataFrames."""
    if not os.path.exists(CLEANED_FILE):
        raise FileNotFoundError(
            f"Cleaned file not found: {CLEANED_FILE}\n"
            f"Make sure you run step2_etl.py first."
        )
    if not os.path.exists(SUMMARY_FILE):
        raise FileNotFoundError(
            f"Summary file not found: {SUMMARY_FILE}\n"
            f"Make sure you run step2_etl.py first."
        )

    df_cleaned = pd.read_csv(CLEANED_FILE)
    df_summary = pd.read_csv(SUMMARY_FILE)

    log.info(f"Loaded holidays_cleaned.csv : {len(df_cleaned)} rows")
    log.info(f"Loaded holidays_summary.csv : {len(df_summary)} rows")

    return df_cleaned, df_summary


def create_warehouse(df_cleaned, df_summary):
    """
    Creates the SQLite database and loads both tables into it.
    If the database already exists it will be overwritten cleanly.
    """
    log.info(f"Creating SQLite database: {DB_FILE}")

    # Connect to SQLite (creates the file if it doesn't exist)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # ── TABLE 1: holidays_cleaned ─────────────────────────────────────────
    log.info("Creating table: holidays_cleaned ...")
    cursor.execute("DROP TABLE IF EXISTS holidays_cleaned")
    cursor.execute("""
        CREATE TABLE holidays_cleaned (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            date          TEXT NOT NULL,
            type          TEXT,
            locale        TEXT,
            locale_name   TEXT,
            description   TEXT,
            transferred   INTEGER,
            year          INTEGER,
            month         INTEGER,
            month_name    TEXT,
            day_of_week   TEXT
        )
    """)

    # Load data from DataFrame into the table
    df_cleaned.to_sql(
        "holidays_cleaned",
        conn,
        if_exists="replace",
        index=False
    )
    log.info(f"Loaded {len(df_cleaned)} rows into holidays_cleaned.")

    # ── TABLE 2: holidays_summary ─────────────────────────────────────────
    log.info("Creating table: holidays_summary ...")
    cursor.execute("DROP TABLE IF EXISTS holidays_summary")
    cursor.execute("""
        CREATE TABLE holidays_summary (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            year           INTEGER,
            month          INTEGER,
            month_name     TEXT,
            type           TEXT,
            locale         TEXT,
            holiday_count  INTEGER
        )
    """)

    # Load data from DataFrame into the table
    df_summary.to_sql(
        "holidays_summary",
        conn,
        if_exists="replace",
        index=False
    )
    log.info(f"Loaded {len(df_summary)} rows into holidays_summary.")

    # ── VERIFY ────────────────────────────────────────────────────────────
    log.info("Verifying tables in database ...")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    log.info(f"Tables in database: {[t[0] for t in tables]}")

    # Show first 3 rows of each table as a quick check
    for table in ["holidays_cleaned", "holidays_summary"]:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        log.info(f"  {table}: {count} rows confirmed.")

    conn.commit()
    conn.close()
    log.info(f"SQLite database saved: {DB_FILE}")


def run_sample_queries():
    """
    Runs a few sample SQL queries to verify the data warehouse works.
    These are the same queries QuickSight would run for the dashboard.
    """
    log.info("--- Running Sample Queries to Verify Warehouse ---")
    conn = sqlite3.connect(DB_FILE)

    queries = [
        (
            "Total holidays by type",
            "SELECT type, COUNT(*) as total FROM holidays_cleaned GROUP BY type ORDER BY total DESC"
        ),
        (
            "Total holidays by locale",
            "SELECT locale, COUNT(*) as total FROM holidays_cleaned GROUP BY locale ORDER BY total DESC"
        ),
        (
            "Holidays per year",
            "SELECT year, COUNT(*) as total FROM holidays_cleaned GROUP BY year ORDER BY year"
        ),
        (
            "Top 5 busiest months",
            """SELECT month_name, COUNT(*) as total 
               FROM holidays_cleaned 
               GROUP BY month_name 
               ORDER BY total DESC 
               LIMIT 5"""
        ),
    ]

    for title, query in queries:
        log.info(f"\n  Query: {title}")
        df = pd.read_sql_query(query, conn)
        log.info(f"\n{df.to_string(index=False)}")

    conn.close()


def upload_to_s3():
    """Uploads the SQLite database file to S3."""
    s3 = boto3.client("s3", region_name=REGION)
    log.info(f"Uploading database to S3: s3://{BUCKET_NAME}/{S3_DB_KEY}")
    s3.upload_file(DB_FILE, BUCKET_NAME, S3_DB_KEY)
    log.info("Database upload complete.")


def main():
    log.info("=" * 55)
    log.info("STEP 3: Warehouse — processed/ → SQLite → S3")
    log.info("=" * 55)

    ensure_folders_exist()

    # Read processed CSVs
    df_cleaned, df_summary = read_processed_files()

    # Load into SQLite data warehouse
    create_warehouse(df_cleaned, df_summary)

    # Run sample queries to verify
    run_sample_queries()

    # Upload database to S3
    upload_to_s3()

    log.info("=" * 55)
    log.info("STEP 3 COMPLETE.")
    log.info(f"  Local database : {DB_FILE}")
    log.info(f"  S3 database    : s3://{BUCKET_NAME}/{S3_DB_KEY}")
    log.info("  Tables created : holidays_cleaned, holidays_summary")
    log.info("=" * 55)


if __name__ == "__main__":
    main()
