import boto3
import os
import logging
import pandas as pd

# CONFIGURATION 
BUCKET_NAME = "cm2606-coursework-holidays"        # ← Same bucket as step1
REGION = "us-east-1"

# Folder paths (relative to this script inside src/)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_DIR = os.path.join(BASE_DIR, "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "processed")

RAW_FILE = os.path.join(RAW_DIR, "holidays_events.csv")
CLEANED_FILE = os.path.join(PROCESSED_DIR, "holidays_cleaned.csv")
SUMMARY_FILE = os.path.join(PROCESSED_DIR, "holidays_summary.csv")

# S3 keys for processed files
S3_CLEANED_KEY = "processed/holidays_cleaned.csv"
S3_SUMMARY_KEY = "processed/holidays_summary.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def ensure_folders_exist():
    """Creates the processed/ folder if it doesn't exist."""
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    log.info(f"Folder ready: {PROCESSED_DIR}")


def read_raw_csv():
    """Reads the raw CSV from the raw/ folder."""
    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError(
            f"Raw file not found: {RAW_FILE}\n"
            f"Make sure you run step1_ingest.py first."
        )
    df = pd.read_csv(RAW_FILE)
    log.info(f"Loaded raw CSV: {len(df)} rows, {len(df.columns)} columns")
    return df


# TRANSFORMATIONS 

def handle_missing_values(df):
    """
    TRANSFORMATION 1: Missing Value Handling
    Drops rows where critical columns are null.
    Fills non-critical columns with safe defaults.
    """
    log.info("--- Transformation 1: Missing Value Handling ---")
    before = len(df)
    df = df.dropna(subset=["date", "type", "locale", "locale_name"])
    df["description"] = df["description"].fillna("Unknown")
    df["transferred"] = df["transferred"].fillna(False)
    log.info(f"Removed {before - len(df)} rows with missing values. Rows remaining: {len(df)}")
    return df


def handle_duplicates(df):
    """
    TRANSFORMATION 2: Duplicate Handling
    Removes rows that are exact duplicates on date + description + locale_name.
    """
    log.info("--- Transformation 2: Duplicate Handling ---")
    before = len(df)
    df = df.drop_duplicates(subset=["date", "description", "locale_name"])
    log.info(f"Removed {before - len(df)} duplicate rows. Rows remaining: {len(df)}")
    return df


def convert_data_types(df):
    """
    TRANSFORMATION 3: Data Type Conversion
    Parses the date string to a proper datetime.
    Extracts year, month, day_of_week as new columns.
    Converts transferred to boolean.
    Drops rows with corrupt/unparseable dates.
    """
    log.info("--- Transformation 3: Data Type Conversion ---")
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")

    corrupt = df["date"].isna().sum()
    if corrupt > 0:
        log.warning(f"Dropping {corrupt} rows with corrupt/unparseable dates.")
    df = df.dropna(subset=["date"])

    # Extract useful date parts for BI reporting
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["month_name"] = df["date"].dt.strftime("%B")
    df["day_of_week"] = df["date"].dt.strftime("%A")

    # Convert transferred to boolean
    df["transferred"] = df["transferred"].astype(str).str.lower().map(
        {"true": True, "false": False, "1": True, "0": False}
    ).fillna(False).astype(bool)

    # Convert date back to string for clean CSV output
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    log.info("Data type conversion complete.")
    return df


def standardise_text(df):
    """
    TRANSFORMATION 4: Data Standardisation
    Normalises all text columns to Title Case for consistency.
    E.g. 'national' → 'National', 'HOLIDAY' → 'Holiday'
    """
    log.info("--- Transformation 4: Data Standardisation ---")
    for col in ["type", "locale", "locale_name", "description"]:
        df[col] = df[col].astype(str).str.strip().str.title()
    log.info("Text standardisation complete.")
    return df


def aggregate_summary(df):
    """
    TRANSFORMATION 5: Data Aggregation
    Creates a summary table grouped by year, month, type, and locale.
    This is used as the fact table for the BI dashboard.
    """
    log.info("--- Transformation 5: Data Aggregation ---")
    summary = (
        df.groupby(["year", "month", "month_name", "type", "locale"])
        .agg(holiday_count=("date", "count"))
        .reset_index()
        .sort_values(["year", "month"])
    )
    log.info(f"Summary table created: {len(summary)} rows.")
    return summary


# ── SAVE & UPLOAD ─────────────────────────────────────────────────────────────

def save_to_processed(df, summary_df):
    """Saves both DataFrames as CSV files in the processed/ folder."""
    df.to_csv(CLEANED_FILE, index=False)
    log.info(f"Saved cleaned CSV : {CLEANED_FILE} ({len(df)} rows)")

    summary_df.to_csv(SUMMARY_FILE, index=False)
    log.info(f"Saved summary CSV : {SUMMARY_FILE} ({len(summary_df)} rows)")


def upload_to_s3():
    """Uploads both processed CSV files to S3."""
    s3 = boto3.client("s3", region_name=REGION)

    for local_file, s3_key in [
        (CLEANED_FILE, S3_CLEANED_KEY),
        (SUMMARY_FILE, S3_SUMMARY_KEY),
    ]:
        log.info(f"Uploading to S3: s3://{BUCKET_NAME}/{s3_key}")
        s3.upload_file(local_file, BUCKET_NAME, s3_key)
        log.info(f"Upload complete.")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 55)
    log.info("STEP 2: ETL — raw/ → processed/ → S3")
    log.info("=" * 55)

    ensure_folders_exist()

    # EXTRACT
    df = read_raw_csv()

    # TRANSFORM
    df = handle_missing_values(df)
    df = handle_duplicates(df)
    df = convert_data_types(df)
    df = standardise_text(df)
    summary_df = aggregate_summary(df)

    # LOAD
    save_to_processed(df, summary_df)
    upload_to_s3()

    log.info("=" * 55)
    log.info("STEP 2 COMPLETE.")
    log.info(f"  Local cleaned  : {CLEANED_FILE}")
    log.info(f"  Local summary  : {SUMMARY_FILE}")
    log.info(f"  S3 cleaned     : s3://{BUCKET_NAME}/{S3_CLEANED_KEY}")
    log.info(f"  S3 summary     : s3://{BUCKET_NAME}/{S3_SUMMARY_KEY}")
    log.info("=" * 55)


if __name__ == "__main__":
    main()