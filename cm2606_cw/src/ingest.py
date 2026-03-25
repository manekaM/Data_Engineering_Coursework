import boto3
import os
import shutil
import logging

# ─── CONFIGURATION ────────────────────────────────────────────────────────────
BUCKET_NAME = "cm2606-coursework-holidays"        # ← Change this to your S3 bucket name
REGION = "us-east-1"                    # ← Change if your region is different

# Folder paths (relative to this script inside src/)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_SOURCE_DIR = os.path.join(BASE_DIR, "data_source")
RAW_DIR = os.path.join(BASE_DIR, "raw")

SOURCE_FILE = os.path.join(DATA_SOURCE_DIR, "holidays_events.csv")
RAW_FILE = os.path.join(RAW_DIR, "holidays_events.csv")

# S3 key for the raw file
S3_RAW_KEY = "raw/holidays_events.csv"
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def ensure_folders_exist():
    """Creates raw/ and processed/ folders if they don't exist yet."""
    os.makedirs(RAW_DIR, exist_ok=True)
    log.info(f"Folder ready: {RAW_DIR}")


def copy_to_raw():
    """Copies the CSV from data_source/ into the raw/ folder."""
    if not os.path.exists(SOURCE_FILE):
        raise FileNotFoundError(
            f"Source file not found: {SOURCE_FILE}\n"
            f"Make sure holidays_events.csv is inside the data_source/ folder."
        )
    shutil.copy2(SOURCE_FILE, RAW_FILE)
    log.info(f"Copied to raw/: {RAW_FILE}")


def upload_to_s3():
    """Uploads the raw CSV from local raw/ folder to S3 Data Lake."""
    s3 = boto3.client("s3", region_name=REGION)

    # Create bucket if it doesn't exist
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
        log.info(f"S3 bucket exists: {BUCKET_NAME}")
    except Exception:
        log.info(f"Creating S3 bucket: {BUCKET_NAME}")
        if REGION == "us-east-1":
            s3.create_bucket(Bucket=BUCKET_NAME)
        else:
            s3.create_bucket(
                Bucket=BUCKET_NAME,
                CreateBucketConfiguration={"LocationConstraint": REGION}
            )

    # Upload raw CSV to S3
    log.info(f"Uploading to S3: s3://{BUCKET_NAME}/{S3_RAW_KEY}")
    s3.upload_file(RAW_FILE, BUCKET_NAME, S3_RAW_KEY)
    log.info(f"Upload complete: s3://{BUCKET_NAME}/{S3_RAW_KEY}")


def main():
    log.info("=" * 55)
    log.info("STEP 1: Ingestion — data_source/ → raw/ → S3")
    log.info("=" * 55)

    ensure_folders_exist()
    copy_to_raw()
    upload_to_s3()

    log.info("STEP 1 COMPLETE.")
    log.info(f"  Local raw file : {RAW_FILE}")
    log.info(f"  S3 raw file    : s3://{BUCKET_NAME}/{S3_RAW_KEY}")


if __name__ == "__main__":
    main()