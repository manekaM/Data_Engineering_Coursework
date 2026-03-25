import logging
import time
import traceback
import sys
import os

# Make sure Python can find all steps in the same src/ folder
sys.path.insert(0, os.path.dirname(__file__))

import ingest
import etl
import warehouse

# CONFIGURATION 
PIPELINE_NAME = "Ecuador Holidays ETL Pipeline"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def run_step(step_name, step_fn):
    """
    Runs a single pipeline step.
    Returns True if successful, False if it failed.
    Stops the pipeline immediately on failure (fail-fast).
    """
    log.info("")
    log.info(f"▶  Starting: {step_name}")
    log.info("-" * 55)
    start = time.time()
    try:
        step_fn()
        elapsed = round(time.time() - start, 2)
        log.info(f"✔  Completed: {step_name} ({elapsed}s)")
        return True
    except Exception as e:
        elapsed = round(time.time() - start, 2)
        log.error(f"✘  FAILED: {step_name} ({elapsed}s)")
        log.error(f"   Error: {e}")
        log.error(traceback.format_exc())
        return False


def main():
    log.info("=" * 55)
    log.info(f"  PIPELINE START: {PIPELINE_NAME}")
    log.info("=" * 55)
    log.info("  Flow:")
    log.info("  data_source/ → raw/ → processed/ → warehouse/")
    log.info("=" * 55)

    pipeline_start = time.time()

    # Define all pipeline steps in order
    steps = [
        ("Step 1: Ingestion  (data_source/ → raw/ → S3)",       ingest.main),
        ("Step 2: ETL        (raw/ → processed/ → S3)",          etl.main),
        ("Step 3: Warehouse  (processed/ → SQLite DB → S3)",     warehouse.main),
    ]

    results = []
    for step_name, step_fn in steps:
        success = run_step(step_name, step_fn)
        results.append((step_name, success))
        if not success:
            log.error("")
            log.error("Pipeline halted. Fix the error above and re-run.")
            break

    # Final summary
    total_time = round(time.time() - pipeline_start, 2)
    log.info("")
    log.info("=" * 55)
    log.info("  PIPELINE SUMMARY")
    log.info("=" * 55)
    for step_name, success in results:
        status = "✔ PASSED" if success else "✘ FAILED"
        log.info(f"  {status}  |  {step_name}")

    all_passed = all(s for _, s in results)
    log.info("")
    if all_passed:
        log.info(f"  ✔ ALL STEPS PASSED — finished in {total_time}s")
        log.info("")
        log.info("  Output locations:")
        log.info("    Local  → raw/holidays_events.csv")
        log.info("    Local  → processed/holidays_cleaned.csv")
        log.info("    Local  → processed/holidays_summary.csv")
        log.info("    Local  → warehouse/holidays_dw.db")
        log.info("    S3     → s3://cm2606-coursework-holidays/raw/")
        log.info("    S3     → s3://cm2606-coursework-holidays/processed/")
        log.info("    S3     → s3://cm2606-coursework-holidays/warehouse/")
    else:
        log.error(f"  ✘ PIPELINE FAILED — Total time: {total_time}s")
    log.info("=" * 55)


if __name__ == "__main__":
    main()