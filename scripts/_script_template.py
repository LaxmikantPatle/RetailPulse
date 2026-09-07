"""
scripts/_script_template.py

Copy this file to create a new layer script. Fill in only the marked
section — everything else is the standard RetailPulse contract:
  - CLI args: --date, --config, --full-refresh
  - load_config() before any get_path/get_spark_session calls
  - spark wrapped in try/finally with spark.stop()
  - sys.exit(1) on failure so Airflow marks the task failed
"""

import argparse
import sys

from scripts.utils import load_config, get_spark_session, get_path, get_logger

SCRIPT_NAME = "template"  # <-- rename per script, e.g. "bronze"


def parse_args():
    parser = argparse.ArgumentParser(description=f"RetailPulse {SCRIPT_NAME} job")
    parser.add_argument("--date", required=True, help="Partition date, YYYY-MM-DD")
    parser.add_argument("--config", required=True, help="Path to pipeline config YAML")
    parser.add_argument("--full-refresh", action="store_true", help="Ignore incremental state")
    return parser.parse_args()


def run(spark, args, logger):
    """--- Layer-specific logic goes here --- """
    raise NotImplementedError(f"Fill in run() for {SCRIPT_NAME}")


def main():
    args = parse_args()
    load_config(args.config)
    logger = get_logger(SCRIPT_NAME)
    spark = get_spark_session(app_name=f"retailpulse-{SCRIPT_NAME}")

    try:
        run(spark, args, logger)
        logger.info(f"{SCRIPT_NAME} completed successfully for date={args.date}")
    except Exception:
        logger.exception(f"{SCRIPT_NAME} failed for date={args.date}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
