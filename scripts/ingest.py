"""scripts/ingest.py — lands RetailRocket source files into the bronze layer.

Reads the 4 Kaggle files (events.csv, item_properties_part1/2.csv,
category_tree.csv) from CFG['retailrocket']['raw_dir'] and writes each as
bronze parquet. --date is used only as the write partition tag; the source
files themselves aren't date-partitioned (RetailRocket is a static export).
"""

import argparse
import os
import sys

from pyspark.sql import functions as F

from scripts.utils import load_config, get_spark_session, get_path, get_logger, get_config

SCRIPT_NAME = "ingest"


def parse_args():
    parser = argparse.ArgumentParser(description=f"RetailPulse {SCRIPT_NAME} job")
    parser.add_argument("--date", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--full-refresh", action="store_true")
    return parser.parse_args()


def run(spark, args, logger):
    raw_dir = get_config().get("retailrocket", {}).get("raw_dir", "/opt/airflow/data/raw/retailrocket")
    mode = "overwrite" if args.full_refresh else "append"

    # events.csv is no longer read directly here — it's replayed through
    # Kafka by kafka_producer.py/kafka_consumer.py (see LAYERS in
    # dags/retailpulse_dag.py), which lands the same bronze/events table.

    # --- item_properties_part1.csv + part2.csv: timestamp, itemid, property, value ---
    ip_paths = [
        os.path.join(raw_dir, "item_properties_part1.csv"),
        os.path.join(raw_dir, "item_properties_part2.csv"),
    ]
    existing_ip_paths = [p for p in ip_paths if os.path.exists(p)]
    if not existing_ip_paths:
        raise FileNotFoundError(f"No item_properties files found under {raw_dir}")
    logger.info(f"Reading item properties: {existing_ip_paths}")
    item_props = (
        spark.read.option("header", "true").option("inferSchema", "true").csv(existing_ip_paths)
    )
    out = get_path("bronze", "item_properties", args.date)
    logger.info(f"Writing {item_props.count()} item property rows to {out} (mode={mode})")
    item_props.write.mode(mode).parquet(out)

    # --- category_tree.csv: categoryid, parentid ---
    cat_path = os.path.join(raw_dir, "category_tree.csv")
    logger.info(f"Reading {cat_path}")
    cat_tree = spark.read.option("header", "true").option("inferSchema", "true").csv(cat_path)
    out = get_path("bronze", "category_tree", args.date)
    logger.info(f"Writing {cat_tree.count()} category rows to {out} (mode={mode})")
    cat_tree.write.mode(mode).parquet(out)


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
