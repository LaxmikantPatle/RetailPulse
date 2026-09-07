"""scripts/bronze.py — light cleaning/typing on the three RetailRocket bronze tables."""

import argparse
import sys

from pyspark.sql import functions as F

from scripts.utils import load_config, get_spark_session, get_path, get_logger

SCRIPT_NAME = "bronze"


def parse_args():
    parser = argparse.ArgumentParser(description=f"RetailPulse {SCRIPT_NAME} job")
    parser.add_argument("--date", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--full-refresh", action="store_true")
    return parser.parse_args()


def run(spark, args, logger):
    mode = "overwrite" if args.full_refresh else "append"

    # events: drop exact dupes, require itemid/visitorid/event present
    events = spark.read.parquet(get_path("bronze", "events", args.date))
    events_clean = (
        events.dropDuplicates()
        .filter(F.col("itemid").isNotNull() & F.col("visitorid").isNotNull() & F.col("event").isNotNull())
    )
    out = get_path("bronze", "events_clean", args.date)
    logger.info(f"Writing {events_clean.count()} cleaned event rows to {out} (mode={mode})")
    events_clean.write.mode(mode).parquet(out)

    # item_properties: drop exact dupes, require itemid/property present
    item_props = spark.read.parquet(get_path("bronze", "item_properties", args.date))
    item_props_clean = (
        item_props.dropDuplicates()
        .filter(F.col("itemid").isNotNull() & F.col("property").isNotNull())
    )
    out = get_path("bronze", "item_properties_clean", args.date)
    logger.info(f"Writing {item_props_clean.count()} cleaned item property rows to {out} (mode={mode})")
    item_props_clean.write.mode(mode).parquet(out)

    # category_tree: drop exact dupes, require categoryid present
    cat_tree = spark.read.parquet(get_path("bronze", "category_tree", args.date))
    cat_tree_clean = cat_tree.dropDuplicates().filter(F.col("categoryid").isNotNull())
    out = get_path("bronze", "category_tree_clean", args.date)
    logger.info(f"Writing {cat_tree_clean.count()} cleaned category rows to {out} (mode={mode})")
    cat_tree_clean.write.mode(mode).parquet(out)


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
