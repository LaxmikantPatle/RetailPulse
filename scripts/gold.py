"""scripts/gold.py — builds the daily time series base tables:
  - gold/daily_category_counts: event_date x top_categoryid -> view/cart/transaction counts
  - gold/daily_totals: event_date -> overall view/cart/transaction counts
These feed feature_engineering.py's SARIMA forecasting.
"""

import argparse
import sys

from pyspark.sql import functions as F

from scripts.utils import load_config, get_spark_session, get_path, get_logger

SCRIPT_NAME = "gold"


def parse_args():
    parser = argparse.ArgumentParser(description=f"RetailPulse {SCRIPT_NAME} job")
    parser.add_argument("--date", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--full-refresh", action="store_true")
    return parser.parse_args()


def event_count_aggs():
    return [
        F.sum(F.when(F.col("event") == "view", 1).otherwise(0)).alias("view_count"),
        F.sum(F.when(F.col("event") == "addtocart", 1).otherwise(0)).alias("addtocart_count"),
        F.sum(F.when(F.col("event") == "transaction", 1).otherwise(0)).alias("transaction_count"),
        F.countDistinct("visitorid").alias("distinct_visitors"),
    ]


def run(spark, args, logger):
    enriched = spark.read.parquet(get_path("silver", "enriched_events", args.date))
    mode = "overwrite" if args.full_refresh else "append"

    daily_category = (
        enriched.groupBy("event_date", "top_categoryid")
        .agg(*event_count_aggs())
        .orderBy("event_date", "top_categoryid")
    )
    out = get_path("gold", "daily_category_counts", args.date)
    logger.info(f"Writing {daily_category.count()} category-day rows to {out} (mode={mode})")
    daily_category.write.mode(mode).parquet(out)

    daily_totals = (
        enriched.groupBy("event_date")
        .agg(*event_count_aggs())
        .orderBy("event_date")
    )
    out = get_path("gold", "daily_totals", args.date)
    logger.info(f"Writing {daily_totals.count()} daily total rows to {out} (mode={mode})")
    daily_totals.write.mode(mode).parquet(out)


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
