"""scripts/silver.py — enriches RetailRocket events with each item's category,
rolled up to its top-level ancestor category via category_tree.

Assumptions (verify against your actual data once ingested — RetailRocket's
item_properties "value" column is occasionally prefixed for encoded types;
adjust the categoryid parse below if `--full-refresh` runs show unexpected nulls):
  - property == 'categoryid' rows in item_properties hold the item's category
  - a property can change over time, so we take the row with the max timestamp
    per itemid as the "current" category
  - category_tree.parentid is null/empty for root categories
"""

import argparse
import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from scripts.utils import load_config, get_spark_session, get_path, get_logger

SCRIPT_NAME = "silver"
MAX_CATEGORY_DEPTH = 8  # bound on category_tree rollup hops


def parse_args():
    parser = argparse.ArgumentParser(description=f"RetailPulse {SCRIPT_NAME} job")
    parser.add_argument("--date", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--full-refresh", action="store_true")
    return parser.parse_args()


def latest_item_category(item_props, logger):
    """One row per itemid: its most recent categoryid."""
    cat_props = item_props.filter(F.col("property") == "categoryid")
    w = Window.partitionBy("itemid").orderBy(F.col("timestamp").desc())
    latest = (
        cat_props.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select(
            F.col("itemid"),
            F.col("value").cast("int").alias("categoryid"),
        )
        .filter(F.col("categoryid").isNotNull())
    )
    logger.info(f"Resolved current category for {latest.count()} items")
    return latest


def roll_up_to_root(cat_tree, logger):
    """Maps every categoryid to its top-level (root) ancestor categoryid,
    by walking up parentid links for up to MAX_CATEGORY_DEPTH hops."""
    nodes = cat_tree.select(
        F.col("categoryid").alias("categoryid"),
        F.col("categoryid").alias("root_categoryid"),
        F.col("parentid").alias("current_parent"),
    )

    for hop in range(MAX_CATEGORY_DEPTH):
        parents = cat_tree.select(
            F.col("categoryid").alias("_pid"),
            F.col("parentid").alias("_grandparent"),
        )
        nodes = (
            nodes.join(parents, nodes["current_parent"] == parents["_pid"], "left")
            .withColumn(
                "root_categoryid",
                F.when(F.col("current_parent").isNotNull(), F.col("current_parent")).otherwise(
                    F.col("root_categoryid")
                ),
            )
            .withColumn("current_parent", F.col("_grandparent"))
            .drop("_pid", "_grandparent")
        )

    logger.info(f"Rolled up {nodes.count()} categories to their root ancestor (max {MAX_CATEGORY_DEPTH} hops)")
    return nodes.select("categoryid", "root_categoryid")


def run(spark, args, logger):
    events = spark.read.parquet(get_path("bronze", "events_clean", args.date))
    item_props = spark.read.parquet(get_path("bronze", "item_properties_clean", args.date))
    cat_tree = spark.read.parquet(get_path("bronze", "category_tree_clean", args.date))

    item_category = latest_item_category(item_props, logger)
    category_rollup = roll_up_to_root(cat_tree, logger)

    events_dated = events.withColumn(
        "event_date", F.to_date(F.from_unixtime(F.col("timestamp") / 1000))
    )

    enriched = (
        events_dated.join(item_category, on="itemid", how="left")
        .join(category_rollup, on="categoryid", how="left")
        .select(
            "event_date",
            "visitorid",
            "itemid",
            "event",
            "transactionid",
            "categoryid",
            F.coalesce("root_categoryid", "categoryid").alias("top_categoryid"),
        )
    )

    out_path = get_path("silver", "enriched_events", args.date)
    mode = "overwrite" if args.full_refresh else "append"

    logger.info(f"Writing {enriched.count()} enriched event rows to {out_path} (mode={mode})")
    enriched.write.mode(mode).parquet(out_path)


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
