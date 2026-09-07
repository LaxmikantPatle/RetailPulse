"""scripts/validation.py — data quality checks on the gold time series and
forecast output."""

import argparse
import sys

from pyspark.sql import functions as F

from scripts.utils import load_config, get_spark_session, get_path, get_logger

SCRIPT_NAME = "validation"


def parse_args():
    parser = argparse.ArgumentParser(description=f"RetailPulse {SCRIPT_NAME} job")
    parser.add_argument("--date", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--full-refresh", action="store_true")
    return parser.parse_args()


def run(spark, args, logger):
    totals_path = get_path("gold", "daily_totals", args.date)
    totals = spark.read.parquet(totals_path)
    row_count = totals.count()
    if row_count == 0:
        raise ValueError(f"Validation failed: no rows in {totals_path}")

    negative = totals.filter(
        (F.col("view_count") < 0) | (F.col("addtocart_count") < 0) | (F.col("transaction_count") < 0)
    ).count()
    if negative > 0:
        raise ValueError(f"Validation failed: {negative} rows with negative counts in daily_totals")

    null_dates = totals.filter(F.col("event_date").isNull()).count()
    if null_dates > 0:
        raise ValueError(f"Validation failed: {null_dates} rows with null event_date in daily_totals")

    forecast_path = get_path("gold", "forecast_results", args.date)
    forecast = spark.read.parquet(forecast_path)
    forecast_count = forecast.count()
    if forecast_count == 0:
        raise ValueError(f"Validation failed: no rows in {forecast_path}")

    forecast_rows = forecast.filter(F.col("is_forecast") == True).count()  # noqa: E712
    if forecast_rows == 0:
        raise ValueError("Validation failed: forecast_results has no forecast rows (is_forecast=True)")

    # Accuracy metrics (MAPE/RMSE from feature_engineering.py's backtest) are
    # logged, not gated on -- a high MAPE for one sparse category shouldn't
    # fail the whole pipeline run, but it should be visible in the logs so
    # forecast quality doesn't go unnoticed.
    accuracy_path = get_path("gold", "forecast_accuracy", args.date)
    try:
        accuracy = spark.read.parquet(accuracy_path)
        for row in accuracy.collect():
            mape_str = f"{row['mape']:.1f}%" if row["mape"] is not None else "n/a"
            logger.info(f"Backtest accuracy — {row['entity']}: RMSE={row['rmse']:.2f}, MAPE={mape_str}")
    except Exception:
        logger.info(f"No forecast_accuracy table found at {accuracy_path} -- skipping accuracy logging")

    logger.info(
        f"Validation passed: {row_count} days in daily_totals, "
        f"{forecast_count} rows in forecast_results ({forecast_rows} forecast rows)"
    )


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
