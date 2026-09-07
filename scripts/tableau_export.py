"""
scripts/tableau_export.py — exports the actual + SARIMA forecast time series,
plus backtest accuracy metrics (MAPE/RMSE), for Tableau. CSV by default (zero
extra dependencies). Swap in export_hyper() once `tableauhyperapi` is
installed in the Airflow image for native .hyper extracts.

Output files:
  - forecast_results_<date>.csv: entity, event_date, actual, forecast,
    lower_ci, upper_ci, is_forecast. In Tableau: put event_date on Columns,
    actual and forecast as two measures on Rows, color by is_forecast, and
    use lower_ci/upper_ci for a confidence band.
  - forecast_accuracy_<date>.csv: entity, rmse, mape, backtest_days. Use
    this to build a "model accuracy" KPI/table per category alongside the
    trend chart, so the dashboard shows how much to trust each forecast.
"""

import argparse
import os
import sys

from scripts.utils import load_config, get_spark_session, get_path, get_logger

SCRIPT_NAME = "tableau_export"
EXPORT_DIR = "/opt/airflow/data/exports"


def parse_args():
    parser = argparse.ArgumentParser(description=f"RetailPulse {SCRIPT_NAME} job")
    parser.add_argument("--date", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--full-refresh", action="store_true")
    return parser.parse_args()


def export_csv(df, filename, logger):
    os.makedirs(EXPORT_DIR, exist_ok=True)
    out_file = os.path.join(EXPORT_DIR, filename)
    df.toPandas().to_csv(out_file, index=False)
    logger.info(f"Exported {df.count()} rows to {out_file}")


def export_hyper(df, args, logger):
    """Requires `pip install tableauhyperapi` in the Airflow image.
    Left as a stub — wire this in once the dependency is available."""
    raise NotImplementedError("Install tableauhyperapi and implement export_hyper()")


def run(spark, args, logger):
    in_path = get_path("gold", "forecast_results", args.date)
    logger.info(f"Reading forecast results from {in_path}")
    df = spark.read.parquet(in_path)
    export_csv(df, f"forecast_results_{args.date}.csv", logger)

    accuracy_path = get_path("gold", "forecast_accuracy", args.date)
    try:
        accuracy_df = spark.read.parquet(accuracy_path)
        export_csv(accuracy_df, f"forecast_accuracy_{args.date}.csv", logger)
    except Exception:
        logger.info(f"No forecast_accuracy table found at {accuracy_path} -- skipping accuracy export")


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
