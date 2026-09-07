"""scripts/feature_engineering.py — fits SARIMA models on the daily gold time
series (overall totals + top-N categories) and forecasts forward.

statsmodels' SARIMAX is single-machine, so this pulls the (small, already
aggregated) daily series to a pandas DataFrame with toPandas() rather than
running distributed -- that's expected and fine at this data volume (one row
per day, not per event).

Output: gold/forecast_results -- one row per (entity, date), with actual
history rows (is_forecast=False) and forecast rows (is_forecast=True,
includes 95% confidence interval).
"""

import argparse
import sys

import pandas as pd
from pyspark.sql import functions as F
from statsmodels.tsa.statespace.sarimax import SARIMAX

from scripts.utils import load_config, get_spark_session, get_path, get_logger, get_config

SCRIPT_NAME = "feature_engineering"
METRIC = "transaction_count"


def parse_args():
    parser = argparse.ArgumentParser(description=f"RetailPulse {SCRIPT_NAME} job")
    parser.add_argument("--date", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--full-refresh", action="store_true")
    return parser.parse_args()


def to_daily_series(pdf: pd.DataFrame, date_col: str, value_col: str) -> pd.Series:
    """Reindexes to a continuous daily range, filling gaps with 0."""
    pdf = pdf.copy()
    pdf[date_col] = pd.to_datetime(pdf[date_col])
    pdf = pdf.set_index(date_col).sort_index()
    full_range = pd.date_range(pdf.index.min(), pdf.index.max(), freq="D")
    series = pdf[value_col].reindex(full_range, fill_value=0)
    series.index.name = date_col
    return series


def backtest(entity: str, series: pd.Series, order, seasonal_order, horizon_days, logger) -> dict:
    """Holds out the last `horizon_days` real days, fits SARIMA on everything
    before that, forecasts forward, and scores the forecast against what
    actually happened using MAPE and RMSE. Returns None if there isn't
    enough history left after holding out the test window."""
    if len(series) < horizon_days * 2:
        logger.info(f"Skipping backtest for {entity} -- not enough history to hold out {horizon_days} days")
        return None

    train = series.iloc[:-horizon_days]
    test = series.iloc[-horizon_days:]

    try:
        model = SARIMAX(
            train,
            order=tuple(order),
            seasonal_order=tuple(seasonal_order),
            enforce_stationarity=False,
            enforce_invertibility=False,
        )
        fit = model.fit(disp=False)
        predicted = fit.get_forecast(steps=horizon_days).predicted_mean.values
    except Exception:
        logger.exception(f"Backtest fit failed for entity={entity} -- skipping accuracy scoring")
        return None

    actual = test.values.astype(float)
    predicted = predicted.astype(float)

    # RMSE: always computable
    rmse = float((((actual - predicted) ** 2).mean()) ** 0.5)

    # MAPE: undefined where actual == 0 (division by zero), so those days
    # are excluded from the MAPE average rather than skewing it with inf/nan.
    nonzero_mask = actual != 0
    if nonzero_mask.sum() == 0:
        mape = None
        logger.info(f"MAPE undefined for {entity} -- all held-out actuals are 0")
    else:
        pct_errors = abs((actual[nonzero_mask] - predicted[nonzero_mask]) / actual[nonzero_mask])
        mape = float(pct_errors.mean() * 100)

    logger.info(
        f"Backtest for {entity}: RMSE={rmse:.2f}"
        + (f", MAPE={mape:.1f}%" if mape is not None else ", MAPE=n/a (all-zero actuals)")
    )
    return {"entity": entity, "rmse": rmse, "mape": mape, "backtest_days": horizon_days}


def fit_and_forecast(entity: str, series: pd.Series, order, seasonal_order, horizon_days, logger):
    """Returns a DataFrame with actual history + forecast rows for one entity."""
    try:
        model = SARIMAX(
            series,
            order=tuple(order),
            seasonal_order=tuple(seasonal_order),
            enforce_stationarity=False,
            enforce_invertibility=False,
        )
        fit = model.fit(disp=False)
    except Exception:
        logger.exception(f"SARIMA fit failed for entity={entity} -- skipping")
        return pd.DataFrame()

    forecast = fit.get_forecast(steps=horizon_days)
    forecast_index = pd.date_range(series.index.max() + pd.Timedelta(days=1), periods=horizon_days, freq="D")
    ci = forecast.conf_int(alpha=0.05)

    history_df = pd.DataFrame(
        {
            "entity": entity,
            "event_date": series.index,
            "actual": series.values,
            "forecast": None,
            "lower_ci": None,
            "upper_ci": None,
            "is_forecast": False,
        }
    )
    forecast_df = pd.DataFrame(
        {
            "entity": entity,
            "event_date": forecast_index,
            "actual": None,
            "forecast": forecast.predicted_mean.values,
            "lower_ci": ci.iloc[:, 0].values,
            "upper_ci": ci.iloc[:, 1].values,
            "is_forecast": True,
        }
    )
    return pd.concat([history_df, forecast_df], ignore_index=True)


def run(spark, args, logger):
    cfg = get_config().get("forecast", {})
    order = cfg.get("order", [1, 1, 1])
    seasonal_order = cfg.get("seasonal_order", [1, 1, 1, 7])
    horizon_days = cfg.get("horizon_days", 14)
    min_history_days = cfg.get("min_history_days", 30)
    top_n = cfg.get("top_n_categories", 10)

    results = []
    accuracy_records = []

    # --- overall total ---
    totals = spark.read.parquet(get_path("gold", "daily_totals", args.date)).toPandas()
    if len(totals) >= min_history_days:
        series = to_daily_series(totals, "event_date", METRIC)
        logger.info(f"Fitting SARIMA for TOTAL ({len(series)} days of history)")
        results.append(fit_and_forecast("TOTAL", series, order, seasonal_order, horizon_days, logger))
        acc = backtest("TOTAL", series, order, seasonal_order, horizon_days, logger)
        if acc:
            accuracy_records.append(acc)
    else:
        logger.info(f"Skipping TOTAL forecast -- only {len(totals)} days of history (need {min_history_days})")

    # --- top-N categories by total transaction volume ---
    by_cat = spark.read.parquet(get_path("gold", "daily_category_counts", args.date))
    top_categories = (
        by_cat.groupBy("top_categoryid")
        .agg(F.sum(METRIC).alias("total_txns"))
        .orderBy(F.col("total_txns").desc())
        .limit(top_n)
        .select("top_categoryid")
        .rdd.flatMap(lambda r: r)
        .collect()
    )
    logger.info(f"Forecasting top {len(top_categories)} categories by transaction volume: {top_categories}")

    for cat_id in top_categories:
        cat_pdf = by_cat.filter(F.col("top_categoryid") == cat_id).toPandas()
        if len(cat_pdf) < min_history_days:
            logger.info(f"Skipping category {cat_id} -- only {len(cat_pdf)} days of history")
            continue
        series = to_daily_series(cat_pdf, "event_date", METRIC)
        logger.info(f"Fitting SARIMA for category={cat_id} ({len(series)} days of history)")
        results.append(fit_and_forecast(f"CATEGORY_{cat_id}", series, order, seasonal_order, horizon_days, logger))
        acc = backtest(f"CATEGORY_{cat_id}", series, order, seasonal_order, horizon_days, logger)
        if acc:
            accuracy_records.append(acc)

    if not results:
        raise ValueError("No entities had enough history to forecast -- check min_history_days vs your data range")

    combined = pd.concat(results, ignore_index=True)
    result_df = spark.createDataFrame(combined)

    out_path = get_path("gold", "forecast_results", args.date)
    mode = "overwrite" if args.full_refresh else "append"
    logger.info(f"Writing {result_df.count()} forecast rows to {out_path} (mode={mode})")
    result_df.write.mode(mode).parquet(out_path)

    if accuracy_records:
        accuracy_df = spark.createDataFrame(pd.DataFrame(accuracy_records))
        acc_out_path = get_path("gold", "forecast_accuracy", args.date)
        logger.info(f"Writing {accuracy_df.count()} accuracy rows to {acc_out_path} (mode={mode})")
        accuracy_df.write.mode(mode).parquet(acc_out_path)
    else:
        logger.info("No backtest results to write -- every entity had too little history to hold out a test window")


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
