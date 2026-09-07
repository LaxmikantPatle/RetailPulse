"""scripts/kafka_consumer.py — consumes transaction events from Kafka and
lands micro-batches into the bronze layer as parquet.

Not part of the daily batch DAG (LAYERS) — run standalone or wire into a
separate streaming DAG/task as needed.
"""

import argparse
import json
import sys

from scripts.utils import load_config, get_spark_session, get_path, get_kafka_config, get_logger

SCRIPT_NAME = "kafka_consumer"


def parse_args():
    parser = argparse.ArgumentParser(description=f"RetailPulse {SCRIPT_NAME} job")
    parser.add_argument("--date", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--full-refresh", action="store_true")
    parser.add_argument("--max-messages", type=int, default=1000000, help="Stop after consuming this many messages")
    parser.add_argument("--timeout-ms", type=int, default=60000, help="Consumer poll timeout in ms")
    return parser.parse_args()


def run(spark, args, logger):
    from kafka import KafkaConsumer

    kafka_cfg = get_kafka_config()
    bootstrap_servers = kafka_cfg.get("bootstrap_servers", "localhost:9092")
    topic = kafka_cfg.get("topics", {}).get("transactions", "retail.transactions")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        consumer_timeout_ms=args.timeout_ms,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    logger.info(f"Consuming up to {args.max_messages} messages from '{topic}' at {bootstrap_servers}")
    records = []
    for message in consumer:
        records.append(message.value)
        if len(records) >= args.max_messages:
            break
    consumer.close()

    if not records:
        logger.info("No messages consumed in this window — nothing to write")
        return

    df = spark.createDataFrame(records)
    out_path = get_path("bronze", "events", args.date)
    mode = "overwrite" if args.full_refresh else "append"

    logger.info(f"Writing {df.count()} streamed rows to {out_path} (mode={mode})")
    df.write.mode(mode).parquet(out_path)


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
