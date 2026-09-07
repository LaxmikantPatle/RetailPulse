"""scripts/kafka_producer.py — replays real events.csv rows to Kafka at a
simulated pace, instead of generating fake data. This turns the static
RetailRocket export into a simulated live clickstream.

Not part of the daily batch DAG by default -- add "kafka_produce" /
"kafka_consume" to LAYERS in dags/retailpulse_dag.py to wire it in.
"""

import argparse
import csv
import json
import sys
import time

from scripts.utils import load_config, get_kafka_config, get_logger, get_config

SCRIPT_NAME = "kafka_producer"


def parse_args():
    parser = argparse.ArgumentParser(description=f"RetailPulse {SCRIPT_NAME} job")
    parser.add_argument("--date", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--full-refresh", action="store_true")
    parser.add_argument("--num-events", type=int, default=None, help="Cap on rows to replay; default = entire file (needed for feature_engineering's min_history_days)")
    parser.add_argument("--interval-sec", type=float, default=0.0, help="Delay between events (0 = as fast as Kafka can take them)")
    return parser.parse_args()


def run(args, logger):
    from kafka import KafkaProducer

    kafka_cfg = get_kafka_config()
    bootstrap_servers = kafka_cfg.get("bootstrap_servers", "localhost:9092")
    topic = kafka_cfg.get("topics", {}).get("transactions", "retail.transactions")
    raw_dir = get_config().get("retailrocket", {}).get("raw_dir", "/opt/airflow/data/raw/retailrocket")
    events_path = f"{raw_dir}/events.csv"

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    logger.info(f"Replaying {events_path} to topic '{topic}' at {bootstrap_servers}")
    sent = 0
    with open(events_path, newline="") as f:
        for row in csv.DictReader(f):
            producer.send(topic, value=row)
            sent += 1
            if sent % 5000 == 0:
                logger.info(f"Sent {sent} events")
            if args.interval_sec:
                time.sleep(args.interval_sec)
            if args.num_events and sent >= args.num_events:
                break

    producer.flush()
    producer.close()
    logger.info(f"Producer finished — replayed {sent} real events")


def main():
    args = parse_args()
    load_config(args.config)
    logger = get_logger(SCRIPT_NAME)

    try:
        run(args, logger)
        logger.info(f"{SCRIPT_NAME} completed successfully for date={args.date}")
    except Exception:
        logger.exception(f"{SCRIPT_NAME} failed for date={args.date}")
        sys.exit(1)


if __name__ == "__main__":
    main()
