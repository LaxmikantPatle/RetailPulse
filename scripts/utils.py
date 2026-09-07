"""
scripts/utils.py

Shared utilities for the RetailPulse pipeline.
Every script initializes config first via load_config(args.config), which
populates the global CFG dict. get_path() and get_spark_session() both
depend on CFG being populated.
"""

import logging
import sys
import yaml

CFG = {}


def load_config(config_path: str) -> dict:
    """Load YAML config into the global CFG dict. Must be called before
    get_path() or get_spark_session()."""
    global CFG
    with open(config_path, "r") as f:
        CFG = yaml.safe_load(f)
    return CFG


def get_logger(name: str) -> logging.Logger:
    level = CFG.get("logging", {}).get("level", "INFO") if CFG else "INFO"
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger


def get_path(layer: str, dataset: str, date: str = None) -> str:
    """Resolve an S3A path for a given medallion layer + dataset, optionally
    partitioned by date (YYYY-MM-DD -> dt=YYYY-MM-DD)."""
    if not CFG:
        raise RuntimeError("CFG not initialized. Call load_config() first.")

    root_key = f"{layer}_root"
    root = CFG.get("paths", {}).get(root_key)
    if root is None:
        raise ValueError(f"Unknown layer '{layer}' - no '{root_key}' in config paths.")

    path = f"{root}/{dataset}"
    if date:
        path = f"{path}/dt={date}"
    return path


def get_spark_session(app_name: str):
    """Build (or fetch) a SparkSession configured for S3A.
    Works against either MinIO (local dev) or real AWS S3, depending on
    whether config['storage']['endpoint'] is set. Caller is responsible for
    calling spark.stop() in a finally block."""
    from pyspark.sql import SparkSession

    if not CFG:
        raise RuntimeError("CFG not initialized. Call load_config() first.")

    storage = CFG.get("storage", {})
    spark_cfg = CFG.get("spark", {})
    endpoint = storage.get("endpoint")  # blank/None => real AWS S3

    builder = (
        SparkSession.builder.appName(app_name)
        .master(spark_cfg.get("master", "local[*]"))
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", str(spark_cfg.get("shuffle_partitions", 8)))
        .config("spark.executor.memory", spark_cfg.get("executor_memory", "2g"))
        .config("spark.driver.memory", spark_cfg.get("driver_memory", "1g"))
    )

    if endpoint:
        # MinIO / any S3-compatible local endpoint
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", storage.get("access_key"))
            .config("spark.hadoop.fs.s3a.secret.key", storage.get("secret_key"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )
    else:
        # Real AWS S3: virtual-hosted-style addressing, real region, and
        # credentials pulled from the environment / IAM role rather than
        # hardcoded in config -- see get_spark_session() docstring.
        builder = (
            builder.config("spark.hadoop.fs.s3a.path.style.access", "false")
            .config("spark.hadoop.fs.s3a.endpoint.region", storage.get("region", "us-east-1"))
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
        )
        # DefaultAWSCredentialsProviderChain reads AWS_ACCESS_KEY_ID /
        # AWS_SECRET_ACCESS_KEY from the environment automatically if set,
        # or falls back to an IAM role if running on EC2/ECS -- no key
        # values need to live in this codebase at all.

    return builder.getOrCreate()


def get_config() -> dict:
    """Returns the current CFG dict. Prefer this over `from scripts.utils
    import CFG`, since load_config() rebinds the module-level CFG name and
    a direct import would capture the stale pre-load reference."""
    if not CFG:
        raise RuntimeError("CFG not initialized. Call load_config() first.")
    return CFG


def get_kafka_config() -> dict:
    if not CFG:
        raise RuntimeError("CFG not initialized. Call load_config() first.")
    return CFG.get("kafka", {})
