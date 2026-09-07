# RetailPulse Analytics Pipeline

A runnable Airflow + PySpark + MinIO + Kafka medallion pipeline. Every layer
script (`ingest.py` → `bronze.py` → `silver.py` → `gold.py` → `validation.py`
→ `feature_engineering.py` → `tableau_export.py`) is implemented and wired
into the DAG, using `local[*]` Spark by default so it runs with no cluster
networking to configure. A standalone Spark cluster (`spark-master` /
`spark-worker`, using the official `apache/spark` image) and Kafka (using
the official `apache/kafka` KRaft image) are included in the compose file
if you want to switch to them later — no Bitnami images are used, since
Bitnami moved most versioned tags to an unsupported legacy repo in 2025.
`minio/minio` and `minio/mc` are pinned to explicit release tags rather than
`:latest`, since untagged pulls can behave inconsistently across Docker
versions.

**This version of the pipeline is built around the [RetailRocket ecommerce
dataset](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset)**:
`events.csv` (view/addtocart/transaction clickstream), `item_properties_part1/2.csv`
(item attributes over time, including category), and `category_tree.csv`
(category hierarchy). The pipeline builds a daily transaction-count time
series per category and overall, then fits a SARIMA model per series and
forecasts forward — exported as a CSV for Tableau to plot actual vs. forecast.

## Prerequisites
- Docker Desktop (running)
- VS Code with the **Dev Containers** extension (optional but recommended) or just a terminal — either works
- ~4 GB free RAM for the containers

## Run it — steps in VS Code

1. **Unzip and open the folder**
   Unzip `retailpulse.zip`, then in VS Code: `File → Open Folder…` → select the `retailpulse` folder.

2. **Open a terminal in VS Code**
   `Terminal → New Terminal` (this runs in the project root, `retailpulse/`).

3. **Build the custom Airflow image** (adds Java + PySpark + deps on top of the official Airflow image)
   ```bash
   docker compose build
   ```

4. **Initialize the Airflow metadata DB and admin user** (one-time)
   ```bash
   docker compose up airflow-init
   ```
   Wait for it to exit with code 0.

5. **Start everything**
   ```bash
   docker compose up -d
   ```
   This brings up Postgres, Airflow webserver + scheduler, Spark master/worker, MinIO (+ bucket init), and Kafka.

6. **Check container health**
   ```bash
   docker compose ps
   ```
   All services should show `Up` (or `Exited (0)` for the one-off `minio-init`).

7. **Place the RetailRocket dataset**
   Download the 4 files from the [Kaggle dataset page](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset)
   and put them in `data/raw/retailrocket/`:
   ```
   data/raw/retailrocket/events.csv
   data/raw/retailrocket/item_properties_part1.csv
   data/raw/retailrocket/item_properties_part2.csv
   data/raw/retailrocket/category_tree.csv
   ```
   This folder is volume-mounted into the Airflow container automatically — no rebuild needed.

8. **Open the Airflow UI**
   Go to **http://localhost:8080** → login `admin` / `admin`.

9. **Trigger the DAG**
   Find `retailpulse_medallion_pipeline` → toggle it **on** (unpause) → **Trigger DAG**.
   `run_date` doesn't need to match anything in the data — the RetailRocket files aren't date-partitioned,
   `--date` is only used to tag output partitions. Leave the default params as-is.

10. **Watch it run**
    Click the graph view — tasks run in order: `ingest → bronze → silver → gold → feature_engineering → validation → tableau_export`.
    `feature_engineering` is the SARIMA fitting step and will take the longest (one model fit per category + one for the overall total).

11. **Check the output**
    - Parquet layers: MinIO console at **http://localhost:9001** (`minioadmin`/`minioadmin`) → buckets `bronze`, `silver`, `gold`
    - Forecast CSV for Tableau: `data/exports/forecast_results_<date>.csv`

## Other useful URLs
| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 |
| Spark master UI | http://localhost:8081 |
| MinIO console | http://localhost:9001 |

## Stopping / resetting
```bash
docker compose down          # stop containers, keep data
docker compose down -v       # stop containers AND wipe volumes (fresh start)
```

## Project structure
```
docker-compose.yml           Airflow, Spark, MinIO, Kafka services
Dockerfile.airflow           Airflow image + Java + pyspark/pyyaml/kafka-python/pandas/statsmodels/boto3
requirements.txt             Python deps baked into the Airflow image
dags/retailpulse_dag.py      DAG: loops over LAYERS, wires tasks in sequence via run_script_task
scripts/utils.py             load_config, get_spark_session, get_path, get_config, get_logger, get_kafka_config
scripts/_script_template.py  Copy this to add a new layer script
scripts/ingest.py            RetailRocket CSVs -> bronze (events, item_properties, category_tree)
scripts/bronze.py            dedupe/clean the three bronze tables
scripts/silver.py            joins events -> item's current category -> rolled up to root category
scripts/gold.py              daily_category_counts + daily_totals (the SARIMA time series base tables)
scripts/feature_engineering.py  fits SARIMA per category + overall total, forecasts forward -> forecast_results
scripts/validation.py        data quality checks on daily_totals and forecast_results
scripts/tableau_export.py    forecast_results -> CSV export for Tableau (Hyper API stub included)
scripts/kafka_producer.py    synthetic/replay event producer (standalone, not in the daily DAG)
scripts/kafka_consumer.py    streams Kafka events into bronze (standalone, not in the daily DAG)
config/pipeline_config.yaml  storage/spark/kafka/retailrocket/forecast config consumed by load_config()
data/raw/retailrocket/       put the 4 Kaggle CSVs here (see step 7 above)
data/exports/                tableau_export.py writes forecast_results_<date>.csv here
```

## Adding a new script
1. `cp scripts/_script_template.py scripts/my_layer.py`
2. Set `SCRIPT_NAME = "my_layer"`, fill in `run(spark, args, logger)`
3. Add `"my_layer"` to `LAYERS` in `dags/retailpulse_dag.py` at the position you want it wired in

Every script automatically gets the standard contract: `--date --config --full-refresh`,
config-first initialization, `try/finally` with `spark.stop()`, and `sys.exit(1)` on failure.

## Notes / known limitations
- `spark.master` defaults to `local[*]` (runs inside the Airflow container) for reliability. To offload to the
  standalone cluster, set `spark.master: "spark://spark-master:7077"` in `config/pipeline_config.yaml` — note this
  requires the cluster's executors to reach back to the driver over the Docker network, which can need extra
  `spark.driver.host` tuning depending on your Docker network setup.
- `tableau_export.py` ships a CSV export so the pipeline runs with zero extra setup. Swap in `export_hyper()` once
  `tableauhyperapi` is added to `requirements.txt` and rebuilt.
- `kafka_producer.py` / `kafka_consumer.py` are standalone utilities (not part of the daily `LAYERS` DAG) — run them
  manually via `docker compose exec airflow-scheduler python scripts/kafka_producer.py --date 2024-01-01 --config config/pipeline_config.yaml`
  once you want to exercise the streaming path.
