"""
dags/retailpulse_dag.py

RetailPulse medallion pipeline. Uses a subprocess execution model instead
of SparkSubmitOperator: run_script_task wraps subprocess.Popen and streams
script output into the Airflow task log.
"""

import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

CONFIG_PATH = "/opt/airflow/config/pipeline_config.yaml"

# Layers executed in order. Each maps to scripts/{name}.py
LAYERS = [
    "ingest",             # lands item_properties + category_tree directly
    "kafka_producer",     # replays events.csv into Kafka (simulated stream)
    "kafka_consumer",     # consumes from Kafka, lands bronze/events
    "bronze",
    "silver",
    "gold",
    "feature_engineering",
    "validation",
    "tableau_export",
]


def run_script_task(script_name: str, **context):
    """Executes scripts/{script_name}.py via subprocess with PYTHONPATH and
    CWD set to /opt/airflow so scripts can `from scripts.utils import ...`."""
    params = context["params"]
    run_date = params.get("run_date") or context["ds"]
    full_refresh = params.get("full_refresh", False)

    cmd = [
        "python",
        f"/opt/airflow/scripts/{script_name}.py",
        "--date",
        run_date,
        "--config",
        CONFIG_PATH,
    ]
    if full_refresh:
        cmd.append("--full-refresh")

    env = os.environ.copy()
    env["PYTHONPATH"] = "/opt/airflow"

    process = subprocess.Popen(
        cmd,
        cwd="/opt/airflow",
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    for line in process.stdout:
        print(line, end="")

    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(f"{script_name} exited with code {return_code}")


default_args = {
    "owner": "retailpulse",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="retailpulse_medallion_pipeline",
    default_args=default_args,
    description="RetailPulse: Bronze -> Silver -> Gold -> Validation -> Features -> Tableau export",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "run_date": Param(None, type=["null", "string"], description="Override run date (YYYY-MM-DD). Defaults to logical date."),
        "full_refresh": Param(False, type="boolean", description="Ignore incremental state and reprocess fully."),
    },
    tags=["retailpulse", "medallion"],
) as dag:

    tasks = {
        layer: PythonOperator(
            task_id=layer,
            python_callable=run_script_task,
            op_kwargs={"script_name": layer},
        )
        for layer in LAYERS
    }

    # Wire the medallion layers in sequence: ingest -> bronze -> silver ->
    # gold -> validation -> feature_engineering -> tableau_export
    for upstream, downstream in zip(LAYERS, LAYERS[1:]):
        tasks[upstream] >> tasks[downstream]
