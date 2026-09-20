# ⚡ RetailPulse: Medallion Lakehouse & Forecasting Pipeline

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5.1-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-2.8.1-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-3.7.0-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![MinIO](https://img.shields.io/badge/MinIO-S3_Compatible-C72C48?style=for-the-badge&logo=minio&logoColor=white)](https://min.io/)
[![Docker](https://img.shields.io/badge/Docker_Compose-Containerized-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)

An enterprise-grade, containerized e-commerce data platform that transforms **2.7M raw clickstream events** into **14-day SARIMA demand forecasts** across a multi-tier **Medallion Lakehouse**.

---

## 🏛️ System Architecture

```mermaid
flowchart TB
    subgraph SOURCES ["📥 1. Raw Sources (RetailRocket)"]
        CSV1["events.csv<br>(2.7M Clicks)"]
        CSV2["item_properties.csv<br>(Attributes)"]
        CSV3["category_tree.csv<br>(Hierarchy)"]
    end

    subgraph INGEST ["⚡ 2. Ingestion Engine"]
        KF_P["kafka_producer.py"]
        KAFKA[("Kafka Topic:<br>retail.transactions")]
        KF_C["kafka_consumer.py"]
        ING["ingest.py"]
    end

    subgraph LAKEHOUSE ["🪣 3. MinIO Object Store (S3A)"]
        direction TB
        B_LAKE[("🥉 BRONZE<br>events_clean<br>item_properties_clean<br>category_tree_clean")]
        S_LAKE[("🥈 SILVER<br>enriched_events<br>(Category Rollup to Root)")]
        G_LAKE[("🥇 GOLD<br>daily_category_counts<br>daily_totals")]
    end

    subgraph ML_VAL ["🔮 4. Analytics & Quality Gate"]
        SARIMA["feature_engineering.py<br>SARIMA(1,1,1)x(1,1,1,7)<br>14-Day Forecast + 95% CI"]
        QC["validation.py<br>Null & Non-Negative Checks<br>Backtest: RMSE & MAPE"]
    end

    subgraph OUTPUTS ["📊 5. BI & Delivery"]
        TAB["tableau_export.py"]
        CSV_OUT["data/exports/<br>forecast_results.csv"]
    end

    CSV1 --> KF_P --> KAFKA --> KF_C --> B_LAKE
    CSV2 & CSV3 --> ING --> B_LAKE

    B_LAKE -->|"scripts/silver.py"| S_LAKE
    S_LAKE -->|"scripts/gold.py"| G_LAKE
    G_LAKE --> SARIMA --> QC --> TAB --> CSV_OUT
```

---

## 🔄 The Medallion Data Lifecycle

```mermaid
sequenceDiagram
    autonumber
    participant Raw as 📂 Raw CSVs
    participant Bronze as 🥉 Bronze Layer
    participant Silver as 🥈 Silver Layer
    participant Gold as 🥇 Gold Layer
    participant ML as 🔮 SARIMA Model
    participant Export as 📊 BI Export

    Raw->>Bronze: Ingest raw files / stream via Kafka
    Note over Bronze: Deduplication & schema validation
    Bronze->>Silver: Resolve item properties & roll up category tree
    Note over Silver: Join events + map items to top-level roots
    Silver->>Gold: Group by date & category
    Note over Gold: Daily views, add-to-carts, transactions
    Gold->>ML: Train SARIMA(1,1,1)x(1,1,1,7)
    Note over ML: 14-day projection + 95% CI + holdout backtest
    ML->>Export: Export CSV for Tableau / PowerBI
```

---

## 🛠️ Tech Stack at a Glance

| Layer | Technology | Function | Port |
| :--- | :--- | :--- | :--- |
| **Orchestration** | Apache Airflow 2.8.1 | DAG scheduling, task retries, execution logging | [`:8080`](http://localhost:8080) |
| **Compute Engine** | Apache Spark 3.5.1 | Distributed transformations, deduplication, joins | [`:8081`](http://localhost:8081) |
| **Object Store** | MinIO (S3-compatible) | Bronze / Silver / Gold Parquet lakehouse storage | [`:9001`](http://localhost:9001) |
| **Event Stream** | Apache Kafka 3.7 (KRaft) | High-throughput clickstream event replay | `:9092` |
| **Stream Monitor** | Kafka-UI | Web inspection of topics, offsets, and messages | [`:8082`](http://localhost:8082) |
| **Forecasting** | statsmodels + pandas | SARIMA time-series model with 14-day forecast | — |
| **Metadata DB** | PostgreSQL 15 | Airflow state, connections, and execution metadata | `:5432` |

---

## 🚀 3-Step Quickstart

```mermaid
graph LR
    Step1["1️⃣ Build & Init<br><code>docker compose build</code><br><code>docker compose up airflow-init</code>"]
    Step2["2️⃣ Start Stack<br><code>docker compose up -d</code>"]
    Step3["3️⃣ Run Pipeline<br>Open Airflow (:8080)<br>Trigger <code>retailpulse_medallion_pipeline</code>"]

    Step1 --> Step2 --> Step3
```

### 1. Build & Initialize
```bash
docker compose build
docker compose up airflow-init
```

### 2. Start Containers
```bash
docker compose up -d
```

### 3. Place Data & Trigger DAG
1. Put the [RetailRocket Kaggle files](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset) in `data/raw/retailrocket/`:
   - `events.csv`
   - `item_properties_part1.csv`
   - `item_properties_part2.csv`
   - `category_tree.csv`
2. Open Airflow at [http://localhost:8080](http://localhost:8080) (`admin` / `admin`).
3. Unpause and trigger `retailpulse_medallion_pipeline`.

---

## 📂 Repository Structure

```
retailpulse/
├── dags/
│   └── retailpulse_dag.py         # Airflow DAG definition
├── scripts/
│   ├── ingest.py                  # Raw CSV -> Bronze Parquet
│   ├── kafka_producer.py          # Clickstream stream producer
│   ├── kafka_consumer.py          # Kafka stream -> Bronze Parquet
│   ├── bronze.py                  # Deduplication & null filtering
│   ├── silver.py                  # Category hierarchy rollup to root
│   ├── gold.py                    # Daily category & platform aggregations
│   ├── feature_engineering.py     # SARIMA fitting, 14-day forecast & backtest
│   ├── validation.py              # Quality gates & error checks
│   ├── tableau_export.py          # Generates Tableau-ready CSV exports
│   └── utils.py                   # Shared Spark/S3/Kafka utilities
├── config/
│   └── pipeline_config.yaml       # Central configuration
├── data/
│   ├── raw/retailrocket/          # Source CSV datasets
│   └── exports/                   # Output forecast CSVs
├── docker-compose.yml             # Full-stack container definitions
└── Dockerfile.airflow             # Airflow + PySpark + dependencies
```

---

## 📈 Deliverables & Visual Dashboards

- 📊 **[Interactive Tableau Demand Forecast Dashboard](https://public.tableau.com/app/profile/laxmikant.patle/viz/RetailPulse/RetailPulseDemandForecastDashboard?publish=yes)**
- `data/exports/forecast_results_<date>.csv`: Historical actuals + 14-day forecasts with 95% confidence bounds.
- `data/exports/forecast_accuracy_<date>.csv`: Model evaluation metrics (RMSE & MAPE) per category.
