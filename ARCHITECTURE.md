# 🚀 DataNexus — Technical Architecture Specification

---

## 📌 1. System Overview

**DataNexus** is a production-grade, multi-tier data platform engineered to solve enterprise data fragmentation, high-latency reporting, and real-time analytics challenges. 

The architecture handles both **batch** (ETL) and **real-time streaming** (ELT) pipelines, organizing data through a **Medallion Data Lake Architecture** (Bronze, Silver, Gold layers) stored in **Google Cloud Storage (GCS)**, transformed inside **Google BigQuery** using **dbt**, and served via **FastAPI** REST services and **Grafana / Looker Studio** dashboards.

---

## 🏗️ 2. Comprehensive System Architecture Diagram

```
╔══════════════════════════════════════════════════════════════════════╗
║                        DATA SOURCES LAYER                            ║
║                                                                      ║
║  ┌─────────────┐  ┌──────────────┐  ┌──────────┐  ┌─────────────┐  ║
║  │   MySQL DB  │  │ PostgreSQL DB│  │ REST APIs│  │  Clickstream│  ║
║  │  (Orders,   │  │  (Users,     │  │ (Weather,│  │   Events    │  ║
║  │  Payments)  │  │  Inventory)  │  │ Currency)│  │  (Kafka)    │  ║
║  └──────┬──────┘  └──────┬───────┘  └────┬─────┘  └──────┬──────┘  ║
╚═════════╪═══════════════╪════════════════╪════════════════╪═════════╝
          │               │                │                │
          ▼               ▼                ▼                ▼
╔══════════════════════════════════════════════════════════════════════╗
║                     INGESTION LAYER                                  ║
║                                                                      ║
║  ┌──────────────┐  ┌────────────────┐  ┌──────────────────────────┐ ║
║  │   Debezium   │  │ Python API     │  │      Apache Kafka         │ ║
║  │  (CDC Tool)  │  │ Ingestor       │  │  (Message Queue/Broker)   │ ║
║  │ Captures DB  │  │ Pulls data     │  │  Producers → Topics →     │ ║
║  │  changes     │  │ from APIs      │  │  Consumers                │ ║
║  └──────┬───────┘  └──────┬─────────┘  └────────────┬─────────────┘ ║
╚═════════╪════════════════╪═══════════════════════════╪══════════════╝
          │                │                           │
          └────────────────┴───────────────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
                    ▼ BATCH                         ▼ STREAMING
╔══════════════════════════╗     ╔══════════════════════════════════╗
║   BATCH PROCESSING       ║     ║   STREAM PROCESSING              ║
║                          ║     ║                                  ║
║  ┌────────────────────┐  ║     ║  ┌──────────────────────────┐   ║
║  │   Apache Spark     │  ║     ║  │   Spark Streaming         │   ║
║  │   (ETL Jobs)       │  ║     ║  │   (Real-time Processing)  │   ║
║  │                    │  ║     ║  │                           │   ║
║  │ Orchestrated by    │  ║     ║  │  Kafka → Spark → GCS      │   ║
║  │ Apache Airflow     │  ║     ║  │  (Bronze Zone)            │   ║
║  └────────────────────┘  ║     ║  └──────────────────────────┘   ║
╚══════════════════════════╝     ╚══════════════════════════════════╝
                    │                               │
                    └───────────────┬───────────────┘
                                    ▼
╔══════════════════════════════════════════════════════════════════════╗
║                     DATA LAKE (Google Cloud Storage)                 ║
║                                                                      ║
║  ┌───────────────┐    ┌────────────────┐    ┌─────────────────────┐ ║
║  │ BRONZE ZONE   │    │  SILVER ZONE   │    │    GOLD ZONE        │ ║
║  │               │    │                │    │                     │ ║
║  │ Raw data,     │ →  │ Cleaned data,  │ →  │ Aggregated,         │ ║
║  │ exact copy,   │    │ deduplicated,  │    │ business-ready,     │ ║
║  │ immutable     │    │ standardized   │    │ joined tables       │ ║
║  └───────────────┘    └────────────────┘    └─────────────────────┘ ║
║                    (Managed via Delta Lake Format)                    ║
╚══════════════════════════════════════════════════════════════════════╝
                                    │
                                    ▼ ELT Load
╔══════════════════════════════════════════════════════════════════════╗
║                   DATA WAREHOUSE (Google BigQuery)                   ║
║                                                                      ║
║  ┌────────────────────────────────────────────────────────────────┐ ║
║  │                    dbt (Data Build Tool)                        │ ║
║  │                                                                 │ ║
║  │  Staging Models ──▶ Dimensional Modeling (Fact/Dim Tables)     │ ║
║  │  Data Quality Assertions ──▶ Lineage & Auto-Documentation      │ ║
║  └────────────────────────────────────────────────────────────────┘ ║
╚══════════════════════════════════════════════════════════════════════╝
                                    │
                                    ▼
╔══════════════════════════════════════════════════════════════════════╗
║                        SERVING LAYER                                 ║
║                                                                      ║
║  ┌─────────────────────┐         ┌──────────────────────────────┐   ║
║  │  FastAPI (REST API) │         │  Grafana / Looker Studio      │   ║
║  │  Serves processed   │         │  Business Dashboards          │   ║
║  │  data endpoints     │         │  KPIs, Charts, Operational    │   ║
║  └─────────────────────┘         └──────────────────────────────┘   ║
╚══════════════════════════════════════════════════════════════════════╝
                                    │
╔══════════════════════════════════════════════════════════════════════╗
║               INFRASTRUCTURE & PLATFORM GOVERNANCE LAYER             ║
║                                                                      ║
║  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐  ║
║  │   Terraform  │  │    Docker    │  │    Google Cloud Platform  │  ║
║  │  (IaC Tool)  │  │ (Containers) │  │  (GCS, BigQuery, GKE,    │  ║
║  │  Provisions  │  │  Local Dev   │  │   Cloud Run, IAM, etc.)  │  ║
║  │  GCP infra   │  │  Environment │  │                          │  ║
║  └──────────────┘  └──────────────┘  └──────────────────────────┘  ║
╚══════════════════════════════════════════════════════════════════════╝
```

---

## 📚 3. Component Breakdown & Technological Roles

### 1. 🐬 MySQL (Orders Microservice OLTP)
* **Role:** Primary transactional database for the Orders domain.
* **Tables Managed:** `orders`, `order_items`, `payments`.
* **CDC Readiness:** Configured with Binary Logging (`log_bin = ON`, `binlog_format = ROW`).

### 2. 🐘 PostgreSQL (User & Inventory Microservice OLTP)
* **Role:** Transactional database for User profiles and Inventory management.
* **Tables Managed:** `users`, `addresses`, `inventory`.
* **CDC Readiness:** Configured with Logical Replication (`wal_level = logical`).

### 3. 🌐 REST API Ingestor
* **Role:** Ingests unstructured/semi-structured third-party external data (Weather, Currency Exchange Rates).
* **Execution:** Python scripts running via Airflow operators storing responses directly in GCS Bronze.

### 4. 📨 Apache Kafka & Zookeeper (Event Streaming Backbone)
* **Role:** Distributed event bus handling high-volume event ingestion.
* **Key Topics:** `clickstream_events`, `orders_cdc_topic`, `users_cdc_topic`.
* **Guarantees:** Fault-tolerant log retention with configurable partition offsets.

### 5. 🔄 Debezium (Change Data Capture — CDC)
* **Role:** Reads MySQL binlog and PostgreSQL WAL in real time without querying or locking tables, broadcasting row changes as JSON events directly into Kafka topics.

### 6. ⚡ Apache Spark & Spark Streaming (Distributed Compute Engine)
* **Role:** Core transformation engine for heavy dataset processing.
* **Batch Mode:** Executes PySpark jobs converting raw Bronze JSON/CSV files into optimized, deduplicated Parquet/Delta files in the Silver and Gold zones.
* **Streaming Mode:** Consumes micro-batches from Kafka topics, performing windowed aggregations and streaming into GCS Bronze.

### 7. ☁️ Google Cloud Storage & Delta Lake (Medallion Data Lake)
* **Role:** Central object storage repository organized into 3 distinct zones:
  * **Bronze Zone (`gs://datanexus/bronze/`):** Immutable raw files stored exactly as received.
  * **Silver Zone (`gs://datanexus/silver/`):** Cleaned, deduplicated, and typed data stored as Delta Lake Parquet tables.
  * **Gold Zone (`gs://datanexus/gold/`):** Enriched, business-aggregated datasets joined across domains.

### 8. 🏭 Google BigQuery & dbt (Data Warehouse & ELT Layer)
* **Role:** Serverless analytical query engine and transformation framework.
* **BigQuery Datasets:** `raw_layer`, `staging_layer`, `analytics_layer`.
* **dbt Transformations:** Transforms raw BigQuery tables into Star Schema dimensional models (`fact_orders`, `dim_users`, `dim_products`, `agg_daily_revenue`).

### 9. 🌬️ Apache Airflow (Workflow Orchestration)
* **Role:** DAG-based scheduler automating pipeline execution, dependency management, retry logic, and failure alerts.

### 10. ✅ Great Expectations (Data Quality Governance)
* **Role:** Automated data validation suite validating data integrity across every zone boundary.

### 11. ⚙️ FastAPI & Grafana / Looker Studio (Serving Layer)
* **FastAPI:** Provides REST endpoints (`/api/v1/revenue`, `/api/v1/trending`) for downstream clients.
* **Grafana:** Displays system telemetry, pipeline lag, and container performance.
* **Looker Studio:** Displays executive sales and user analytics dashboards.

### 12. 🏗️ Terraform & Docker (Infrastructure & Containerization)
* **Terraform:** Declaratively provisions GCP buckets, BigQuery schemas, service accounts, and IAM roles.
* **Docker & Docker Compose:** Standardizes local multi-container development.

---

## 🗂️ 4. Complete Project Directory Structure

```
DataNexus/
├── ARCHITECTURE.md                ← Technical Architecture Specification
├── PROBLEM_STATEMENT.md           ← Business Context & System Objectives
├── STEPS_ROADMAP.md               ← Master Step-by-Step Implementation Roadmap
├── docs/
│   ├── step_01_local_setup.md     ← Detailed Step 1 Guide
│   ├── step_02_infrastructure.md  ← Detailed Step 2 Guide
│   └── ...
├── infrastructure/                ← Terraform GCP configurations
│   ├── main.tf
│   ├── gcs.tf
│   ├── bigquery.tf
│   └── variables.tf
├── docker/                        ← Local Docker Compose environment
│   ├── docker-compose.yml
│   ├── mysql/
│   └── postgres/
├── ingestion/                     ← Ingestion connectors & scripts
│   ├── debezium/
│   ├── kafka_producers/
│   └── api_ingestor/
├── processing/                    ← PySpark batch & streaming jobs
│   ├── batch/
│   └── streaming/
├── warehouse/                     ← dbt project inside BigQuery
│   └── dbt_datanexus/
├── orchestration/                 ← Airflow DAGs
│   └── dags/
├── quality/                       ← Great Expectations suites
├── serving/                       ← FastAPI REST application
└── dashboard/                     ← Grafana & Looker configurations
```

---

## 🔁 5. End-to-End Data Pipeline Flow

```
1. OLTP Databases / Web Apps / APIs
      ↓
2. Debezium CDC / Kafka Producers / Python Script
      ↓
3. Kafka Topics (clickstream, cdc_events)
      ↓
4. Spark Streaming (Reads Kafka) ➔ GCS Bronze Zone (gs://datanexus/bronze/)
      ↓
5. Spark Batch Jobs (Cleans & Joins) ➔ GCS Silver Zone ➔ GCS Gold Zone (Parquet/Delta)
      ↓
6. BigQuery External Load ➔ Raw Schema
      ↓
7. dbt Transformations ➔ Staging Schema ➔ Analytics Schema (Fact & Dim Tables)
      ↓
8. FastAPI / Grafana / Looker Studio Serving Layer
```
