# DataNexus: Enterprise Real-Time E-Commerce Intelligence Platform

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5.0-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-7.5.0-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-1.5+-844FBA?style=for-the-badge&logo=terraform&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-S3_Glue_Athena-FF9900?style=for-the-badge&logo=amazonwebservices&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.7+-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-009688?style=for-the-badge&logo=fastapi&logoColor=white)

---

## 📌 Overview

**DataNexus** is an industry-grade, end-to-end Data Engineering platform built to solve enterprise data fragmentation across distributed microservices. It unifies transactional data (MySQL orders, PostgreSQL users/inventory), high-throughput real-time user activity (Kafka clickstream), and external environmental signals (weather, exchange rates) into a **Medallion Data Lake** and a cloud analytical **Data Warehouse**.

---

## 🏛️ High-Level System Architecture

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                   1. DATA SOURCES                                      │
│   MySQL 8.0 (Orders)   │   PostgreSQL 15 (Users/Catalog)   │   External REST APIs     │
└───────────┬────────────┴─────────────────┬─────────────────┴───────────┬───────────────┘
            │ CDC (binlog)                  │ CDC (WAL)                   │ JSON Pull
            ▼                               ▼                             ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                               2. STREAMING INGESTION                                   │
│   Debezium Connect ────▶ Kafka Topics: cdc.mysql.orders, cdc.postgres.users            │
│   Event Producers  ────▶ Kafka Topics: clickstream_events, order_events                │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        3. DISTRIBUTED PROCESSING (APACHE SPARK)                        │
│   - Spark Structured Streaming: Micro-batch ingestion to Bronze Data Lake               │
│   - PySpark Batch ETL: Bronze ➔ Silver (Deduplication, Validation, Type Casting)      │
│   - PySpark Batch ETL: Silver ➔ Gold (Multi-domain Joins, Business Aggregations)      │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                      4. DATA LAKE (MEDALLION DELTA ARCHITECTURE)                       │
│   - Bronze Zone: Raw immutable JSON payloads (Partitioned by Year/Month/Day)           │
│   - Silver Zone: Cleaned, validated, typed Parquet files                               │
│   - Gold Zone: Business-ready aggregate Parquet tables (Revenue, User Behavior)        │
│   * Storage Targets: Amazon S3 (AWS) / Google Cloud Storage (GCP) / Local Disk         │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        5. DATA WAREHOUSE & ELT (DBT + ATHENA / BQ)                     │
│   - AWS Glue Catalog / BigQuery: Schemas (Raw, Staging, Analytics)                     │
│   - dbt Models: Staging views ➔ Fact Orders ➔ Dimension Users ➔ Aggregated Revenue    │
│   - Data Quality: Great Expectations checkpoint gates before promotion                │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                         6. SERVING & OPERATIONAL MONITORING                            │
│   - FastAPI REST API: Sub-second endpoints for revenue, trending products, inventory   │
│   - Grafana & Looker Studio: Real-time pipeline health and executive KPI dashboards   │
│   - Apache Airflow: Scheduled DAG orchestration with failure alerting and retries      │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Technology Stack

| Layer | Component | Implementation |
|---|---|---|
| **OLTP Databases** | Orders & Payments | MySQL 8.0 (Docker) with Binary Log (`binlog_format=ROW`) |
| | Users & Catalog | PostgreSQL 15 (Docker) with Write-Ahead Log (`wal_level=logical`) |
| **Change Data Capture** | DB Change Capture | Debezium 2.4.0 on Kafka Connect |
| **Event Streaming** | Message Broker | Apache Kafka 7.5.0 + Zookeeper |
| **Distributed Compute** | Batch & Streaming | Apache Spark 3.5.0 (PySpark, Structured Streaming) |
| **Data Lake** | Medallion Lake | Amazon S3 (`bronze/`, `silver/`, `gold/`) or Local Parquet |
| **Data Warehouse** | OLAP Engine | AWS Athena / Glue Catalog & Google BigQuery |
| **ELT Transformations** | Warehouse Models | dbt 1.7+ (Staging, Fact, Dimension models) |
| **Pipeline Orchestration**| Workflow Scheduler | Apache Airflow 2.8+ |
| **Data Quality** | Automated Gates | Great Expectations 0.18+ |
| **Serving Layer** | REST APIs | FastAPI + Uvicorn |
| **Infrastructure as Code**| Cloud Provisioning | Terraform 1.5+ (AWS & GCP modules) |
| **Observability** | Dashboards & Metrics| Grafana 10.x & Prometheus |

---

## 📂 Project Directory Structure

```
DataNexus/
├── docker/                     # Multi-container Docker Compose setup
│   ├── mysql/                  # MySQL custom CDC config & schema
│   ├── postgres/               # Postgres logical replication config & schema
│   └── docker-compose.yml      # MySQL, Postgres, Kafka, Zookeeper, Spark, Debezium
├── infrastructure/             # Infrastructure as Code (Terraform)
│   ├── aws/                    # S3 Data Lake, Glue Data Catalog, Athena Workgroup
│   └── *.tf                    # GCP BigQuery, Cloud Storage, Service Accounts
├── ingestion/                  # Multi-source data ingestion
│   ├── debezium/               # MySQL & Postgres CDC connector definitions
│   ├── kafka_producers/        # Real-time clickstream & order event generators
│   └── api_ingestor/           # Open-Meteo weather & FX currency ingestors
├── processing/                 # Distributed data processing
│   ├── batch/                  # PySpark Bronze-to-Silver & Silver-to-Gold ETL jobs
│   └── streaming/              # Spark Structured Streaming jobs from Kafka
├── warehouse/                  # Data Warehouse & ELT
│   └── dbt_datanexus/          # dbt project, staging views, fact/dim models, tests
├── orchestration/              # Workflow scheduling
│   └── dags/                   # Airflow DAGs for ETL, API ingestion, and dbt runs
├── quality/                    # Automated data quality framework
│   └── great_expectations/     # Expectation suites & checkpoint definitions
├── serving/                    # Application serving layer
│   └── api/                    # FastAPI microservice with revenue & product routers
├── dashboard/                  # Monitoring & visualization
│   └── grafana/dashboards/     # Pipeline health JSON dashboard templates
├── datalake/                   # Local Medallion Data Lake storage
│   ├── bronze/                 # Raw ingestion layer
│   ├── silver/                 # Cleansed Parquet layer
│   └── gold/                   # Aggregated business-ready layer
├── scripts/                    # Database seeding and utility scripts
├── documentation/              # End-to-end architecture & project documentation
└── requirements.txt            # Master Python dependencies
```

---

## 🚀 Quick Start Guide

### 1. Launch Local Infrastructure
```bash
cd docker
docker-compose up -d
docker-compose ps
```

### 2. Register Debezium CDC Connectors
```bash
# Register MySQL Orders CDC
curl -X POST -H "Content-Type: application/json" \
  http://localhost:8083/connectors/ -d @../ingestion/debezium/mysql-connector.json

# Register PostgreSQL Users CDC
curl -X POST -H "Content-Type: application/json" \
  http://localhost:8083/connectors/ -d @../ingestion/debezium/postgres-connector.json
```

### 3. Generate Simulated Real-Time Streaming Data
```bash
# Publish Clickstream Events
python ingestion/kafka_producers/clickstream_producer.py

# Publish Order Events
python ingestion/kafka_producers/order_events_producer.py

# Ingest Weather & Currency APIs
python ingestion/api_ingestor/weather_ingestor.py
python ingestion/api_ingestor/exchange_rate_ingestor.py
```

### 4. Run PySpark Batch ETL Jobs
```bash
python processing/batch/bronze_to_silver/orders_etl.py
python processing/batch/bronze_to_silver/users_etl.py
python processing/batch/bronze_to_silver/inventory_etl.py
python processing/batch/silver_to_gold/daily_revenue.py
python processing/batch/silver_to_gold/user_behavior.py
```

### 5. Launch FastAPI Serving Layer
```bash
cd serving/api
uvicorn main:app --reload --port 8000
# Interactive API documentation available at: http://localhost:8000/docs
```

---

## 📄 License & Attribution
Designed and built as an enterprise-grade data engineering showcase.
