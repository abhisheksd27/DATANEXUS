"""
Daily ETL Pipeline DAG
======================
Orchestrates the full daily ETL pipeline for DataNexus:
  1. Ingest weather and exchange-rate data from APIs
  2. Run Spark Bronze→Silver ETL jobs (orders, users, inventory)
  3. Run Spark Silver→Gold aggregation jobs (daily revenue, user behaviour)
  4. Execute dbt transformations and run dbt tests
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "datanexus",
    "depends_on_past": False,
    "email": ["data-eng@datanexus.io"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="daily_etl_pipeline",
    description="Full daily ETL: API ingest → Spark Bronze/Silver/Gold → dbt",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["datanexus", "etl", "daily"],
) as dag:

    # ------------------------------------------------------------------
    # 1. API Ingestion
    # ------------------------------------------------------------------
    def _ingest_weather(**context):
        """Wrapper so Airflow can import the ingestor lazily at runtime."""
        from ingestion.api_ingestor.weather_ingestor import ingest_all_cities

        ingest_all_cities()

    def _ingest_exchange_rates(**context):
        from ingestion.api_ingestor.exchange_rate_ingestor import fetch_exchange_rates

        fetch_exchange_rates()

    ingest_weather = PythonOperator(
        task_id="ingest_weather",
        python_callable=_ingest_weather,
        provide_context=True,
    )

    ingest_exchange_rates = PythonOperator(
        task_id="ingest_exchange_rates",
        python_callable=_ingest_exchange_rates,
        provide_context=True,
    )

    # ------------------------------------------------------------------
    # 2. Spark Bronze → Silver ETL
    # ------------------------------------------------------------------
    SPARK_SUBMIT = (
        "spark-submit "
        "--master ${SPARK_MASTER:-local[*]} "
        "--deploy-mode client "
        "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
        "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
    )
    BRONZE_TO_SILVER = "processing/batch/bronze_to_silver"
    SILVER_TO_GOLD = "processing/batch/silver_to_gold"

    spark_orders_etl = BashOperator(
        task_id="spark_orders_etl",
        bash_command=f"{SPARK_SUBMIT} {BRONZE_TO_SILVER}/orders_etl.py",
    )

    spark_users_etl = BashOperator(
        task_id="spark_users_etl",
        bash_command=f"{SPARK_SUBMIT} {BRONZE_TO_SILVER}/users_etl.py",
    )

    spark_inventory_etl = BashOperator(
        task_id="spark_inventory_etl",
        bash_command=f"{SPARK_SUBMIT} {BRONZE_TO_SILVER}/inventory_etl.py",
    )

    # ------------------------------------------------------------------
    # 3. Spark Silver → Gold Aggregations
    # ------------------------------------------------------------------
    spark_daily_revenue = BashOperator(
        task_id="spark_daily_revenue",
        bash_command=f"{SPARK_SUBMIT} {SILVER_TO_GOLD}/daily_revenue.py",
    )

    spark_user_behavior = BashOperator(
        task_id="spark_user_behavior",
        bash_command=f"{SPARK_SUBMIT} {SILVER_TO_GOLD}/user_behavior.py",
    )

    # ------------------------------------------------------------------
    # 4. dbt Transformations & Tests
    # ------------------------------------------------------------------
    DBT_PROJECT_DIR = "warehouse/dbt_datanexus"

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
    )

    # ------------------------------------------------------------------
    # Task Dependencies
    # ------------------------------------------------------------------
    # API ingestion (parallel) → Spark Bronze→Silver (parallel) →
    # Spark Silver→Gold (parallel) → dbt run → dbt test
    [ingest_weather, ingest_exchange_rates] >> [spark_orders_etl, spark_users_etl, spark_inventory_etl]
    [spark_orders_etl, spark_users_etl, spark_inventory_etl] >> [spark_daily_revenue, spark_user_behavior]
    [spark_daily_revenue, spark_user_behavior] >> run_dbt >> dbt_test
