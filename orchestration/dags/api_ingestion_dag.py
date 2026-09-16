"""
API Ingestion DAG
=================
Runs every hour to pull fresh weather and exchange-rate data from
external APIs and land raw records in the Bronze zone.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "datanexus",
    "depends_on_past": False,
    "email": ["data-eng@datanexus.io"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="api_ingestion",
    description="Hourly ingestion of weather and exchange-rate data into Bronze zone",
    schedule_interval="0 * * * *",  # every hour at minute 0
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["datanexus", "ingestion", "api", "hourly"],
) as dag:

    # ------------------------------------------------------------------
    # Callables
    # ------------------------------------------------------------------
    def _ingest_weather(**context):
        """Call WeatherIngestor to pull data for all configured cities."""
        from ingestion.api.weather_ingestor import WeatherIngestor  # noqa: WPS433

        ingestor = WeatherIngestor()
        ingestor.ingest_all_cities()

    def _ingest_exchange_rates(**context):
        """Call ExchangeRateIngestor to fetch latest FX rates."""
        from ingestion.api.exchange_rate_ingestor import ExchangeRateIngestor  # noqa: WPS433

        ingestor = ExchangeRateIngestor()
        ingestor.fetch_exchange_rates()

    # ------------------------------------------------------------------
    # Tasks
    # ------------------------------------------------------------------
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

    # Both tasks are independent and run in parallel
    [ingest_weather, ingest_exchange_rates]
