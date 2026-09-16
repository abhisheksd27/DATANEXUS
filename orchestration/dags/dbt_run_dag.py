"""
dbt Daily Run DAG
=================
Runs dbt models, executes all tests, and regenerates dbt documentation
every day at 02:30 UTC — after the Spark Silver→Gold jobs have finished.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

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
# Config
# ---------------------------------------------------------------------------
DBT_PROJECT_DIR = "warehouse/dbt_datanexus"
DBT_PROFILES_DIR = "warehouse/dbt_datanexus"

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dbt_daily_run",
    description="Daily dbt run + test + docs-generate at 02:30 UTC",
    schedule_interval="30 2 * * *",  # daily at 02:30 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["datanexus", "dbt", "daily"],
) as dag:

    # ------------------------------------------------------------------
    # Tasks
    # ------------------------------------------------------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"dbt run "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target prod"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target prod"
        ),
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"dbt docs generate "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target prod"
        ),
        # Docs generation is best-effort; pipeline succeeds even if this step fails.
        # Remove trigger_rule below for strict enforcement.
        trigger_rule="all_done",
    )

    # ------------------------------------------------------------------
    # Task Dependencies
    # ------------------------------------------------------------------
    dbt_run >> dbt_test >> dbt_docs_generate
