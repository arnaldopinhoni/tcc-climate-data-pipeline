import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
if not DBT_PROJECT_DIR:
    raise ValueError("Environment variable 'DBT_PROJECT_DIR' is required.")

PROJECT_ROOT_DIR = os.getenv("PROJECT_ROOT_DIR", "/opt/airflow")
UV_VENV_BIN = os.getenv("UV_VENV_BIN", "/opt/airflow/.venv/bin")
DBT_TARGET_PATH = os.getenv("DBT_TARGET_PATH", "/tmp/dbt-target")
DBT_LOG_PATH = os.getenv("DBT_LOG_PATH", "/tmp/dbt-logs")
NEON_SYNC_ENABLED = bool(os.getenv("NEON_DB_HOST"))


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="full_pipeline_open_meteo",
    default_args=default_args,
    description="Pipeline completo: Bronze (Python) -> Silver/Gold (dbt) -> Testes (dbt test)",
    schedule_interval="@daily",
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=["climate", "elt", "dbt", "airflow"],
) as dag:
    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=(
            f"cd {PROJECT_ROOT_DIR} && "
            f"{UV_VENV_BIN}/python -m etl.ingest.open_meteo_ingest"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {PROJECT_ROOT_DIR} && "
            f"{UV_VENV_BIN}/dbt run --project-dir {DBT_PROJECT_DIR} --target dev "
            f"--target-path {DBT_TARGET_PATH} --log-path {DBT_LOG_PATH}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {PROJECT_ROOT_DIR} && "
            f"{UV_VENV_BIN}/dbt test --project-dir {DBT_PROJECT_DIR} --target dev "
            f"--target-path {DBT_TARGET_PATH} --log-path {DBT_LOG_PATH}"
        ),
    )

    bronze_ingest >> dbt_run >> dbt_test

    if NEON_SYNC_ENABLED:
        sync_neon = BashOperator(
            task_id="sync_neon",
            bash_command=(
                f"cd {PROJECT_ROOT_DIR} && "
                f"{UV_VENV_BIN}/python -m etl.sync.sync_to_neon"
            ),
        )

        dbt_test >> sync_neon
