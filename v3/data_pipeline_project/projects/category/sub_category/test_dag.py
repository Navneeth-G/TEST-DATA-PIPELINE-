# drive_table_population_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import pendulum
from pathlib import Path

# Import config and task
from data_pipeline_project.pipeline_logic.config_handler import load_and_resolve_config
from data_pipeline_project.pipeline_logic.tasks.populate_drive_table import main_drive_table_population

# ────────────────────────────────────────────────────────────────────────────────
# CONFIG LOADING
# ────────────────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
CONFIG_RELATIVE_PATH = "projects/category/sub_category/config.json"

config = load_and_resolve_config(CONFIG_RELATIVE_PATH, str(PROJECT_ROOT))

# Extract airflow specific params
DAG_ID = config.get("airflow_dag_name", "default_dag")
OWNER = config.get("airflow_owner", "airflow")
TAGS = config.get("airflow_tags", [])
SCHEDULE_INTERVAL = config.get("airflow_CRON", "0 * * * *")

# Timezone
try:
    DAG_TIMEZONE = pendulum.timezone(config.get("timezone", "UTC"))
except Exception:
    DAG_TIMEZONE = pendulum.timezone("UTC")

# Retry / Alert params
DEFAULT_ARGS = {
    "owner": OWNER,
    "email_on_failure": config.get("dag_email_on_failure", True),
    "email_on_retry": config.get("dag_email_on_retry", False),
    "email": config.get("dag_email_recipients", []),
    "retries": config.get("dag_retries", 1),
    "retry_delay": timedelta(minutes=int(config.get("dag_retry_delay_minutes", 5))),
}

# ────────────────────────────────────────────────────────────────────────────────
# TASK DEFINITIONS
# ────────────────────────────────────────────────────────────────────────────────

def start_task(**context):
    now = pendulum.now(DAG_TIMEZONE)
    print(f"[{DAG_ID}] Start task - current time: {now.to_iso8601_string()}")

def run_drive_table_population(**context):
    now = pendulum.now(DAG_TIMEZONE)
    print(f"[{DAG_ID}] Running drive table population at {now.to_iso8601_string()}")
    main_drive_table_population(config)

def end_task(**context):
    now = pendulum.now(DAG_TIMEZONE)
    print(f"[{DAG_ID}] End task - finished at {now.to_iso8601_string()}")

# ────────────────────────────────────────────────────────────────────────────────
# DAG DEFINITION
# ────────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2025, 1, 1, tz=DAG_TIMEZONE),
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    tags=TAGS,
    default_args=DEFAULT_ARGS,
) as dag:

    start = PythonOperator(
        task_id="start_task",
        python_callable=start_task,
    )

    drive_table_population = PythonOperator(
        task_id="drive_table_population",
        python_callable=run_drive_table_population,
    )

    end = PythonOperator(
        task_id="end_task",
        python_callable=end_task,
    )

    start >> drive_table_population >> end
