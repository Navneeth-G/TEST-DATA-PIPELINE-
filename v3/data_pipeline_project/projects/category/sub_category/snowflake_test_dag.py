from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import pendulum
from pathlib import Path
import snowflake.connector

# Import your config loader
from data_pipeline_project.pipeline_logic.config_handler import main_config_handler

# ────────────────────────────────────────────────────────────────────────────────
# CONFIG LOADING
# ────────────────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
CONFIG_RELATIVE_PATH = "projects/category/sub_category/config.json"

config = main_config_handler(CONFIG_RELATIVE_PATH, str(PROJECT_ROOT))

# Extract Snowflake params from config
sf_config = config['sf_drive_config']

# Extract Airflow specific params
DAG_ID = "snowflake_conn_test"
OWNER = config.get("airflow_owner", "airflow")
TAGS = config.get("airflow_tags", ["test", "snowflake"])
SCHEDULE_INTERVAL = config.get("airflow_CRON", "@once")

# Timezone
try:
    DAG_TIMEZONE = pendulum.timezone(config.get("timezone", "UTC"))
except Exception:
    DAG_TIMEZONE = pendulum.timezone("UTC")

# Retry / Alert params
DEFAULT_ARGS = {
    "owner": OWNER,
    "email_on_failure": config.get("dag_email_on_failure", False),
    "email_on_retry": config.get("dag_email_on_retry", False),
    "email": config.get("dag_email_recipients", []),
    "retries": config.get("dag_retries", 0),
    "retry_delay": timedelta(minutes=int(config.get("dag_retry_delay_minutes", 1))),
}

# ────────────────────────────────────────────────────────────────────────────────
# TASK DEFINITIONS
# ────────────────────────────────────────────────────────────────────────────────

def snowflake_test_connection(**context):
    print(f"[{DAG_ID}] Connecting to Snowflake with config:")
    for key, value in sf_config.items():
        print(f"   {key}: {value}")

    conn = snowflake.connector.connect(
        user=sf_config["user"],
        password=sf_config["password"],
        account=sf_config["account"],
        warehouse=sf_config["warehouse"],
        database=sf_config["database"],
        schema=sf_config["schema"],
        role=sf_config["role"]
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()
            print(f"[{DAG_ID}] Snowflake version: {version[0]}")
    finally:
        conn.close()
        print(f"[{DAG_ID}] Snowflake connection closed.")

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

    snowflake_test = PythonOperator(
        task_id="test_snowflake_connection",
        python_callable=snowflake_test_connection,
    )
