import json
from pathlib import Path
from airflow.models import Variable
from pprint import pprint

# ────────────────────────────────────────────────────────────────────────────────
# FETCH AIRFLOW VARIABLES
# ────────────────────────────────────────────────────────────────────────────────

def airflow_var(index_host_id):
    airflow_vars = {
        "env": Variable.get("env", default_var="dev"),
        "host_name": Variable.get(f"{index_host_id}_host_name"),
        "es_username": Variable.get(f"{index_host_id}_user"),
        "es_password": Variable.get(f"{index_host_id}_password"),
        "es_ca_certs_path": Variable.get(f"{index_host_id}_ca_certs_path"),
        "es_header": Variable.get(f"{index_host_id}_header", deserialize_json=True),

        # AWS S3 config
        "aws_access_key_id": Variable.get("s3_access_key_id"),
        "aws_secret_access_key": Variable.get("s3_secret_access_key"),
        "s3_region": Variable.get("s3_region"),
        "s3_bucket": Variable.get("s3_bucket_name"),

        # Snowflake credentials
        "sf_user": Variable.get("snowflake_user"),
        "sf_password": Variable.get("snowflake_password"),
        "sf_account": Variable.get("snowflake_account"),
        "sf_warehouse": Variable.get("snowflake_warehouse"),
        "sf_role": Variable.get("snowflake_role"),

        # Optional Airflow overrides
        "airflow_dag_name": Variable.get("airflow_dag_name", default_var=None),
        "airflow_tags": Variable.get("airflow_tags", default_var=None)
    }
    return airflow_vars

# ────────────────────────────────────────────────────────────────────────────────
# MAIN CONFIG HANDLER
# ────────────────────────────────────────────────────────────────────────────────

def main_config_handler(config_relative_path: str, project_root: str) -> dict:
    # Step 1: Load config file & resolve placeholders
    config_sub = load_and_resolve_config(config_relative_path, project_root)

    # Step 2: Get index_host_id and fetch Airflow Variables
    index_host_id = config_sub.get('index_host_id')
    airflow_vars = airflow_var(index_host_id)

    # Step 3: Update config_sub with the env from Airflow Variables
    config_sub['env'] = airflow_vars.get('env', config_sub.get('env', 'dev'))
    config_sub['airflow_dag_name'] = airflow_vars.get('airflow_dag_name', config_sub.get('airflow_dag_name'))
    config_sub['airflow_tags'] = airflow_vars.get('airflow_tags', config_sub.get('airflow_tags'))

    # Step 4: Merge all matching airflow vars
    for key, value in airflow_vars.items():
        if value is not None:
            if key in config_sub:
                print(f"[ConfigMerge] Overriding key: {key} | Old: {config_sub[key]} | New: {value}")
            config_sub[key] = value

    # Step 5: Build Snowflake connection configs
    config_sub['sf_drive_config'] = {
        'table': config_sub["drive_table"],
        'schema': config_sub["drive_schema"],
        'database': config_sub["drive_database"],
        'user': config_sub["sf_user"],
        'password': config_sub["sf_password"],
        'account': config_sub["sf_account"],
        'role': config_sub["sf_role"],
        'warehouse': config_sub["sf_warehouse"],
    }

    config_sub['sf_target_config'] = {
        'table': config_sub["target_table"],
        'schema': config_sub["target_schema"],
        'database': config_sub["target_database"],
        'user': config_sub["sf_user"],
        'password': config_sub["sf_password"],
        'account': config_sub["sf_account"],
        'role': config_sub["sf_role"],
        'warehouse': config_sub["sf_warehouse"],
    }

    # Step 6: Re-resolve placeholders now that env and other values are present
    resolved_config = _resolve_placeholders(config_sub, config_sub)

    return resolved_config

# ────────────────────────────────────────────────────────────────────────────────
# CONFIG FILE LOADER & PLACEHOLDER RESOLVER
# ────────────────────────────────────────────────────────────────────────────────

def load_and_resolve_config(config_relative_path: str, project_root: str) -> dict:
    project_root_path = Path(project_root).resolve()
    config_relative_path = Path(config_relative_path.strip("/\\"))
    config_path = (project_root_path / config_relative_path).resolve()

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found at: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        raw_config = json.load(f)

    # Initial resolution to handle non-env placeholders
    resolved_config = _resolve_placeholders(raw_config, raw_config)
    return resolved_config

def _resolve_placeholders(value, context):
    if isinstance(value, str):
        return value.format_map(_SafeFormatDict(context))
    elif isinstance(value, list):
        return [_resolve_placeholders(item, context) for item in value]
    elif isinstance(value, dict):
        return {k: _resolve_placeholders(v, context) for k, v in value.items()}
    else:
        return value

class _SafeFormatDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"

    def __getitem__(self, key):
        return dict.get(self, key, "{" + key + "}")
