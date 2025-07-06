


import json
from pathlib import Path
from airflow.models import Varibale
from pprint import pprint

def airflow_var(index_host_id):
    ariflow_vars = {
        "env": Variable.get("env", default_var="dev"),
        "host_name": Variable.get(f"{index_host_id}_host_name"),
        "es_username": Variable.get(f"{index_host_id}_user"),
        "es_password": Variable.get(f"{index_host_id}_password"),
        "es_ca_certs_path": Variable.get(f"{index_host_id}_ca_certs_path"),
        # "es_header": Variable.get(f"{host_name}_header"), # commented out in the image
        # "es_header": elastic_creds_1.get("header"), # commented out in the image
        "es_header": Variable.get(f"{index_host_id}_header", deserialize_json=True),
        # AWS S3 config
        "aws_access_key_id": Variable.get(f"s3_access_key_id"),
        "aws_secret_access_key": Variable.get(f"s3_secret_access_key"),
        "s3_region": Variable.get(f"s3_region"),
        "s3_bucket": Variable.get(f"s3_bucket_name"),

        # Snowflake credentials
        "sf_user": Variable.get(f"snowflake_user"),
        "sf_password": Variable.get(f"snowflake_password"),
        "sf_account": Variable.get(f"snowflake_account"),
        "sf_warehouse": Variable.get(f"snowflake_warehouse"),
        "sf_role": Variable.get(f"snowflake_role"),
    }
    # pprint(ariflow_vars) # commented out in the image
    return ariflow_vars


def main_config_handler(config_relative_path: str, project_root: str) -> dict:
    # Step 1: Load config file & resolve placeholders
    config_sub = load_and_resolve_config(config_relative_path, project_root)

    # Step 2: Get index_host_id and fetch Airflow Variables
    index_host_id = config_sub.get('index_host_id')
    airflow_vars = airflow_var(index_host_id)

    # Step 3: Update config_sub with the env from Airflow Variables
    config_sub['env'] = airflow_vars.get('env', 'dev')

    # Step 4: Update config_sub with Airflow vars where the key matches
    for key, value in airflow_vars.items():
        if key in config_sub:
            print(f"[ConfigMerge] Overriding key: {key} | Old: {config_sub[key]} | New: {value}")
        config_sub[key] = value

    # Step 5: Build drive and target Snowflake configs
    sf_drive_config = {
        'table': config_sub["drive_table"],
        'schema': config_sub["drive_schema"],
        'database': config_sub["drive_database"],
        'user': config_sub["sf_user"],
        'password': config_sub["sf_password"],
        'account': config_sub["sf_account"],
        'role': config_sub["sf_role"],
        'warehouse': config_sub["sf_warehouse"]
    }
    config_sub['sf_drive_config'] = sf_drive_config

    sf_target_config = {
        'table': config_sub["target_table"],
        'schema': config_sub["target_schema"],
        'database': config_sub["target_database"],
        'user': config_sub["sf_user"],
        'password': config_sub["sf_password"],
        'account': config_sub["sf_account"],
        'role': config_sub["sf_role"],
        'warehouse': config_sub["sf_warehouse"]
    }
    config_sub['sf_target_config'] = sf_target_config

    # Step 6: Done
    return config_sub




def load_and_resolve_config(config_relative_path: str, project_root: str) -> dict:
    """
    Load a JSON config file and resolve all {placeholders} using the config dictionary itself,
    except for {env}, which remains as-is.
    
    Args:
        config_relative_path (str): Relative path to the config file from project root.
        project_root (str): Absolute path to project root as a string.
    
    Returns:
        dict: Fully resolved config.
    """
    project_root_path = Path(project_root).resolve()
    config_relative_path = Path(config_relative_path.strip("/\\"))
    config_path = (project_root_path / config_relative_path).resolve()

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found at: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        raw_config = json.load(f)

    # Do full recursive resolution
    resolved_config = _resolve_placeholders(raw_config, raw_config)
    return resolved_config


def _resolve_placeholders(value, context):
    """
    Recursively resolve placeholders in the config.
    """
    if isinstance(value, str):
        return value.format_map(_SafeFormatDict(context))
    elif isinstance(value, list):
        return [_resolve_placeholders(item, context) for item in value]
    elif isinstance(value, dict):
        return {k: _resolve_placeholders(v, context) for k, v in value.items()}
    else:
        return value  # leave other types as-is


class _SafeFormatDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"

    def __getitem__(self, key):
        if key == "env":
            return "{" + key + "}"
        return dict.get(self, key, "{" + key + "}")
