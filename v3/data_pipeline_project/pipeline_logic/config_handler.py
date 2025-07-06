


import json
from pathlib import Path


def main_config_handler(config_relative_path: str, project_root: str) -> dict:
    config_sub = load_and_resolve_config(config_relative_path,project_root)
    sf_drive_config = {
        'table':config_sub["drive_table"],
        'schema':config_sub["drive_schema"],
        'database':config_sub["drive_database"],

        'user':config_sub["sf_user"],
        'password':config_sub["sf_password"],
        'account':config_sub["sf_account"],      
        'role':config_sub["sf_role"],
        'warehouse':config_sub["sf_warehouse"]
    }
    config_sub['sf_drive_config'] = sf_drive_config
    sf_target_config = {
        'table':config_sub["target_table"],
        'schema':config_sub["target_schema"],
        'database':config_sub["target_database"],

        'user':config_sub["sf_user"],
        'password':config_sub["sf_password"],
        'account':config_sub["sf_account"],      
        'role':config_sub["sf_role"],
        'warehouse':config_sub["sf_warehouse"]
    }
    config_sub['sf_target_config'] = sf_target_config


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
