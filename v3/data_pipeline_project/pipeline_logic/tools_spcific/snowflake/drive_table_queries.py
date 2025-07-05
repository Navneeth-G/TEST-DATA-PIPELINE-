import snowflake.connector
import pendulum
from typing import Set, List, Dict


def get_snowflake_connection(sf_config: dict):
    """
    Create a Snowflake connection from the sf_drive_config dict.
    """
    allowed_keys = ['user', 'password', 'account', 'warehouse', 'database', 'schema', 'role']
    connection_params = {k: sf_config[k] for k in allowed_keys if k in sf_config}
    return snowflake.connector.connect(**connection_params)


def convert_snowflake_date(value, timezone: str) -> pendulum.DateTime:
    """
    Convert a Snowflake DATE/TIMESTAMP field to pendulum.DateTime, applying the configured timezone.
    """
    if value is None:
        raise ValueError("Cannot convert None to pendulum.DateTime")
    if hasattr(value, 'isoformat'):
        return pendulum.parse(value.isoformat(), tz='UTC').in_timezone(timezone)
    if isinstance(value, str):
        return pendulum.parse(value, tz='UTC').in_timezone(timezone)
    raise TypeError(f"Unsupported type for Snowflake date conversion: {type(value)}")


def is_target_day_complete(target_day: str, config: dict) -> bool:
    sf_config = config['sf_drive_config']
    timezone = config['timezone']
    query = f"""
        SELECT COUNT(1)
        FROM {sf_config['table']}
        WHERE TARGET_DAY = %s
          AND SOURCE_COMPLETE_CATEGORY = %s
          AND PIPELINE_NAME = %s
          AND PIPELINE_PRIORITY = %s
          AND CONTINUITY_CHECK_PERFORMED != 'YES'
    """
    params = [
        target_day,
        config['SOURCE_COMPLETE_CATEGORY'],
        config['PIPELINE_NAME'],
        config['PIPELINE_PRIORITY'],
    ]

    conn = get_snowflake_connection(sf_config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchone()[0] == 0
    finally:
        conn.close()


def delete_target_day_records(target_day: str, config: dict) -> None:
    sf_config = config['sf_drive_config']
    query = f"""
        DELETE FROM {sf_config['table']}
        WHERE TARGET_DAY = %s
          AND SOURCE_COMPLETE_CATEGORY = %s
          AND PIPELINE_NAME = %s
          AND PIPELINE_PRIORITY = %s
    """
    params = [
        target_day,
        config['SOURCE_COMPLETE_CATEGORY'],
        config['PIPELINE_NAME'],
        config['PIPELINE_PRIORITY'],
    ]

    conn = get_snowflake_connection(sf_config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
    finally:
        conn.close()


def bulk_insert_records(records_list: List[Dict], config: dict) -> None:
    sf_config = config['sf_drive_config']
    if not records_list:
        return

    column_names = list(records_list[0].keys())
    placeholders = ", ".join(["%s"] * len(column_names))
    columns_str = ", ".join(column_names)

    query = f"INSERT INTO {sf_config['table']} ({columns_str}) VALUES ({placeholders})"
    values = [tuple(record[col] for col in column_names) for record in records_list]

    conn = get_snowflake_connection(sf_config)
    try:
        with conn.cursor() as cursor:
            cursor.executemany(query, values)
    finally:
        conn.close()


def fetch_incomplete_target_days(config: dict) -> Set[pendulum.DateTime]:
    sf_config = config['sf_drive_config']
    timezone = config['timezone']
    query = f"""
        SELECT DISTINCT TARGET_DAY
        FROM {sf_config['table']}
        WHERE SOURCE_COMPLETE_CATEGORY = %s
          AND PIPELINE_NAME = %s
          AND PIPELINE_PRIORITY = %s
          AND CONTINUITY_CHECK_PERFORMED != 'YES'
    """
    params = [
        config['SOURCE_COMPLETE_CATEGORY'],
        config['PIPELINE_NAME'],
        config['PIPELINE_PRIORITY'],
    ]

    conn = get_snowflake_connection(sf_config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            return {convert_snowflake_date(row[0], timezone) for row in cursor.fetchall()}
    finally:
        conn.close()


def fetch_all_target_days(config: dict) -> Set[pendulum.DateTime]:
    sf_config = config['sf_drive_config']
    timezone = config['timezone']
    query = f"""
        SELECT DISTINCT TARGET_DAY
        FROM {sf_config['table']}
        WHERE SOURCE_COMPLETE_CATEGORY = %s
          AND PIPELINE_NAME = %s
          AND PIPELINE_PRIORITY = %s
    """
    params = [
        config['SOURCE_COMPLETE_CATEGORY'],
        config['PIPELINE_NAME'],
        config['PIPELINE_PRIORITY'],
    ]

    conn = get_snowflake_connection(sf_config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            return {convert_snowflake_date(row[0], timezone) for row in cursor.fetchall()}
    finally:
        conn.close()
