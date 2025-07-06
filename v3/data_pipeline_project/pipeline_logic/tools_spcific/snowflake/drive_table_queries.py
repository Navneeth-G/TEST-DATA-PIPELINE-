import pendulum
import snowflake.connector
from snowflake.connector import DictCursor
from typing import Set, List, Dict

from data_pipeline_project.pipeline_logic.utils.pipeline_logger import PipelineLogger


logger = PipelineLogger()


logger = PipelineLogger()



def get_snowflake_connection(sf_drive_config: dict):
    """
    Create a Snowflake connection from the sf_drive_config dict.
    """
    allowed_keys = ['user', 'password', 'account', 'warehouse', 'database', 'schema', 'role']
    connection_params = {k: sf_drive_config[k] for k in allowed_keys if k in sf_drive_config}
    return snowflake.connector.connect(**connection_params)


def is_target_day_complete(target_day: str, config: dict) -> bool:
    """
    Return True if the target day has NO incomplete records.
    """
    sf_drive_config = config['sf_drive_config']
    query = f"""
        SELECT COUNT(1)
        FROM {sf_drive_config['table']}
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

    conn = get_snowflake_connection(sf_drive_config)
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid
            print(f"Snowflake Query ID: {query_id}")            
            return cursor.fetchone()[0] == 0  # No incomplete rows found
    finally:
        conn.close()


def delete_target_day_records(target_day: str, config: dict) -> None:
    """
    Delete all records for the given target day.
    """
    sf_drive_config = config['sf_drive_config']
    query = f"""
        DELETE FROM {sf_drive_config['table']}
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

    conn = get_snowflake_connection(sf_drive_config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid
            print(f"Snowflake Query ID: {query_id}")              
    finally:
        conn.close()


def bulk_insert_records(records_list: List[Dict], config: dict) -> None:
    """
    Bulk insert the given list of records into the Snowflake drive table.
    """
    sf_drive_config = config['sf_drive_config']
    if not records_list:
        return

    column_names = list(records_list[0].keys())
    placeholders = ", ".join(["%s"] * len(column_names))
    columns_str = ", ".join(column_names)

    query = f"INSERT INTO {sf_drive_config['table']} ({columns_str}) VALUES ({placeholders})"
    values = [tuple(record[col] for col in column_names) for record in records_list]

    conn = get_snowflake_connection(sf_drive_config)
    try:
        with conn.cursor() as cursor:
            cursor.executemany(query, values)
            query_id = cursor.sfqid
            print(f"Snowflake Query ID: {query_id}")              
    finally:
        conn.close()


def fetch_incomplete_target_days(config: dict) -> Set[str]:
    """
    Fetch all distinct TARGET_DAY values where CONTINUITY_CHECK_PERFORMED != 'YES'.
    """
    sf_drive_config = config['sf_drive_config']
    query = f"""
        SELECT DISTINCT TARGET_DAY
        FROM {sf_drive_config['table']}
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

    conn = get_snowflake_connection(sf_drive_config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid
            print(f"Snowflake Query ID: {query_id}")              
            return {row[0] for row in cursor.fetchall()}
    finally:
        conn.close()


def fetch_all_target_days(config: dict) -> Set[str]:
    """
    Fetch all distinct TARGET_DAY values present in the drive table.
    """
    sf_drive_config = config['sf_drive_config']
    query = f"""
        SELECT DISTINCT TARGET_DAY
        FROM {sf_drive_config['table']}
        WHERE SOURCE_COMPLETE_CATEGORY = %s
          AND PIPELINE_NAME = %s
          AND PIPELINE_PRIORITY = %s
    """
    params = [
        config['SOURCE_COMPLETE_CATEGORY'],
        config['PIPELINE_NAME'],
        config['PIPELINE_PRIORITY'],
    ]

    conn = get_snowflake_connection(sf_drive_config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid
            print(f"Snowflake Query ID: {query_id}")              
            return {row[0] for row in cursor.fetchall()}
    finally:
        conn.close()


def fetch_pending_drive_records(config: dict, limit: int = 10) -> List[Dict]:
    """
    Fetch pending drive table records where WINDOW_END_TIME is older than (now - x_time_back - granularity).
    
    Args:
        config (dict): Pipeline configuration with Snowflake connection details.
        limit (int): Maximum number of records to fetch.
    
    Returns:
        List[Dict]: List of pending pipeline records as dictionaries.
    """
    from snowflake.connector import DictCursor
    timezone = config['timezone']

    # Calculate the cutoff timestamp
    now = pendulum.now(timezone)
    x_time_back_secs = parse_granularity_to_seconds(config['x_time_back'])
    granularity_secs = parse_granularity_to_seconds(config['granularity'])
    cutoff_time = now.subtract(seconds=x_time_back_secs + granularity_secs).to_iso8601_string()

    # Build query
    sf_drive_config = config['sf_drive_config']
    query = f"""
        SELECT *
        FROM {sf_drive_config['table']}
        WHERE SOURCE_COMPLETE_CATEGORY = %s
          AND PIPELINE_NAME = %s
          AND PIPELINE_PRIORITY = %s
          AND PIPELINE_STATUS = 'PENDING'
          AND WINDOW_END_TIME <= %s
        ORDER BY TARGET_DAY ASC, WINDOW_START_TIME ASC
        LIMIT {limit}
    """

    params = [
        config['SOURCE_COMPLETE_CATEGORY'],
        config['PIPELINE_NAME'],
        config['PIPELINE_PRIORITY'],
        cutoff_time,
    ]

    # Execute query
    conn = get_snowflake_connection(sf_drive_config)
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
    finally:
        conn.close()

def change_pipeline_status_and_retry_number(    pipeline_id: str,    new_status: str,    retry_attempt: int,    config: dict ):
    """
    Update PIPELINE_STATUS, RECORD_LAST_UPDATED_TIME, and RETRY_ATTEMPT
    for a single PIPELINE_ID.

    Args:
        pipeline_id (str): The PIPELINE_ID of the record to update.
        new_status (str): New pipeline status (e.g., 'PROCESSING', 'FAILED', etc.).
        retry_attempt (int): Retry attempt number to set.
        config (dict): Pipeline config containing Snowflake connection info.
    """
    sf_drive_config = config['sf_drive_config']
    now_str = pendulum.now(config['timezone']).to_iso8601_string()

    query = f"""
        UPDATE {sf_drive_config['table']}
        SET PIPELINE_STATUS = %s,
            RECORD_LAST_UPDATED_TIME = %s,
            RETRY_ATTEMPT = %s
        WHERE PIPELINE_ID = %s
    """

    params = [new_status, now_str, retry_attempt, pipeline_id]

    conn = get_snowflake_connection(sf_drive_config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid
            print(f"Snowflake Query ID: {query_id}")              
    finally:
        conn.close()


def update_completed_phase_and_duration(    pipeline_id: str,    completed_phase: str,    phase_duration_seconds: int,    config: dict ):
    """
    Update COMPLETED_PHASE, COMPLETED_PHASE_DURATION, and RECORD_LAST_UPDATED_TIME for a pipeline record.

    Args:
        pipeline_id (str): The PIPELINE_ID of the record to update.
        completed_phase (str): The phase just completed (e.g., 'PRE VALIDATION', 'AUDIT', etc.).
        phase_duration_seconds (int): Duration of the phase in seconds.
        config (dict): Pipeline config containing Snowflake connection info.
    """
    sf_drive_config = config['sf_drive_config']
    now_str = pendulum.now(config['timezone']).to_iso8601_string()

    # Convert seconds to a human-readable format like '1h30m20s'
    phase_duration_str = convert_seconds_to_granularity(phase_duration_seconds)

    query = f"""
        UPDATE {sf_drive_config['table']}
        SET COMPLETED_PHASE = %s,
            COMPLETED_PHASE_DURATION = %s,
            RECORD_LAST_UPDATED_TIME = %s
        WHERE PIPELINE_ID = %s
    """

    params = [completed_phase, phase_duration_str, now_str, pipeline_id]

    conn = get_snowflake_connection(sf_drive_config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid
            print(f"Snowflake Query ID: {query_id}")              
    finally:
        conn.close()


def reset_pipeline_record_on_count_mismatch(    pipeline_id: str,    retry_attempt: int,     config: dict ):
    """
    Reset specific fields in the pipeline record when count mismatch is detected.

    Args:
        pipeline_id (str): The PIPELINE_ID of the record to reset.
        retry_attempt (int): Retry attempt number to set.
        config (dict): Pipeline config containing Snowflake connection info.
    """
    sf_drive_config = config['sf_drive_config']
    now_str = pendulum.now(config['timezone']).to_iso8601_string()

    query = f"""
        UPDATE {sf_drive_config['table']}
        SET COMPLETED_PHASE = NULL,
            COMPLETED_PHASE_DURATION = NULL,
            PIPELINE_STATUS = 'PENDING',
            PIPELINE_START_TIME = NULL,
            PIPELINE_END_TIME = NULL,            
            RECORD_LAST_UPDATED_TIME = %s,
            SOURCE_COUNT = NULL,
            TARGET_COUNT = NULL,
            COUNT_DIFF = NULL,
            COUNT_DIFF_PERCENTAGE = NULL,
            RETRY_ATTEMPT = %s
        WHERE PIPELINE_ID = %s
    """

    params = [now_str, retry_attempt, pipeline_id]

    conn = get_snowflake_connection(sf_drive_config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid
            print(f"Snowflake Query ID: {query_id}")
    finally:
        conn.close()



def update_pre_validation_results( pipeline_id: str,    source_count: int,    target_count: int,    count_diff: int,    count_diff_percentage: float,    completed_phase: str,    phase_duration_str: str,  config: dict):

    query = f"""
        UPDATE {sf_drive_config['table']}
        SET SOURCE_COUNT = %s,
            TARGET_COUNT = %s,
            COUNT_DIFF = %s,
            COUNT_DIFF_PERCENTAGE = %s,
            COMPLETED_PHASE = %s,
            COMPLETED_PHASE_DURATION = %s,
            RECORD_LAST_UPDATED_TIME = %s
        WHERE PIPELINE_ID = %s
    """

    params = [
        source_count,
        target_count,
        count_diff,
        round(count_diff_percentage, 2) if count_diff_percentage is not None else None,
        completed_phase,
        phase_duration_str,
        now_str,
        pipeline_id,
    ]

    conn = get_snowflake_connection(sf_drive_config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid
            print(f"Snowflake Query ID (Pre-Validation Update): {query_id}")
    finally:
        conn.close()




def is_phase_complete(record: dict, config: dict, expected_phase: str) -> bool:
    """
    Check if the given record has already completed the expected phase.

    Args:
        record (dict): The pipeline record containing PIPELINE_ID.
        config (dict): Pipeline config containing Snowflake connection info.
        expected_phase (str): The phase name to check, e.g., 'PRE VALIDATION'.

    Returns:
        bool: True if the phase is already completed, False otherwise.
    """

    sf_drive_config = config['sf_drive_config']
    pipeline_id = record["PIPELINE_ID"]

    query = f"""
        SELECT COMPLETED_PHASE
        FROM {sf_drive_config['table']}
        WHERE PIPELINE_ID = %s
    """

    params = [pipeline_id]

    conn = get_snowflake_connection(sf_drive_config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid

            row = cursor.fetchone()
            completed_phase = row[0] if row else None

            logger.info(
                subject="DRIVE_TABLE_QUERY",
                message="Checked completed phase",
                log_key="DriveTable",
                PIPELINE_ID=pipeline_id,
                EXPECTED_PHASE=expected_phase,
                COMPLETED_PHASE=completed_phase,
                QUERY_ID=query_id
            )

            return completed_phase == expected_phase

    finally:
        conn.close()


def update_audit_results(    pipeline_id: str,    source_count: int,    target_count: int,    count_diff: int,    count_diff_percentage: float,    audit_result: str,    config: dict ):
    """
    Update the drive table after audit is successful.

    Args:
        pipeline_id (str): The pipeline ID for the record.
        source_count (int): Final source count.
        target_count (int): Final target count.
        count_diff (int): Difference between source and target counts.
        count_diff_percentage (float): Percentage difference.
        audit_result (str): Audit result (e.g., 'SUCCESS').
        config (dict): Pipeline config containing Snowflake connection info.
    """

    sf_config = config['sf_drive_config']
    now_str = pendulum.now(config.get('timezone')).to_iso8601_string()

    query = f"""
        UPDATE {sf_config['table']}
        SET COMPLETED_PHASE = %s,
            PIPELINE_STATUS = %s,
            PIPELINE_END_TIME = %s,
            RECORD_LAST_UPDATED_TIME = %s,
            SOURCE_COUNT = %s,
            TARGET_COUNT = %s,
            COUNT_DIFF = %s,
            COUNT_DIFF_PERCENTAGE = %s,
            AUDIT_RESULT = %s
        WHERE PIPELINE_ID = %s
    """

    params = [
        "AUDIT",
        "SUCCESS",
        now_str,
        now_str,
        source_count,
        target_count,
        count_diff,
        round(count_diff_percentage, 2) if count_diff_percentage is not None else None,
        audit_result,
        pipeline_id
    ]

    conn = get_snowflake_connection(sf_config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid

            logger.info(
                subject="AUDIT_UPDATE",
                message="Drive table updated after audit success",
                log_key="AuditUpdate",
                PIPELINE_ID=pipeline_id,
                COMPLETED_PHASE="AUDIT",
                PIPELINE_STATUS="SUCCESS",
                QUERY_ID=query_id
            )

    finally:
        conn.close()


import pendulum
from data_pipeline_project.pipeline_logic.tools_spcific.drive_table_queries import get_snowflake_connection
from data_pipeline_project.pipeline_logic.utils.pipeline_logger import PipelineLogger

logger = PipelineLogger()


def mark_pre_validation_success(
    pipeline_id: str,
    source_count: int,
    target_count: int,
    count_diff: int,
    count_diff_percentage: float,
    config: dict
):
    """
    Mark a pipeline record as SUCCESS during the pre-validation phase if the counts match.

    Args:
        pipeline_id (str): Pipeline ID.
        source_count (int): Source count.
        target_count (int): Target count.
        count_diff (int): Difference (should be 0).
        count_diff_percentage (float): Percentage difference.
        config (dict): Pipeline config.
    """

    sf_config = config['sf_drive_config']
    now_str = pendulum.now(config.get('timezone')).to_iso8601_string()

    query = f"""
        UPDATE {sf_config['table']}
        SET COMPLETED_PHASE = %s,
            PIPELINE_STATUS = %s,
            PIPELINE_START_TIME = %s,
            PIPELINE_END_TIME = %s,
            RECORD_LAST_UPDATED_TIME = %s,
            SOURCE_COUNT = %s,
            TARGET_COUNT = %s,
            COUNT_DIFF = %s,
            COUNT_DIFF_PERCENTAGE = %s,
            AUDIT_RESULT = %s
        WHERE PIPELINE_ID = %s
    """

    params = [
        "PRE VALIDATION",
        "SUCCESS",
        now_str,
        now_str,
        now_str,
        source_count,
        target_count,
        count_diff,
        round(count_diff_percentage, 2) if count_diff_percentage is not None else None,
        "SUCCESS",
        pipeline_id
    ]

    conn = get_snowflake_connection(sf_config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid

            logger.info(
                subject="PRE_VALIDATION_UPDATE",
                message="Drive table updated as SUCCESS in pre-validation phase",
                log_key="PreValidationUpdate",
                PIPELINE_ID=pipeline_id,
                QUERY_ID=query_id
            )

    finally:
        conn.close()






















