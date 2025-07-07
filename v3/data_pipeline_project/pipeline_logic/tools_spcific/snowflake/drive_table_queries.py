import pendulum
import snowflake.connector
from snowflake.connector import DictCursor
from typing import Set, List, Dict
import time

from data_pipeline_project.pipeline_logic.utils.pipeline_logger import PipelineLogger

logger = PipelineLogger()


def get_snowflake_connection(sf_drive_config: dict):
    """
    Create a Snowflake connection from the sf_drive_config dict.
    """
    start_time = time.time()
    
    try:
        logger.info(
            subject="CONNECTION_ATTEMPT",
            message="Attempting Snowflake connection",
            log_key="SnowflakeConnection",
            ACCOUNT=sf_drive_config.get('account'),
            DATABASE=sf_drive_config.get('database'),
            SCHEMA=sf_drive_config.get('schema'),
            WAREHOUSE=sf_drive_config.get('warehouse')
        )
        
        allowed_keys = ['user', 'password', 'account', 'warehouse', 'database', 'schema', 'role']
        connection_params = {k: sf_drive_config[k] for k in allowed_keys if k in sf_drive_config}
        
        conn = snowflake.connector.connect(**connection_params)
        
        duration = time.time() - start_time
        logger.info(
            subject="CONNECTION_SUCCESS",
            message="Snowflake connection established successfully",
            log_key="SnowflakeConnection",
            CONNECTION_TIME_SECONDS=round(duration, 3),
            ACCOUNT=sf_drive_config.get('account')
        )
        
        return conn
        
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            subject="CONNECTION_FAILED",
            message="Failed to establish Snowflake connection",
            log_key="SnowflakeConnection",
            ERROR=str(e),
            CONNECTION_TIME_SECONDS=round(duration, 3),
            ACCOUNT=sf_drive_config.get('account')
        )
        raise


def is_target_day_complete(target_day: str, config: dict) -> bool:
    """
    Return True if the target day has NO incomplete records.
    """
    start_time = time.time()
    
    logger.info(
        subject="TARGET_DAY_CHECK_START",
        message="Starting target day completeness check",
        log_key="TargetDayCheck",
        TARGET_DAY=target_day,
        PIPELINE_NAME=config.get('PIPELINE_NAME'),
        PIPELINE_PRIORITY=config.get('PIPELINE_PRIORITY')
    )
    
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

    conn = None
    try:
        conn = get_snowflake_connection(sf_drive_config)
        
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid
            incomplete_count = cursor.fetchone()[0]
            is_complete = incomplete_count == 0
            
            duration = time.time() - start_time
            logger.info(
                subject="TARGET_DAY_CHECK_COMPLETE",
                message="Target day completeness check completed",
                log_key="TargetDayCheck",
                TARGET_DAY=target_day,
                INCOMPLETE_COUNT=incomplete_count,
                IS_COMPLETE=is_complete,
                QUERY_ID=query_id,
                EXECUTION_TIME_SECONDS=round(duration, 3)
            )
            
            return is_complete
            
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            subject="TARGET_DAY_CHECK_FAILED",
            message="Target day completeness check failed",
            log_key="TargetDayCheck",
            TARGET_DAY=target_day,
            ERROR=str(e),
            EXECUTION_TIME_SECONDS=round(duration, 3)
        )
        raise
    finally:
        if conn:
            try:
                conn.close()
                logger.debug(
                    subject="CONNECTION_CLOSED",
                    message="Snowflake connection closed",
                    log_key="TargetDayCheck"
                )
            except Exception as e:
                logger.warning(
                    subject="CONNECTION_CLOSE_WARNING",
                    message="Warning while closing connection",
                    log_key="TargetDayCheck",
                    ERROR=str(e)
                )


def delete_target_day_records(target_day: str, config: dict) -> None:
    """
    Delete all records for the given target day.
    """
    start_time = time.time()
    
    logger.info(
        subject="DELETE_RECORDS_START",
        message="Starting target day records deletion",
        log_key="DeleteRecords",
        TARGET_DAY=target_day,
        PIPELINE_NAME=config.get('PIPELINE_NAME'),
        PIPELINE_PRIORITY=config.get('PIPELINE_PRIORITY')
    )
    
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

    conn = None
    try:
        conn = get_snowflake_connection(sf_drive_config)
        
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid
            rows_deleted = cursor.rowcount
            
            duration = time.time() - start_time
            logger.info(
                subject="DELETE_RECORDS_COMPLETE",
                message="Target day records deletion completed",
                log_key="DeleteRecords",
                TARGET_DAY=target_day,
                ROWS_DELETED=rows_deleted,
                QUERY_ID=query_id,
                EXECUTION_TIME_SECONDS=round(duration, 3)
            )
            
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            subject="DELETE_RECORDS_FAILED",
            message="Target day records deletion failed",
            log_key="DeleteRecords",
            TARGET_DAY=target_day,
            ERROR=str(e),
            EXECUTION_TIME_SECONDS=round(duration, 3)
        )
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception as e:
                logger.warning(
                    subject="CONNECTION_CLOSE_WARNING",
                    message="Warning while closing connection",
                    log_key="DeleteRecords",
                    ERROR=str(e)
                )


def bulk_insert_records(records_list: List[Dict], config: dict) -> None:
    """
    Bulk insert the given list of records into the Snowflake drive table.
    """
    start_time = time.time()
    
    logger.info(
        subject="BULK_INSERT_START",
        message="Starting bulk insert operation",
        log_key="BulkInsert",
        RECORD_COUNT=len(records_list) if records_list else 0,
        TABLE=config['sf_drive_config'].get('table')
    )
    
    sf_drive_config = config['sf_drive_config']
    if not records_list:
        logger.warning(
            subject="BULK_INSERT_EMPTY",
            message="No records to insert, skipping bulk insert",
            log_key="BulkInsert"
        )
        return

    try:
        column_names = list(records_list[0].keys())
        placeholders = ", ".join(["%s"] * len(column_names))
        columns_str = ", ".join(column_names)

        query = f"INSERT INTO {sf_drive_config['table']} ({columns_str}) VALUES ({placeholders})"
        values = [tuple(record[col] for col in column_names) for record in records_list]

        logger.debug(
            subject="BULK_INSERT_QUERY_PREPARED",
            message="Bulk insert query prepared",
            log_key="BulkInsert",
            COLUMN_COUNT=len(column_names),
            COLUMNS=column_names
        )

        conn = None
        try:
            conn = get_snowflake_connection(sf_drive_config)
            
            with conn.cursor() as cursor:
                cursor.executemany(query, values)
                query_id = cursor.sfqid
                rows_inserted = cursor.rowcount
                
                duration = time.time() - start_time
                logger.info(
                    subject="BULK_INSERT_COMPLETE",
                    message="Bulk insert operation completed successfully",
                    log_key="BulkInsert",
                    RECORDS_INSERTED=rows_inserted,
                    QUERY_ID=query_id,
                    EXECUTION_TIME_SECONDS=round(duration, 3)
                )
                
        except Exception as e:
            duration = time.time() - start_time
            logger.error(
                subject="BULK_INSERT_FAILED",
                message="Bulk insert operation failed",
                log_key="BulkInsert",
                ERROR=str(e),
                RECORD_COUNT=len(records_list),
                EXECUTION_TIME_SECONDS=round(duration, 3)
            )
            raise
        finally:
            if conn:
                try:
                    conn.close()
                except Exception as e:
                    logger.warning(
                        subject="CONNECTION_CLOSE_WARNING",
                        message="Warning while closing connection",
                        log_key="BulkInsert",
                        ERROR=str(e)
                    )
                    
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            subject="BULK_INSERT_PREPARATION_FAILED",
            message="Failed to prepare bulk insert operation",
            log_key="BulkInsert",
            ERROR=str(e),
            EXECUTION_TIME_SECONDS=round(duration, 3)
        )
        raise


def fetch_incomplete_target_days(config: dict) -> Set[str]:
    """
    Fetch all distinct TARGET_DAY values where CONTINUITY_CHECK_PERFORMED != 'YES'.
    """
    start_time = time.time()
    
    logger.info(
        subject="FETCH_INCOMPLETE_DAYS_START",
        message="Starting fetch of incomplete target days",
        log_key="FetchIncompleteDays",
        PIPELINE_NAME=config.get('PIPELINE_NAME'),
        PIPELINE_PRIORITY=config.get('PIPELINE_PRIORITY')
    )
    
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

    conn = None
    try:
        conn = get_snowflake_connection(sf_drive_config)
        
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid
            rows = cursor.fetchall()
            target_days = {row[0] for row in rows}
            
            duration = time.time() - start_time
            logger.info(
                subject="FETCH_INCOMPLETE_DAYS_COMPLETE",
                message="Fetch incomplete target days completed",
                log_key="FetchIncompleteDays",
                INCOMPLETE_DAYS_COUNT=len(target_days),
                QUERY_ID=query_id,
                EXECUTION_TIME_SECONDS=round(duration, 3)
            )
            
            return target_days
            
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            subject="FETCH_INCOMPLETE_DAYS_FAILED",
            message="Fetch incomplete target days failed",
            log_key="FetchIncompleteDays",
            ERROR=str(e),
            EXECUTION_TIME_SECONDS=round(duration, 3)
        )
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception as e:
                logger.warning(
                    subject="CONNECTION_CLOSE_WARNING",
                    message="Warning while closing connection",
                    log_key="FetchIncompleteDays",
                    ERROR=str(e)
                )


def fetch_all_target_days(config: dict) -> Set[str]:
    """
    Fetch all distinct TARGET_DAY values present in the drive table.
    """
    start_time = time.time()
    
    logger.info(
        subject="FETCH_ALL_DAYS_START",
        message="Starting fetch of all target days",
        log_key="FetchAllDays",
        PIPELINE_NAME=config.get('PIPELINE_NAME'),
        PIPELINE_PRIORITY=config.get('PIPELINE_PRIORITY')
    )
    
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

    conn = None
    try:
        conn = get_snowflake_connection(sf_drive_config)
        
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid
            rows = cursor.fetchall()
            target_days = {row[0] for row in rows}
            
            duration = time.time() - start_time
            logger.info(
                subject="FETCH_ALL_DAYS_COMPLETE",
                message="Fetch all target days completed",
                log_key="FetchAllDays",
                TOTAL_DAYS_COUNT=len(target_days),
                QUERY_ID=query_id,
                EXECUTION_TIME_SECONDS=round(duration, 3)
            )
            
            return target_days
            
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            subject="FETCH_ALL_DAYS_FAILED",
            message="Fetch all target days failed",
            log_key="FetchAllDays",
            ERROR=str(e),
            EXECUTION_TIME_SECONDS=round(duration, 3)
        )
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception as e:
                logger.warning(
                    subject="CONNECTION_CLOSE_WARNING",
                    message="Warning while closing connection",
                    log_key="FetchAllDays",
                    ERROR=str(e)
                )


def fetch_pending_drive_records(config: dict, limit: int = 10) -> List[Dict]:
    """
    Fetch pending drive table records where WINDOW_END_TIME is older than (now - x_time_back - granularity).
    """
    start_time = time.time()
    
    logger.info(
        subject="FETCH_PENDING_RECORDS_START",
        message="Starting fetch of pending drive records",
        log_key="FetchPendingRecords",
        LIMIT=limit,
        PIPELINE_NAME=config.get('PIPELINE_NAME'),
        TIMEZONE=config.get('timezone')
    )
    
    try:
        timezone = config['timezone']
        
        # Calculate the cutoff timestamp
        now = pendulum.now(timezone)
        x_time_back_secs = parse_granularity_to_seconds(config['x_time_back'])
        granularity_secs = parse_granularity_to_seconds(config['granularity'])
        cutoff_time = now.subtract(seconds=x_time_back_secs + granularity_secs).to_iso8601_string()
        
        logger.debug(
            subject="CUTOFF_TIME_CALCULATED",
            message="Cutoff time calculated for pending records",
            log_key="FetchPendingRecords",
            CUTOFF_TIME=cutoff_time,
            X_TIME_BACK_SECS=x_time_back_secs,
            GRANULARITY_SECS=granularity_secs
        )

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

        conn = None
        try:
            conn = get_snowflake_connection(sf_drive_config)
            
            with conn.cursor(DictCursor) as cursor:
                cursor.execute(query, params)
                query_id = cursor.sfqid
                records = cursor.fetchall()
                
                duration = time.time() - start_time
                logger.info(
                    subject="FETCH_PENDING_RECORDS_COMPLETE",
                    message="Fetch pending drive records completed",
                    log_key="FetchPendingRecords",
                    RECORDS_FOUND=len(records),
                    QUERY_ID=query_id,
                    EXECUTION_TIME_SECONDS=round(duration, 3)
                )
                
                return records
                
        except Exception as e:
            duration = time.time() - start_time
            logger.error(
                subject="FETCH_PENDING_RECORDS_FAILED",
                message="Fetch pending drive records failed",
                log_key="FetchPendingRecords",
                ERROR=str(e),
                EXECUTION_TIME_SECONDS=round(duration, 3)
            )
            raise
        finally:
            if conn:
                try:
                    conn.close()
                except Exception as e:
                    logger.warning(
                        subject="CONNECTION_CLOSE_WARNING",
                        message="Warning while closing connection",
                        log_key="FetchPendingRecords",
                        ERROR=str(e)
                    )
                    
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            subject="FETCH_PENDING_RECORDS_PREPARATION_FAILED",
            message="Failed to prepare pending records fetch",
            log_key="FetchPendingRecords",
            ERROR=str(e),
            EXECUTION_TIME_SECONDS=round(duration, 3)
        )
        raise


def change_pipeline_status_and_retry_number(pipeline_id: str, new_status: str, retry_attempt: int, config: dict):
    """     Update PIPELINE_STATUS, RECORD_LAST_UPDATED_TIME, and RETRY_ATTEMPT for a single PIPELINE_ID.   """
    start_time = time.time()
    
    logger.info(
        subject="STATUS_UPDATE_START",
        message="Starting pipeline status and retry update",
        log_key="StatusUpdate",
        PIPELINE_ID=pipeline_id,
        NEW_STATUS=new_status,
        RETRY_ATTEMPT=retry_attempt
    )
    
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

    conn = None
    try:
        conn = get_snowflake_connection(sf_drive_config)
        
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid
            rows_updated = cursor.rowcount
            
            duration = time.time() - start_time
            logger.info(
                subject="STATUS_UPDATE_COMPLETE",
                message="Pipeline status and retry update completed",
                log_key="StatusUpdate",
                PIPELINE_ID=pipeline_id,
                NEW_STATUS=new_status,
                RETRY_ATTEMPT=retry_attempt,
                ROWS_UPDATED=rows_updated,
                QUERY_ID=query_id,
                EXECUTION_TIME_SECONDS=round(duration, 3)
            )
            
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            subject="STATUS_UPDATE_FAILED",
            message="Pipeline status and retry update failed",
            log_key="StatusUpdate",
            PIPELINE_ID=pipeline_id,
            NEW_STATUS=new_status,
            RETRY_ATTEMPT=retry_attempt,
            ERROR=str(e),
            EXECUTION_TIME_SECONDS=round(duration, 3)
        )
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception as e:
                logger.warning(
                    subject="CONNECTION_CLOSE_WARNING",
                    message="Warning while closing connection",
                    log_key="StatusUpdate",
                    ERROR=str(e)
                )


def update_completed_phase_and_duration(pipeline_id: str, completed_phase: str, phase_duration_seconds: int, config: dict):
    """  Update COMPLETED_PHASE, COMPLETED_PHASE_DURATION, and RECORD_LAST_UPDATED_TIME for a pipeline record.   """
    start_time = time.time()
    
    logger.info(
        subject="PHASE_UPDATE_START",
        message="Starting phase completion update",
        log_key="PhaseUpdate",
        PIPELINE_ID=pipeline_id,
        COMPLETED_PHASE=completed_phase,
        PHASE_DURATION_SECONDS=phase_duration_seconds
    )
    
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

    conn = None
    try:
        conn = get_snowflake_connection(sf_drive_config)
        
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid
            rows_updated = cursor.rowcount
            
            duration = time.time() - start_time
            logger.info(
                subject="PHASE_UPDATE_COMPLETE",
                message="Phase completion update completed",
                log_key="PhaseUpdate",
                PIPELINE_ID=pipeline_id,
                COMPLETED_PHASE=completed_phase,
                PHASE_DURATION_STR=phase_duration_str,
                ROWS_UPDATED=rows_updated,
                QUERY_ID=query_id,
                EXECUTION_TIME_SECONDS=round(duration, 3)
            )
            
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            subject="PHASE_UPDATE_FAILED",
            message="Phase completion update failed",
            log_key="PhaseUpdate",
            PIPELINE_ID=pipeline_id,
            COMPLETED_PHASE=completed_phase,
            ERROR=str(e),
            EXECUTION_TIME_SECONDS=round(duration, 3)
        )
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception as e:
                logger.warning(
                    subject="CONNECTION_CLOSE_WARNING",
                    message="Warning while closing connection",
                    log_key="PhaseUpdate",
                    ERROR=str(e)
                )


def reset_pipeline_record_on_count_mismatch(pipeline_id: str, retry_attempt: int, config: dict):
    """
    Reset specific fields in the pipeline record when count mismatch is detected.
    """
    start_time = time.time()
    
    logger.info(
        subject="RECORD_RESET_START",
        message="Starting pipeline record reset due to count mismatch",
        log_key="RecordReset",
        PIPELINE_ID=pipeline_id,
        RETRY_ATTEMPT=retry_attempt
    )
    
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

    conn = None
    try:
        conn = get_snowflake_connection(sf_drive_config)
        
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid
            rows_updated = cursor.rowcount
            
            duration = time.time() - start_time
            logger.info(
                subject="RECORD_RESET_COMPLETE",
                message="Pipeline record reset completed successfully",
                log_key="RecordReset",
                PIPELINE_ID=pipeline_id,
                RETRY_ATTEMPT=retry_attempt,
                ROWS_UPDATED=rows_updated,
                QUERY_ID=query_id,
                EXECUTION_TIME_SECONDS=round(duration, 3)
            )
            
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            subject="RECORD_RESET_FAILED",
            message="Pipeline record reset failed",
            log_key="RecordReset",
            PIPELINE_ID=pipeline_id,
            RETRY_ATTEMPT=retry_attempt,
            ERROR=str(e),
            EXECUTION_TIME_SECONDS=round(duration, 3)
        )
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception as e:
                logger.warning(
                    subject="CONNECTION_CLOSE_WARNING",
                    message="Warning while closing connection",
                    log_key="RecordReset",
                    ERROR=str(e)
                )


def update_pre_validation_results(pipeline_id: str, source_count: int, target_count: int, count_diff: int, count_diff_percentage: float, completed_phase: str, phase_duration_str: str, config: dict):
    """     Update pre-validation results in the drive table.  """
    start_time = time.time()
    
    logger.info(
        subject="PRE_VALIDATION_UPDATE_START",
        message="Starting pre-validation results update",
        log_key="PreValidationUpdate",
        PIPELINE_ID=pipeline_id,
        SOURCE_COUNT=source_count,
        TARGET_COUNT=target_count,
        COUNT_DIFF=count_diff,
        COUNT_DIFF_PERCENTAGE=count_diff_percentage,
        COMPLETED_PHASE=completed_phase
    )
    
    sf_drive_config = config['sf_drive_config']
    now_str = pendulum.now(config['timezone']).to_iso8601_string()

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

    conn = None
    try:
        conn = get_snowflake_connection(sf_drive_config)
        
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid
            rows_updated = cursor.rowcount
            
            duration = time.time() - start_time
            logger.info(
                subject="PRE_VALIDATION_UPDATE_COMPLETE",
                message="Pre-validation results update completed",
                log_key="PreValidationUpdate",
                PIPELINE_ID=pipeline_id,
                SOURCE_COUNT=source_count,
                TARGET_COUNT=target_count,
                COUNT_DIFF=count_diff,
                ROWS_UPDATED=rows_updated,
                QUERY_ID=query_id,
                EXECUTION_TIME_SECONDS=round(duration, 3)
            )
            
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            subject="PRE_VALIDATION_UPDATE_FAILED",
            message="Pre-validation results update failed",
            log_key="PreValidationUpdate",
            PIPELINE_ID=pipeline_id,
            ERROR=str(e),
            EXECUTION_TIME_SECONDS=round(duration, 3)
        )
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception as e:
                logger.warning(
                    subject="CONNECTION_CLOSE_WARNING",
                    message="Warning while closing connection",
                    log_key="PreValidationUpdate",
                    ERROR=str(e)
                )


def is_phase_complete(record: dict, config: dict, expected_phase: str) -> bool:
   """
   Check if the given record has already completed the expected phase.
   """
   start_time = time.time()
   pipeline_id = record["PIPELINE_ID"]
   
   logger.info(
       subject="PHASE_CHECK_START",
       message="Starting phase completion check",
       log_key="PhaseCheck",
       PIPELINE_ID=pipeline_id,
       EXPECTED_PHASE=expected_phase
   )

   sf_drive_config = config['sf_drive_config']

   query = f"""
       SELECT COMPLETED_PHASE
       FROM {sf_drive_config['table']}
       WHERE PIPELINE_ID = %s
   """

   params = [pipeline_id]

   conn = None
   try:
       conn = get_snowflake_connection(sf_drive_config)
       
       with conn.cursor() as cursor:
           cursor.execute(query, params)
           query_id = cursor.sfqid

           row = cursor.fetchone()
           completed_phase = row[0] if row else None
           is_complete = completed_phase == expected_phase

           duration = time.time() - start_time
           logger.info(
               subject="PHASE_CHECK_COMPLETE",
               message="Phase completion check completed",
               log_key="PhaseCheck",
               PIPELINE_ID=pipeline_id,
               EXPECTED_PHASE=expected_phase,
               COMPLETED_PHASE=completed_phase,
               IS_PHASE_COMPLETE=is_complete,
               QUERY_ID=query_id,
               EXECUTION_TIME_SECONDS=round(duration, 3)
           )

           return is_complete

   except Exception as e:
       duration = time.time() - start_time
       logger.error(
           subject="PHASE_CHECK_FAILED",
           message="Phase completion check failed",
           log_key="PhaseCheck",
           PIPELINE_ID=pipeline_id,
           EXPECTED_PHASE=expected_phase,
           ERROR=str(e),
           EXECUTION_TIME_SECONDS=round(duration, 3)
       )
       raise
   finally:
       if conn:
           try:
               conn.close()
           except Exception as e:
               logger.warning(
                   subject="CONNECTION_CLOSE_WARNING",
                   message="Warning while closing connection",
                   log_key="PhaseCheck",
                   ERROR=str(e)
               )


def update_audit_results(pipeline_id: str, source_count: int, target_count: int, count_diff: int, count_diff_percentage: float, audit_result: str, config: dict):
   """  Update the drive table after audit is successful. """
   start_time = time.time()
   
   logger.info(
       subject="AUDIT_UPDATE_START",
       message="Starting audit results update",
       log_key="AuditUpdate",
       PIPELINE_ID=pipeline_id,
       SOURCE_COUNT=source_count,
       TARGET_COUNT=target_count,
       COUNT_DIFF=count_diff,
       COUNT_DIFF_PERCENTAGE=count_diff_percentage,
       AUDIT_RESULT=audit_result
   )

   sf_drive_config = config['sf_drive_config']
   now_str = pendulum.now(config.get('timezone')).to_iso8601_string()

   query = f"""
       UPDATE {sf_drive_config['table']}
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

   conn = None
   try:
       conn = get_snowflake_connection(sf_drive_config)
       
       with conn.cursor() as cursor:
           cursor.execute(query, params)
           query_id = cursor.sfqid
           rows_updated = cursor.rowcount

           duration = time.time() - start_time
           logger.info(
               subject="AUDIT_UPDATE_COMPLETE",
               message="Audit results update completed successfully",
               log_key="AuditUpdate",
               PIPELINE_ID=pipeline_id,
               COMPLETED_PHASE="AUDIT",
               PIPELINE_STATUS="SUCCESS",
               AUDIT_RESULT=audit_result,
               ROWS_UPDATED=rows_updated,
               QUERY_ID=query_id,
               EXECUTION_TIME_SECONDS=round(duration, 3)
           )

   except Exception as e:
       duration = time.time() - start_time
       logger.error(
           subject="AUDIT_UPDATE_FAILED",
           message="Audit results update failed",
           log_key="AuditUpdate",
           PIPELINE_ID=pipeline_id,
           AUDIT_RESULT=audit_result,
           ERROR=str(e),
           EXECUTION_TIME_SECONDS=round(duration, 3)
       )
       raise
   finally:
       if conn:
           try:
               conn.close()
           except Exception as e:
               logger.warning(
                   subject="CONNECTION_CLOSE_WARNING",
                   message="Warning while closing connection",
                   log_key="AuditUpdate",
                   ERROR=str(e)
               )


def mark_pre_validation_success(pipeline_id: str, source_count: int, target_count: int, count_diff: int, count_diff_percentage: float, config: dict):
   """
   Mark a pipeline record as SUCCESS during the pre-validation phase if the counts match.
   """
   start_time = time.time()
   
   logger.info(
       subject="PRE_VALIDATION_SUCCESS_START",
       message="Starting pre-validation success marking",
       log_key="PreValidationSuccess",
       PIPELINE_ID=pipeline_id,
       SOURCE_COUNT=source_count,
       TARGET_COUNT=target_count,
       COUNT_DIFF=count_diff,
       COUNT_DIFF_PERCENTAGE=count_diff_percentage
   )

   sf_drive_config = config['sf_drive_config']
   now_str = pendulum.now(config.get('timezone')).to_iso8601_string()

   query = f"""
       UPDATE {sf_drive_config['table']}
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
       "COMPLETED",
       now_str,
       now_str,
       now_str,
       source_count,
       target_count,
       count_diff,
       round(count_diff_percentage, 2) if count_diff_percentage is not None else None,
       "MATCHED",
       pipeline_id
   ]

   conn = None
   try:
       conn = get_snowflake_connection(sf_drive_config)
       
       with conn.cursor() as cursor:
           cursor.execute(query, params)
           query_id = cursor.sfqid
           rows_updated = cursor.rowcount

           duration = time.time() - start_time
           logger.info(
               subject="PRE_VALIDATION_SUCCESS_COMPLETE",
               message="Pre-validation success marking completed",
               log_key="PreValidationSuccess",
               PIPELINE_ID=pipeline_id,
               COMPLETED_PHASE="PRE VALIDATION",
               PIPELINE_STATUS="SUCCESS",
               SOURCE_COUNT=source_count,
               TARGET_COUNT=target_count,
               COUNT_DIFF=count_diff,
               ROWS_UPDATED=rows_updated,
               QUERY_ID=query_id,
               EXECUTION_TIME_SECONDS=round(duration, 3)
           )

   except Exception as e:
       duration = time.time() - start_time
       logger.error(
           subject="PRE_VALIDATION_SUCCESS_FAILED",
           message="Pre-validation success marking failed",
           log_key="PreValidationSuccess",
           PIPELINE_ID=pipeline_id,
           ERROR=str(e),
           EXECUTION_TIME_SECONDS=round(duration, 3)
       )
       raise
   finally:
       if conn:
           try:
               conn.close()
           except Exception as e:
               logger.warning(
                   subject="CONNECTION_CLOSE_WARNING",
                   message="Warning while closing connection",
                   log_key="PreValidationSuccess",
                   ERROR=str(e)
               )


# Additional utility functions that appear to be missing but referenced in the code
def parse_granularity_to_seconds(granularity: str) -> int:
   """
   Parse granularity string to seconds. Add logging for robustness.
   """
   start_time = time.time()
   
   logger.debug(
       subject="GRANULARITY_PARSE_START",
       message="Starting granularity parsing",
       log_key="GranularityParse",
       GRANULARITY=granularity
   )
   
   try:
       # Implementation would go here based on your granularity format
       # This is a placeholder - replace with actual implementation
       
       # Example implementation for common formats
       granularity = granularity.lower().strip()
       
       if granularity.endswith('s'):
           seconds = int(granularity[:-1])
       elif granularity.endswith('m'):
           seconds = int(granularity[:-1]) * 60
       elif granularity.endswith('h'):
           seconds = int(granularity[:-1]) * 3600
       elif granularity.endswith('d'):
           seconds = int(granularity[:-1]) * 86400
       else:
           # Assume it's already in seconds
           seconds = int(granularity)
       
       duration = time.time() - start_time
       logger.debug(
           subject="GRANULARITY_PARSE_COMPLETE",
           message="Granularity parsing completed",
           log_key="GranularityParse",
           GRANULARITY=granularity,
           SECONDS=seconds,
           EXECUTION_TIME_SECONDS=round(duration, 3)
       )
       
       return seconds
       
   except Exception as e:
       duration = time.time() - start_time
       logger.error(
           subject="GRANULARITY_PARSE_FAILED",
           message="Granularity parsing failed",
           log_key="GranularityParse",
           GRANULARITY=granularity,
           ERROR=str(e),
           EXECUTION_TIME_SECONDS=round(duration, 3)
       )
       raise


def convert_seconds_to_granularity(seconds: int) -> str:
   """
   Convert seconds to human-readable format. Add logging for robustness.
   """
   start_time = time.time()
   
   logger.debug(
       subject="SECONDS_CONVERSION_START",
       message="Starting seconds to granularity conversion",
       log_key="SecondsConversion",
       SECONDS=seconds
   )
   
   try:
       if seconds < 60:
           result = f"{seconds}s"
       elif seconds < 3600:
           minutes = seconds // 60
           remaining_seconds = seconds % 60
           if remaining_seconds == 0:
               result = f"{minutes}m"
           else:
               result = f"{minutes}m{remaining_seconds}s"
       elif seconds < 86400:
           hours = seconds // 3600
           remaining_seconds = seconds % 3600
           minutes = remaining_seconds // 60
           remaining_seconds = remaining_seconds % 60
           
           result = f"{hours}h"
           if minutes > 0:
               result += f"{minutes}m"
           if remaining_seconds > 0:
               result += f"{remaining_seconds}s"
       else:
           days = seconds // 86400
           remaining_seconds = seconds % 86400
           hours = remaining_seconds // 3600
           remaining_seconds = remaining_seconds % 3600
           minutes = remaining_seconds // 60
           remaining_seconds = remaining_seconds % 60
           
           result = f"{days}d"
           if hours > 0:
               result += f"{hours}h"
           if minutes > 0:
               result += f"{minutes}m"
           if remaining_seconds > 0:
               result += f"{remaining_seconds}s"
       
       duration = time.time() - start_time
       logger.debug(
           subject="SECONDS_CONVERSION_COMPLETE",
           message="Seconds to granularity conversion completed",
           log_key="SecondsConversion",
           SECONDS=seconds,
           RESULT=result,
           EXECUTION_TIME_SECONDS=round(duration, 3)
       )
       
       return result
       
   except Exception as e:
       duration = time.time() - start_time
       logger.error(
           subject="SECONDS_CONVERSION_FAILED",
           message="Seconds to granularity conversion failed",
           log_key="SecondsConversion",
           SECONDS=seconds,
           ERROR=str(e),
           EXECUTION_TIME_SECONDS=round(duration, 3)
       )
       raise


def do_target_day_has_CONTINUITY_CHECK_PERFORMED_no(target_day: str, config: dict) -> bool:
    """
    Return True if the target day has records where CONTINUITY_CHECK_PERFORMED != 'YES'.
    """
    start_time = time.time()
    
    logger.info(
        subject="CONTINUITY_CHECK_NO_START",
        message="Starting check for target day with no continuity check performed",
        log_key="ContinuityCheckNo",
        TARGET_DAY=target_day,
        PIPELINE_NAME=config.get('PIPELINE_NAME'),
        PIPELINE_PRIORITY=config.get('PIPELINE_PRIORITY')
    )
    
    sf_drive_config = config['sf_drive_config']
    query = f"""
        SELECT count(*) FROM {sf_drive_config['table']}
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

    conn = None
    try:
        conn = get_snowflake_connection(sf_drive_config)
        
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid
            records_without_continuity_check = cursor.fetchone()[0]
            has_incomplete_continuity = records_without_continuity_check > 0
            
            duration = time.time() - start_time
            logger.info(
                subject="CONTINUITY_CHECK_NO_COMPLETE",
                message="Continuity check no query completed",
                log_key="ContinuityCheckNo",
                TARGET_DAY=target_day,
                RECORDS_WITHOUT_CONTINUITY_CHECK=records_without_continuity_check,
                HAS_INCOMPLETE_CONTINUITY=has_incomplete_continuity,
                QUERY_ID=query_id,
                EXECUTION_TIME_SECONDS=round(duration, 3)
            )
            
            return has_incomplete_continuity
            
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            subject="CONTINUITY_CHECK_NO_FAILED",
            message="Continuity check no query failed",
            log_key="ContinuityCheckNo",
            TARGET_DAY=target_day,
            ERROR=str(e),
            EXECUTION_TIME_SECONDS=round(duration, 3)
        )
        raise
    finally:
        if conn:
            try:
                conn.close()
                logger.debug(
                    subject="CONNECTION_CLOSED",
                    message="Snowflake connection closed",
                    log_key="ContinuityCheckNo"
                )
            except Exception as e:
                logger.warning(
                    subject="CONNECTION_CLOSE_WARNING",
                    message="Warning while closing connection",
                    log_key="ContinuityCheckNo",
                    ERROR=str(e)
                )


def do_target_day_complete(target_day: str, config: dict) -> bool:
    """
    Return True if the target day has NO incomplete records (all records have CONTINUITY_CHECK_PERFORMED = 'YES').
    """
    start_time = time.time()
    
    logger.info(
        subject="TARGET_DAY_COMPLETE_START",
        message="Starting check for target day completion status",
        log_key="TargetDayComplete",
        TARGET_DAY=target_day,
        PIPELINE_NAME=config.get('PIPELINE_NAME'),
        PIPELINE_PRIORITY=config.get('PIPELINE_PRIORITY')
    )
    
    sf_drive_config = config['sf_drive_config']
    query = f"""
        SELECT count(*) FROM {sf_drive_config['table']}
        WHERE TARGET_DAY = %s
          AND SOURCE_COMPLETE_CATEGORY = %s
          AND PIPELINE_NAME = %s
          AND PIPELINE_PRIORITY = %s
          AND CONTINUITY_CHECK_PERFORMED = 'YES'
    """
    
    params = [
        target_day,
        config['SOURCE_COMPLETE_CATEGORY'],
        config['PIPELINE_NAME'],
        config['PIPELINE_PRIORITY'],
    ]

    conn = None
    try:
        conn = get_snowflake_connection(sf_drive_config)
        
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid
            records_with_continuity_check = cursor.fetchone()[0]
            is_day_complete = records_with_continuity_check > 0
            
            duration = time.time() - start_time
            logger.info(
                subject="TARGET_DAY_COMPLETE_COMPLETE",
                message="Target day completion check completed",
                log_key="TargetDayComplete",
                TARGET_DAY=target_day,
                RECORDS_WITH_CONTINUITY_CHECK=records_with_continuity_check,
                IS_DAY_COMPLETE=is_day_complete,
                QUERY_ID=query_id,
                EXECUTION_TIME_SECONDS=round(duration, 3)
            )
            
            # Additional business logic validation
            if is_day_complete and records_with_continuity_check > 0:
                logger.info(
                    subject="TARGET_DAY_VALIDATED_COMPLETE",
                    message="Target day validated as complete with continuity checks",
                    log_key="TargetDayComplete",
                    TARGET_DAY=target_day,
                    VALIDATED_RECORDS_COUNT=records_with_continuity_check
                )
            elif not is_day_complete:
                logger.info(
                    subject="TARGET_DAY_NOT_COMPLETE",
                    message="Target day not complete - no records with continuity check",
                    log_key="TargetDayComplete",
                    TARGET_DAY=target_day
                )
            
            return is_day_complete
            
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            subject="TARGET_DAY_COMPLETE_FAILED",
            message="Target day completion check failed",
            log_key="TargetDayComplete",
            TARGET_DAY=target_day,
            ERROR=str(e),
            EXECUTION_TIME_SECONDS=round(duration, 3)
        )
        raise
    finally:
        if conn:
            try:
                conn.close()
                logger.debug(
                    subject="CONNECTION_CLOSED",
                    message="Snowflake connection closed",
                    log_key="TargetDayComplete"
                )
            except Exception as e:
                logger.warning(
                    subject="CONNECTION_CLOSE_WARNING",
                    message="Warning while closing connection",
                    log_key="TargetDayComplete",
                    ERROR=str(e)
                )



def update_single_record_pipeline_status(record: Dict, config: dict):
    """
    Update a single drive table record to:
      - Set PIPELINE_STATUS = 'PENDING'
      - Increment RETRY_ATTEMPT by 1 (or set to 1 if null)
      - Reset PIPELINE_START_TIME and PIPELINE_END_TIME to NULL

    Args:
        record: Single pipeline record (must contain 'PIPELINE_ID').
        config: Pipeline config dict.
    """

    sf_drive_config = config["sf_drive_config"]
    pipeline_id = record.get("PIPELINE_ID")

    if not pipeline_id:
        logger.warning(
            subject="RETRY_HANDLER",
            message="Missing PIPELINE_ID in record. Skipping update.",
            log_key="RetryHandler"
        )
        return

    query = f"""
        UPDATE {sf_drive_config['table']}
        SET PIPELINE_STATUS = 'PENDING',
            RETRY_ATTEMPT = COALESCE(RETRY_ATTEMPT, 0) + 1,
            PIPELINE_START_TIME = NULL,
            PIPELINE_END_TIME = NULL
        WHERE PIPELINE_ID = %s
    """

    conn = get_snowflake_connection(sf_drive_config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (pipeline_id,))
            logger.info(
                subject="RETRY_HANDLER",
                message="Updated single record to PENDING with retry incremented",
                log_key="RetryHandler",
                PIPELINE_ID=pipeline_id
            )
    except Exception as e:
        logger.error(
            subject="RETRY_HANDLER",
            message="Failed to update single record to PENDING",
            log_key="RetryHandler",
            PIPELINE_ID=pipeline_id,
            ERROR=str(e)
        )
        raise
    finally:
        conn.close()



















