import time
from data_pipeline_project.pipeline_logic.utils.pipeline_logger import PipelineLogger
from data_pipeline_project.pipeline_logic.tools_spcific.stage_count_and_delete import count_target_records

logger = PipelineLogger()


def wait_for_target_count(record: dict, source_count: int, config: dict) -> int:
    """
    Keep checking the target count until it matches the source count, or stops changing,
    or a max wait time is reached.

    Args:
        record (dict): Pipeline record.
        source_count (int): Expected count from the source.
        config (dict): Pipeline config containing max wait settings.

    Returns:
        int: Final target count when exiting the loop.
    """

    pipeline_id = record["PIPELINE_ID"]
    sleep_interval = config.get("ingestion_check_interval", 120)  # in seconds, default: 2 minutes
    max_wait_time = config.get("ingestion_max_time", 900)         # in seconds, default: 15 minutes
    elapsed_time = 0

    # Initial count
    last_target_count = count_target_records(record, config)
    logger.info(
        subject="TARGET_COUNT",
        message="Initial target count fetched",
        log_key="TargetCount",
        PIPELINE_ID=pipeline_id,
        TARGET_COUNT=last_target_count,
        SOURCE_COUNT=source_count,
        ELAPSED_TIME=elapsed_time
    )

    # Keep checking until counts match, or count stops changing, or timeout reached
    while elapsed_time < max_wait_time:
        # If target == source, done
        if last_target_count == source_count:
            logger.info(
                subject="TARGET_COUNT",
                message="Target count matches source count, ingestion complete",
                log_key="TargetCount",
                PIPELINE_ID=pipeline_id,
                FINAL_TARGET_COUNT=last_target_count
            )
            return last_target_count

        # Sleep before rechecking
        time.sleep(sleep_interval)
        elapsed_time += sleep_interval

        # Get new count
        current_target_count = count_target_records(record, config)

        logger.info(
            subject="TARGET_COUNT",
            message="Checked target count again",
            log_key="TargetCount",
            PIPELINE_ID=pipeline_id,
            TARGET_COUNT=current_target_count,
            SOURCE_COUNT=source_count,
            ELAPSED_TIME=elapsed_time
        )

        # If no change since last check → break early
        if current_target_count == last_target_count:
            logger.info(
                subject="TARGET_COUNT",
                message="Target count has not changed, stopping further checks",
                log_key="TargetCount",
                PIPELINE_ID=pipeline_id,
                FINAL_TARGET_COUNT=current_target_count,
                ELAPSED_TIME=elapsed_time
            )
            return current_target_count

        # If changed → update and loop again
        last_target_count = current_target_count

    # Max wait time reached → exit with last known count
    logger.info(
        subject="TARGET_COUNT",
        message="Max wait time reached, exiting count check",
        log_key="TargetCount",
        PIPELINE_ID=pipeline_id,
        FINAL_TARGET_COUNT=last_target_count,
        ELAPSED_TIME=elapsed_time
    )
    return last_target_count


from data_pipeline_project.pipeline_logic.utils.pipeline_logger import PipelineLogger
from data_pipeline_project.pipeline_logic.tools_spcific.stage_count_and_delete import build_stage_prefix
from data_pipeline_project.pipeline_logic.tools_spcific.drive_table_queries import get_snowflake_connection

logger = PipelineLogger()


def count_target_records(record: dict, config: dict) -> int:
    """
    Count the number of rows in the target table matching the file prefix.

    Args:
        record (dict): Pipeline record.
        config (dict): Pipeline config.

    Returns:
        int: Number of matching rows in the target table.
    """

    pipeline_id = record["PIPELINE_ID"]
    sf_config = config.get("sf_target_config")

    # Build the file prefix (reusing stage prefix logic)
    s3_prefix_list = config.get("s3_prefix_list", [])
    file_prefix = build_stage_prefix(record, s3_prefix_list)

    # Build the COUNT query
    query = f"""
        SELECT COUNT(*) FROM {sf_config['database']}.{sf_config['schema']}.{sf_config['table']}
        WHERE FILE_NAME LIKE %s
    """

    params = [f"{file_prefix}%"]

    conn = get_snowflake_connection(sf_config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid

            count = cursor.fetchone()[0]

            logger.info(
                subject="TARGET_COUNT",
                message="Fetched target row count",
                log_key="TargetCount",
                PIPELINE_ID=pipeline_id,
                FILE_PREFIX=file_prefix,
                ROW_COUNT=count,
                QUERY_ID=query_id
            )

            return count

    finally:
        conn.close()


