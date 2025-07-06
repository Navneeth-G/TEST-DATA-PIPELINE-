from data_pipeline_project.pipeline_logic.utils.pipeline_logger import PipelineLogger
from data_pipeline_project.pipeline_logic.tools_spcific.stage_count_and_delete import build_stage_prefix
from data_pipeline_project.pipeline_logic.tools_spcific.drive_table_queries import get_snowflake_connection

logger = PipelineLogger()


def clean_target_location(record: dict, config: dict):
    """
    Clean the target table before starting a new load.
    Deletes rows where FILE_NAME matches the stage prefix.

    Args:
        record (dict): Pipeline record.
        config (dict): Pipeline config.
    """

    pipeline_id = record["PIPELINE_ID"]
    sf_config = config.get("sf_target_config")

    # Build the file prefix (reuse your stage prefix logic)
    s3_prefix_list = config.get("s3_prefix_list", [])
    file_prefix = build_stage_prefix(record, s3_prefix_list)


    # Build the DELETE query
    query = f"""
        DELETE FROM {sf_config['database']}.{sf_config['schema']}.{sf_config['table']}
        WHERE FILE_NAME LIKE %s
    """

    params = [f"{file_prefix}%"]


    logger.info(
        subject="TARGET_CLEANUP",
        message="Deleting target rows matching file prefix",
        log_key="TargetCleanup",
        PIPELINE_ID=pipeline_id,
        TARGET_DAY=record["TARGET_DAY"],
        WINDOW_START_TIME=record["WINDOW_START_TIME"],
        FILE_PREFIX=file_prefix
    )

    # Execute the query
    conn = get_snowflake_connection(sf_config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            query_id = cursor.sfqid

            logger.info(
                subject="TARGET_CLEANUP",
                message="Target cleanup completed",
                log_key="TargetCleanup",
                PIPELINE_ID=pipeline_id,
                ROWS_DELETED=cursor.rowcount,
                QUERY_ID=query_id
            )

    finally:
        conn.close()




