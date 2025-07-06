from data_pipeline_project.pipeline_logic.utils.pipeline_logger import PipelineLogger
from data_pipeline_project.pipeline_logic.tools_spcific.stage_count_and_delete import clean_stage_location
from data_pipeline_project.pipeline_logic.tools_spcific.drive_table_queries import change_pipeline_status_and_retry_number

import time
import snowflake.connector
from snowflake.connector import ProgrammingError

logger = PipelineLogger()

def stage_to_target_phase(record: dict, config: dict):
    pipeline_id = record["PIPELINE_ID"]

    try:
        # _____________________________
        # STEP 1: Clean Target Before Starting
        # _____________________________
        clean_target_location(record, config)

        # _____________________________
        # STEP 2: Log start
        # _____________________________
        logger.info(
            subject="STAGE_TO_TARGET",
            message="Starting stage to target load",
            log_key="StageTarget",
            PIPELINE_ID=pipeline_id
        )

        # _____________________________
        # STEP 3: Run Transfer (Pipe Refresh)
        # _____________________________
        run_stage_to_target_transfer(record, config)

        # _____________________________
        # STEP 4: Mark Phase as Completed
        # _____________________________
        change_pipeline_status_and_retry_number(
            pipeline_id=pipeline_id,
            new_status="STAGE_TO_TARGET",
            retry_attempt=record.get("RETRY_ATTEMPT", 0),
            config=config
        )

        logger.info(
            subject="STAGE_TO_TARGET",
            message="Completed stage to target load",
            log_key="StageTarget",
            PIPELINE_ID=pipeline_id
        )

    except Exception as e:
        # _____________________________
        # STEP 5: Clean target on failure
        # _____________________________
        logger.error(
            subject="STAGE_TO_TARGET",
            message="Error during stage to target load, cleaning target location",
            log_key="StageTarget",
            PIPELINE_ID=pipeline_id,
            ERROR=str(e)
        )
        clean_target_location(record, config)
        raise



# _____________________________
# Snowflake Pipe Refresh Logic
# _____________________________

def run_stage_to_target_transfer(record: dict, config: dict):
    """
    Trigger Snowflake pipe refresh and wait for it to complete.

    Args:
        record (dict): Pipeline record.
        config (dict): Pipeline config.
    """

    pipeline_id = record["PIPELINE_ID"]
    sf_config = config.get("sf_target_config")

    pipe_name = build_pipe_name(record, config)

    logger.info(
        subject="STAGE_TO_TARGET",
        message=f"Triggering Snowflake pipe refresh: {pipe_name}",
        log_key="StageTarget",
        PIPELINE_ID=pipeline_id
    )

    # ---- Setup Snowflake connection ----
    conn = snowflake.connector.connect(
        user=sf_config["user"],
        password=sf_config["password"],
        account=sf_config["account"],
        warehouse=sf_config["warehouse"],
        database=sf_config["database"],
        schema=sf_config["schema"],
        role=sf_config.get("role")
    )

    cur = conn.cursor()

    try:
        # ---- Submit async pipe refresh ----
        query = f"EXECUTE TASK {task_name} ;"
        cur.execute_async(query)
        query_id = cur.sfqid

        logger.info(
            subject="STAGE_TO_TARGET",
            message="Submitted Snowflake pipe refresh",
            log_key="StageTarget",
            PIPELINE_ID=pipeline_id,
            QUERY_ID=query_id,
            PIPE_NAME=pipe_name
        )

        # ---- Poll until query is done ----
        try:
            while conn.is_still_running(conn.get_query_status_throw_if_error(query_id)):
                logger.info(
                    subject="STAGE_TO_TARGET",
                    message="Waiting for pipe refresh to complete...",
                    log_key="StageTarget",
                    PIPELINE_ID=pipeline_id,
                    QUERY_ID=query_id
                )
                time.sleep(2)

            logger.info(
                subject="STAGE_TO_TARGET",
                message="Pipe refresh completed",
                log_key="StageTarget",
                PIPELINE_ID=pipeline_id,
                QUERY_ID=query_id
            )

        except ProgrammingError as err:
            logger.error(
                subject="STAGE_TO_TARGET",
                message="Snowflake pipe refresh failed",
                log_key="StageTarget",
                PIPELINE_ID=pipeline_id,
                QUERY_ID=query_id,
                ERROR=str(err)
            )
            raise

        # ---- Optional: Get query results ----
        result_cur = conn.cursor()
        result_cur.get_results_from_sfqid(query_id)
        results = result_cur.fetchall()

        logger.info(
            subject="STAGE_TO_TARGET",
            message="Pipe refresh query results",
            log_key="StageTarget",
            PIPELINE_ID=pipeline_id,
            RESULTS=results
        )

    finally:
        cur.close()
        conn.close()


# _____________________________
# Helper to Build Task Name
# _____________________________

def build_pipe_name(record: dict, config: dict) -> str:
    """
    Build the fully qualified pipe name.

    Example: {database}.{schema}.{task_name}
    """

    sf_config = config.get("sf_target_config")
    task = config.get("target_task", "unknown")

    return f"{sf_config['database']}.{sf_config['schema']}.{task}"
