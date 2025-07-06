from data_pipeline_project.pipeline_logic.utils.pipeline_logger import PipelineLogger
from data_pipeline_project.pipeline_logic.tools_spcific.stage_count_and_delete import clean_stage_location
from data_pipeline_project.pipeline_logic.tools_spcific.drive_table_queries import change_pipeline_status_and_retry_number
import subprocess

logger = PipelineLogger()

def source_to_stage_phase(record: dict, config: dict):
    """
    Run the source to stage transfer process:
    - Clean stage before starting
    - Run the transfer
    - Update drive table on success
    - Clean stage on failure
    """

    pipeline_id = record["PIPELINE_ID"]

    try:
        # ───────────────────────────────
        # STEP 1: Clean Stage Before Starting
        # ───────────────────────────────
        clean_stage_location(record, config)

        # ───────────────────────────────
        # STEP 2: Log start
        # ───────────────────────────────
        logger.info(
            subject="SOURCE_TO_STAGE",
            message="Starting source to stage transfer",
            log_key="SourceStage",
            PIPELINE_ID=pipeline_id
        )

        # ───────────────────────────────
        # STEP 3: Run Transfer Subprocess
        # ───────────────────────────────
        run_source_to_stage_subprocess(record, config)

        # ───────────────────────────────
        # STEP 4: Mark Phase as Completed in Drive Table
        # ───────────────────────────────
        change_pipeline_status_and_retry_number(
            pipeline_id=pipeline_id,
            new_status="SOURCE_TO_STAGE",
            retry_attempt=record.get("RETRY_ATTEMPT", 0),
            config=config
        )

        logger.info(
            subject="SOURCE_TO_STAGE",
            message="Completed source to stage transfer",
            log_key="SourceStage",
            PIPELINE_ID=pipeline_id
        )

    except Exception as e:
        # ───────────────────────────────
        # STEP 5: Clean Stage on Failure
        # ───────────────────────────────
        logger.error(
            subject="SOURCE_TO_STAGE",
            message="Error during source to stage transfer, cleaning stage location",
            log_key="SourceStage",
            PIPELINE_ID=pipeline_id,
            ERROR=str(e)
        )
        clean_stage_location(record, config)  # cleanup again on error
        raise


# ───────────────────────────────
# Placeholder for Transfer Logic
# ───────────────────────────────

def run_source_to_stage_subprocess(record: dict, config: dict):
    """
    Run a subprocess that performs the source to stage data transfer.
    Example: A Python script that reads from Elasticsearch and writes to S3.
    """

    pipeline_id = record["PIPELINE_ID"]
    window_start = record["WINDOW_START_TIME"]

    command = [
        "python",
        "ingest_es_to_s3.py",
        "--pipeline_id", pipeline_id,
        "--window_start", window_start
    ]

    logger.info(
        subject="SOURCE_TO_STAGE",
        message="Running subprocess for source to stage transfer",
        log_key="SourceStage",
        PIPELINE_ID=pipeline_id,
        COMMAND=" ".join(command)
    )

    result = subprocess.run(command, check=True, capture_output=True, text=True)

    logger.info(
        subject="SOURCE_TO_STAGE",
        message="Subprocess completed",
        log_key="SourceStage",
        PIPELINE_ID=pipeline_id,
        RETURN_CODE=result.returncode,
        STDOUT=result.stdout.strip(),
        STDERR=result.stderr.strip()
    )
