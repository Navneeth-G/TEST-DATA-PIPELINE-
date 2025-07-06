from data_pipeline_project.pipeline_logic.utils.pipeline_logger import PipelineLogger
from data_pipeline_project.pipeline_logic.tools_spcific.source_count import count_source_records
from data_pipeline_project.pipeline_logic.tools_spcific.target_count import wait_for_target_count
from data_pipeline_project.pipeline_logic.tools_spcific.stage_count_and_delete import clean_stage_location
from data_pipeline_project.pipeline_logic.tools_spcific.target_count_and_delete import clean_target_location
from data_pipeline_project.pipeline_logic.tools_spcific.drive_table_queries import (
    update_audit_results,
    reset_pipeline_record_on_count_mismatch
)

logger = PipelineLogger()


def audit_phase(record: dict, config: dict):
    """
    Run the final audit:
    - Get source count
    - Wait for the target count to stabilize
    - If counts match → mark as SUCCESS
    - If counts mismatch → clean stage & target, reset, and increment retry.
    """

    pipeline_id = record["PIPELINE_ID"]

    # -------------------------------------
    # STEP 1: Get Source Count
    # -------------------------------------
    source_count = count_source_records(record, config)

    logger.info(
        subject="AUDIT_PHASE",
        message="Fetched source count, waiting for target ingestion",
        log_key="Audit",
        PIPELINE_ID=pipeline_id,
        SOURCE_COUNT=source_count
    )

    # -------------------------------------
    # STEP 2: Wait for Target Count to Settle
    # -------------------------------------
    final_target_count = wait_for_target_count(record, source_count, config)

    # -------------------------------------
    # STEP 3: Calculate Differences
    # -------------------------------------
    count_diff = source_count - final_target_count
    diff_percentage = (count_diff / source_count * 100.0) if source_count else None

    logger.info(
        subject="AUDIT_PHASE",
        message="Final audit counts fetched",
        log_key="Audit",
        PIPELINE_ID=pipeline_id,
        SOURCE_COUNT=source_count,
        TARGET_COUNT=final_target_count,
        COUNT_DIFF=count_diff,
        COUNT_DIFF_PERCENTAGE=round(diff_percentage, 2) if diff_percentage is not None else None
    )

    # -------------------------------------
    # STEP 4: If Counts Match → Mark Success
    # -------------------------------------
    if source_count == final_target_count:
        logger.info(
            subject="AUDIT_PHASE",
            message="Source and target counts match, marking as SUCCESS",
            log_key="Audit",
            PIPELINE_ID=pipeline_id
        )

        update_audit_results(
            pipeline_id=pipeline_id,
            source_count=source_count,
            target_count=final_target_count,
            count_diff=count_diff,
            count_diff_percentage=diff_percentage,
            audit_result="SUCCESS",
            config=config
        )
        return

    # -------------------------------------
    # STEP 5: If Counts Mismatch → Clean & Reset
    # -------------------------------------
    logger.error(
        subject="AUDIT_PHASE",
        message="Count mismatch detected after waiting, resetting pipeline state",
        log_key="Audit",
        PIPELINE_ID=pipeline_id
    )

    # Clean stage and target
    clean_stage_location(record, config)
    clean_target_location(record, config)

    # Reset drive table and increment retry attempt
    reset_pipeline_record_on_count_mismatch(
        pipeline_id=pipeline_id,
        retry_attempt=record.get("RETRY_ATTEMPT", 0) + 1,
        config=config
    )



