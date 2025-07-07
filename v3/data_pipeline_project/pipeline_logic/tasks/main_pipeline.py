import time
from typing import Dict

from data_pipeline_project.pipeline_logic.utils.pipeline_logger import PipelineLogger
from data_pipeline_project.pipeline_logic.tools_spcific.drive_table_queries import is_phase_complete
# from data_pipeline_project.pipeline_logic.tasks.pre_validation import run_pre_validation_phase_per_record
from data_pipeline_project.pipeline_logic.tools_spcific.source_to_stage_transfer import source_to_stage_phase
from data_pipeline_project.pipeline_logic.tools_spcific.stage_to_target_transfer import stage_to_target_phase
from data_pipeline_project.pipeline_logic.tools_spcific.audit_phase import audit_phase

logger = PipelineLogger()



def main_pipeline_per_record(record: Dict, config: dict, record_index: int) -> str:
    pipeline_id = record["PIPELINE_ID"]

    # ───────────────────────────────
    # STEP 0: Pause (Staggered Start per Thread Slot)
    # ───────────────────────────────
    pause_base = config.get("parallel_pause_time", 20)
    parallel_runs = config.get("number_of_parallel_runs", 2)

    # Calculate thread slot-based pause
    thread_slot = record_index % parallel_runs
    pause_seconds = thread_slot * pause_base

    logger.info(
        subject="MAIN_PIPELINE",
        message=f"Pausing {pause_seconds} sec for thread slot {thread_slot} before starting ingestion",
        log_key="MainPipeline",
        PIPELINE_ID=pipeline_id,
        RECORD_INDEX=record_index,
        PARALLEL_RUNS=parallel_runs
    )

    if pause_seconds > 0:
        time.sleep(pause_seconds)

    try:
        # ───────────────────────────────
        # STEP 2: Source to Stage Phase
        # ───────────────────────────────
        if not is_phase_complete(record, config, expected_phase="SOURCE_TO_STAGE"):
            logger.info(
                subject="MAIN_PIPELINE",
                message="Source to stage not complete, running now",
                log_key="MainPipeline",
                PIPELINE_ID=pipeline_id
            )
            source_to_stage_phase(record, config)

        # ───────────────────────────────
        # STEP 3: Stage to Target Phase
        # ───────────────────────────────
        if not is_phase_complete(record, config, expected_phase="STAGE_TO_TARGET"):
            logger.info(
                subject="MAIN_PIPELINE",
                message="Stage to target not complete, running now",
                log_key="MainPipeline",
                PIPELINE_ID=pipeline_id
            )
            stage_to_target_phase(record, config)

        # ───────────────────────────────
        # STEP 4: Audit Phase
        # ───────────────────────────────
        audit_phase(record, config)

        # ───────────────────────────────
        # STEP 5: Log Success
        # ───────────────────────────────
        logger.info(
            subject="MAIN_PIPELINE",
            message="Pipeline completed successfully for record",
            log_key="MainPipeline",
            PIPELINE_ID=pipeline_id
        )

        return "SUCCESS"

    except Exception as e:
        # ───────────────────────────────
        # STEP 6: Log Failure
        # ───────────────────────────────
        logger.error(
            subject="MAIN_PIPELINE",
            message="Pipeline failed for record",
            log_key="MainPipeline",
            PIPELINE_ID=pipeline_id,
            ERROR=str(e)
        )
        raise
