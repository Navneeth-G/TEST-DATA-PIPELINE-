from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict

from data_pipeline_project.pipeline_logic.utils.pipeline_logger import PipelineLogger
from data_pipeline_project.pipeline_logic.tasks.main_pipeline import main_pipeline_per_record 
from data_pipeline_project.pipeline_logic.tasks.pre_validation import run_pre_validation_phase_for_all
from data_pipeline_project.pipeline_logic.tasks.handle_stale_records  import handle_expired_in_progress_records
from data_pipeline_project.pipeline_logic.tools_spcific.drive_table_queries import fetch_pending_drive_records


logger = PipelineLogger()


def run_main_pipeline_for_all(config: dict):
    """
    Main orchestrator for the pipeline run:
      - Step 1: Handle expired in-progress records
      - Step 2: Fetch pending records and pre-validate
      - Step 3: Run valid records in parallel, respecting pause times
    """

    # ───────────────────────────────
    # STEP 1: Handle expired in-progress records
    # ───────────────────────────────
    handle_expired_in_progress_records(config)

    # ───────────────────────────────
    # STEP 2: Fetch pending records
    # ───────────────────────────────
    limit = config.get("limit", 10)
    pending_records = fetch_pending_drive_records(config, limit=limit)

    if not pending_records:
        logger.info(subject="MAIN_PIPELINE", message="No pending records found. Exiting.", log_key="MainPipeline")
        return

    # ───────────────────────────────
    # STEP 3: Pre-validation
    # ───────────────────────────────
    records = run_pre_validation_phase_for_all(pending_records, config)

    if not records:
        logger.info(subject="MAIN_PIPELINE", message="No valid records found after pre-validation. Exiting.", log_key="MainPipeline")
        return

    # ───────────────────────────────
    # STEP 4: Run in parallel with pause staggering
    # ───────────────────────────────
    parallel_runs = config.get("number_of_parallel_runs", 2)

    logger.info(
        subject="MAIN_PIPELINE",
        message=f"Starting main pipeline for {len(records)} records with {parallel_runs} parallel runs",
        log_key="MainPipeline"
    )

    with ThreadPoolExecutor(max_workers=parallel_runs) as executor:
        future_to_record = {
            executor.submit(main_pipeline_per_record, record, config, record_index): record
            for record_index, record in enumerate(records)
        }

        for future in as_completed(future_to_record):
            record = future_to_record[future]
            try:
                result = future.result()
                logger.info(
                    subject="MAIN_PIPELINE",
                    message="Pipeline completed for record",
                    log_key="MainPipeline",
                    PIPELINE_ID=record['PIPELINE_ID'],
                    RESULT=result
                )
            except Exception as e:
                logger.error(
                    subject="MAIN_PIPELINE",
                    message="Pipeline failed for record",
                    log_key="MainPipeline",
                    PIPELINE_ID=record['PIPELINE_ID'],
                    ERROR=str(e)
                )

    logger.info(subject="MAIN_PIPELINE", message="All records processed.", log_key="MainPipeline")




