from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict

from data_pipeline_project.pipeline_logic.utils.pipeline_logger import PipelineLogger
from data_pipeline_project.pipeline_logic.tasks.main_pipeline import main_pipeline_per_record  # adjust import based on file location

logger = PipelineLogger()


def run_main_pipeline_for_all(records: List[Dict], config: dict):
    """
    Run main pipeline for all valid records in parallel,
    respecting the number of parallel runs and per-record pause time.

    Args:
        records (List[Dict]): List of pipeline records.
        config (dict): Pipeline config containing 'number_of_parallel_runs' and 'parallel_pause_time'.
    """
    parallel_runs = config.get("number_of_parallel_runs", 2)

    logger.info(
        subject="MAIN_PIPELINE",
        message=f"Starting main pipeline for {len(records)} records with {parallel_runs} parallel runs",
        log_key="MainPipeline"
    )

    with ThreadPoolExecutor(max_workers=parallel_runs) as executor:
        # Submit each record for parallel processing, pass its index to control pause time
        future_to_record = {
            executor.submit(
                main_pipeline_per_record,
                record,
                config,
                record_index
            ): record
            for record_index, record in enumerate(records)
        }

        # Wait for all tasks to complete
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

    logger.info(
        subject="MAIN_PIPELINE",
        message="All records processed.",
        log_key="MainPipeline"
    )

