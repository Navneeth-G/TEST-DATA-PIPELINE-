import time
from typing import List, Dict

from data_pipeline_project.pipeline_logic.utils.pipeline_logger import PipelineLogger
from data_pipeline_project.pipeline_logic.tools_spcific.source_count import source_count_func
from data_pipeline_project.pipeline_logic.tools_spcific.target_count import target_count_func
from data_pipeline_project.pipeline_logic.tools_spcific.drive_table_queries import (
    update_pre_validation_results,
    mark_pre_validation_success
)


logger = PipelineLogger()







def run_pre_validation_phase_for_all(    records: List[Dict],    config: dict ) -> List[Dict]:
    """
    Run pre-validation phase for all input records. 
    Return only those records that need ELT processing.

    Args:
        records (List[Dict]): List of pipeline records.
        config (dict): Pipeline config.

    Returns:
        List[Dict]: List of records that passed pre-validation and need ELT.
    """

    valid_records = []

    for record in records:
        # For each record, decide whether it needs ELT
        needs_elt = pre_validation_phase_per_record(
            record=record,
            config=config
        )

        # If yes, add it to the output list
        if needs_elt:
            valid_records.append(record)

    logger.info(
        subject="PRE_VALIDATION_BATCH",
        message=f"Pre-validation complete. {len(valid_records)} records need ELT.",
        log_key="PreValidation"
    )

    return valid_records






def pre_validation_phase_per_record(record: dict, config: dict) -> bool:
    """
    Run pre-validation on a single record. Update the drive table with counts and phase info.

    Returns:
        bool: True if this record needs ELT processing, False if it can be skipped.
    """

    pipeline_id = record['PIPELINE_ID']
    start_time = time.time()

    # Run count queries
    source_count = source_count_func(record, config)
    target_count = target_count_func(record, config)

    # Calculate difference
    count_diff = source_count - target_count
    diff_pct = (count_diff / source_count * 100.0) if source_count > 0 else None

    # If already equal, we skip ELT and mark as SUCCESS
    needs_elt = source_count != target_count

    if not needs_elt:
        # Counts matched → mark entire pipeline as success
        mark_pre_validation_success(
            pipeline_id=pipeline_id,
            source_count=source_count,
            target_count=target_count,
            count_diff=count_diff,
            count_diff_percentage=diff_pct,
            config=config
        )
    else:
        # Counts mismatch → mark only pre-validation complete
        duration_secs = int(time.time() - start_time)

        update_pre_validation_results(
            pipeline_id=pipeline_id,
            source_count=source_count,
            target_count=target_count,
            count_diff=count_diff,
            count_diff_percentage=diff_pct,
            completed_phase="PRE VALIDATION",
            phase_duration_seconds=duration_secs,
            config=config
        )

    # Log the pre-validation result
    logger.info(
        subject="PRE_VALIDATION",
        message="Pre-validation completed for record",
        log_key="PreValidation",
        PIPELINE_ID=pipeline_id,
        NEEDS_ELT=needs_elt,
        SOURCE_COUNT=source_count,
        TARGET_COUNT=target_count,
        COUNT_DIFF=count_diff,
        ACTION="PASS TO MAIN INGESTION" if needs_elt else "SKIP - ALREADY PROCESSED"
    )

    return needs_elt



