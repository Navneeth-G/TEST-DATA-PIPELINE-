from typing import Set, List, Dict
import time
import pendulum
from data_pipeline_project.pipeline_logic.utils.pipeline_logger import PipelineLogger

logger = PipelineLogger()


def handle_expired_in_progress_records(config: dict):
    """
    Fetch all in-progress records and reset the ones that exceeded acceptable process duration.
    Each expired record is updated one at a time.
    """

    # Step 1: Fetch all in-progress records (no time filter applied)
    all_in_progress_records = fetch_all_in_progress_records(config)

    if not all_in_progress_records:
        logger.info(
            subject="RETRY_HANDLER",
            message="No in-progress records found. Nothing to check.",
            log_key="RetryHandler"
        )
        return

    # Step 2: Process each record individually
    timezone = config.get("timezone", "UTC")
    now = pendulum.now(timezone)
    acceptable_secs = parse_granularity_to_seconds(config.get("acceptable_process_duration", "1h"))

    expired_count = 0

    for record in all_in_progress_records:
        pipeline_id = record.get("PIPELINE_ID")
        start_time_str = record.get("PIPELINE_START_TIME")

        if not start_time_str:
            logger.warning(
                subject="RETRY_HANDLER",
                message="Skipping record without PIPELINE_START_TIME",
                log_key="RetryHandler",
                PIPELINE_ID=pipeline_id
            )
            continue

        try:
            start_time = pendulum.parse(start_time_str)
            elapsed_secs = (now - start_time).in_seconds()

            if elapsed_secs > acceptable_secs:
                # This record has expired → update it
                logger.info(
                    subject="RETRY_HANDLER",
                    message="Record expired. Updating to PENDING.",
                    log_key="RetryHandler",
                    PIPELINE_ID=pipeline_id,
                    ELAPSED_SECONDS=elapsed_secs,
                    ALLOWED_SECONDS=acceptable_secs
                )

                update_single_record_pipeline_status(record, config)
                expired_count += 1

            else:
                logger.info(
                    subject="RETRY_HANDLER",
                    message="Record still within allowed process duration. Skipping.",
                    log_key="RetryHandler",
                    PIPELINE_ID=pipeline_id,
                    ELAPSED_SECONDS=elapsed_secs,
                    ALLOWED_SECONDS=acceptable_secs
                )

        except Exception as e:
            logger.error(
                subject="RETRY_HANDLER",
                message="Error processing record for expiration check",
                log_key="RetryHandler",
                PIPELINE_ID=pipeline_id,
                ERROR=str(e)
            )

    # Final log summary
    logger.info(
        subject="RETRY_HANDLER",
        message=f"Expired record handling complete. {expired_count} records updated to PENDING.",
        log_key="RetryHandler"
    )



