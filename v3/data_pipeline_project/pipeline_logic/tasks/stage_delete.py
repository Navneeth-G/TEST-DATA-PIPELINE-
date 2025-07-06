from data_pipeline_project.pipeline_logic.utils.pipeline_logger import PipelineLogger
import boto3
from botocore.exceptions import ClientError

logger = PipelineLogger()


def clean_stage_location(record: dict, config: dict):
    """
    Clean the stage location for this pipeline record.
    This deletes the relevant folder/prefix in the S3 bucket.

    Args:
        record (dict): Pipeline record containing stage details.
        config (dict): Pipeline config containing S3 connection info.
    """

    pipeline_id = record["PIPELINE_ID"]
    s3_bucket = config.get("s3_bucket")
    s3_prefix_list = config.get("s3_prefix_list", [])

    # Build stage prefix from record and config
    stage_prefix = build_stage_prefix(record, s3_prefix_list)

    logger.info(
        subject="STAGE_CLEANUP",
        message="Starting stage cleanup",
        log_key="StageCleanup",
        PIPELINE_ID=pipeline_id,
        S3_BUCKET=s3_bucket,
        STAGE_PREFIX=stage_prefix
    )

    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=config.get("aws_access_key_id"),
            aws_secret_access_key=config.get("aws_secret_access_key"),
            region_name=config.get("aws_region_name", "us-west-2")
        )

        # List and delete all objects under the stage prefix
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=s3_bucket, Prefix=stage_prefix)

        objects_to_delete = []
        for page in pages:
            for obj in page.get("Contents", []):
                objects_to_delete.append({'Key': obj['Key']})

        if objects_to_delete:
            s3.delete_objects(
                Bucket=s3_bucket,
                Delete={'Objects': objects_to_delete}
            )

            logger.info(
                subject="STAGE_CLEANUP",
                message=f"Deleted {len(objects_to_delete)} objects from stage",
                log_key="StageCleanup",
                PIPELINE_ID=pipeline_id
            )
        else:
            logger.info(
                subject="STAGE_CLEANUP",
                message="No objects found to delete in stage",
                log_key="StageCleanup",
                PIPELINE_ID=pipeline_id
            )

    except ClientError as e:
        logger.error(
            subject="STAGE_CLEANUP",
            message="Failed to clean stage location",
            log_key="StageCleanup",
            PIPELINE_ID=pipeline_id,
            ERROR=str(e)
        )
        raise


# ──────────────────────────────────────────
# Helper to Build Stage Prefix Path
# ──────────────────────────────────────────

def build_stage_prefix(record: dict, s3_prefix_list: list) -> str:
    """
    Build the stage prefix path for S3 cleanup based on the record.

    Example Output:
    warehouse_related_folder/C1/x1/2025-07-06/14-00/

    Args:
        record (dict): Pipeline record.
        s3_prefix_list (list): List of S3 folder levels from config.

    Returns:
        str: Full S3 prefix.
    """

    date_part = record["WINDOW_START_TIME"][0:10]   # 'YYYY-MM-DD'
    time_part = record["WINDOW_START_TIME"][11:16].replace(':', '-')  # 'HH-mm'

    # Join s3_prefix_list and build the full path
    base_prefix = "/".join(s3_prefix_list)  # e.g., warehouse_related_folder/C1/x1
    full_prefix = f"{base_prefix}/{date_part}/{time_part}/"

    return full_prefix



