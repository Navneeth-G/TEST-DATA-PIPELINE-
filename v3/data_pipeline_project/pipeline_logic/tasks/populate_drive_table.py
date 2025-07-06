import pendulum
import hashlib
import re
import pprint
from typing import Dict, List, Tuple, Set

from data_pipeline_project.pipeline_logic.tools_spcific.snowflake.drive_table_queries import (
    is_target_day_complete,
    delete_target_day_records,
    bulk_insert_records,
    fetch_incomplete_target_days,
    fetch_all_target_days
)

from data_pipeline_project.pipeline_logic.utils.pipeline_logger import PipelineLogger

logger = PipelineLogger()
pp = pprint.PrettyPrinter(compact=True)

# ────────────────────────────────────────────────────────────────────────────────
# MAIN ENTRYPOINT
# ────────────────────────────────────────────────────────────────────────────────

def main_drive_table_population(config: dict) -> bool:
    try:
        target_day_str = calculate_target_day(config)
        logger.info(subject="TargetDayCalculation", message=f"Target day calculated: {target_day_str}", calculated_day=target_day_str, type_of_target_day=type(target_day_str))
        print(f"[INFO] Target Day: {target_day_str}, Type: {type(target_day_str)}")

        orchestrate_drive_table_population_given_target_day(target_day_str, config)
        return True
    except Exception as e:
        logger.error(subject="DriveTablePopulation", message="Drive table population failed", error=str(e))
        raise

# ────────────────────────────────────────────────────────────────────────────────
# MAIN ORCHESTRATION
# ────────────────────────────────────────────────────────────────────────────────

def orchestrate_drive_table_population_given_target_day(target_day: str, config: dict) -> None:
    logger.info(subject="ContinuityCheck", message=f"Processing drive table population for target day {target_day}")
    print(f"[ContinuityCheck] Processing target day: {target_day} (Type: {type(target_day)})")

    # Step 1: Process target day
    if is_target_day_complete(target_day, config):
        logger.info(subject="ContinuityCheck", message=f"Target day {target_day} already complete.")
        print(f"[ContinuityCheck] Target day {target_day} already complete.")
    else:
        logger.info(subject="ContinuityCheck", message=f"Inserting/rebuilding target day: {target_day}")
        delete_target_day_records(target_day, config)

        day_dt = pendulum.parse(target_day, tz=config["timezone"])
        records = generate_records_for_target_day(config, day_dt)

        logger.info(subject="RecordGeneration", message=f"Generated {len(records)} records for {target_day}")
        print(f"[RecordGeneration] Generated {len(records)} records for {target_day}")

        bulk_insert_records(records, config)

    # Step 2: Rebuild incomplete days
    incomplete_days = fetch_incomplete_target_days(config)
    print(f"[ContinuityCheck] Incomplete days before discard: {incomplete_days} (Type: {type(incomplete_days)})")
    incomplete_days.discard(target_day)
    print(f"[ContinuityCheck] Incomplete days after discard: {incomplete_days}")

    for day_str in sorted(incomplete_days):
        logger.info(subject="ContinuityCheck", message=f"Rebuilding incomplete day: {day_str}")
        delete_target_day_records(day_str, config)

        day_dt = pendulum.parse(day_str, tz=config["timezone"])
        records = generate_records_for_target_day(config, day_dt)

        logger.info(subject="RecordGeneration", message=f"Generated {len(records)} records for incomplete day {day_str}")
        print(f"[RecordGeneration] Generated {len(records)} records for incomplete day {day_str}")

        bulk_insert_records(records, config)

    # Step 3: Fill missing days
    all_target_days = fetch_all_target_days(config)
    print(f"[ContinuityCheck] All target days: {all_target_days} (Type: {type(all_target_days)})")

    missing_days = find_missing_target_days(all_target_days)
    print(f"[ContinuityCheck] Missing target days: {missing_days}")

    for day_str in sorted(missing_days):
        logger.info(subject="ContinuityCheck", message=f"Filling missing day: {day_str}")

        day_dt = pendulum.parse(day_str, tz=config["timezone"])
        records = generate_records_for_target_day(config, day_dt)

        logger.info(subject="RecordGeneration", message=f"Generated {len(records)} records for missing day {day_str}")
        print(f"[RecordGeneration] Generated {len(records)} records for missing day {day_str}")

        bulk_insert_records(records, config)

    logger.info(subject="ContinuityCheck", message="Drive table continuity ensured.")

# ────────────────────────────────────────────────────────────────────────────────
# RECORD GENERATION
# ────────────────────────────────────────────────────────────────────────────────

def generate_records_for_target_day(config: Dict, target_day: pendulum.DateTime) -> List[Dict]:
    duration_seconds = parse_granularity_to_seconds(config.get('granularity'))
    print(f"[RecordGeneration] Duration seconds: {duration_seconds} (Type: {type(duration_seconds)})")

    windows = generate_consecutive_windows_for_target_day(target_day, duration_seconds)
    print(f"[RecordGeneration] Generated {len(windows)} time windows.")

    records = []
    for _, window_start, window_end, interval_str in windows:
        record = create_single_record(config, target_day, window_start, window_end, interval_str)
        records.append(record)

        # Print each record as single-line dict
        print("[RECORD GENERATED]:", pp.pformat(record))

    return records

def generate_consecutive_windows_for_target_day(target_day: pendulum.DateTime, duration_seconds: int) -> List[Tuple]:
    windows = []
    day_start = target_day.start_of('day')
    next_day_start = day_start.add(days=1)

    window_start = day_start
    while window_start < next_day_start:
        window_end = min(window_start.add(seconds=duration_seconds), next_day_start)
        interval_str = convert_seconds_to_granularity((window_end - window_start).in_seconds())
        windows.append((target_day, window_start, window_end, interval_str))
        window_start = window_end

    return windows

def create_single_record(config: Dict, target_day: pendulum.DateTime,  window_start: pendulum.DateTime, window_end: pendulum.DateTime, time_interval: str) -> Dict:

    source_category = generate_SOURCE_COMPLETE_CATEGORY(config, window_start, window_end)
    stage_category = generate_STAGE_COMPLETE_CATEGORY(config, window_start, window_end)
    target_category = generate_TARGET_COMPLETE_CATEGORY(config, window_start, window_end)

    pipeline_id = generate_PIPELINE_ID(
        config['PIPELINE_NAME'],
        source_category,
        stage_category,
        target_category,
        window_start,
        window_end
    )

    now_str = pendulum.now(config['timezone']).to_iso8601_string()

    return {
        "PIPELINE_NAME": config['PIPELINE_NAME'],
        "SOURCE_COMPLETE_CATEGORY": source_category,
        "STAGE_COMPLETE_CATEGORY": stage_category,
        "TARGET_COMPLETE_CATEGORY": target_category,
        "PIPELINE_ID": pipeline_id,
        "TARGET_DAY": target_day.to_date_string(),
        "WINDOW_START_TIME": window_start.to_iso8601_string(),
        "WINDOW_END_TIME": window_end.to_iso8601_string(),
        "TIME_INTERVAL": time_interval,
        "COMPLETED_PHASE": None,
        "COMPLETED_PHASE_DURATION": None,
        "PIPELINE_STATUS": "PENDING",
        "PIPELINE_START_TIME": None,
        "PIPELINE_END_TIME": None,
        "PIPELINE_PRIORITY": config.get('PIPELINE_PRIORITY', 1.3),
        "CONTINUITY_CHECK_PERFORMED": "YES",
        "CAN_ACCESS_HISTORICAL_DATA": config.get('CAN_ACCESS_HISTORICAL_DATA', 'YES'),
        "RECORD_FIRST_CREATED_TIME": now_str,
        "RECORD_LAST_UPDATED_TIME": now_str,
        "SOURCE_COUNT": None,
        "TARGET_COUNT": None,
        "COUNT_DIFF": None,
        "COUNT_DIFF_PERCENTAGE": None,
        "AUDIT_RESULT":None
        "RETRY_ATTEMPT":None
    }



# ────────────────────────────────────────────────────────────────────────────────
# CATEGORY GENERATION
# ────────────────────────────────────────────────────────────────────────────────

def generate_SOURCE_COMPLETE_CATEGORY(config: Dict, window_start: pendulum.DateTime, window_end: pendulum.DateTime) -> str:
    value = f"{config.get('index_group')}|{config.get('index_name')}"
    print(f"[CategoryGeneration] SOURCE_COMPLETE_CATEGORY: {value}")
    return value

def generate_TARGET_COMPLETE_CATEGORY(config: Dict, window_start: pendulum.DateTime, window_end: pendulum.DateTime) -> str:
    target_database = config.get("target_database")
    target_schema = config.get("target_schema")
    target_table = config.get("target_table")
    value = f"{target_database}.{target_schema}.{target_table}|target_path_placeholder"
    print(f"[CategoryGeneration] TARGET_COMPLETE_CATEGORY: {value}")
    return value

def generate_STAGE_COMPLETE_CATEGORY(config: Dict, window_start: pendulum.DateTime, window_end: pendulum.DateTime) -> str:
    s3_bucket = config.get("s3_bucket")
    s3_prefix = '/'.join(config.get("s3_prefix_list", []))
    date_part = window_start.format('YYYY-MM-DD')
    time_part = window_start.format('HH-mm')
    epoch = int(pendulum.now(config.get("timezone")).timestamp())

    value = f"{s3_bucket}|s3://{s3_bucket}/{s3_prefix}/{date_part}/{time_part}/{config.get('index_id')}_****.json"
    print(f"[CategoryGeneration] STAGE_COMPLETE_CATEGORY: {value}")
    return value

def generate_PIPELINE_ID(pipeline_name: str, source_cat: str, stage_cat: str,
                         target_cat: str, window_start: pendulum.DateTime,
                         window_end: pendulum.DateTime) -> str:
    base_str = "|".join([
        pipeline_name,
        source_cat,
        stage_cat,
        target_cat,
        window_start.to_iso8601_string(),
        window_end.to_iso8601_string()
    ])
    hash_value = hashlib.sha256(base_str.encode()).hexdigest()
    print(f"[CategoryGeneration] PIPELINE_ID generated: {hash_value}")
    return hash_value

# ────────────────────────────────────────────────────────────────────────────────
# UTILS
# ────────────────────────────────────────────────────────────────────────────────

def calculate_target_day(config: dict) -> str:
    secs = parse_granularity_to_seconds(config.get('x_time_back'))
    target_time = pendulum.now(config["timezone"]).subtract(seconds=secs)
    value = target_time.to_date_string()
    print(f"[TargetDayCalculation] Calculated Target Day: {value}")
    return value

def find_missing_target_days(existing_days: Set[str]) -> Set[str]:
    if not existing_days:
        return set()
    dates = [pendulum.parse(d) for d in existing_days]
    min_day = min(dates).start_of('day')
    max_day = max(dates).start_of('day')
    full_range = {min_day.add(days=i).to_date_string() for i in range((max_day - min_day).days + 1)}
    missing = full_range - existing_days
    print(f"[ContinuityCheck] Found missing target days: {missing}")
    return missing

def parse_granularity_to_seconds(granularity: str) -> int:
    units = {'d': 86400, 'h': 3600, 'm': 60, 's': 1}
    matches = re.findall(r'(\d+)([dhms])', granularity.lower())
    total_seconds = sum(int(val) * units[unit] for val, unit in matches)
    print(f"[GranularityParsing] Parsed granularity '{granularity}' to {total_seconds} seconds.")
    return total_seconds

def convert_seconds_to_granularity(seconds: int) -> str:
    d, r = divmod(seconds, 86400)
    h, r = divmod(r, 3600)
    m, s = divmod(r, 60)
    parts = [f"{v}{u}" for v, u in zip([d, h, m, s], 'dhms') if v]
    result = ''.join(parts) if parts else '0s'
    print(f"[GranularityParsing] Converted {seconds} seconds to granularity string '{result}'")
    return result

