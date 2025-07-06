import re
import hashlib
import pendulum
import logging
from typing import Dict, List, Tuple, Union, Set
from tabulate import tabulate

from pipeline_logic.tools_spcific.snowflake.drive_table_queries import (
    is_target_day_complete,
    delete_target_day_records,
    bulk_insert_records,
    fetch_incomplete_target_days,
    fetch_all_target_days
)


def main_drive_table_population(target_day, config):
    """
    Main entrypoint for drive table population with error propagation.
    Args:
        target_day: pendulum.DateTime target day.
        config: Pipeline config dict.
    """
    try:
        orchestrate_drive_table_population_given_target_day(target_day, config)
        return True
    except Exception:
        raise  



def orchestrate_drive_table_population_given_target_day(target_day: str, config: dict) -> None:
    """
    Main orchestration function to ensure Drive table data continuity.

    Args:
        target_day: The target day for this DAG run, as a string (YYYY-MM-DD).
        config: Dictionary with configuration details.
    """
    # ---------------- STEP 1: Process DAG target day ----------------
    if is_target_day_complete(target_day):
        logger.info(f"Target day {target_day} already complete. Skipping insert.")
    else:
        logger.info(f"Inserting or rebuilding target day: {target_day}")
        delete_target_day_records(target_day)
        fresh_records = generate_records_for_target_day(config, target_day)
        bulk_insert_records(fresh_records)

    # ---------------- STEP 2: Fix previously incomplete days ----------------
    incomplete_days = fetch_incomplete_target_days()
    incomplete_days.discard(target_day)  # Already handled in step 1
    for incomplete_day in sorted(incomplete_days):
        logger.info(f"Rebuilding incomplete day: {incomplete_day}")
        delete_target_day_records(incomplete_day)
        fresh_records = generate_records_for_target_day(config, incomplete_day)
        bulk_insert_records(fresh_records)

    # ---------------- STEP 3: Fill continuity gaps ----------------
    all_target_days = fetch_all_target_days()
    missing_days = find_missing_target_days(all_target_days)
    for missing_day in sorted(missing_days):
        logger.info(f"Filling missing day: {missing_day}")
        fresh_records = generate_records_for_target_day(config, missing_day)
        bulk_insert_records(fresh_records)

    logger.info("Drive table continuity ensured.")




def get_start_of_day_relative_to_now(timezone: str, ago_seconds: int) -> pendulum.DateTime:
    """
    Return the start of the day for the timestamp obtained by subtracting ago_seconds from current time.

    Args:
        timezone (str): Timezone (e.g., 'UTC', 'Asia/Kolkata').
        ago_seconds (int): Seconds to subtract from now.

    Returns:
        pendulum.DateTime: Start of the day in the given timezone.
    """
    current_time = pendulum.now(timezone)
    adjusted_time = current_time.subtract(seconds=ago_seconds)
    return adjusted_time.start_of('day')


def calculate_window_end_time_within_day(WINDOW_START_TIME: pendulum.DateTime, excess_seconds: int) -> pendulum.DateTime:
    """
    Calculate the end timestamp for a window starting at WINDOW_START_TIME.
    Caps the end time to the start of the next day.

    Args:
        WINDOW_START_TIME (pendulum.DateTime): Start of the window.
        excess_seconds (int): Duration in seconds to add to the start time.

    Returns:
        pendulum.DateTime: Computed end timestamp, capped at next day's start.
    """
    # Start of the next day in the same timezone
    next_day_start = WINDOW_START_TIME.start_of('day').add(days=1)

    # Add excess seconds to window start time
    proposed_end_time = WINDOW_START_TIME.add(seconds=excess_seconds)

    # Return the smaller of the two times
    return min(proposed_end_time, next_day_start)


def generate_consecutive_windows_for_target_day(     target_day: pendulum.DateTime,    duration_seconds: int ) -> List[Tuple[pendulum.DateTime, pendulum.DateTime, pendulum.DateTime, str]]:
    """
    Generate a list of consecutive time windows for the given target day.

    Returns a tuple of:
      - target_day (pendulum.DateTime)
      - window_start_time (pendulum.DateTime)
      - window_end_time (pendulum.DateTime)
      - time_interval (str in granularity format)

    Args:
        target_day: Any datetime on the target day.
        duration_seconds: Window size in seconds.

    Returns:
        List of tuples.
    """
    windows = []
    day_start = target_day.start_of('day')
    next_day_start = day_start.add(days=1)

    window_start = day_start

    while window_start < next_day_start:
        # Calculate end time (capped at next day start)
        window_end = calculate_window_end_time_within_day(window_start, duration_seconds)

        # Compute the window duration
        duration = (window_end - window_start).in_seconds()

        # Format the duration as a string
        time_interval = convert_seconds_to_granularity(duration)

        # Add the full tuple
        windows.append((target_day, window_start, window_end, time_interval))

        # Move to the next window
        window_start = window_end

    return windows



def parse_granularity_to_seconds(granularity: str) -> int:
    """
    Parse granularity string to seconds - supports complex formats with comprehensive error handling
    
    Args:
        granularity: String like "1h", "30m", "1d2h30m40s", "2h30m"
        
    Returns:
        Total seconds as integer
        
    Raises:
        ValueError: If granularity format is invalid
        
    Examples:
        "1h" -> 3600
        "30m" -> 1800  
        "1d2h30m40s" -> 95440
        "2h30m" -> 9000
    """
    
    try:
        logger.info(f"Parsing granularity: '{granularity}'")
        
        # Validate input
        if not granularity:
            raise ValueError("Granularity cannot be None or empty")
        
        if not isinstance(granularity, str):
            raise ValueError(f"Granularity must be a string, got {type(granularity)}")
        
        # Remove spaces and convert to lowercase
        granularity = granularity.strip().lower()
        
        if not granularity:
            raise ValueError("Granularity cannot be empty after stripping whitespace")
        
        logger.debug(f"Cleaned granularity: '{granularity}'")
        
        # Regex pattern to find all number+unit combinations
        pattern = r'(\d+)([dhms])'
        matches = re.findall(pattern, granularity)
        
        if not matches:
            raise ValueError(f"Invalid granularity format: '{granularity}'. Expected format: '1h', '30m', '1d2h30m40s', etc.")
        
        logger.debug(f"Regex matches: {matches}")
        
        # Check for duplicate units
        units_found = [match[1] for match in matches]
        if len(units_found) != len(set(units_found)):
            duplicate_units = [unit for unit in set(units_found) if units_found.count(unit) > 1]
            raise ValueError(f"Duplicate units found in granularity '{granularity}': {duplicate_units}")
        
        # Conversion multipliers
        multipliers = {
            'd': 86400,    # days to seconds
            'h': 3600,     # hours to seconds  
            'm': 60,       # minutes to seconds
            's': 1         # seconds to seconds
        }
        
        total_seconds = 0
        
        # Sum all components
        for value_str, unit in matches:
            try:
                value = int(value_str)
                if value < 0:
                    raise ValueError(f"Negative values not allowed: {value}{unit}")
                if value > 365:  # Reasonable upper limit for days
                    logger.warning(f"Large value detected: {value}{unit} - this may be unintentional")
                
                seconds_for_unit = value * multipliers[unit]
                total_seconds += seconds_for_unit
                logger.debug(f"Unit {value}{unit} = {seconds_for_unit} seconds")
                
            except ValueError as e:
                raise ValueError(f"Invalid numeric value in granularity '{granularity}': {value_str}{unit}")
        
        # Validate that we parsed the entire string (no leftover characters)
        reconstructed = ''.join([f"{value}{unit}" for value, unit in matches])
        if reconstructed != granularity:
            leftover = granularity.replace(reconstructed, '')
            raise ValueError(f"Invalid characters in granularity '{granularity}': '{leftover}'. Only numbers and units (d,h,m,s) allowed.")
        
        # Validate reasonable bounds
        if total_seconds <= 0:
            raise ValueError(f"Granularity must result in positive seconds, got {total_seconds}")
        
        if total_seconds > 86400 * 365:  # More than a year
            logger.warning(f"Very large granularity detected: {total_seconds} seconds ({total_seconds/86400:.1f} days)")
        
        logger.info(f" Granularity '{granularity}' parsed to {total_seconds} seconds")
        return total_seconds
        
    except Exception as e:
        logger.error(f" Failed to parse granularity '{granularity}': {str(e)}")
        raise


def convert_seconds_to_granularity(seconds: int) -> str:
    """
    Convert total seconds to a compact granularity string like '1d2h30m40s'.
    Skips units with zero values unless total seconds is zero.

    Args:
        seconds (int): Total seconds to convert.

    Returns:
        str: Granularity string like '1h30m', '2d', or '0s' if zero.
    """
    if not isinstance(seconds, (int, float)):
        raise ValueError(f"Seconds must be numeric, got {type(seconds)}")
    if seconds < 0:
        raise ValueError(f"Seconds cannot be negative, got {seconds}")

    seconds = int(seconds)
    if seconds == 0:
        return "0s"

    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)

    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if secs:
        parts.append(f"{secs}s")

    return ''.join(parts)



def generate_SOURCE_COMPLETE_CATEGORY(config: Dict, WINDOW_START_TIME: pendulum.DateTime, WINDOW_END_TIME: pendulum.DateTime) -> str:
    """
    Generate source complete category based on business logic with error handling
    
    Args:
        config: Pipeline configuration
        WINDOW_START_TIME: Window start time
        WINDOW_END_TIME: Window end time
        
    Returns:
        Source complete category string: "index_group|index_name"
    """
    
    try:
        logger.debug("Generating SOURCE_COMPLETE_CATEGORY...")
        
        # Validate inputs
        if not config:
            raise ValueError("Config is required")
        
        index_group = config.get("index_group")
        index_name = config.get("index_name")

        result = f"{index_group}|{index_name}"
        logger.debug(f"SOURCE_COMPLETE_CATEGORY generated: {result}")
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to generate SOURCE_COMPLETE_CATEGORY: {str(e)}")
        raise


def generate_TARGET_COMPLETE_CATEGORY(config: Dict, WINDOW_START_TIME: pendulum.DateTime, WINDOW_END_TIME: pendulum.DateTime) -> str:
    """
    Generate target complete category based on business logic with error handling
    
    Args:
        config: Pipeline configuration
        WINDOW_START_TIME: Window start time
        WINDOW_END_TIME: Window end time
        
    Returns:
        Target complete category string: "target_table_three_part_name|prefix/YYYY-MM-DD/HH-mm/"
    """
    
    try:
        logger.debug("Generating TARGET_COMPLETE_CATEGORY...")
        
        # Get required config fields
        s3_prefix_list = config.get("s3_prefix_list")
        database_path = config.get("target_table_three_part_name")
        
        # Build s3_prefix_sub from list
        s3_prefix_sub = '/'.join(s3_prefix_list)
        
        # Extract datetime parts from WINDOW_START_TIME
        date_part = WINDOW_START_TIME.format('YYYY-MM-DD')  # e.g., "2025-01-01"
        time_part = WINDOW_START_TIME.format('HH-mm')       # e.g., "14-30"

        
        # Construct target path
        target_path = f"{s3_prefix_sub}/{date_part}/{time_part}/"
        
        result = f"{database_path}|{target_path}"
        logger.debug(f"TARGET_COMPLETE_CATEGORY generated: {result}")
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to generate TARGET_COMPLETE_CATEGORY: {str(e)}")
        raise


def generate_STAGE_COMPLETE_CATEGORY(config: Dict, WINDOW_START_TIME: pendulum.DateTime, WINDOW_END_TIME: pendulum.DateTime) -> str:
    """
    Generate stage complete category based on business logic with error handling
    
    Args:
        config: Pipeline configuration
        WINDOW_START_TIME: Window start time
        WINDOW_END_TIME: Window end time
        
    Returns:
        Stage complete category string: "s3_bucket|s3://bucket/prefix/YYYY-MM-DD/HH-mm/indexid_epochtime.json"
    """
    
    try:
        logger.debug("Generating STAGE_COMPLETE_CATEGORY...")
                
        # Get required config fields
        s3_prefix_list = config.get("s3_prefix_list")
        s3_bucket = config.get("s3_bucket")
        index_id = config.get("index_id")
        
        # Build s3_prefix_sub from list
        s3_prefix_sub = '/'.join(s3_prefix_list)
        
        # Extract datetime parts from WINDOW_START_TIME

        date_part = WINDOW_START_TIME.format('YYYY-MM-DD')  # e.g., "2025-01-01"
        time_part = WINDOW_START_TIME.format('HH-mm')       # e.g., "14-30"

        
        # Get current epoch time in config timezone
        timezone = config.get('timezone')

        current_time = pendulum.now(timezone)
        epoch_timestamp = int(current_time.timestamp())
        
        # Construct S3 path with actual epoch timestamp
        s3_path = f"s3://{s3_bucket}/{s3_prefix_sub}/{date_part}/{time_part}/{index_id}_{epoch_timestamp}.json"
        
        result = f"{s3_bucket}|{s3_path}"
        logger.debug(f"STAGE_COMPLETE_CATEGORY generated: {result}")
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to generate STAGE_COMPLETE_CATEGORY: {str(e)}")
        raise


def generate_PIPELINE_ID(PIPELINE_NAME: str, SOURCE_COMPLETE_CATEGORY: str, STAGE_COMPLETE_CATEGORY: str, 
                        TARGET_COMPLETE_CATEGORY: str, WINDOW_START_TIME: pendulum.DateTime, WINDOW_END_TIME: pendulum.DateTime) -> str:
    """
    Generate unique pipeline ID by hashing all components with error handling
    
    Args:
        PIPELINE_NAME: Name of the pipeline
        SOURCE_COMPLETE_CATEGORY: Source complete category
        STAGE_COMPLETE_CATEGORY: Stage complete category
        TARGET_COMPLETE_CATEGORY: Target complete category
        WINDOW_START_TIME: Window start time
        WINDOW_END_TIME: Window end time
        
    Returns:
        SHA256 hash as hexadecimal string
    """
    
    try:
        logger.debug("Generating PIPELINE_ID...")
              
        # Create string to hash from all components
        try:
            start_time_str = WINDOW_START_TIME.to_iso8601_string()
            end_time_str = WINDOW_END_TIME.to_iso8601_string()
        except Exception as e:
            raise ValueError(f"Failed to convert datetime objects to ISO strings: {str(e)}")
        
        components = [
            PIPELINE_NAME,
            SOURCE_COMPLETE_CATEGORY,
            STAGE_COMPLETE_CATEGORY,
            TARGET_COMPLETE_CATEGORY,
            start_time_str,
            end_time_str
        ]
        
        # Join with separator and encode
        hash_input = "|".join(components)
        
        try:
            hash_input_encoded = hash_input.encode('utf-8')
        except Exception as e:
            raise ValueError(f"Failed to encode hash input string: {str(e)}")
        
        # Generate SHA256 hash
        try:
            hash_object = hashlib.sha256(hash_input_encoded)
            pipeline_id = hash_object.hexdigest()
        except Exception as e:
            raise ValueError(f"Failed to generate SHA256 hash: {str(e)}")
        
        logger.debug(f"PIPELINE_ID generated: {pipeline_id}")
        return pipeline_id
        
    except Exception as e:
        logger.error(f"Failed to generate PIPELINE_ID: {str(e)}")
        raise


def single_record_creation_for_given_time_windows( config: Dict, target_day: pendulum.DateTime,   window_start_time: pendulum.DateTime,    window_end_time: pendulum.DateTime,    time_interval: str) -> Dict:
    """
    Build a single pipeline record where all timestamps are stored as ISO 8601 strings.

    Args:
        config: Pipeline configuration.
        target_day: The day for which the record is created (pendulum.DateTime).
        window_start_time: Start of the window (pendulum.DateTime).
        window_end_time: End of the window (pendulum.DateTime).
        time_interval: Time interval string in granularity format.

    Returns:
        A dictionary representing the complete record.
    """
    # Get current timestamp in configured timezone
    timezone = config.get('timezone')
    current_time = pendulum.now(timezone)

    # Generate categories
    source_category = generate_SOURCE_COMPLETE_CATEGORY(config, window_start_time, window_end_time)
    stage_category = generate_STAGE_COMPLETE_CATEGORY(config, window_start_time, window_end_time)
    target_category = generate_TARGET_COMPLETE_CATEGORY(config, window_start_time, window_end_time)

    # Generate pipeline ID
    pipeline_id = generate_PIPELINE_ID(
        config['PIPELINE_NAME'],
        source_category,
        stage_category,
        target_category,
        window_start_time,
        window_end_time
    )

    # Build and return the record with all timestamps as ISO strings
    return {
        "PIPELINE_NAME": config['PIPELINE_NAME'],
        "SOURCE_COMPLETE_CATEGORY": source_category,
        "STAGE_COMPLETE_CATEGORY": stage_category,
        "TARGET_COMPLETE_CATEGORY": target_category,
        "PIPELINE_ID": pipeline_id,
        "TARGET_DAY": target_day.to_date_string(),
        "WINDOW_START_TIME": window_start_time.to_iso8601_string(),
        "WINDOW_END_TIME": window_end_time.to_iso8601_string(),
        "TIME_INTERVAL": time_interval,
        "COMPLETED_PHASE": None,
        "COMPLETED_PHASE_DURATION": None,
        "PIPELINE_STATUS": "PENDING",
        "PIPELINE_START_TIME": None,
        "PIPELINE_END_TIME": None,
        "PIPELINE_PRIORITY": config.get('PIPELINE_PRIORITY', 1.3),
        "CONTINUITY_CHECK_PERFORMED": "YES",
        "CAN_ACCESS_HISTORICAL_DATA": config.get('CAN_ACCESS_HISTORICAL_DATA', 'YES'),
        "RECORD_FIRST_CREATED_TIME": current_time.to_iso8601_string(),
        "RECORD_LAST_UPDATED_TIME": current_time.to_iso8601_string(),
        "SOURCE_COUNT": None,
        "TARGET_COUNT": None,
        "COUNT_DIFF": None,
        "COUNT_DIFF_PERCENTAGE": None,
    }


def generate_records_for_target_day(config: Dict, target_day: pendulum.DateTime) -> List[Dict]:
    """
    Create a list of records for the given target day, split by the configured granularity.

    Args:
        config: Pipeline configuration containing 'granularity' and other fields.
        target_day: Target day for which to generate records.

    Returns:
        List[Dict]: List of pipeline records (one for each window).
    """
    # Convert granularity to seconds
    granularity_str = config.get('granularity')
    if not granularity_str:
        raise ValueError("Missing 'granularity' in config")

    duration_seconds = parse_granularity_to_seconds(granularity_str)

    # Generate all windows for the day
    windows = generate_consecutive_windows_for_target_day(target_day, duration_seconds)

    # Create a record for each window
    records = []
    for tgt_day, window_start, window_end, interval_str in windows:
        record = single_record_creation_for_given_time_windows(
            config=config,
            target_day=tgt_day,
            window_start_time=window_start,
            window_end_time=window_end,
            time_interval=interval_str
        )
        records.append(record)

    return records



def find_missing_target_days(existing_days: Set[str]) -> Set[str]:
    """
    Given a set of existing target days (YYYY-MM-DD as strings), find missing days
    between the min and max target day (inclusive).

    Args:
        existing_days (Set[str]): Set of target days as YYYY-MM-DD strings.

    Returns:
        Set[str]: Set of missing target days as YYYY-MM-DD strings.
    """

    if not existing_days:
        return set()

    # Convert to pendulum.DateTime for range operations
    day_objects = [pendulum.parse(day) for day in existing_days]

    min_day = min(day_objects).start_of('day')
    max_day = max(day_objects).start_of('day')

    # Build full set of days from min to max
    all_days = {min_day.add(days=i).to_date_string() for i in range((max_day - min_day).days + 1)}

    # Find missing days
    missing_days = all_days - existing_days

    return missing_days




# # Setup basic logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# if __name__ == "__main__":
#     # Example config
#     config = {
#         'PIPELINE_NAME': 'test_pipeline',
#         'granularity': '9h',
#         'timezone': 'America/Los_Angeles',
#         'index_group': 'group1',
#         'index_name': 'index1',
#         's3_bucket': 'test-bucket',
#         's3_prefix_list': ['data', 'stage'],
#         'index_id': 'IDX001',
#         'target_table_three_part_name': 'mydb.schema.table',
#         'PIPELINE_PRIORITY': 1.0,
#         'CAN_ACCESS_HISTORICAL_DATA': 'YES'
#     }

#     # Calculate target day (yesterday)
#     target_day = get_start_of_day_relative_to_now(config['timezone'], ago_seconds=86400)

#     # Generate records
#     records = generate_records_for_target_day(config, target_day)

#     # Prepare rows for tabular print
#     table = []
#     headers = ["PIPELINE_ID", "TARGET_DAY", "WINDOW_START_TIME", "WINDOW_END_TIME", "TIME_INTERVAL", "STAGE_COMPLETE_CATEGORY"]
    
#     for record in records:
#         row = [
#             record["PIPELINE_ID"][:8],  # Shorten hash for readability
#             record["TARGET_DAY"],
#             record["WINDOW_START_TIME"],
#             record["WINDOW_END_TIME"],
#             record["TIME_INTERVAL"],
#             record["STAGE_COMPLETE_CATEGORY"]
#         ]
#         table.append(row)

#     # Print table
#     print("\nGenerated Pipeline Records:\n")
#     print(tabulate(table, headers=headers, tablefmt="pretty"))



