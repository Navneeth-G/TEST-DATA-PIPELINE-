# pipleine_logic/handle_drive_table_records/record_creation/create_single_drive_record.py
import re
import hashlib
import pendulum
from typing import Dict, List, Tuple, Union



def seconds_to_readable(seconds: int) -> str:
    """Convert seconds to HH:MM:SS format"""
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def create_single_record(config: Dict, TARGET_DAY: str, WINDOW_START_TIME: pendulum.DateTime, 
                                WINDOW_END_TIME: pendulum.DateTime, TIME_INTERVAL: str) -> Dict:
   """
   Build complete pipeline record following the schema
   
   Args:
       config: Pipeline configuration
       TARGET_DAY: Target day as string (YYYY-MM-DD)
       WINDOW_START_TIME: Window start time
       WINDOW_END_TIME: Window end time
       TIME_INTERVAL: Formatted time interval string
       
   Returns:
       Complete record dictionary ready for insertion
   """
   
   # Get current time for audit fields
   current_time = pendulum.now(config.get('timezone', 'UTC'))
   
   # Generate categories
   SOURCE_COMPLETE_CATEGORY = generate_SOURCE_COMPLETE_CATEGORY(config, WINDOW_START_TIME, WINDOW_END_TIME)
   STAGE_COMPLETE_CATEGORY = generate_STAGE_COMPLETE_CATEGORY(config, WINDOW_START_TIME, WINDOW_END_TIME)
   TARGET_COMPLETE_CATEGORY = generate_TARGET_COMPLETE_CATEGORY(config, WINDOW_START_TIME, WINDOW_END_TIME)

   # Generate pipeline ID
   PIPELINE_ID = generate_PIPELINE_ID(
       config['PIPELINE_NAME'],
       SOURCE_COMPLETE_CATEGORY,
       STAGE_COMPLETE_CATEGORY,
       TARGET_COMPLETE_CATEGORY,
       WINDOW_START_TIME,
       WINDOW_END_TIME
   )
   
   # Build complete record following your schema
   record = {
       "PIPELINE_NAME": config['PIPELINE_NAME'],
       "SOURCE_COMPLETE_CATEGORY": SOURCE_COMPLETE_CATEGORY,
       "STAGE_COMPLETE_CATEGORY": STAGE_COMPLETE_CATEGORY,
       "TARGET_COMPLETE_CATEGORY": TARGET_COMPLETE_CATEGORY,
       "PIPELINE_ID": PIPELINE_ID,
       "TARGET_DAY": TARGET_DAY,
       "WINDOW_START_TIME": WINDOW_START_TIME,
       "WINDOW_END_TIME": WINDOW_END_TIME,
       "TIME_INTERVAL": TIME_INTERVAL,
       "COMPLETED_PHASE": None,
       "COMPLETED_PHASE_DURATION": None,
       "PIPELINE_STATUS": "PENDING",
       "PIPELINE_START_TIME": None,
       "PIPELINE_END_TIME": None,
       "PIPELINE_PRIORITY": config.get('PIPELINE_PRIORITY', 1.0),
       "CONTINUITY_CHECK_PERFORMED": "YES",
       "CAN_ACCESS_HISTORICAL_DATA": config.get('CAN_ACCESS_HISTORICAL_DATA', 'YES'),
       "RECORD_FIRST_CREATED_TIME": current_time,
       "RECORD_LAST_UPDATED_TIME": current_time,
       "SOURCE_COUNT": 0,
       "TARGET_COUNT": 0,
       "COUNT_DIFF": 0,
       "COUNT_DIFF_PERCENTAGE": 0.0
   }
   
   return record


def generate_SOURCE_COMPLETE_CATEGORY(config: Dict, WINDOW_START_TIME: pendulum.DateTime, WINDOW_END_TIME: pendulum.DateTime) -> str:
   """
   Generate source complete category based on business logic
   
   Args:
       config: Pipeline configuration
       WINDOW_START_TIME: Window start time
       WINDOW_END_TIME: Window end time
       
   Returns:
       Source complete category string: "index_group|index_name"
   """
   
   index_group = config["index_group"]
   index_name = config["index_name"]
   
   return f"{index_group}|{index_name}"


def generate_TARGET_COMPLETE_CATEGORY(config: Dict, WINDOW_START_TIME: pendulum.DateTime, WINDOW_END_TIME: pendulum.DateTime) -> str:
   """
   Generate target complete category based on business logic
   
   Args:
       config: Pipeline configuration
       WINDOW_START_TIME: Window start time
       WINDOW_END_TIME: Window end time
       
   Returns:
       Target complete category string: "database.schema.table|prefix/YYYY-MM-DD/HH-mm/"
   """
   
   # Build s3_prefix_sub from list
   s3_prefix_sub = '/'.join(config["s3_prefix_list"])
   
   # Extract datetime parts from WINDOW_START_TIME
   date_part = WINDOW_START_TIME.format('YYYY-MM-DD')  # e.g., "2025-01-01"
   time_part = WINDOW_START_TIME.format('HH-mm')       # e.g., "14-30"
   
   # Get database path
   database_path = config["database.schema.table"]
   
   # Construct target path
   target_path = f"{s3_prefix_sub}/{date_part}/{time_part}/"
   
   return f"{database_path}|{target_path}"


def generate_STAGE_COMPLETE_CATEGORY(config: Dict, WINDOW_START_TIME: pendulum.DateTime, WINDOW_END_TIME: pendulum.DateTime) -> str:
   """
   Generate stage complete category based on business logic
   
   Args:
       config: Pipeline configuration
       WINDOW_START_TIME: Window start time
       WINDOW_END_TIME: Window end time
       
   Returns:
       Stage complete category string: "s3_bucket|s3://bucket/prefix/YYYY-MM-DD/HH-mm/indexid_epochtime.json"
   """
   
   # Build s3_prefix_sub from list
   s3_prefix_sub = '/'.join(config["s3_prefix_list"])
   
   # Extract datetime parts from WINDOW_START_TIME
   date_part = WINDOW_START_TIME.format('YYYY-MM-DD')  # e.g., "2025-01-01"
   time_part = WINDOW_START_TIME.format('HH-mm')       # e.g., "14-30"
   
   # Get config values
   s3_bucket = config["s3_bucket"]
   index_id = config["index_id"]
   
   # Get current epoch time in config timezone
   current_time = pendulum.now(config.get('timezone', 'UTC'))
   epoch_timestamp = int(current_time.timestamp())
   
   # Construct S3 path with actual epoch timestamp
   s3_path = f"s3://{s3_bucket}/{s3_prefix_sub}/{date_part}/{time_part}/{index_id}_{epoch_timestamp}.json"
   
   return f"{s3_bucket}|{s3_path}"


def generate_PIPELINE_ID(PIPELINE_NAME: str, source_cat: str, stage_cat: str, 
                       target_cat: str, WINDOW_START_TIME: pendulum.DateTime, WINDOW_END_TIME: pendulum.DateTime) -> str:
   """
   Generate unique pipeline ID by hashing all components
   
   Args:
       PIPELINE_NAME: Name of the pipeline
       source_cat: Source complete category
       stage_cat: Stage complete category
       target_cat: Target complete category
       WINDOW_START_TIME: Window start time
       WINDOW_END_TIME: Window end time
       
   Returns:
       SHA256 hash as hexadecimal string
   """
   
   # Create string to hash from all components
   components = [
       PIPELINE_NAME,
       source_cat,
       stage_cat,
       target_cat,
       WINDOW_START_TIME.to_iso8601_string(),
       WINDOW_END_TIME.to_iso8601_string()
   ]
   
   # Join with separator and encode
   hash_input = "|".join(components).encode('utf-8')
   
   # Generate SHA256 hash
   hash_object = hashlib.sha256(hash_input)
   return hash_object.hexdigest()

def rebuild_single_record_for_given_window(record, config):
    # Validate window boundaries first
    WINDOW_START_TIME, WINDOW_END_TIME, TARGET_DAY = validate_window_boundaries(record, config)
    
    # Calculate time interval
    time_interval_seconds = int((WINDOW_END_TIME - WINDOW_START_TIME).total_seconds())
    TIME_INTERVAL = seconds_to_readable(time_interval_seconds)

    # Create rebuilt record
    rebuilt_record = create_single_record(config, TARGET_DAY, WINDOW_START_TIME, WINDOW_END_TIME, TIME_INTERVAL)

    return rebuilt_record


def validate_window_boundaries(record, config):
    """
    Validate window times are present, properly formatted, and logically consistent
    
    Args:
        record: Record dictionary with window times
        config: Configuration dictionary
        
    Returns:
        tuple: (WINDOW_START_TIME, WINDOW_END_TIME, TARGET_DAY) as pendulum objects
        
    Raises:
        ValueError: If validation fails
    """
    # Extract from record using same field names
    WINDOW_START_TIME = record.get("WINDOW_START_TIME")
    WINDOW_END_TIME = record.get("WINDOW_END_TIME") 
    TARGET_DAY = record.get("TARGET_DAY")
    
    # Check presence
    if WINDOW_START_TIME is None or WINDOW_END_TIME is None:
        raise ValueError("Window-specific records must have both WINDOW_START_TIME and WINDOW_END_TIME")
    
    # Convert to pendulum objects if needed
    if not hasattr(WINDOW_START_TIME, 'timestamp'):
        WINDOW_START_TIME = pendulum.parse(str(WINDOW_START_TIME))
    if not hasattr(WINDOW_END_TIME, 'timestamp'):
        WINDOW_END_TIME = pendulum.parse(str(WINDOW_END_TIME))
    if not hasattr(TARGET_DAY, 'to_date_string'):
        TARGET_DAY = pendulum.parse(str(TARGET_DAY))
    
    # Check WINDOW_START_TIME < WINDOW_END_TIME
    if WINDOW_START_TIME >= WINDOW_END_TIME:
        raise ValueError(f"WINDOW_START_TIME ({WINDOW_START_TIME}) must be before WINDOW_END_TIME ({WINDOW_END_TIME})")
    
    # Check both times are within TARGET_DAY
    target_date = TARGET_DAY.date() if hasattr(TARGET_DAY, 'date') else pendulum.parse(str(TARGET_DAY)).date()
    start_date = WINDOW_START_TIME.date()
    end_date = WINDOW_END_TIME.date()
    
    if start_date != target_date:
        raise ValueError(f"WINDOW_START_TIME date ({start_date}) must match TARGET_DAY ({target_date})")
    
    if end_date != target_date and end_date != target_date.add(days=1):
        raise ValueError(f"WINDOW_END_TIME date ({end_date}) must be within TARGET_DAY ({target_date}) or start of next day")
    
    # Special case: WINDOW_END_TIME can be 00:00:00 of next day (end of target day)
    if end_date == target_date.add(days=1):
        if WINDOW_END_TIME.hour != 0 or WINDOW_END_TIME.minute != 0 or WINDOW_END_TIME.second != 0:
            raise ValueError(f"If WINDOW_END_TIME is next day, it must be 00:00:00, got {WINDOW_END_TIME.time()}")
    
    print(f"✅ Window boundaries valid: {WINDOW_START_TIME} to {WINDOW_END_TIME}")
    return (WINDOW_START_TIME, WINDOW_END_TIME, TARGET_DAY)


