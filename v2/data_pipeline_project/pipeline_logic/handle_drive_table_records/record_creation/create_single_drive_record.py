# pipleine_logic/handle_drive_table_records/record_creation/create_single_drive_record.py
import re
import hashlib
import pendulum
from typing import Dict, List, Tuple, Union
import logging

# Configure logging for Airflow
logger = logging.getLogger(__name__)


def seconds_to_readable(seconds: int) -> str:
    """Convert seconds to HH:MM:SS format with validation"""
    try:
        if not isinstance(seconds, (int, float)):
            raise ValueError(f"Seconds must be numeric, got {type(seconds)}")
        
        if seconds < 0:
            raise ValueError(f"Seconds cannot be negative, got {seconds}")
        
        seconds = int(seconds)
        hours, remainder = divmod(seconds, 3600)
        minutes, secs = divmod(remainder, 60)
        
        result = f"{hours:02d}:{minutes:02d}:{secs:02d}"
        logger.debug(f"Converted {seconds} seconds to {result}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error converting seconds to readable format: {str(e)}")
        raise


def create_single_record(config: Dict, TARGET_DAY: str, WINDOW_START_TIME: pendulum.DateTime, 
                        WINDOW_END_TIME: pendulum.DateTime, TIME_INTERVAL: str) -> Dict:
    """
    Build complete pipeline record following the schema with comprehensive error handling
    
    Args:
        config: Pipeline configuration
        TARGET_DAY: Target day as string (YYYY-MM-DD)
        WINDOW_START_TIME: Window start time
        WINDOW_END_TIME: Window end time
        TIME_INTERVAL: Formatted time interval string
        
    Returns:
        Complete record dictionary ready for insertion
        
    Raises:
        ValueError: If inputs are invalid
        Exception: If record creation fails
    """
    
    try:
        logger.info("Creating single record...")
        
        # Validate all inputs
        _validate_create_single_record_inputs(config, TARGET_DAY, WINDOW_START_TIME, WINDOW_END_TIME, TIME_INTERVAL)
        
        logger.info(f"Creating record for TARGET_DAY: {TARGET_DAY}")
        logger.info(f"Window: {WINDOW_START_TIME} to {WINDOW_END_TIME}")
        logger.info(f"Time interval: {TIME_INTERVAL}")
        
        # Get current time for audit fields
        timezone = config.get('timezone', 'UTC')
        try:
            current_time = pendulum.now(timezone)
        except Exception as e:
            logger.warning(f"Failed to get current time in timezone {timezone}, using UTC: {str(e)}")
            current_time = pendulum.now('UTC')
        
        logger.debug(f"Current time: {current_time}")
        
        # Generate categories with error handling
        logger.info("Generating record categories...")
        
        try:
            SOURCE_COMPLETE_CATEGORY = generate_SOURCE_COMPLETE_CATEGORY(config, WINDOW_START_TIME, WINDOW_END_TIME)
            logger.debug(f"SOURCE_COMPLETE_CATEGORY: {SOURCE_COMPLETE_CATEGORY}")
        except Exception as e:
            logger.error(f"Failed to generate SOURCE_COMPLETE_CATEGORY: {str(e)}")
            raise
        
        try:
            STAGE_COMPLETE_CATEGORY = generate_STAGE_COMPLETE_CATEGORY(config, WINDOW_START_TIME, WINDOW_END_TIME)
            logger.debug(f"STAGE_COMPLETE_CATEGORY: {STAGE_COMPLETE_CATEGORY}")
        except Exception as e:
            logger.error(f"Failed to generate STAGE_COMPLETE_CATEGORY: {str(e)}")
            raise
        
        try:
            TARGET_COMPLETE_CATEGORY = generate_TARGET_COMPLETE_CATEGORY(config, WINDOW_START_TIME, WINDOW_END_TIME)
            logger.debug(f"TARGET_COMPLETE_CATEGORY: {TARGET_COMPLETE_CATEGORY}")
        except Exception as e:
            logger.error(f"Failed to generate TARGET_COMPLETE_CATEGORY: {str(e)}")
            raise
        
        # Generate pipeline ID with error handling
        logger.info("Generating PIPELINE_ID...")
        try:
            PIPELINE_ID = generate_PIPELINE_ID(
                config['PIPELINE_NAME'],
                SOURCE_COMPLETE_CATEGORY,
                STAGE_COMPLETE_CATEGORY,
                TARGET_COMPLETE_CATEGORY,
                WINDOW_START_TIME,
                WINDOW_END_TIME
            )
            logger.debug(f"PIPELINE_ID: {PIPELINE_ID}")
        except Exception as e:
            logger.error(f"Failed to generate PIPELINE_ID: {str(e)}")
            raise
        
        # Build complete record following schema
        logger.info("Building complete record...")
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
        
        # Validate created record
        _validate_created_record(record)
        
        logger.info(f"Record created successfully - PIPELINE_ID: {PIPELINE_ID}")
        return record
        
    except Exception as e:
        logger.error(f"Failed to create single record: {str(e)}")
        logger.error(f"TARGET_DAY: {TARGET_DAY}")
        logger.error(f"Window: {WINDOW_START_TIME} to {WINDOW_END_TIME}")
        raise


def _validate_create_single_record_inputs(config, TARGET_DAY, WINDOW_START_TIME, WINDOW_END_TIME, TIME_INTERVAL):
    """Validate all inputs for create_single_record"""
    
    try:
        logger.debug("Validating create_single_record inputs...")
        
        # Validate config
        if not config:
            raise ValueError("Config is required and cannot be None or empty")
        
        if not isinstance(config, dict):
            raise ValueError(f"Config must be a dictionary, got {type(config)}")
        
        # Check required config fields
        required_fields = ['PIPELINE_NAME', 'index_group', 'index_name', 'index_id', 's3_bucket', 's3_prefix_list', 'database.schema.table']
        missing_fields = [field for field in required_fields if not config.get(field)]
        if missing_fields:
            raise ValueError(f"Config missing required fields: {missing_fields}")
        
        # Validate TARGET_DAY
        if not TARGET_DAY:
            raise ValueError("TARGET_DAY is required and cannot be None or empty")
        
        # Validate datetime objects
        if not WINDOW_START_TIME:
            raise ValueError("WINDOW_START_TIME is required and cannot be None")
        
        if not WINDOW_END_TIME:
            raise ValueError("WINDOW_END_TIME is required and cannot be None")
        
        if not hasattr(WINDOW_START_TIME, 'timestamp'):
            raise ValueError(f"WINDOW_START_TIME must be a datetime object, got {type(WINDOW_START_TIME)}")
        
        if not hasattr(WINDOW_END_TIME, 'timestamp'):
            raise ValueError(f"WINDOW_END_TIME must be a datetime object, got {type(WINDOW_END_TIME)}")
        
        # Validate TIME_INTERVAL
        if not TIME_INTERVAL:
            raise ValueError("TIME_INTERVAL is required and cannot be None or empty")
        
        if not isinstance(TIME_INTERVAL, str):
            raise ValueError(f"TIME_INTERVAL must be a string, got {type(TIME_INTERVAL)}")
        
        logger.debug("Input validation passed")
        
    except Exception as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise


def _validate_created_record(record):
    """Validate the created record has all required fields"""
    
    try:
        logger.debug("Validating created record...")
        
        if not record:
            raise ValueError("Created record is None or empty")
        
        if not isinstance(record, dict):
            raise ValueError(f"Created record must be a dictionary, got {type(record)}")
        
        # Check required fields
        required_fields = [
            'PIPELINE_NAME', 'SOURCE_COMPLETE_CATEGORY', 'STAGE_COMPLETE_CATEGORY',
            'TARGET_COMPLETE_CATEGORY', 'PIPELINE_ID', 'TARGET_DAY',
            'WINDOW_START_TIME', 'WINDOW_END_TIME', 'TIME_INTERVAL'
        ]
        
        missing_fields = [field for field in required_fields if field not in record or record[field] is None]
        if missing_fields:
            raise ValueError(f"Created record missing required fields: {missing_fields}")
        
        # Validate field types
        if not isinstance(record['PIPELINE_ID'], str) or not record['PIPELINE_ID'].strip():
            raise ValueError("PIPELINE_ID must be a non-empty string")
        
        if not isinstance(record['TARGET_DAY'], str) or not record['TARGET_DAY'].strip():
            raise ValueError("TARGET_DAY must be a non-empty string")
        
        logger.debug("Created record validation passed")
        
    except Exception as e:
        logger.error(f"Created record validation failed: {str(e)}")
        raise


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
        
        if not index_group:
            raise ValueError("Config missing required field: index_group")
        
        if not index_name:
            raise ValueError("Config missing required field: index_name")
        
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
        Target complete category string: "database.schema.table|prefix/YYYY-MM-DD/HH-mm/"
    """
    
    try:
        logger.debug("Generating TARGET_COMPLETE_CATEGORY...")
        
        # Validate inputs
        if not config:
            raise ValueError("Config is required")
        
        if not WINDOW_START_TIME or not hasattr(WINDOW_START_TIME, 'format'):
            raise ValueError("WINDOW_START_TIME must be a valid datetime object")
        
        # Get required config fields
        s3_prefix_list = config.get("s3_prefix_list")
        database_path = config.get("database.schema.table")
        
        if not s3_prefix_list:
            raise ValueError("Config missing required field: s3_prefix_list")
        
        if not database_path:
            raise ValueError("Config missing required field: database.schema.table")
        
        if not isinstance(s3_prefix_list, list):
            raise ValueError(f"s3_prefix_list must be a list, got {type(s3_prefix_list)}")
        
        # Build s3_prefix_sub from list
        s3_prefix_sub = '/'.join(s3_prefix_list)
        
        # Extract datetime parts from WINDOW_START_TIME
        try:
            date_part = WINDOW_START_TIME.format('YYYY-MM-DD')  # e.g., "2025-01-01"
            time_part = WINDOW_START_TIME.format('HH-mm')       # e.g., "14-30"
        except Exception as e:
            raise ValueError(f"Failed to format WINDOW_START_TIME: {str(e)}")
        
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
        
        # Validate inputs
        if not config:
            raise ValueError("Config is required")
        
        if not WINDOW_START_TIME or not hasattr(WINDOW_START_TIME, 'format'):
            raise ValueError("WINDOW_START_TIME must be a valid datetime object")
        
        # Get required config fields
        s3_prefix_list = config.get("s3_prefix_list")
        s3_bucket = config.get("s3_bucket")
        index_id = config.get("index_id")
        
        if not s3_prefix_list:
            raise ValueError("Config missing required field: s3_prefix_list")
        
        if not s3_bucket:
            raise ValueError("Config missing required field: s3_bucket")
        
        if not index_id:
            raise ValueError("Config missing required field: index_id")
        
        if not isinstance(s3_prefix_list, list):
            raise ValueError(f"s3_prefix_list must be a list, got {type(s3_prefix_list)}")
        
        # Build s3_prefix_sub from list
        s3_prefix_sub = '/'.join(s3_prefix_list)
        
        # Extract datetime parts from WINDOW_START_TIME
        try:
            date_part = WINDOW_START_TIME.format('YYYY-MM-DD')  # e.g., "2025-01-01"
            time_part = WINDOW_START_TIME.format('HH-mm')       # e.g., "14-30"
        except Exception as e:
            raise ValueError(f"Failed to format WINDOW_START_TIME: {str(e)}")
        
        # Get current epoch time in config timezone
        timezone = config.get('timezone', 'UTC')
        try:
            current_time = pendulum.now(timezone)
            epoch_timestamp = int(current_time.timestamp())
        except Exception as e:
            logger.warning(f"Failed to get epoch time in timezone {timezone}, using UTC: {str(e)}")
            current_time = pendulum.now('UTC')
            epoch_timestamp = int(current_time.timestamp())
        
        # Construct S3 path with actual epoch timestamp
        s3_path = f"s3://{s3_bucket}/{s3_prefix_sub}/{date_part}/{time_part}/{index_id}_{epoch_timestamp}.json"
        
        result = f"{s3_bucket}|{s3_path}"
        logger.debug(f"STAGE_COMPLETE_CATEGORY generated: {result}")
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to generate STAGE_COMPLETE_CATEGORY: {str(e)}")
        raise


def generate_PIPELINE_ID(PIPELINE_NAME: str, source_cat: str, stage_cat: str, 
                        target_cat: str, WINDOW_START_TIME: pendulum.DateTime, WINDOW_END_TIME: pendulum.DateTime) -> str:
    """
    Generate unique pipeline ID by hashing all components with error handling
    
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
    
    try:
        logger.debug("Generating PIPELINE_ID...")
        
        # Validate inputs
        required_params = {
            'PIPELINE_NAME': PIPELINE_NAME,
            'source_cat': source_cat,
            'stage_cat': stage_cat,
            'target_cat': target_cat
        }
        
        for param_name, param_value in required_params.items():
            if not param_value:
                raise ValueError(f"{param_name} is required and cannot be None or empty")
            if not isinstance(param_value, str):
                raise ValueError(f"{param_name} must be a string, got {type(param_value)}")
        
        if not WINDOW_START_TIME or not hasattr(WINDOW_START_TIME, 'to_iso8601_string'):
            raise ValueError("WINDOW_START_TIME must be a valid datetime object")
        
        if not WINDOW_END_TIME or not hasattr(WINDOW_END_TIME, 'to_iso8601_string'):
            raise ValueError("WINDOW_END_TIME must be a valid datetime object")
        
        # Create string to hash from all components
        try:
            start_time_str = WINDOW_START_TIME.to_iso8601_string()
            end_time_str = WINDOW_END_TIME.to_iso8601_string()
        except Exception as e:
            raise ValueError(f"Failed to convert datetime objects to ISO strings: {str(e)}")
        
        components = [
            PIPELINE_NAME,
            source_cat,
            stage_cat,
            target_cat,
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


def rebuild_single_record_for_given_window(record, config):
    """
    Rebuild single record for given window with comprehensive error handling
    
    Args:
        record: Original record with window information
        config: Configuration dictionary
        
    Returns:
        Rebuilt record dictionary
    """
    
    try:
        logger.info("Rebuilding single record for given window...")
        
        # Validate inputs
        if not record:
            raise ValueError("Record is required and cannot be None or empty")
        
        if not isinstance(record, dict):
            raise ValueError(f"Record must be a dictionary, got {type(record)}")
        
        PIPELINE_ID = record.get("PIPELINE_ID", "Unknown")
        TARGET_DAY = record.get("TARGET_DAY", "Unknown")
        
        logger.info(f"Rebuilding record - PIPELINE_ID: {PIPELINE_ID}, TARGET_DAY: {TARGET_DAY}")
        
        # Validate window boundaries first
        WINDOW_START_TIME, WINDOW_END_TIME, TARGET_DAY_validated = validate_window_boundaries(record, config)
        
        # Calculate time interval
        try:
            time_interval_seconds = int((WINDOW_END_TIME - WINDOW_START_TIME).total_seconds())
            if time_interval_seconds <= 0:
                raise ValueError(f"Invalid time interval: {time_interval_seconds} seconds")
            
            TIME_INTERVAL = seconds_to_readable(time_interval_seconds)
        except Exception as e:
            raise ValueError(f"Failed to calculate time interval: {str(e)}")
        
        logger.info(f"Time interval calculated: {TIME_INTERVAL} ({time_interval_seconds}s)")
        
        # Create rebuilt record
        rebuilt_record = create_single_record(config, str(TARGET_DAY_validated), WINDOW_START_TIME, WINDOW_END_TIME, TIME_INTERVAL)
        
        if not rebuilt_record:
            raise ValueError("create_single_record returned None")
        
        logger.info(f"Record rebuilt successfully - new PIPELINE_ID: {rebuilt_record.get('PIPELINE_ID', 'Unknown')}")
        
        return rebuilt_record
        
    except Exception as e:
        logger.error(f"Failed to rebuild single record: {str(e)}")
        logger.error(f"Original PIPELINE_ID: {record.get('PIPELINE_ID', 'Unknown') if record else 'Unknown'}")
        raise


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
    
    try:
        logger.debug("Validating window boundaries...")
        
        # Extract from record using same field names
        WINDOW_START_TIME = record.get("WINDOW_START_TIME")
        WINDOW_END_TIME = record.get("WINDOW_END_TIME") 
        TARGET_DAY = record.get("TARGET_DAY")
        
        # Check presence
        if WINDOW_START_TIME is None or WINDOW_END_TIME is None:
            raise ValueError("Window-specific records must have both WINDOW_START_TIME and WINDOW_END_TIME")
        
        if not TARGET_DAY:
            raise ValueError("TARGET_DAY is required")
        
        # Convert to pendulum objects if needed
        try:
            if not hasattr(WINDOW_START_TIME, 'timestamp'):
                WINDOW_START_TIME = pendulum.parse(str(WINDOW_START_TIME))
            if not hasattr(WINDOW_END_TIME, 'timestamp'):
                WINDOW_END_TIME = pendulum.parse(str(WINDOW_END_TIME))
            if not hasattr(TARGET_DAY, 'to_date_string'):
                TARGET_DAY = pendulum.parse(str(TARGET_DAY))
        except Exception as e:
            raise ValueError(f"Failed to parse datetime objects: {str(e)}")
        
        # Check WINDOW_START_TIME < WINDOW_END_TIME
        if WINDOW_START_TIME >= WINDOW_END_TIME:
            raise ValueError(f"WINDOW_START_TIME ({WINDOW_START_TIME}) must be before WINDOW_END_TIME ({WINDOW_END_TIME})")
        
        # Check both times are within TARGET_DAY
        try:
            target_date = TARGET_DAY.date() if hasattr(TARGET_DAY, 'date') else pendulum.parse(str(TARGET_DAY)).date()
            start_date = WINDOW_START_TIME.date()
            end_date = WINDOW_END_TIME.date()
        except Exception as e:
            raise ValueError(f"Failed to extract dates for validation: {str(e)}")
        
        if start_date != target_date:
            raise ValueError(f"WINDOW_START_TIME date ({start_date}) must match TARGET_DAY ({target_date})")
        
        if end_date != target_date and end_date != target_date.add(days=1):
            raise ValueError(f"WINDOW_END_TIME date ({end_date}) must be within TARGET_DAY ({target_date}) or start of next day")
        
        # Special case: WINDOW_END_TIME can be 00:00:00 of next day (end of target day)
        if end_date == target_date.add(days=1):
            if WINDOW_END_TIME.hour != 0 or WINDOW_END_TIME.minute != 0 or WINDOW_END_TIME.second != 0:
                raise ValueError(f"If WINDOW_END_TIME is next day, it must be 00:00:00, got {WINDOW_END_TIME.time()}")
        
        logger.debug(f"Window boundaries valid: {WINDOW_START_TIME} to {WINDOW_END_TIME}")
        return (WINDOW_START_TIME, WINDOW_END_TIME, TARGET_DAY)
        
    except Exception as e:
        logger.error(f"Window boundary validation failed: {str(e)}")
        raise