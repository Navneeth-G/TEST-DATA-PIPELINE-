# pipleine_logic/handle_drive_table_records/record_creation/create_multiple_drive_record.py
import re
import pendulum
from typing import Dict, List, Any
import logging

# Configure logging for Airflow
logger = logging.getLogger(__name__)

from pipleine_logic.handle_drive_table_records.record_creation.create_single_drive_record import create_single_record, seconds_to_readable


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


def create_all_records_for_target_day(TARGET_DAY, config):
    """
    Core function: Create all window records for a complete TARGET_DAY with comprehensive error handling
    
    Args:
        TARGET_DAY: Target day string (YYYY-MM-DD)
        config: Configuration dictionary
        
    Returns:
        List of complete records for the entire day
        
    Raises:
        ValueError: If inputs are invalid
        Exception: If record creation fails
    """
    
    try:
        logger.info("=" * 60)
        logger.info("CREATING ALL RECORDS FOR TARGET DAY")
        logger.info("=" * 60)
        
        # Validate inputs
        _validate_create_all_records_inputs(TARGET_DAY, config)
        
        TARGET_DAY_str = str(TARGET_DAY)
        logger.info(f"Creating records for TARGET_DAY: {TARGET_DAY_str}")
        
        # Get expected granularity from config
        expected_granularity = config.get("expected_granularity", "1h")  # Default 1 hour
        logger.info(f"Expected granularity: {expected_granularity}")
        
        # Parse granularity to seconds
        granularity_seconds = parse_granularity_to_seconds(expected_granularity)
        logger.info(f"Granularity in seconds: {granularity_seconds}")
        
        # Generate day boundaries
        tz = config.get('timezone', 'UTC')
        logger.info(f"Using timezone: {tz}")
        
        try:
            day_start = pendulum.parse(f"{TARGET_DAY_str}T00:00:00").in_timezone(tz)
            day_end = pendulum.parse(f"{TARGET_DAY_str}T23:59:59").in_timezone(tz).add(seconds=1)  # End of day
        except Exception as e:
            raise ValueError(f"Failed to parse TARGET_DAY '{TARGET_DAY_str}' or timezone '{tz}': {str(e)}")
        
        logger.info(f"Day boundaries: {day_start} to {day_end}")
        
        # Calculate expected number of windows
        total_day_seconds = int((day_end - day_start).total_seconds())
        expected_windows = (total_day_seconds + granularity_seconds - 1) // granularity_seconds  # Ceiling division
        logger.info(f"Expected number of windows: {expected_windows}")
        
        # Generate all window records for the day
        records_list = []
        current_window_start = day_start
        window_count = 0
        
        logger.info("Starting window generation...")
        
        while current_window_start < day_end:
            window_count += 1
            
            try:
                logger.debug(f"Creating window {window_count}/{expected_windows}")
                
                # Calculate window end
                proposed_window_end = current_window_start.add(seconds=granularity_seconds)
                
                # Apply day boundary constraint
                if proposed_window_end > day_end:
                    actual_window_end = day_end
                    logger.debug(f"Window {window_count}: End time adjusted to day boundary")
                else:
                    actual_window_end = proposed_window_end
                
                logger.debug(f"Window {window_count}: {current_window_start} to {actual_window_end}")
                
                # Calculate TIME_INTERVAL
                time_interval_seconds = int((actual_window_end - current_window_start).total_seconds())
                TIME_INTERVAL = seconds_to_readable(time_interval_seconds)
                
                logger.debug(f"Window {window_count}: TIME_INTERVAL = {TIME_INTERVAL} ({time_interval_seconds}s)")
                
                # Create single record using existing function
                single_record = create_single_record(
                    config, 
                    TARGET_DAY_str, 
                    current_window_start, 
                    actual_window_end, 
                    TIME_INTERVAL
                )
                
                if not single_record:
                    raise ValueError(f"create_single_record returned None for window {window_count}")
                
                if not isinstance(single_record, dict):
                    raise ValueError(f"create_single_record returned invalid type {type(single_record)} for window {window_count}")
                
                # Validate required fields in created record
                required_fields = ['PIPELINE_ID', 'TARGET_DAY', 'WINDOW_START_TIME', 'WINDOW_END_TIME']
                missing_fields = [field for field in required_fields if field not in single_record or single_record[field] is None]
                if missing_fields:
                    raise ValueError(f"Created record missing required fields: {missing_fields}")
                
                # Append to records list
                records_list.append(single_record)
                
                logger.debug(f" Window {window_count} created successfully - PIPELINE_ID: {single_record.get('PIPELINE_ID', 'Unknown')}")
                
                # Move to next window
                current_window_start = actual_window_end
                
                # Break if we've reached the end
                if actual_window_end >= day_end:
                    logger.debug(f"Reached end of day after window {window_count}")
                    break
                
                # Safety check to prevent infinite loops
                if window_count > 1000:  # Reasonable upper limit
                    raise ValueError(f"Too many windows generated ({window_count}). Possible infinite loop detected.")
                
            except Exception as e:
                logger.error(f" Error creating window {window_count}: {str(e)}")
                raise
        
        # Final validation
        if not records_list:
            raise ValueError("No records were created")
        
        if len(records_list) != window_count:
            logger.warning(f"Records list length ({len(records_list)}) doesn't match window count ({window_count})")
        
        logger.info("=" * 60)
        logger.info(" RECORD CREATION COMPLETED SUCCESSFULLY")
        logger.info(f"TARGET_DAY: {TARGET_DAY_str}")
        logger.info(f"Records created: {len(records_list)}")
        logger.info(f"Expected windows: {expected_windows}")
        logger.info(f"Granularity: {expected_granularity} ({granularity_seconds}s)")
        logger.info(f"Time range: {day_start} to {day_end}")
        
        # Log sample record for debugging
        if records_list:
            sample_record = records_list[0]
            logger.info(f"Sample record PIPELINE_ID: {sample_record.get('PIPELINE_ID', 'Unknown')}")
            logger.info(f"Sample record TIME_INTERVAL: {sample_record.get('TIME_INTERVAL', 'Unknown')}")
        
        logger.info("=" * 60)
        
        return records_list
        
    except Exception as e:
        logger.error(f" Failed to create records for TARGET_DAY '{TARGET_DAY}': {str(e)}")
        raise


def _validate_create_all_records_inputs(TARGET_DAY, config):
    """
    Validate inputs for create_all_records_for_target_day
    
    Args:
        TARGET_DAY: Target day to validate
        config: Configuration to validate
        
    Raises:
        ValueError: If validation fails
    """
    
    try:
        logger.info("Validating inputs for create_all_records_for_target_day...")
        
        # Validate TARGET_DAY
        if not TARGET_DAY:
            raise ValueError("TARGET_DAY is required and cannot be None or empty")
        
        TARGET_DAY_str = str(TARGET_DAY)
        if not TARGET_DAY_str.strip():
            raise ValueError("TARGET_DAY cannot be empty after string conversion")
        
        # Basic TARGET_DAY format validation (should be YYYY-MM-DD or parseable)
        try:
            parsed_date = pendulum.parse(TARGET_DAY_str)
            logger.debug(f"TARGET_DAY '{TARGET_DAY_str}' parsed successfully to {parsed_date}")
        except Exception as e:
            raise ValueError(f"TARGET_DAY '{TARGET_DAY_str}' is not a valid date format: {str(e)}")
        
        # Validate config
        if not config:
            raise ValueError("Config is required and cannot be None or empty")
        
        if not isinstance(config, dict):
            raise ValueError(f"Config must be a dictionary, got {type(config)}")
        
        # Check required config fields
        required_config_fields = ['PIPELINE_NAME', 'index_group', 'index_name', 'index_id', 's3_bucket', 's3_prefix_list', 'database.schema.table']
        missing_config_fields = [field for field in required_config_fields if not config.get(field)]
        if missing_config_fields:
            raise ValueError(f"Config missing required fields: {missing_config_fields}")
        
        # Validate optional fields with defaults
        expected_granularity = config.get("expected_granularity", "1h")
        if not expected_granularity:
            raise ValueError("expected_granularity cannot be empty")
        
        timezone = config.get('timezone', 'UTC')
        if not timezone:
            raise ValueError("timezone cannot be empty")
        
        # Test timezone validity
        try:
            pendulum.now(timezone)
        except Exception as e:
            raise ValueError(f"Invalid timezone '{timezone}': {str(e)}")
        
        logger.info(" Input validation passed")
        logger.info(f"TARGET_DAY: {TARGET_DAY_str}")
        logger.info(f"Expected granularity: {expected_granularity}")
        logger.info(f"Timezone: {timezone}")
        
    except Exception as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise


def rebuild_all_records_for_given_target_day(record, config):
    """
    Wrapper function: Extract TARGET_DAY from record and delegate with error handling
    
    Args:
        record: Original record dictionary
        config: Configuration dictionary
        
    Returns:
        List of complete records for the entire day
        
    Raises:
        ValueError: If inputs are invalid
        Exception: If record creation fails
    """
    
    try:
        logger.info("Rebuilding all records for given target day...")
        
        # Validate inputs
        if not record:
            raise ValueError("Record is required and cannot be None or empty")
        
        if not isinstance(record, dict):
            raise ValueError(f"Record must be a dictionary, got {type(record)}")
        
        TARGET_DAY = record.get("TARGET_DAY")
        PIPELINE_ID = record.get("PIPELINE_ID", "Unknown")
        
        logger.info(f"Rebuilding records for TARGET_DAY: {TARGET_DAY}, original PIPELINE_ID: {PIPELINE_ID}")
        
        if not TARGET_DAY:
            raise ValueError("TARGET_DAY is required in record and cannot be None or empty")
        
        # Delegate to main function
        result = create_all_records_for_target_day(TARGET_DAY, config)
        
        logger.info(f" Successfully rebuilt {len(result) if result else 0} records for TARGET_DAY: {TARGET_DAY}")
        
        return result
        
    except Exception as e:
        logger.error(f" Failed to rebuild all records for given target day: {str(e)}")
        logger.error(f"Original PIPELINE_ID: {record.get('PIPELINE_ID', 'Unknown') if record else 'Unknown'}")
        logger.error(f"TARGET_DAY: {record.get('TARGET_DAY', 'Unknown') if record else 'Unknown'}")
        raise


# Additional utility function for validation
def validate_granularity_format(granularity: str) -> bool:
    """
    Validate granularity format without parsing (for pre-validation)
    
    Args:
        granularity: Granularity string to validate
        
    Returns:
        bool: True if format is valid, False otherwise
    """
    
    try:
        if not granularity or not isinstance(granularity, str):
            return False
        
        granularity = granularity.strip().lower()
        if not granularity:
            return False
        
        pattern = r'^(\d+[dhms])+$'
        return bool(re.match(pattern, granularity))
        
    except Exception:
        return False


def calculate_expected_windows(TARGET_DAY: str, granularity_seconds: int, timezone: str = 'UTC') -> int:
    """
    Calculate expected number of windows for a target day
    
    Args:
        TARGET_DAY: Target day string
        granularity_seconds: Granularity in seconds
        timezone: Timezone for calculations
        
    Returns:
        int: Expected number of windows
    """
    
    try:
        day_start = pendulum.parse(f"{TARGET_DAY}T00:00:00").in_timezone(timezone)
        day_end = pendulum.parse(f"{TARGET_DAY}T23:59:59").in_timezone(timezone).add(seconds=1)
        
        total_day_seconds = int((day_end - day_start).total_seconds())
        expected_windows = (total_day_seconds + granularity_seconds - 1) // granularity_seconds
        
        return expected_windows
        
    except Exception as e:
        logger.error(f"Error calculating expected windows: {str(e)}")
        raise