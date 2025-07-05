# pipleine_logic/handle_drive_table_records/handle_record_with_continuity_check_no.py
import logging
from typing import List, Dict, Union, Set

# Configure logging for Airflow
logger = logging.getLogger(__name__)

from pipleine_logic.handle_drive_table_records.record_creation.create_single_drive_record import rebuild_single_record_for_given_window
from pipleine_logic.handle_drive_table_records.record_creation.create_multiple_drive_record import rebuild_all_records_for_given_target_day, create_all_records_for_target_day


def handle_record_with_continuity_check_as_no(record, config, target_days_list_with_continuity_check_as_yes):
    """
    Main function: Handle record with CONTINUITY_CHECK_PERFORMED = NO
    Routes through the complete decision tree flow with comprehensive error handling
    
    Args:
        record: Single record dictionary with CONTINUITY_CHECK_PERFORMED = NO
        config: Configuration dictionary
        target_days_list_with_continuity_check_as_yes: Set/List of target days with CONTINUITY_CHECK_PERFORMED = YES
        
    Returns:
        List of records (single record for Branch 1A, multiple for Branch 1B/1C)
        
    Raises:
        ValueError: If input validation fails
        Exception: If record processing fails
    """
    
    try:
        logger.info("=" * 50)
        logger.info("HANDLING RECORD WITH CONTINUITY_CHECK_PERFORMED = NO")
        logger.info("=" * 50)
        
        # Step 1: Validate inputs
        _validate_inputs(record, config, target_days_list_with_continuity_check_as_yes)
        
        TARGET_DAY = record.get("TARGET_DAY")
        PIPELINE_ID = record.get("PIPELINE_ID", "Unknown")
        
        logger.info(f"Processing record - TARGET_DAY: {TARGET_DAY}, PIPELINE_ID: {PIPELINE_ID}")
        
        # Step 2: Check TARGET_DAY Status
        logger.info("Step 1: Checking TARGET_DAY status...")
        target_day_processed = _check_target_day_status(TARGET_DAY, target_days_list_with_continuity_check_as_yes)
        
        if target_day_processed:
            logger.info(f" TARGET_DAY {TARGET_DAY} already processed - checking windows")
            
            # Step 3: Check Window Times
            logger.info("Step 2: Checking window times...")
            window_result = _check_window_times(record)
            
            if window_result["has_complete_windows"]:
                # Branch 1A: Rebuild single record
                logger.info(" Branch 1A: Rebuilding single record with windows")
                return _execute_branch_1a(record, config)
            else:
                # Branch 1B: Missing window data
                logger.info(" Branch 1B: Missing window data - creating all records for day")
                return _execute_branch_1b(record, config)
        
        else:
            # Branch 1C: New target day
            logger.info(f" Branch 1C: New TARGET_DAY {TARGET_DAY} - creating all records for day")
            return _execute_branch_1c(TARGET_DAY, config)
    
    except Exception as e:
        logger.error(f" Error handling record with CONTINUITY_CHECK_PERFORMED = NO")
        logger.error(f"TARGET_DAY: {record.get('TARGET_DAY', 'Unknown')}")
        logger.error(f"PIPELINE_ID: {record.get('PIPELINE_ID', 'Unknown')}")
        logger.error(f"Error: {str(e)}")
        raise


def _validate_inputs(record, config, target_days_list_with_continuity_check_as_yes):
    """
    Validate all input parameters
    
    Args:
        record: Record dictionary to validate
        config: Configuration dictionary to validate
        target_days_list_with_continuity_check_as_yes: Target days list to validate
        
    Raises:
        ValueError: If validation fails
    """
    
    try:
        logger.info("Validating inputs...")
        
        # Validate record
        if not record:
            raise ValueError("Record is None or empty")
        
        if not isinstance(record, dict):
            raise ValueError(f"Record must be a dictionary, got {type(record)}")
        
        # Check CONTINUITY_CHECK_PERFORMED
        continuity_check = record.get("CONTINUITY_CHECK_PERFORMED")
        if continuity_check != "NO":
            raise ValueError(f"This function only handles records with CONTINUITY_CHECK_PERFORMED = NO, got '{continuity_check}'")
        
        # Check TARGET_DAY
        TARGET_DAY = record.get("TARGET_DAY")
        if not TARGET_DAY:
            raise ValueError("Record is missing TARGET_DAY field")
        
        # Validate config
        if not config:
            raise ValueError("Config is None or empty")
        
        if not isinstance(config, dict):
            raise ValueError(f"Config must be a dictionary, got {type(config)}")
        
        # Validate target_days_list
        if target_days_list_with_continuity_check_as_yes is None:
            raise ValueError("target_days_list_with_continuity_check_as_yes is None")
        
        # Convert to list if it's a set for easier processing
        if not isinstance(target_days_list_with_continuity_check_as_yes, (list, set)):
            raise ValueError(f"target_days_list_with_continuity_check_as_yes must be a list or set, got {type(target_days_list_with_continuity_check_as_yes)}")
        
        logger.info(" Input validation passed")
        logger.info(f"Record TARGET_DAY: {TARGET_DAY}")
        logger.info(f"Record PIPELINE_ID: {record.get('PIPELINE_ID', 'Unknown')}")
        logger.info(f"Processed target days count: {len(target_days_list_with_continuity_check_as_yes)}")
        
    except Exception as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise


def _check_target_day_status(TARGET_DAY, target_days_list_with_continuity_check_as_yes):
    """
    Check if TARGET_DAY exists in the processed target days list
    
    Args:
        TARGET_DAY: Target day to check
        target_days_list_with_continuity_check_as_yes: List/Set of processed target days
        
    Returns:
        bool: True if TARGET_DAY is already processed, False otherwise
    """
    
    try:
        logger.info(f"Checking if TARGET_DAY {TARGET_DAY} is in processed list...")
        
        # Handle both string and date comparisons
        target_day_exists = False
        
        # Convert target_days_list to a set of strings for comparison
        processed_days_str = set()
        for day in target_days_list_with_continuity_check_as_yes:
            if hasattr(day, 'strftime'):
                # Date object
                processed_days_str.add(day.strftime('%Y-%m-%d'))
            elif hasattr(day, 'to_date_string'):
                # Pendulum date
                processed_days_str.add(day.to_date_string())
            else:
                # String
                processed_days_str.add(str(day))
        
        # Convert TARGET_DAY to string for comparison
        target_day_str = str(TARGET_DAY)
        if hasattr(TARGET_DAY, 'strftime'):
            target_day_str = TARGET_DAY.strftime('%Y-%m-%d')
        elif hasattr(TARGET_DAY, 'to_date_string'):
            target_day_str = TARGET_DAY.to_date_string()
        
        target_day_exists = target_day_str in processed_days_str
        
        logger.info(f"TARGET_DAY {TARGET_DAY} ({'exists' if target_day_exists else 'does not exist'}) in processed list")
        logger.debug(f"Processed days: {sorted(list(processed_days_str))}")
        
        return target_day_exists
        
    except Exception as e:
        logger.error(f"Error checking TARGET_DAY status: {str(e)}")
        logger.error(f"TARGET_DAY: {TARGET_DAY}")
        logger.error(f"Processed days count: {len(target_days_list_with_continuity_check_as_yes)}")
        raise


def _check_window_times(record):
    """
    Check if record has complete window time information
    
    Args:
        record: Record dictionary to check
        
    Returns:
        dict: Result containing window status information
    """
    
    try:
        logger.info("Checking window times...")
        
        WINDOW_START_TIME = record.get("WINDOW_START_TIME")
        WINDOW_END_TIME = record.get("WINDOW_END_TIME")
        
        logger.info(f"WINDOW_START_TIME: {WINDOW_START_TIME}")
        logger.info(f"WINDOW_END_TIME: {WINDOW_END_TIME}")
        
        # Check if both window times are present and not None
        has_start_time = WINDOW_START_TIME is not None
        has_end_time = WINDOW_END_TIME is not None
        has_complete_windows = has_start_time and has_end_time
        
        result = {
            "has_start_time": has_start_time,
            "has_end_time": has_end_time,
            "has_complete_windows": has_complete_windows,
            "WINDOW_START_TIME": WINDOW_START_TIME,
            "WINDOW_END_TIME": WINDOW_END_TIME
        }
        
        logger.info(f"Window check result: {result}")
        
        if has_complete_windows:
            logger.info(" Record has complete window times")
        else:
            missing_fields = []
            if not has_start_time:
                missing_fields.append("WINDOW_START_TIME")
            if not has_end_time:
                missing_fields.append("WINDOW_END_TIME")
            logger.info(f" Record missing window fields: {missing_fields}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error checking window times: {str(e)}")
        raise


def _execute_branch_1a(record, config):
    """
    Execute Branch 1A: Rebuild single record with windows
    
    Args:
        record: Record with complete window information
        config: Configuration dictionary
        
    Returns:
        List containing single rebuilt record
    """
    
    try:
        logger.info("Executing Branch 1A: Rebuilding single record with windows")
        
        TARGET_DAY = record.get("TARGET_DAY")
        PIPELINE_ID = record.get("PIPELINE_ID", "Unknown")
        
        logger.info(f"Rebuilding single record for TARGET_DAY: {TARGET_DAY}, PIPELINE_ID: {PIPELINE_ID}")
        
        # Call the rebuild function
        rebuilt_record = rebuild_single_record_for_given_window(record, config)
        
        if not rebuilt_record:
            raise ValueError("rebuild_single_record_for_given_window returned None or empty record")
        
        if not isinstance(rebuilt_record, dict):
            raise ValueError(f"rebuild_single_record_for_given_window returned invalid type: {type(rebuilt_record)}")
        
        logger.info(" Branch 1A completed successfully")
        logger.info(f"Rebuilt record PIPELINE_ID: {rebuilt_record.get('PIPELINE_ID', 'Unknown')}")
        
        return [rebuilt_record]
        
    except Exception as e:
        logger.error(f" Branch 1A failed: {str(e)}")
        logger.error(f"TARGET_DAY: {record.get('TARGET_DAY', 'Unknown')}")
        logger.error(f"PIPELINE_ID: {record.get('PIPELINE_ID', 'Unknown')}")
        raise


def _execute_branch_1b(record, config):
    """
    Execute Branch 1B: Missing window data - rebuild all records for day
    
    Args:
        record: Record with missing window information
        config: Configuration dictionary
        
    Returns:
        List of rebuilt records for the entire day
    """
    
    try:
        logger.info("Executing Branch 1B: Missing window data - creating all records for day")
        
        TARGET_DAY = record.get("TARGET_DAY")
        PIPELINE_ID = record.get("PIPELINE_ID", "Unknown")
        
        logger.info(f"Rebuilding all records for TARGET_DAY: {TARGET_DAY}, original PIPELINE_ID: {PIPELINE_ID}")
        
        # Call the rebuild all function
        rebuilt_records = rebuild_all_records_for_given_target_day(record, config)
        
        if not rebuilt_records:
            raise ValueError("rebuild_all_records_for_given_target_day returned None or empty list")
        
        if not isinstance(rebuilt_records, list):
            raise ValueError(f"rebuild_all_records_for_given_target_day returned invalid type: {type(rebuilt_records)}")
        
        if len(rebuilt_records) == 0:
            raise ValueError("rebuild_all_records_for_given_target_day returned empty list")
        
        logger.info(" Branch 1B completed successfully")
        logger.info(f"Rebuilt {len(rebuilt_records)} records for TARGET_DAY: {TARGET_DAY}")
        
        # Log sample of rebuilt records
        if rebuilt_records:
            sample_record = rebuilt_records[0]
            logger.info(f"Sample rebuilt record PIPELINE_ID: {sample_record.get('PIPELINE_ID', 'Unknown')}")
        
        return rebuilt_records
        
    except Exception as e:
        logger.error(f" Branch 1B failed: {str(e)}")
        logger.error(f"TARGET_DAY: {record.get('TARGET_DAY', 'Unknown')}")
        logger.error(f"PIPELINE_ID: {record.get('PIPELINE_ID', 'Unknown')}")
        raise


def _execute_branch_1c(TARGET_DAY, config):
    """
    Execute Branch 1C: New target day - create all records for day
    
    Args:
        TARGET_DAY: New target day to create records for
        config: Configuration dictionary
        
    Returns:
        List of new records for the entire day
    """
    
    try:
        logger.info("Executing Branch 1C: New TARGET_DAY - creating all records for day")
        
        logger.info(f"Creating all records for new TARGET_DAY: {TARGET_DAY}")
        
        # Call the create all function
        new_records = create_all_records_for_target_day(TARGET_DAY, config)
        
        if not new_records:
            raise ValueError("create_all_records_for_target_day returned None or empty list")
        
        if not isinstance(new_records, list):
            raise ValueError(f"create_all_records_for_target_day returned invalid type: {type(new_records)}")
        
        if len(new_records) == 0:
            raise ValueError("create_all_records_for_target_day returned empty list")
        
        logger.info(" Branch 1C completed successfully")
        logger.info(f"Created {len(new_records)} records for new TARGET_DAY: {TARGET_DAY}")
        
        # Log sample of new records
        if new_records:
            sample_record = new_records[0]
            logger.info(f"Sample new record PIPELINE_ID: {sample_record.get('PIPELINE_ID', 'Unknown')}")
        
        return new_records
        
    except Exception as e:
        logger.error(f" Branch 1C failed: {str(e)}")
        logger.error(f"TARGET_DAY: {TARGET_DAY}")
        raise


# Additional helper function for debugging
def log_decision_tree_summary(record, target_days_list_with_continuity_check_as_yes, branch_taken, result_count):
    """
    Log a summary of the decision tree path taken
    
    Args:
        record: Original record
        target_days_list_with_continuity_check_as_yes: Processed target days
        branch_taken: Which branch was executed (1A, 1B, 1C)
        result_count: Number of records returned
    """
    
    try:
        logger.info("=" * 50)
        logger.info("DECISION TREE SUMMARY")
        logger.info("=" * 50)
        
        TARGET_DAY = record.get("TARGET_DAY", "Unknown")
        PIPELINE_ID = record.get("PIPELINE_ID", "Unknown")
        
        logger.info(f"Original record - TARGET_DAY: {TARGET_DAY}, PIPELINE_ID: {PIPELINE_ID}")
        logger.info(f"Processed target days count: {len(target_days_list_with_continuity_check_as_yes)}")
        logger.info(f"Branch taken: {branch_taken}")
        logger.info(f"Result records count: {result_count}")
        
        branch_descriptions = {
            "1A": "Single record rebuild (TARGET_DAY exists + complete windows)",
            "1B": "Full day recreation (TARGET_DAY exists + missing windows)", 
            "1C": "Full day creation (New TARGET_DAY)"
        }
        
        logger.info(f"Branch description: {branch_descriptions.get(branch_taken, 'Unknown branch')}")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.warning(f"Error logging decision tree summary: {str(e)}")