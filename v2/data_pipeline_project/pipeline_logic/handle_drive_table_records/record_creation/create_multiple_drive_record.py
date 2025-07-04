# pipleine_logic/handle_drive_table_records/record_creation/create_multiple_drive_record.py
import re
import pendulum
from typing import Dict, List, Any
from pipleine_logic.handle_drive_table_records.record_creation.create_single_drive_record import create_single_record, seconds_to_readable


def parse_granularity_to_seconds(granularity: str) -> int:
    """
    Parse granularity string to seconds - supports complex formats
    
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
    
    if not granularity or not granularity.strip():
        raise ValueError("Granularity cannot be empty")
    
    # Remove spaces and convert to lowercase
    granularity = granularity.strip().lower()
    
    # Regex pattern to find all number+unit combinations
    pattern = r'(\d+)([dhms])'
    matches = re.findall(pattern, granularity)
    
    if not matches:
        raise ValueError(f"Invalid granularity format: '{granularity}'. Expected format: '1h', '30m', '1d2h30m40s', etc.")
    
    # Check for duplicate units
    units_found = [match[1] for match in matches]
    if len(units_found) != len(set(units_found)):
        raise ValueError(f"Duplicate units found in granularity: '{granularity}'")
    
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
        value = int(value_str)
        total_seconds += value * multipliers[unit]
    
    # Validate that we parsed the entire string (no leftover characters)
    reconstructed = ''.join([f"{value}{unit}" for value, unit in matches])
    if reconstructed != granularity:
        raise ValueError(f"Invalid characters in granularity: '{granularity}'. Only numbers and units (d,h,m,s) allowed.")
    
    return total_seconds


def create_all_records_for_target_day(TARGET_DAY, config):
    """
    Core function: Create all window records for a complete TARGET_DAY
    
    Args:
        TARGET_DAY: Target day string (YYYY-MM-DD)
        config: Configuration dictionary
        
    Returns:
        List of complete records for the entire day
    """
    
    if not TARGET_DAY:
        raise ValueError("TARGET_DAY is required")
    
    # Get expected granularity from config
    expected_granularity = config.get("expected_granularity", "1h")  # Default 1 hour
    
    # Parse granularity to seconds
    granularity_seconds = parse_granularity_to_seconds(expected_granularity)
    
    # Generate day boundaries
    tz = config.get('timezone', 'UTC')
    day_start = pendulum.parse(f"{TARGET_DAY}T00:00:00").in_timezone(tz)
    day_end = pendulum.parse(f"{TARGET_DAY}T23:59:59").in_timezone(tz).add(seconds=1)  # End of day
    
    # Generate all window records for the day
    records_list = []
    current_window_start = day_start
    
    while current_window_start < day_end:
        # Calculate window end
        proposed_window_end = current_window_start.add(seconds=granularity_seconds)
        
        # Apply day boundary constraint
        if proposed_window_end > day_end:
            actual_window_end = day_end
        else:
            actual_window_end = proposed_window_end
        
        # Calculate TIME_INTERVAL
        time_interval_seconds = int((actual_window_end - current_window_start).total_seconds())
        TIME_INTERVAL = seconds_to_readable(time_interval_seconds)
        
        # Create single record using your existing function
        single_record = create_single_record(
            config, 
            TARGET_DAY, 
            current_window_start, 
            actual_window_end, 
            TIME_INTERVAL
        )
        
        # Append to records list
        records_list.append(single_record)
        
        # Move to next window
        current_window_start = actual_window_end
        
        # Break if we've reached the end
        if actual_window_end >= day_end:
            break
    
    print(f"Created {len(records_list)} records for TARGET_DAY: {TARGET_DAY}")
    return records_list


def rebuild_all_records_for_given_target_day(record, config):
    """
    Wrapper function: Extract TARGET_DAY from record and delegate
    
    Args:
        record: Original record dictionary
        config: Configuration dictionary
        
    Returns:
        List of complete records for the entire day
    """
    TARGET_DAY = record.get("TARGET_DAY")
    if not TARGET_DAY:
        raise ValueError("TARGET_DAY is required in record")
    
    return create_all_records_for_target_day(TARGET_DAY, config)