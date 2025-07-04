# data_pipeline_project/framework/tasks/continuity_checker.py

import pendulum
import hashlib
from typing import Dict, Any, List, Tuple
from framework.utilities.time_utility import parse_granularity
from framework.snowflake_specific.state_operations import (
    select_newest_state_from_snowflake,
    insert_new_state_to_snowflake,
    update_existing_state_in_snowflake
)

def continuity_check_and_record_creations(config: Dict[str, Any], 
                                        expected_granularity: str, 
                                        x_time_back: str) -> bool:
    """
    Perform continuity check and create missing records
    
    Args:
        config: Pipeline configuration
        expected_granularity: Window size like "1h", "2h30m"
        x_time_back: How far back to process like "2d", "1w"
        
    Returns:
        bool: True if successful, False if failed
    """
    
    try:
        print("Starting continuity check and record creation...")
        
        # Step 1: Process unprocessed records
        print("Step 1: Processing unprocessed records")
        success = process_all_unprocessed_records(config, expected_granularity)
        if not success:
            print("Failed to process unprocessed records")
            return False
        
        # Step 2: Fill all gaps in processed sequence
        print("Step 2: Filling gaps in processed sequence")
        success = fill_all_gaps_in_processed_sequence(config, expected_granularity)
        if not success:
            print("Failed to fill gaps")
            return False
        
        # Step 3: Continue from latest processed day
        print("Step 3: Continuing from latest processed day")
        success = continue_from_latest_processed(config, expected_granularity, x_time_back)
        if not success:
            print("Failed to continue from latest")
            return False
        
        print("Continuity check and record creation completed successfully")
        return True
        
    except Exception as e:
        print(f"Error in continuity check: {e}")
        return False

def process_all_unprocessed_records(config: Dict[str, Any], expected_granularity: str) -> bool:
    """Process all records with CONTINUITY_CHECK_PERFORMED = NO"""
    
    try:
        # Get all unprocessed records
        unprocessed_records = get_unprocessed_records(config)
        
        if not unprocessed_records:
            print("No unprocessed records found")
            return True
        
        # Get unique target days
        unprocessed_days = list(set([record['TARGET_DAY'] for record in unprocessed_records]))
        unprocessed_days.sort()
        
        print(f"Found unprocessed days: {unprocessed_days}")
        
        # Process each day
        for target_day in unprocessed_days:
            print(f"Processing unprocessed day: {target_day}")
            
            # Create complete day records
            success = create_complete_day_records(config, target_day, expected_granularity)
            if not success:
                print(f"Failed to create records for day: {target_day}")
                return False
            
            # Mark day as processed
            mark_day_as_processed(config, target_day)
            print(f"Marked day as processed: {target_day}")
        
        return True
        
    except Exception as e:
        print(f"Error processing unprocessed records: {e}")
        return False

def fill_all_gaps_in_processed_sequence(config: Dict[str, Any], expected_granularity: str) -> bool:
    """Fill all gaps in the processed sequence"""
    
    try:
        # Get all processed days
        processed_days = get_all_processed_days(config)
        
        if len(processed_days) < 2:
            print("Not enough processed days to detect gaps")
            return True
        
        # Detect gaps
        gaps = detect_gaps_in_sequence(processed_days)
        
        if not gaps:
            print("No gaps found in processed sequence")
            return True
        
        print(f"Found gaps: {gaps}")
        
        # Fill each gap
        for gap_day in gaps:
            print(f"Filling gap day: {gap_day}")
            
            success = create_complete_day_records(config, gap_day, expected_granularity)
            if not success:
                print(f"Failed to create records for gap day: {gap_day}")
                return False
            
            mark_day_as_processed(config, gap_day)
            print(f"Filled and marked gap day: {gap_day}")
        
        return True
        
    except Exception as e:
        print(f"Error filling gaps: {e}")
        return False

def continue_from_latest_processed(config: Dict[str, Any], expected_granularity: str, x_time_back: str) -> bool:
    """Continue from latest processed day to target end day"""
    
    try:
        # Get latest processed day
        latest_day = get_latest_processed_day(config)
        
        if not latest_day:
            # No processed records - cold start
            print("No processed records found - starting cold start")
            return handle_cold_start(config, expected_granularity, x_time_back)
        
        # Calculate target end day
        current_time = pendulum.now(config.get('timezone', 'UTC'))
        x_time_back_seconds = parse_granularity(x_time_back)
        target_end_time = current_time.subtract(seconds=x_time_back_seconds)
        target_end_day = target_end_time.to_date_string()
        
        print(f"Latest processed day: {latest_day}")
        print(f"Target end day: {target_end_day}")
        
        # Fill from latest+1 to target_end_day
        start_date = pendulum.parse(latest_day).add(days=1)
        end_date = pendulum.parse(target_end_day)
        
        current_date = start_date
        while current_date <= end_date:
            fill_day = current_date.to_date_string()
            
            # Check if day already has records
            if not day_already_processed(config, fill_day):
                print(f"Creating records for continuation day: {fill_day}")
                
                success = create_complete_day_records(config, fill_day, expected_granularity)
                if not success:
                    print(f"Failed to create records for day: {fill_day}")
                    return False
                
                mark_day_as_processed(config, fill_day)
                print(f"Created and marked day: {fill_day}")
            else:
                print(f"Day already processed: {fill_day}")
            
            current_date = current_date.add(days=1)
        
        return True
        
    except Exception as e:
        print(f"Error continuing from latest: {e}")
        return False

def handle_cold_start(config: Dict[str, Any], expected_granularity: str, x_time_back: str) -> bool:
    """Handle cold start when no records exist"""
    
    try:
        print("Handling cold start")
        
        # Calculate start day
        current_time = pendulum.now(config.get('timezone', 'UTC'))
        x_time_back_seconds = parse_granularity(x_time_back)
        start_time = current_time.subtract(seconds=x_time_back_seconds)
        start_day = start_time.to_date_string()
        
        print(f"Cold start day: {start_day}")
        
        # Create records for start day
        success = create_complete_day_records(config, start_day, expected_granularity)
        if not success:
            print(f"Failed to create cold start records for: {start_day}")
            return False
        
        mark_day_as_processed(config, start_day)
        print(f"Cold start completed for day: {start_day}")
        
        return True
        
    except Exception as e:
        print(f"Error in cold start: {e}")
        return False

def create_complete_day_records(config: Dict[str, Any], target_day: str, expected_granularity: str) -> bool:
    """Create all window records for a complete day"""
    
    try:
        print(f"Creating complete day records for: {target_day}")
        
        # Parse granularity
        granularity_seconds = parse_granularity(expected_granularity)
        
        # Day boundaries
        tz = config.get('timezone', 'UTC')
        day_start = pendulum.parse(f"{target_day}T00:00:00").in_timezone(tz)
        day_end = pendulum.parse(f"{target_day}T23:59:59").in_timezone(tz)
        
        # Generate all windows for the day
        records = []
        current_window_start = day_start
        
        while current_window_start < day_end:
            # Calculate window end
            proposed_window_end = current_window_start.add(seconds=granularity_seconds)
            
            # Apply day boundary constraint
            if proposed_window_end > day_end:
                actual_window_end = day_end
            else:
                actual_window_end = proposed_window_end
            
            # Create single record
            record = create_single_window_record(config, current_window_start, actual_window_end, target_day)
            records.append(record)
            
            # Move to next window
            current_window_start = actual_window_end
            
            # Break if we've reached the end
            if actual_window_end >= day_end:
                break
        
        print(f"Created {len(records)} records for day: {target_day}")
        
        # Insert all records
        for record in records:
            success = insert_new_state_to_snowflake(record, config)
            if not success:
                print(f"Failed to insert record: {record['PIPELINE_ID']}")
                return False
        
        print(f"Successfully inserted all records for day: {target_day}")
        return True
        
    except Exception as e:
        print(f"Error creating day records: {e}")
        return False

def create_single_window_record(config: Dict[str, Any], window_start: pendulum.DateTime, 
                               window_end: pendulum.DateTime, target_day: str) -> Dict:
    """Create a single window record"""
    
    # Build complete categories
    source_complete, stage_complete, target_complete = build_complete_categories(config, window_start, window_end)
    
    # Build pipeline name
    pipeline_name = f"{config['SOURCE_NAME']}_{config['STAGE_NAME']}_{config['TARGET_NAME']}"
    
    # Generate pipeline ID
    pipeline_id = generate_pipeline_id(source_complete, stage_complete, target_complete, 
                                     pipeline_name, window_start, window_end)
    
    # Calculate time interval
    time_interval_seconds = int((window_end - window_start).total_seconds())
    time_interval = seconds_to_readable(time_interval_seconds)
    
    # Current timestamp
    current_time = pendulum.now(config.get('timezone', 'UTC'))
    
    # Build complete record
    record = {
        "PIPELINE_NAME": pipeline_name,
        "SOURCE_COMPLETE_CATEGORY": source_complete,
        "STAGE_COMPLETE_CATEGORY": stage_complete,
        "TARGET_COMPLETE_CATEGORY": target_complete,
        "PIPELINE_ID": pipeline_id,
        "TARGET_DAY": target_day,
        "WINDOW_START_TIME": window_start,
        "WINDOW_END_TIME": window_end,
        "TIME_INTERVAL": time_interval,
        "COMPLETED_PHASE": "NONE",
        "PIPELINE_STATUS": "PENDING",
        "PIPELINE_START_TIME": None,
        "PIPELINE_END_TIME": None,
        "PIPELINE_PRIORITY": config.get('pipeline_priority', 1.0),
        "CONTINUITY_CHECK_PERFORMED": "YES",  # Mark as processed
        "RECORD_FIRST_CREATED_TIME": current_time,
        "RECORD_LAST_UPDATED_TIME": current_time
    }
    
    return record

def build_complete_categories(config: Dict[str, Any], window_start: pendulum.DateTime, 
                            window_end: pendulum.DateTime) -> Tuple[str, str, str]:
    """Build pipe-separated complete category strings"""
    
    # SOURCE: static from config
    source_complete = f"{config['SOURCE_NAME']}|{config['SOURCE_CATEGORY']}|{config.get('index_name', 'default')}"
    
    # STAGE: dynamic with time-based path
    s3_prefix_list = config.get('s3_prefix_list', [])
    date_time_path = window_start.strftime('%Y-%m-%d/%H-%M')
    stage_path = f"s3://{config['STAGE_CATEGORY']}/{'/'.join(s3_prefix_list)}/{date_time_path}"
    stage_complete = f"{config['STAGE_NAME']}|{config['STAGE_CATEGORY']}|{stage_path}"
    
    # TARGET: dynamic with time-based path
    target_path = f"{'/'.join(s3_prefix_list)}/{date_time_path}"
    target_complete = f"{config['TARGET_NAME']}|{config['TARGET_CATEGORY']}|{target_path}"
    
    return source_complete, stage_complete, target_complete

def generate_pipeline_id(source_complete: str, stage_complete: str, target_complete: str,
                        pipeline_name: str, window_start: pendulum.DateTime, 
                        window_end: pendulum.DateTime) -> str:
    """Generate unique pipeline ID"""
    combined = f"{source_complete}|{stage_complete}|{target_complete}|{pipeline_name}|{window_start}|{window_end}"
    return hashlib.md5(combined.encode()).hexdigest()[:12]

def seconds_to_readable(seconds: int) -> str:
    """Convert seconds to HH:MM:SS format"""
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"

# Query helper functions
def get_unprocessed_records(config: Dict[str, Any]) -> List[Dict]:
    """Get all records with CONTINUITY_CHECK_PERFORMED = NO"""
    # Implementation depends on your Snowflake query function
    # Return list of unprocessed records
    pass

def get_all_processed_days(config: Dict[str, Any]) -> List[str]:
    """Get all TARGET_DAY values with CONTINUITY_CHECK_PERFORMED = YES"""
    # Implementation depends on your Snowflake query function
    # Return sorted list of processed days
    pass

def get_latest_processed_day(config: Dict[str, Any]) -> str:
    """Get latest TARGET_DAY with CONTINUITY_CHECK_PERFORMED = YES"""
    # Implementation depends on your Snowflake query function
    # Return latest processed day string
    pass

def detect_gaps_in_sequence(processed_days: List[str]) -> List[str]:
    """Detect missing days in processed sequence"""
    if len(processed_days) < 2:
        return []
    
    gaps = []
    processed_days.sort()
    
    for i in range(len(processed_days) - 1):
        current_day = pendulum.parse(processed_days[i])
        next_day = pendulum.parse(processed_days[i + 1])
        
        # Check for gaps
        check_day = current_day.add(days=1)
        while check_day < next_day:
            gaps.append(check_day.to_date_string())
            check_day = check_day.add(days=1)
    
    return gaps

def mark_day_as_processed(config: Dict[str, Any], target_day: str):
    """Mark all records for a day as CONTINUITY_CHECK_PERFORMED = YES"""
    # Implementation depends on your Snowflake update function
    pass

def day_already_processed(config: Dict[str, Any], target_day: str) -> bool:
    """Check if day already has processed records"""
    # Implementation depends on your Snowflake query function
    pass

# Usage example
if __name__ == "__main__":
    config = {
        'SOURCE_NAME': 'ELASTICSEARCH',
        'SOURCE_CATEGORY': 'CLUSTERIQ',
        'STAGE_NAME': 'AWS_S3', 
        'STAGE_CATEGORY': 'my-data-bucket',
        'TARGET_NAME': 'SNOWFLAKE',
        'TARGET_CATEGORY': 'PROD.ANALYTICS.SALES',
        'timezone': 'UTC',
        'pipeline_priority': 1.5,
        'index_name': 'pending_jobs',
        's3_prefix_list': ['raw_data', 'elasticsearch', 'daily_exports']
    }
    
    success = continuity_check_and_record_creations(config, "1h", "2d")
    print(f"Continuity check result: {success}")