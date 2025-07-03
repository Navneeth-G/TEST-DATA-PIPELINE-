# framework/tasks/target_day_processor.py

import pendulum
from typing import Dict, List, Set
from data_pipeline_project.framework.snowflake.snowflake_functions import (
    get_unprocessed_records, 
    get_target_days_for_pipeline,
    check_target_day_exists  
)
from data_pipeline_project.framework.tasks.create_records_and_insert import (
    create_and_insert_records_bulk,
    validate_config,
    parse_granularity_to_seconds,
    generate_source_complete_category
)

def extract_target_days_from_unprocessed(unprocessed_records: List[Dict], timezone: str = 'UTC') -> List[str]:
    """
    Extract unique target days from unprocessed records
    
    Args:
        unprocessed_records: List of record dictionaries with TARGET_DAY field
        timezone: Timezone for date consistency
        
    Returns:
        List of unique target day strings (YYYY-MM-DD format) sorted ascending
    """
    
    if not unprocessed_records:
        return []
    
    target_days = set()
    
    for record in unprocessed_records:
        target_day = record.get('TARGET_DAY')
        if target_day is None:
            continue
            
        # Handle different TARGET_DAY formats
        if isinstance(target_day, str):
            # Already string format
            date_str = target_day
        else:
            # Convert pendulum/datetime to date string
            if hasattr(target_day, 'to_date_string'):
                date_str = target_day.to_date_string()
            else:
                # Parse and extract date
                parsed_date = pendulum.parse(str(target_day))
                date_str = parsed_date.to_date_string()
        
        target_days.add(date_str)
    
    # Return sorted list
    return sorted(list(target_days))


def find_missing_days_in_range(processed_days: List[str], start_date: str = None, end_date: str = None) -> List[str]:
    """
    Find missing days between start and end dates
    
    Args:
        processed_days: List of processed target days (YYYY-MM-DD format)
        start_date: Start date (YYYY-MM-DD). If None, uses min processed day
        end_date: End date (YYYY-MM-DD). If None, uses max processed day
        
    Returns:
        List of missing target days (YYYY-MM-DD format) sorted ascending
    """
    
    if not processed_days:
        return []
    
    # Convert to pendulum dates for easier manipulation
    processed_dates = [pendulum.parse(day).date() for day in processed_days]
    processed_dates_set = set(processed_dates)
    
    # Determine date range
    if start_date is None:
        start_date_obj = min(processed_dates)
    else:
        start_date_obj = pendulum.parse(start_date).date()
    
    if end_date is None:
        end_date_obj = max(processed_dates)
    else:
        end_date_obj = pendulum.parse(end_date).date()
    
    # Find missing days in range
    missing_days = []
    current_date = start_date_obj
    
    while current_date <= end_date_obj:
        if current_date not in processed_dates_set:
            missing_days.append(current_date.to_date_string())
        current_date = current_date.add(days=1)
    
    return missing_days


def run_daily_processing(config: Dict, granularity: str) -> Dict:
    """
    Convenience function for daily processing - handles both unprocessed and gaps
    
    Args:
        config: Pipeline configuration dictionary
        granularity: Time granularity like "1h", "30m", "1d2h30m"
        
    Returns:
        Dictionary with processing results
    """
    
    return process_unprocessed_and_gaps(
        config=config,
        granularity=granularity,
        fill_gaps=True
    )



def find_gaps_in_processed_days(processed_days: List[str]) -> List[str]:
    """
    Find gaps between consecutive processed days - your efficient approach
    
    Args:
        processed_days: List of processed target days (YYYY-MM-DD format)
        
    Returns:
        List of missing target days (YYYY-MM-DD format) sorted ascending
        
    Algorithm:
        1. Sort processed days (remove duplicates)
        2. Generate expected consecutive sequence from min to max
        3. Use set difference to find missing days
    """
    
    if len(processed_days) <= 1:
        return []
    
    # Convert to pendulum dates, remove duplicates, and sort
    processed_dates = sorted(set(pendulum.parse(day).date() for day in processed_days))
    
    # Generate expected consecutive sequence
    start_date = processed_dates[0]
    end_date = processed_dates[-1]
    days_diff = (end_date - start_date).days + 1
    
    # Create expected days set
    expected_days_set = {
        start_date.add(days=i).to_date_string() 
        for i in range(days_diff)
    }
    
    # Create processed days set  
    processed_days_set = {date.to_date_string() for date in processed_dates}
    
    # Find missing days using set difference
    missing_days = sorted(expected_days_set - processed_days_set)
    
    return missing_days


def process_unprocessed_and_gaps(config: Dict, granularity: str) -> Dict:
    """
    Process unprocessed records and fill ALL gaps - no exceptions
    
    Args:
        config: Pipeline configuration dictionary
        granularity: Time granularity like "1h", "30m", "1d2h30m"
        
    Returns:
        Dictionary with processing results
        
    Logic:
        1. Process all unprocessed days (CONTINUITY_CHECK_PERFORMED = NO)
        2. Get all processed days (CONTINUITY_CHECK_PERFORMED = YES) 
        3. Find ALL gaps between earliest and latest processed days
        4. Fill every single gap - no matter how many
    """
    
    try:
        # Validate config
        # validate_config(config)
        
        print("=== Starting Complete Target Day Processing ===")
        
        # Step 1: Get and process unprocessed records
        print("Step 1: Getting unprocessed records...")
        unprocessed_records = get_unprocessed_records(config)
        
        unprocessed_days = extract_target_days_from_unprocessed(
            unprocessed_records, 
            config.get('timezone', 'UTC')
        )
        
        print(f"Found {len(unprocessed_records)} unprocessed records")
        print(f"Unique unprocessed target days: {len(unprocessed_days)}")
        if unprocessed_days:
            print(f"Unprocessed days: {min(unprocessed_days)} to {max(unprocessed_days)}")
        
        # Step 2: Process unprocessed days
        unprocessed_result = None
        if unprocessed_days:
            print(f"\nStep 2: Processing {len(unprocessed_days)} unprocessed days...")
            unprocessed_result = create_and_insert_records_bulk(
                unprocessed_days, granularity, config
            )
            print(f"Unprocessed days result: {unprocessed_result['successful_days']}/{len(unprocessed_days)} successful")
        else:
            print("\nStep 2: No unprocessed days found")
        
        time.sleep(20)
        # Step 3: Get all processed days and find gaps
        print(f"\nStep 3: Finding ALL gaps in processed days...")
        processed_days = get_target_days_for_pipeline(config)
        
        print(f"Found {len(processed_days)} total processed days")
        
        gap_days = []
        gap_result = None
        
        if processed_days:
            print(f"Processed days range: {min(processed_days)} to {max(processed_days)}")
            
            # Find ALL gaps using the efficient method
            gap_days = find_gaps_in_processed_days_efficient(processed_days)
            
            print(f"Found {len(gap_days)} gap days")
            if gap_days:
                print(f"Gap days range: {min(gap_days)} to {max(gap_days)}")
                print(f"Sample gaps: {gap_days[:10]}{'...' if len(gap_days) > 10 else ''}")
            
            # Remove any overlap with unprocessed days (avoid double processing)
            original_gap_count = len(gap_days)
            gap_days = [day for day in gap_days if day not in unprocessed_days]
            
            if original_gap_count != len(gap_days):
                print(f"Removed {original_gap_count - len(gap_days)} overlapping days")
            
            # Process ALL gaps - no conditions
            if gap_days:
                print(f"\nStep 4: Processing {len(gap_days)} gap days...")
                gap_result = create_and_insert_records_bulk(
                    gap_days, granularity, config
                )
                print(f"Gap days result: {gap_result['successful_days']}/{len(gap_days)} successful")
            else:
                print("\nStep 4: No gap days to process (after removing overlaps)")
        else:
            print("No processed days found - this might be the first run")
        
        # Compile results
        total_days_processed = len(unprocessed_days) + len(gap_days)
        successful_unprocessed = unprocessed_result['successful_days'] if unprocessed_result else 0
        successful_gaps = gap_result['successful_days'] if gap_result else 0
        total_successful = successful_unprocessed + successful_gaps
        
        overall_result = {
            'processing_summary': {
                'total_unprocessed_days': len(unprocessed_days),
                'total_gap_days': len(gap_days),
                'total_days_processed': total_days_processed,
                'total_successful_days': total_successful,
                'granularity': granularity,
                'execution_completed_at': pendulum.now().to_iso8601_string()
            },
            'unprocessed_results': unprocessed_result,
            'gap_results': gap_result,
            'unprocessed_days': unprocessed_days,
            'gap_days': gap_days
        }
        
        print(f"\n=== Processing Complete ===")
        print(f"Total days processed: {total_days_processed}")
        print(f"Total successful: {total_successful}")
        print(f"  - Unprocessed: {successful_unprocessed}/{len(unprocessed_days)}")
        print(f"  - Gaps filled: {successful_gaps}/{len(gap_days)}")
        
        return overall_result
        
    except Exception as e:
        raise Exception(f"Failed to process unprocessed records and gaps: {e}")




def calculate_target_day_from_time_back(timezone: str, x_time_back: str) -> str:
    """
    Calculate target day based on current time - x_time_back
    
    Args:
        timezone: Target timezone (e.g., 'UTC', 'America/New_York')
        x_time_back: Time to go back (e.g., "1d", "2d", "48h")
        
    Returns:
        Target day as string in YYYY-MM-DD format
    """
    
    try:
        # Get current time in specified timezone
        current_time = pendulum.now(timezone)
        
        # Parse time back to seconds
        seconds_back = parse_granularity_to_seconds(x_time_back)
        
        # Calculate target time
        target_time = current_time.subtract(seconds=seconds_back)
        
        # Extract date part
        target_day = target_time.to_date_string()  # YYYY-MM-DD
        
        return target_day
        
    except Exception as e:
        raise Exception(f"Failed to calculate target day: {e}")


def process_new_target_day(config: Dict, granularity: str, x_time_back: str) -> Dict:
    """
    Process new target day calculated from current time - x_time_back
    Only creates records if target day doesn't already exist
    
    Args:
        config: Pipeline configuration dictionary
        granularity: Time granularity like "1h", "30m", "1d2h30m"
        x_time_back: Time to go back from current time (e.g., "1d", "2d", "48h")
        
    Returns:
        Dictionary with processing results
        
    Logic (very similar to process_unprocessed_and_gaps):
        1. Calculate target_day = current_time(timezone) - x_time_back
        2. Generate source_complete_category for checking
        3. Check if target_day already exists in Snowflake
        4. If exists: Skip processing
        5. If not exists: Create and insert records for that day
    """
    
    try:
        # Validate config
        validate_config(config)
        
        # Validate inputs
        if not granularity:
            raise ValueError("granularity cannot be empty")
        if not x_time_back:
            raise ValueError("x_time_back cannot be empty")
        
        print("=== Starting New Target Day Processing ===")
        
        # Step 1: Calculate target day
        timezone = config.get('timezone', 'UTC')
        target_day = calculate_target_day_from_time_back(timezone, x_time_back)
        
        print(f"Calculated target day: {target_day}")
        print(f"Based on: current_time({timezone}) - {x_time_back}")
        
        # Step 2: Generate source_complete_category (needed for existence check)
        target_date = pendulum.parse(target_day).date()
        dummy_window_start = pendulum.parse(f"{target_date.to_date_string()}T00:00:00").in_timezone(timezone)
        dummy_window_end = dummy_window_start.add(hours=1)
        
        source_complete_category = generate_source_complete_category(
            config, dummy_window_start, dummy_window_end
        )
        
        # Add to config for checking
        config_with_category = config.copy()
        config_with_category['source_complete_category'] = source_complete_category
        
        print(f"Generated source_complete_category: {source_complete_category}")
        
        # Step 3: Check if target day already exists (same pattern as other functions)
        exists = check_target_day_exists(config_with_category, target_day)
        
        if exists:
            # Target day already processed - skip (same pattern)
            print(f" Target day {target_day} already exists - skipping processing")
            
            result = {
                'processing_summary': {
                    'target_day': target_day,
                    'x_time_back': x_time_back,
                    'timezone': timezone,
                    'granularity': granularity,
                    'source_complete_category': source_complete_category,
                    'already_exists': True,
                    'total_days_processed': 0,
                    'total_successful_days': 0,
                    'processing_skipped': True,
                    'execution_completed_at': pendulum.now().to_iso8601_string()
                },
                'creation_results': {
                    'total_days_requested': 0,
                    'successful_days': 0,
                    'failed_days': 0,
                    'total_records_created': 0,
                    'granularity': granularity,
                    'day_results': []
                }
            }
            
            return result
        
        # Step 4: Target day doesn't exist - create records (same pattern as other functions)
        print(f" Target day {target_day} not found - creating records...")
        
        creation_result = create_and_insert_records_bulk([target_day], granularity, config)
        
        # Compile results (same pattern)
        total_successful = creation_result['successful_days']
        
        result = {
            'processing_summary': {
                'target_day': target_day,
                'x_time_back': x_time_back,
                'timezone': timezone,
                'granularity': granularity,
                'source_complete_category': source_complete_category,
                'already_exists': False,
                'total_days_processed': 1,
                'total_successful_days': total_successful,
                'processing_skipped': False,
                'execution_completed_at': pendulum.now().to_iso8601_string()
            },
            'creation_results': creation_result
        }
        
        if total_successful > 0:
            print(f" Successfully created {creation_result['total_records_created']} records for {target_day}")
        else:
            print(f" Failed to create records for {target_day}")
        
        print(f"\n=== Processing Complete ===")
        print(f"Target day: {target_day}")
        print(f"Records created: {creation_result['total_records_created']}")
        print(f"Success: {total_successful}/1 days")
        
        return result
        
    except Exception as e:
        raise Exception(f"Failed to process new target day: {e}")





# # Example usage
# if __name__ == "__main__":
    
#     # Example config (same as before)
#     config = {
#         'pipeline_name': 'elasticsearch_to_snowflake',
#         'index_group': 'LOGS',
#         'index_name': 'APPLICATION_LOGS',
#         's3_bucket': 'my-data-bucket',
#         's3_prefix_list': ['raw', 'elasticsearch', 'logs'],
#         'index_id': 'app_logs_v1',
#         'database.schema.table': 'PROD.ANALYTICS.APPLICATION_LOGS',
#         'pipeline_priority': 1.0,
#         'timezone': 'UTC',
#         'CAN_ACCESS_HISTORICAL_DATA': 'YES',
#         'source_complete_category': 'LOGS|APPLICATION_LOGS',
#         'sf_drive_config': {
#             'sf_user': 'service_account',
#             'sf_password': 'password',
#             'sf_account': 'company.snowflakecomputing.com',
#             'sf_warehouse': 'COMPUTE_WH',
#             'sf_database': 'ANALYTICS',
#             'sf_schema': 'PIPELINE_STATE',
#             'sf_table': 'pipeline_states',
#             'timestamp_fields': ['WINDOW_START_TIME', 'WINDOW_END_TIME', 'RECORD_FIRST_CREATED_TIME', 'RECORD_LAST_UPDATED_TIME']
#         }
#     }
    
#     # Daily processing
#     print("=== Daily Processing Example ===")
#     daily_result = run_daily_processing(config, "1h")
    
#     # Backfill processing
#     print("\n=== Backfill Processing Example ===")
#     backfill_result = run_backfill_processing(config, "1h", "2025-01-01", "2025-01-31")