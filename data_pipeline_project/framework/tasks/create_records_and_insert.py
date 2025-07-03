# data_pipeline_project/framework/tasks/create_records_and_insert.py

import pendulum
import hashlib
from typing import Dict, List, Tuple, Union
import re
from data_pipeline_project.framework.snowflake.snowflake_functions import insert_records


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


def generate_time_windows(target_day_start: pendulum.DateTime, granularity_seconds: int) -> List[Tuple[pendulum.DateTime, pendulum.DateTime]]:
   """
   Generate time windows for a target day split by granularity
   Never crosses day boundary
   
   Args:
       target_day_start: Start of target day (00:00:00)
       granularity_seconds: Window size in seconds
       
   Returns:
       List of (window_start, window_end) tuples
   """
   
   windows = []
   next_day_start = target_day_start.add(days=1)
   
   current_start = target_day_start
   
   while current_start < next_day_start:
       # Calculate proposed end time
       proposed_end = current_start.add(seconds=granularity_seconds)
       
       # Apply day boundary constraint
       actual_end = min(proposed_end, next_day_start)
       
       # Add window
       windows.append((current_start, actual_end))
       
       # Move to next window
       current_start = actual_end
   
   return windows


def format_time_interval(start: pendulum.DateTime, end: pendulum.DateTime) -> str:
   """
   Format time duration as readable string
   
   Args:
       start: Window start time
       end: Window end time
       
   Returns:
       Formatted string like "1h", "30m", "1d"
   """
   
   total_seconds = int((end - start).total_seconds())
   
   # Convert to most appropriate unit
   if total_seconds >= 86400 and total_seconds % 86400 == 0:
       # Days
       days = total_seconds // 86400
       return f"{days}d"
   elif total_seconds >= 3600 and total_seconds % 3600 == 0:
       # Hours
       hours = total_seconds // 3600
       return f"{hours}h"
   elif total_seconds >= 60 and total_seconds % 60 == 0:
       # Minutes
       minutes = total_seconds // 60
       return f"{minutes}m"
   else:
       # Seconds
       return f"{total_seconds}s"


def generate_pipeline_id(pipeline_name: str, source_cat: str, stage_cat: str, 
                       target_cat: str, window_start: pendulum.DateTime, window_end: pendulum.DateTime) -> str:
   """
   Generate unique pipeline ID by hashing all components
   
   Args:
       pipeline_name: Name of the pipeline
       source_cat: Source complete category
       stage_cat: Stage complete category
       target_cat: Target complete category
       window_start: Window start time
       window_end: Window end time
       
   Returns:
       SHA256 hash as hexadecimal string
   """
   
   # Create string to hash from all components
   components = [
       pipeline_name,
       source_cat,
       stage_cat,
       target_cat,
       window_start.to_iso8601_string(),
       window_end.to_iso8601_string()
   ]
   
   # Join with separator and encode
   hash_input = "|".join(components).encode('utf-8')
   
   # Generate SHA256 hash
   hash_object = hashlib.sha256(hash_input)
   return hash_object.hexdigest()


def create_single_pipeline_record(config: Dict, target_day: str, window_start: pendulum.DateTime, 
                                window_end: pendulum.DateTime, time_interval: str) -> Dict:
   """
   Build complete pipeline record following the schema
   
   Args:
       config: Pipeline configuration
       target_day: Target day as string (YYYY-MM-DD)
       window_start: Window start time
       window_end: Window end time  
       time_interval: Formatted time interval string
       
   Returns:
       Complete record dictionary ready for insertion
   """
   
   # Get current time for audit fields
   current_time = pendulum.now(config.get('timezone', 'UTC'))
   
   # Generate categories
   source_complete_category = generate_source_complete_category(config, window_start, window_end)
   stage_complete_category = generate_stage_complete_category(config, window_start, window_end)
   target_complete_category = generate_target_complete_category(config, window_start, window_end)
   
   # Generate pipeline ID
   pipeline_id = generate_pipeline_id(
       config['pipeline_name'],
       source_complete_category,
       stage_complete_category,
       target_complete_category,
       window_start,
       window_end
   )
   
   # Build complete record following your schema
   record = {
       "PIPELINE_NAME": config['pipeline_name'],
       "SOURCE_COMPLETE_CATEGORY": source_complete_category,
       "STAGE_COMPLETE_CATEGORY": stage_complete_category,
       "TARGET_COMPLETE_CATEGORY": target_complete_category,
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
       "CAN_ACCESS_HISTORICAL_DATA": config.get('CAN_ACCESS_HISTORICAL_DATA', 'YES'),
       "RECORD_FIRST_CREATED_TIME": current_time,
       "RECORD_LAST_UPDATED_TIME": current_time
   }
   
   return record


def create_pipeline_records_for_target_day(target_day: Union[str, pendulum.DateTime], 
                                         granularity: str, config: Dict) -> List[Dict]:
   """
   Create multiple pipeline records for a target day, split by granularity windows
   
   Args:
       target_day: Target day as string (YYYY-MM-DD) or pendulum datetime
       granularity: Time granularity like "1h", "30m", "1d"
       config: Pipeline configuration dictionary
       
   Returns:
       List of record dictionaries ready for insertion
       
   Raises:
       ValueError: If granularity format is invalid
       Exception: If record creation fails
   """
   
   try:
       # Convert target_day to date string and pendulum start time
       if isinstance(target_day, str):
           # Parse string to date
           target_date = pendulum.parse(target_day).date()
       else:
           # Extract date from pendulum datetime
           target_date = target_day.date()
       
       # Get timezone from config
       timezone = config.get('timezone', 'UTC')
       
       # Create start of target day in specified timezone
       target_day_start = pendulum.parse(f"{target_date.to_date_string()}T00:00:00").in_timezone(timezone)
       target_day_str = target_date.to_date_string()  # YYYY-MM-DD format
       
       # Parse granularity to seconds
       granularity_seconds = parse_granularity_to_seconds(granularity)
       
       # Generate time windows for the day
       windows = generate_time_windows(target_day_start, granularity_seconds)
       
       # Create records for each window
       records = []
       for window_start, window_end in windows:
           # Format time interval
           time_interval = format_time_interval(window_start, window_end)
           
           # Create single record
           record = create_single_pipeline_record(
               config, target_day_str, window_start, window_end, time_interval
           )
           
           records.append(record)
       
       print(f"Created {len(records)} records for target day {target_day_str} with granularity {granularity}")
       return records
       
   except Exception as e:
       raise Exception(f"Failed to create pipeline records for target day {target_day}: {e}")


def generate_source_complete_category(config: Dict, window_start: pendulum.DateTime, window_end: pendulum.DateTime) -> str:
   """
   Generate source complete category based on business logic
   
   Args:
       config: Pipeline configuration
       window_start: Window start time
       window_end: Window end time
       
   Returns:
       Source complete category string: "index_group|index_name"
   """
   
   index_group = config["index_group"]
   index_name = config["index_name"]
   
   return f"{index_group}|{index_name}"


def generate_target_complete_category(config: Dict, window_start: pendulum.DateTime, window_end: pendulum.DateTime) -> str:
   """
   Generate target complete category based on business logic
   
   Args:
       config: Pipeline configuration
       window_start: Window start time
       window_end: Window end time
       
   Returns:
       Target complete category string: "database.schema.table|prefix/YYYY-MM-DD/HH-mm/"
   """
   
   # Build s3_prefix_sub from list
   s3_prefix_sub = '/'.join(config["s3_prefix_list"])
   
   # Extract datetime parts from window_start
   date_part = window_start.format('YYYY-MM-DD')  # e.g., "2025-01-01"
   time_part = window_start.format('HH-mm')       # e.g., "14-30"
   
   # Get database path
   database_path = config["database.schema.table"]
   
   # Construct target path
   target_path = f"{s3_prefix_sub}/{date_part}/{time_part}/"
   
   return f"{database_path}|{target_path}"


def generate_stage_complete_category(config: Dict, window_start: pendulum.DateTime, window_end: pendulum.DateTime) -> str:
   """
   Generate stage complete category based on business logic
   
   Args:
       config: Pipeline configuration
       window_start: Window start time
       window_end: Window end time
       
   Returns:
       Stage complete category string: "s3_bucket|s3://bucket/prefix/YYYY-MM-DD/HH-mm/indexid_epochtime.json"
   """
   
   # Build s3_prefix_sub from list
   s3_prefix_sub = '/'.join(config["s3_prefix_list"])
   
   # Extract datetime parts from window_start
   date_part = window_start.format('YYYY-MM-DD')  # e.g., "2025-01-01"
   time_part = window_start.format('HH-mm')       # e.g., "14-30"
   
   # Get config values
   s3_bucket = config["s3_bucket"]
   index_id = config["index_id"]
   
   # Get current epoch time in config timezone
   current_time = pendulum.now(config.get('timezone', 'UTC'))
   epoch_timestamp = int(current_time.timestamp())
   
   # Construct S3 path with actual epoch timestamp
   s3_path = f"s3://{s3_bucket}/{s3_prefix_sub}/{date_part}/{time_part}/{index_id}_{epoch_timestamp}.json"
   
   return f"{s3_bucket}|{s3_path}"


def insert_records_per_day(target_day: str, granularity: str, config: Dict) -> Dict:
   """
   Create and insert all records for a single target day
   
   Args:
       target_day: Target day as string (YYYY-MM-DD)
       granularity: Time granularity like "1h", "30m", "1d2h30m"
       config: Pipeline configuration dictionary
       
   Returns:
       Dictionary with day processing results
       
   Raises:
       Exception: If record creation or insertion fails
   """
   
   try:
       print(f"Processing target day: {target_day} with granularity: {granularity}")
       
       # Convert target_day to date and pendulum start time
       target_date = pendulum.parse(target_day).date()
       timezone = config.get('timezone', 'UTC')
       target_day_start = pendulum.parse(f"{target_date.to_date_string()}T00:00:00").in_timezone(timezone)
       target_day_str = target_date.to_date_string()  # YYYY-MM-DD format
       
       # Parse granularity to seconds
       granularity_seconds = parse_granularity_to_seconds(granularity)
       
       # Generate time windows for the day
       windows = generate_time_windows(target_day_start, granularity_seconds)
       
       print(f"Generated {len(windows)} time windows for {target_day}")
       
       # Create records for each window
       daily_records = []
       for window_start, window_end in windows:
           # Format time interval
           time_interval = format_time_interval(window_start, window_end)
           
           # Create single record
           record = create_single_pipeline_record(
               config, target_day_str, window_start, window_end, time_interval
           )
           
           daily_records.append(record)
       
       print(f"Created {len(daily_records)} records for {target_day}")
       
       # Insert all records for this day in bulk
       insert_success = insert_records(config, daily_records)
       
       result = {
           'target_day': target_day,
           'records_created': len(daily_records),
           'windows_generated': len(windows),
           'insert_success': insert_success,
           'granularity': granularity,
           'execution_time': pendulum.now().to_iso8601_string()
       }
       
       if insert_success:
           print(f"Successfully inserted {len(daily_records)} records for {target_day}")
       else:
           print(f"Failed to insert records for {target_day}")
       
       return result
       
   except Exception as e:
       error_result = {
           'target_day': target_day,
           'records_created': 0,
           'windows_generated': 0,
           'insert_success': False,
           'error': str(e),
           'granularity': granularity,
           'execution_time': pendulum.now().to_iso8601_string()
       }
       
       print(f"Error processing {target_day}: {e}")
       return error_result


def create_and_insert_records_bulk(target_days: List[str], granularity: str, config: Dict) -> Dict:
   """
   Create and insert records for multiple target days
   Each day is processed and inserted separately
   
   Args:
       target_days: List of target days as strings (YYYY-MM-DD)
       granularity: Time granularity like "1h", "30m", "1d2h30m"
       config: Pipeline configuration dictionary
       
   Returns:
       Dictionary with overall processing results
   """
   
   try:
       print(f"Starting bulk processing for {len(target_days)} days with granularity: {granularity}")
       print(f"Target days: {target_days}")
       
       # Process each day individually
       day_results = []
       total_records_created = 0
       successful_days = 0
       failed_days = 0
       
       for target_day in target_days:
           try:
               # Process single day
               day_result = insert_records_per_day(target_day, granularity, config)
               day_results.append(day_result)
               
               # Update counters
               total_records_created += day_result['records_created']
               
               if day_result['insert_success']:
                   successful_days += 1
               else:
                   failed_days += 1
                   
           except Exception as e:
               # Handle individual day failure
               failed_day_result = {
                   'target_day': target_day,
                   'records_created': 0,
                   'windows_generated': 0,
                   'insert_success': False,
                   'error': str(e),
                   'granularity': granularity,
                   'execution_time': pendulum.now().to_iso8601_string()
               }
               day_results.append(failed_day_result)
               failed_days += 1
               print(f"Failed to process day {target_day}: {e}")
       
       # Compile overall results
       overall_result = {
           'total_days_requested': len(target_days),
           'successful_days': successful_days,
           'failed_days': failed_days,
           'total_records_created': total_records_created,
           'granularity': granularity,
           'day_results': day_results,
           'execution_summary': {
               'start_date': min(target_days) if target_days else None,
               'end_date': max(target_days) if target_days else None,
               'processing_completed_at': pendulum.now().to_iso8601_string()
           }
       }
       
       print(f"Bulk processing completed:")
       print(f"  - Total days: {len(target_days)}")
       print(f"  - Successful: {successful_days}")
       print(f"  - Failed: {failed_days}")
       print(f"  - Total records created: {total_records_created}")
       
       return overall_result
       
   except Exception as e:
       raise Exception(f"Failed bulk processing: {e}")






























# # Example usage and testing
# if __name__ == "__main__":
   
#    # Example config
#    config = {
#        'pipeline_name': 'elasticsearch_to_snowflake',
#        'index_group': 'LOGS',
#        'index_name': 'APPLICATION_LOGS',
#        's3_bucket': 'my-data-bucket',
#        's3_prefix_list': ['raw', 'elasticsearch', 'logs'],
#        'index_id': 'app_logs_v1',
#        'database.schema.table': 'PROD.ANALYTICS.APPLICATION_LOGS',
#        'pipeline_priority': 1.0,
#        'timezone': 'UTC',
#        'CAN_ACCESS_HISTORICAL_DATA': 'YES',
#        'sf_drive_config': {
#            'sf_user': 'service_account',
#            'sf_password': 'password',
#            'sf_account': 'company.snowflakecomputing.com',
#            'sf_warehouse': 'COMPUTE_WH',
#            'sf_database': 'ANALYTICS',
#            'sf_schema': 'PIPELINE_STATE',
#            'sf_table': 'pipeline_states',
#            'timestamp_fields': ['WINDOW_START_TIME', 'WINDOW_END_TIME', 'RECORD_FIRST_CREATED_TIME', 'RECORD_LAST_UPDATED_TIME']
#        }
#    }
   
#    # Test single day
#    print("=== Testing Single Day Processing ===")
#    single_day_result = insert_records_per_day("2025-01-01", "2h", config)
#    print(f"Single day result: {single_day_result}")
   
#    print("\n=== Testing Bulk Processing ===")
#    # Test multiple days
#    target_days = ["2025-01-01", "2025-01-02", "2025-01-03"]
#    bulk_result = create_and_insert_records_bulk(target_days, "1h", config)
#    print(f"Bulk processing completed with {bulk_result['successful_days']} successful days")









