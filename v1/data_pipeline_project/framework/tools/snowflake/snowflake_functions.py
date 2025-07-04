# framework/snowflake/snowflake_functions.py

import snowflake.connector
from typing import Dict, List, Tuple, Union

import pendulum

def convert_to_pendulum(timestamp_value, timezone: str = 'UTC'):
    """
    Convert timestamp value to pendulum object with timezone
    Handles Snowflake datetime objects by converting to ISO string first
    
    Args:
        timestamp_value: Timestamp value (Snowflake datetime, string, etc.)
        timezone: Target timezone (default: 'UTC') - only used if timestamp has no timezone info
        
    Returns:
        Pendulum datetime object in original timezone or specified timezone
        
    Raises:
        Exception: If conversion fails
    """
    
    try:
        if timestamp_value is None:
            return None
        
        # Convert Snowflake datetime to ISO string first
        iso_string = timestamp_value.isoformat()
        
        # Parse to pendulum
        pendulum_obj = pendulum.parse(iso_string)
        
        # If pendulum object has no timezone info, apply the specified timezone
        if pendulum_obj.timezone is None or pendulum_obj.timezone.name == 'UTC' and 'T' in iso_string and '+' not in iso_string and 'Z' not in iso_string:
            pendulum_obj = pendulum_obj.in_timezone(timezone)
        
        return pendulum_obj
        
    except Exception as e:
        raise Exception(f"Failed to convert timestamp to pendulum: {e}")



def create_snowflake_connection(config: Dict) -> snowflake.connector.SnowflakeConnection:
    """
    Create and return Snowflake connection
    
    Args:
        config: Dictionary with sf_drive_config containing connection details
        
    Returns:
        Snowflake connection object
        
    Raises:
        Exception: If connection fails for any reason
    """
    
    try:
        sf_config = config['sf_drive_config']
        
        connection = snowflake.connector.connect(
            user=sf_config['sf_user'],
            password=sf_config['sf_password'],
            account=sf_config['sf_account'],
            warehouse=sf_config['sf_warehouse'],
            database=sf_config['sf_database'],
            schema=sf_config['sf_schema']
        )
        
        return connection
        
    except Exception as e:
        raise Exception(f"Failed to create Snowflake connection: {e}")



def convert_timestamp_fields_to_pendulum(records, timestamp_fields: List[str], timezone: str = 'UTC'):
    """
    Convert timestamp fields in records to pendulum objects
    
    Args:
        records: Single record dict OR list of record dictionaries
        timestamp_fields: List of timestamp field names to convert
        timezone: Target timezone (default: 'UTC') - only used if timestamp has no timezone info
        
    Returns:
        Single record dict OR list of record dicts with timestamp fields converted to pendulum
    """
    
    # Handle single record case
    if isinstance(records, dict):
        # Make a copy of the single record
        record_copy = records.copy()
        
        for field in timestamp_fields:
            if field in record_copy and record_copy[field] is not None:
                record_copy[field] = convert_to_pendulum(record_copy[field], timezone)
        
        return record_copy
    
    # Handle list of records case
    elif isinstance(records, list):
        # Make a copy of the list and each record
        records_copy = []
        
        for record in records:
            record_copy = record.copy()
            
            for field in timestamp_fields:
                if field in record_copy and record_copy[field] is not None:
                    record_copy[field] = convert_to_pendulum(record_copy[field], timezone)
            
            records_copy.append(record_copy)
        
        return records_copy
    
    else:
        raise Exception(f"Expected dict or list, got {type(records)}")



def get_unprocessed_record(config: Dict) -> List[Dict]:
    """
    Get all records with CONTINUITY_CHECK_PERFORMED = NO and convert timestamps to pendulum
    
    Args:
        config: Dictionary with sf_drive_config, timezone, SOURCE_COMPLETE_CATEGORY, and PIPELINE_PRIORITY
        
    Returns:
        List of record dictionaries with timestamp fields converted to pendulum objects
        
    Raises:
        Exception: If query fails for any reason
    """
    
    try:
        # Create connection
        connection = create_snowflake_connection(config)
        cursor = connection.cursor()
        
        # Build query
        sf_config = config['sf_drive_config']
        table_name = sf_config['sf_table']
        query = f"""
        SELECT * FROM {table_name} 
        WHERE 
            PIPELINE_NAME = %(PIPELINE_NAME)s
            AND SOURCE_COMPLETE_CATEGORY = %(SOURCE_COMPLETE_CATEGORY)s 
            AND PIPELINE_PRIORITY = %(PIPELINE_PRIORITY)s
            AND CONTINUITY_CHECK_PERFORMED = 'NO' 
        ORDER BY TARGET_DAY ASC
        """
        
        # Prepare parameters
        params = {
            'PIPELINE_NAME': sf_config['PIPELINE_NAME'],
            'SOURCE_COMPLETE_CATEGORY': config['SOURCE_COMPLETE_CATEGORY'],
            'PIPELINE_PRIORITY': config['PIPELINE_PRIORITY']
        }
        
        print(f"Executing query: {query}")
        print(f"Parameters: {params}")
        
        # Execute query with parameters
        cursor.execute(query, params)
        df = cursor.fetch_pandas_all()
        
        # Close connection
        cursor.close()
        connection.close()
        
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        
        print(f"Found {len(records)} unprocessed records")
        print(f"Records type: {type(records)}")
        
        # Get timezone and timestamp fields from config
        timezone = config.get('timezone', 'UTC')
        timestamp_fields = sf_config.get('timestamp_fields', [])
        
        # Convert timestamp fields to pendulum objects
        records = convert_timestamp_fields_to_pendulum(records, timestamp_fields, timezone)
        
        print(f"Converted timestamp fields using timezone: {timezone}")
        return records
        
    except Exception as e:
        raise Exception(f"Failed to get unprocessed records: {e}")
    

def get_target_days_for_pipeline(config: Dict) -> List[str]:
    """
    Get all TARGET_DAY values for given PIPELINE_NAME and SOURCE_COMPLETE_CATEGORY
    Used to find discontinuity in days
    
    Args:
        config: Dictionary with sf_drive_config, PIPELINE_NAME, SOURCE_COMPLETE_CATEGORY, and PIPELINE_PRIORITY
        
    Returns:
        List of TARGET_DAY strings (YYYY-MM-DD format) sorted in ascending order
        
    Raises:
        Exception: If query fails for any reason
    """
    
    try:
        # Create connection
        connection = create_snowflake_connection(config)
        cursor = connection.cursor()
        
        # Build query
        sf_config = config['sf_drive_config']
        table_name = sf_config['sf_table']
        query = f"""
        SELECT DISTINCT TARGET_DAY FROM {table_name} 
        WHERE 
            PIPELINE_NAME = %(PIPELINE_NAME)s 
            AND SOURCE_COMPLETE_CATEGORY = %(SOURCE_COMPLETE_CATEGORY)s
            AND CONTINUITY_CHECK_PERFORMED = 'YES'
            AND PIPELINE_PRIORITY = %(PIPELINE_PRIORITY)s
        ORDER BY TARGET_DAY ASC
        """
        
        # Prepare parameters
        params = {
            'PIPELINE_NAME': config['PIPELINE_NAME'],
            'SOURCE_COMPLETE_CATEGORY': config['SOURCE_COMPLETE_CATEGORY'],
            'PIPELINE_PRIORITY': config['PIPELINE_PRIORITY']
        }
        
        print(f"Executing query: {query}")
        print(f"Parameters: {params}")
        
        # Execute query with parameters
        cursor.execute(query, params)
        df = cursor.fetch_pandas_all()
        
        # Close connection
        cursor.close()
        connection.close()
        
        # Convert TARGET_DAY timestamps to pendulum objects, then to date strings
        timezone = config.get('timezone', 'UTC')
        target_days = []
        
        for target_day_timestamp in df['TARGET_DAY'].tolist():
            # Convert to pendulum and extract date string
            pendulum_obj = convert_to_pendulum(target_day_timestamp, timezone)
            date_string = pendulum_obj.to_date_string()  # YYYY-MM-DD format
            target_days.append(date_string)
        
        # Remove duplicates and sort (in case same date appears multiple times)
        target_days = sorted(list(set(target_days)))
        
        print(f"Found {len(target_days)} distinct target days")
        return target_days
        
    except Exception as e:
        raise Exception(f"Failed to get target days: {e}")


def insert_records(config: Dict, records) -> bool:
    try:
        # Handle single record case
        if isinstance(records, dict):
            records_list = [records]
        elif isinstance(records, list):
            records_list = records
        else:
            raise Exception(f"Expected dict or list, got {type(records)}")
        
        if not records_list:
            print("No records to insert")
            return True
        
        # Create connection
        connection = create_snowflake_connection(config)
        cursor = connection.cursor()
        
        try:
            # Get config
            sf_config = config['sf_drive_config']
            table_name = sf_config['sf_table']
            timezone = config.get('timezone', 'UTC')
            timestamp_fields = sf_config.get('timestamp_fields', [])
            
            # Prepare all records for bulk insert
            processed_records = []
            for record in records_list:
                record_copy = record.copy()
                
                # Convert pendulum objects to ISO strings
                for field in timestamp_fields:
                    if field in record_copy and record_copy[field] is not None:
                        record_copy[field] = record_copy[field].to_iso8601_string()
                
                processed_records.append(record_copy)
            
            # Build bulk insert query
            if processed_records:
                columns = list(processed_records[0].keys())
                column_names = ', '.join(columns)
                placeholders = ', '.join([f'%({col.lower()})s' for col in columns])
                
                query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
                
                # Convert all records to have lowercase keys
                bulk_params = []
                for record in processed_records:
                    params = {key.lower(): value for key, value in record.items()}
                    bulk_params.append(params)
                
                # Execute bulk insert
                cursor.executemany(query, bulk_params)
                connection.commit()
                
                print(f"Successfully bulk inserted {len(records_list)} records")
                return True
            
        except Exception as e:
            connection.rollback()
            raise e
            
        finally:
            cursor.close()
            connection.close()
            
    except Exception as e:
        raise Exception(f"Failed to insert records: {e}")

def delete_records_by_target_days(config: Dict, target_days) -> bool:
    """
    Delete all records for specific target days
    
    Args:
        config: Dictionary with sf_drive_config, PIPELINE_NAME, SOURCE_COMPLETE_CATEGORY, and PIPELINE_PRIORITY
        target_days: Single target day string OR list of target day strings (YYYY-MM-DD format)
        
    Returns:
        bool: True if successful, False if failed
        
    Raises:
        Exception: If delete fails for any reason
    """
    
    try:
        # Handle single target day case
        if isinstance(target_days, str):
            target_days_list = [target_days]
        elif isinstance(target_days, list):
            target_days_list = target_days
        else:
            raise Exception(f"Expected str or list, got {type(target_days)}")
        
        if not target_days_list:
            print("No target days to delete")
            return True
        
        # Create connection
        connection = create_snowflake_connection(config)
        cursor = connection.cursor()
        
        # Get table name
        sf_config = config['sf_drive_config']
        table_name = sf_config['sf_table']
        
        print(f"Deleting records for {len(target_days_list)} target days from {table_name}")
        
        # Build placeholders for IN clause
        placeholders = ', '.join([f'%(target_day_{i})s' for i in range(len(target_days_list))])
        
        # Build DELETE query
        query = f"""
        DELETE FROM {table_name} 
        WHERE 
            PIPELINE_NAME = %(PIPELINE_NAME)s 
            AND SOURCE_COMPLETE_CATEGORY = %(SOURCE_COMPLETE_CATEGORY)s
            AND PIPELINE_PRIORITY = %(PIPELINE_PRIORITY)s
            AND DATE(TARGET_DAY) IN ({placeholders})
        """
        
        # Prepare parameters
        params = {
            'PIPELINE_NAME': config['PIPELINE_NAME'],
            'SOURCE_COMPLETE_CATEGORY': config['SOURCE_COMPLETE_CATEGORY'],
            'PIPELINE_PRIORITY': config['PIPELINE_PRIORITY']
        }
        
        # Add target day parameters
        for i, target_day in enumerate(target_days_list):
            params[f'target_day_{i}'] = target_day
        
        print(f"Executing query: {query}")
        print(f"Parameters: {params}")
        
        # Execute delete
        cursor.execute(query, params)
        rows_deleted = cursor.rowcount
        
        # Commit deletion
        connection.commit()
        
        # Close connection
        cursor.close()
        connection.close()
        
        print(f"Successfully deleted {rows_deleted} records for target days: {target_days_list}")
        return True
        
    except Exception as e:
        raise Exception(f"Failed to delete records by target days: {e}")


def check_target_day_exists(config: Dict, target_day: str) -> bool:
    """
    Check if target day already has records in Snowflake
    
    Args:
        config: Dictionary with sf_drive_config, PIPELINE_NAME, SOURCE_COMPLETE_CATEGORY, and PIPELINE_PRIORITY
        target_day: Target day to check (YYYY-MM-DD format)
        
    Returns:
        True if target day exists, False if not found
        
    Raises:
        Exception: If database query fails
    """
    
    try:
        # Create connection
        connection = create_snowflake_connection(config)
        cursor = connection.cursor()
        
        # Build query to check for existing records
        sf_config = config['sf_drive_config']
        table_name = sf_config['sf_table']
        
        query = f"""
        SELECT COUNT(*) as record_count
        FROM {table_name} 
        WHERE 
            PIPELINE_NAME = %(PIPELINE_NAME)s
            AND SOURCE_COMPLETE_CATEGORY = %(SOURCE_COMPLETE_CATEGORY)s 
            AND PIPELINE_PRIORITY = %(PIPELINE_PRIORITY)s
            AND DATE(TARGET_DAY) = %(target_day)s
        """
        
        # Prepare parameters
        params = {
            'PIPELINE_NAME': config['PIPELINE_NAME'],
            'SOURCE_COMPLETE_CATEGORY': config['SOURCE_COMPLETE_CATEGORY'],
            'PIPELINE_PRIORITY': config['PIPELINE_PRIORITY'],
            'target_day': target_day
        }
        
        print(f"Checking if target day {target_day} exists...")
        print(f"Query: {query}")
        print(f"Parameters: {params}")
        
        # Execute query
        cursor.execute(query, params)
        result = cursor.fetchone()
        
        # Close connection
        cursor.close()
        connection.close()
        
        # Check result
        record_count = result[0] if result else 0
        exists = record_count > 0
        
        print(f"Target day {target_day}: {'EXISTS' if exists else 'NOT FOUND'} ({record_count} records)")
        
        return exists
        
    except Exception as e:
        raise Exception(f"Failed to check if target day exists: {e}")


def get_n_oldest_pending_records(config: Dict, n: int, x_time_back: str, granularity: str) -> List[Dict]:
    """
    Get N oldest pending records that are not future/unstable data
    Uses buffer time to ensure data stability
    
    Args:
        config: Dictionary with sf_drive_config, pipeline filters, and timezone
        n: Number of records to fetch (must be > 0)
        x_time_back: Time to go back from current time (e.g., "2h", "1d")
        granularity: Additional buffer time (e.g., "1h", "30m")
        
    Returns:
        List of record dictionaries with timestamp fields converted to pendulum
        Maximum N records, ordered by WINDOW_START_TIME ASC
        
    Raises:
        Exception: If query fails or invalid parameters
        
    Buffer Logic:
        max_window_end_time = current_time - x_time_back - granularity
        Only records with WINDOW_END_TIME <= max_window_end_time are selected
    """
    
    try:
        # Input validation
        if not isinstance(n, int) or n <= 0:
            raise ValueError(f"n must be a positive integer, got: {n}")
        
        if not x_time_back or not x_time_back.strip():
            raise ValueError("x_time_back cannot be empty")
            
        if not granularity or not granularity.strip():
            raise ValueError("granularity cannot be empty")
        
        # Get timezone from config
        timezone = config.get('timezone', 'UTC')
        
        # Calculate buffer cutoff time
        current_time = pendulum.now(timezone)
        x_time_back_seconds = parse_granularity_to_seconds(x_time_back)
        granularity_seconds = parse_granularity_to_seconds(granularity)
        
        max_window_end_time = current_time.subtract(seconds=x_time_back_seconds + granularity_seconds)
        
        print(f"Current time ({timezone}): {current_time.to_iso8601_string()}")
        print(f"Buffer calculation: current_time - {x_time_back} - {granularity}")
        print(f"Max window_end_time allowed: {max_window_end_time.to_iso8601_string()}")
        
        # Create connection
        connection = create_snowflake_connection(config)
        cursor = connection.cursor()
        
        # Build query
        sf_config = config['sf_drive_config']
        table_name = sf_config['sf_table']
        query = f"""
        SELECT * FROM {table_name} 
        WHERE 
            PIPELINE_NAME = %(PIPELINE_NAME)s 
            AND SOURCE_COMPLETE_CATEGORY = %(SOURCE_COMPLETE_CATEGORY)s
            AND CONTINUITY_CHECK_PERFORMED = 'YES'
            AND PIPELINE_PRIORITY = %(PIPELINE_PRIORITY)s
            AND PIPELINE_STATUS = 'PENDING'
            AND WINDOW_END_TIME <= %(max_window_end_time)s
        ORDER BY WINDOW_START_TIME ASC
        LIMIT %(limit_n)s
        """
        
        # Prepare parameters
        params = {
            'PIPELINE_NAME': config['PIPELINE_NAME'],
            'SOURCE_COMPLETE_CATEGORY': config['SOURCE_COMPLETE_CATEGORY'],
            'PIPELINE_PRIORITY': config['PIPELINE_PRIORITY'],
            'max_window_end_time': max_window_end_time.to_iso8601_string(),
            'limit_n': n
        }
        
        print(f"Executing query for {n} oldest pending records")
        print(f"Parameters: {params}")
        
        # Execute query with parameters
        cursor.execute(query, params)
        df = cursor.fetch_pandas_all()
        
        # Close connection
        cursor.close()
        connection.close()
        
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        
        print(f"Found {len(records)} pending records (requested: {n})")
        
        if not records:
            print("No pending records found within buffer time constraints")
            return []
        
        # Convert timestamp fields to pendulum objects
        timestamp_fields = sf_config.get('timestamp_fields', [])
        records = convert_timestamp_fields_to_pendulum(records, timestamp_fields, timezone)
        
        print(f"Successfully retrieved {len(records)} oldest pending records")
        
        # Log first and last record for debugging
        if records:
            first_window_start = records[0].get('WINDOW_START_TIME')
            last_window_start = records[-1].get('WINDOW_START_TIME')
            print(f"Window start time range: {first_window_start} to {last_window_start}")
        
        return records
        
    except Exception as e:
        raise Exception(f"Failed to get {n} oldest pending records: {e}")


def count_all_records_for_day(config: Dict, target_day: str) -> int:
    """
    Count total records for a target day (both processed and unprocessed)
    
    Args:
        config: Pipeline configuration with pipeline_priority filter
        target_day: Target day to count (YYYY-MM-DD format)
        
    Returns:
        Total count of records for the day
    """
    
    try:
        connection = create_snowflake_connection(config)
        cursor = connection.cursor()
        
        sf_config = config['sf_drive_config']
        table_name = sf_config['sf_table']
        
        query = f"""
        SELECT COUNT(*) as total_count
        FROM {table_name} 
        WHERE 
            PIPELINE_NAME = %(pipeline_name)s
            AND SOURCE_COMPLETE_CATEGORY = %(source_complete_category)s 
            AND PIPELINE_PRIORITY = %(pipeline_priority)s
            AND DATE(TARGET_DAY) = %(target_day)s
        """
        
        params = {
            'pipeline_name': config['pipeline_name'],
            'source_complete_category': config['source_complete_category'],
            'pipeline_priority': config['pipeline_priority'],
            'target_day': target_day
        }
        
        cursor.execute(query, params)
        result = cursor.fetchone()
        
        cursor.close()
        connection.close()
        
        total_count = result[0] if result else 0
        
        print(f"Target day {target_day}: {total_count} total records")
        return total_count
        
    except Exception as e:
        raise Exception(f"Failed to count records for day: {e}")


def delete_all_records_for_day(config: Dict, target_day: str) -> bool:
    """
    Delete ALL records for a specific target day (day-level regeneration)
    
    Args:
        config: Pipeline configuration with pipeline_priority filter
        target_day: Target day to delete (YYYY-MM-DD format)
        
    Returns:
        True if successful
    """
    
    try:
        connection = create_snowflake_connection(config)
        cursor = connection.cursor()
        
        sf_config = config['sf_drive_config']
        table_name = sf_config['sf_table']
        
        query = f"""
        DELETE FROM {table_name} 
        WHERE 
            PIPELINE_NAME = %(pipeline_name)s
            AND SOURCE_COMPLETE_CATEGORY = %(source_complete_category)s 
            AND PIPELINE_PRIORITY = %(pipeline_priority)s
            AND DATE(TARGET_DAY) = %(target_day)s
        """
        
        params = {
            'pipeline_name': config['pipeline_name'],
            'source_complete_category': config['source_complete_category'],
            'pipeline_priority': config['pipeline_priority'],
            'target_day': target_day
        }
        
        print(f"Deleting ALL records for target day: {target_day}")
        
        cursor.execute(query, params)
        rows_deleted = cursor.rowcount
        
        connection.commit()
        cursor.close()
        connection.close()
        
        print(f"Successfully deleted {rows_deleted} records for {target_day}")
        return True
        
    except Exception as e:
        raise Exception(f"Failed to delete all records for day: {e}")


def find_and_delete_matching_window(config: Dict, unprocessed_record: Dict) -> bool:
    """
    Find and delete record matching the specific time window
    
    Args:
        config: Pipeline configuration
        unprocessed_record: Record with specific WINDOW_START_TIME and WINDOW_END_TIME
        
    Returns:
        True if found and deleted, False if no match found
    """
    
    try:
        connection = create_snowflake_connection(config)
        cursor = connection.cursor()
        
        sf_config = config['sf_drive_config']
        table_name = sf_config['sf_table']
        
        # Extract window times
        window_start = unprocessed_record['WINDOW_START_TIME']
        window_end = unprocessed_record['WINDOW_END_TIME']
        target_day = unprocessed_record['TARGET_DAY']
        
        # Convert to ISO strings for comparison
        if hasattr(window_start, 'to_iso8601_string'):
            window_start_str = window_start.to_iso8601_string()
        else:
            window_start_str = str(window_start)
            
        if hasattr(window_end, 'to_iso8601_string'):
            window_end_str = window_end.to_iso8601_string()
        else:
            window_end_str = str(window_end)
            
        if hasattr(target_day, 'to_date_string'):
            target_day_str = target_day.to_date_string()
        else:
            target_day_str = str(target_day)
        
        query = f"""
        DELETE FROM {table_name} 
        WHERE 
            PIPELINE_NAME = %(pipeline_name)s
            AND SOURCE_COMPLETE_CATEGORY = %(source_complete_category)s 
            AND PIPELINE_PRIORITY = %(pipeline_priority)s
            AND DATE(TARGET_DAY) = %(target_day)s
            AND WINDOW_START_TIME = %(window_start_time)s
            AND WINDOW_END_TIME = %(window_end_time)s
            AND CONTINUITY_CHECK_PERFORMED = 'YES'
        """
        
        params = {
            'pipeline_name': config['pipeline_name'],
            'source_complete_category': config['source_complete_category'],
            'pipeline_priority': config['pipeline_priority'],
            'target_day': target_day_str,
            'window_start_time': window_start_str,
            'window_end_time': window_end_str
        }
        
        print(f"Looking for matching window: {window_start_str} to {window_end_str}")
        
        cursor.execute(query, params)
        rows_deleted = cursor.rowcount
        
        connection.commit()
        cursor.close()
        connection.close()
        
        if rows_deleted > 0:
            print(f"✅ Deleted {rows_deleted} matching window record(s)")
            return True
        else:
            print(f"⚠️  No matching window found to delete")
            return False
            
    except Exception as e:
        raise Exception(f"Failed to find and delete matching window: {e}")


def update_single_record_flag_to_yes(config: Dict, unprocessed_record: Dict) -> bool:
    """
    Update single unprocessed record's flag to YES
    
    Args:
        config: Pipeline configuration
        unprocessed_record: Record to update (uses PIPELINE_ID for exact match)
        
    Returns:
        True if successful
    """
    
    try:
        connection = create_snowflake_connection(config)
        cursor = connection.cursor()
        
        sf_config = config['sf_drive_config']
        table_name = sf_config['sf_table']
        
        pipeline_id = unprocessed_record['PIPELINE_ID']
        
        query = f"""
        UPDATE {table_name} 
        SET 
            CONTINUITY_CHECK_PERFORMED = 'YES',
            RECORD_LAST_UPDATED_TIME = %(updated_time)s
        WHERE 
            PIPELINE_ID = %(pipeline_id)s
            AND PIPELINE_PRIORITY = %(pipeline_priority)s
        """
        
        params = {
            'pipeline_id': pipeline_id,
            'pipeline_priority': config['pipeline_priority'],
            'updated_time': pendulum.now().to_iso8601_string()
        }
        
        cursor.execute(query, params)
        rows_updated = cursor.rowcount
        
        connection.commit()
        cursor.close()
        connection.close()
        
        if rows_updated > 0:
            print(f"✅ Updated record flag to YES")
            return True
        else:
            print(f"⚠️  No records updated - pipeline priority mismatch?")
            return False
            
    except Exception as e:
        raise Exception(f"Failed to update record flag: {e}")












