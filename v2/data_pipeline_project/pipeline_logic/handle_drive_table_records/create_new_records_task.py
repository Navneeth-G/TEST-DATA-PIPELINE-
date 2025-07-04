# pipeline_logic/handle_drive_table_records/create_new_records_task.py

import pendulum
import snowflake.connector
from typing import Set, List, Union, Dict


from pipleine_logic.handle_drive_table_records.process_continuity_check_records_batch import (
    process_continuity_check_no_records_with_database_operations,
    convert_to_pendulum_date,
    insert_multiple_records_to_database
)
from pipleine_logic.handle_drive_table_records.record_creation.create_multiple_drive_record import (
    create_all_records_for_target_day,
    parse_granularity_to_seconds
)


def handle_drive_records_creation_task(config):
    """Main orchestrator - Clean Flow"""
    
    # Step 1: Handle continuity check records first
    continuity_records = check_continuity_check_performed_no_records_exist(config)
    if continuity_records:
        handle_continuity_check_records(continuity_records, config)
        return
    
    # Step 2: Calculate required target day
    required_target_day = calculate_required_target_day(config)
    
    # Step 3: Handle missing days in sequence
    handle_missing_target_days(config, required_target_day)
    
    # Step 4: Handle required target day if needed
    handle_required_target_day_creation(config, required_target_day)


def check_continuity_check_performed_no_records_exist(config) -> List[Dict]:
    """
    Get all records with CONTINUITY_CHECK_PERFORMED = 'NO'
    Reuses existing Snowflake connection pattern
    """
    
    records_list = []
    
    try:
        # Reuse existing connection pattern
        sf_config = config['sf_drive_config']
        
        conn = snowflake.connector.connect(
            user=sf_config['user'],
            password=sf_config['password'],
            account=sf_config['account'],
            warehouse=sf_config['warehouse'],
            database=sf_config['database'],
            schema=sf_config['schema']
        )
        
        cursor = conn.cursor()
        
        # Reuse existing query parameter substitution pattern
        query = """
        SELECT * 
        FROM %(table_name)s 
        WHERE PIPELINE_NAME = %(PIPELINE_NAME)s 
        AND SOURCE_COMPLETE_CATEGORY = %(SOURCE_COMPLETE_CATEGORY)s
        AND PIPELINE_PRIORITY = %(PIPELINE_PRIORITY)s        
        AND CONTINUITY_CHECK_PERFORMED = 'NO'
        """
        
        final_query = query % {
            'table_name': sf_config['table_name'],
            'PIPELINE_NAME': sf_config['PIPELINE_NAME'],
            'SOURCE_COMPLETE_CATEGORY': sf_config['SOURCE_COMPLETE_CATEGORY'],
            'PIPELINE_PRIORITY': sf_config['PIPELINE_PRIORITY']
        }
        
        # Reuse existing logging pattern
        print(f"Executing Snowflake Query:")
        print(f"Query: {final_query}")
        print("-" * 50)
        
        cursor.execute(final_query)
        
        # Reuse existing query ID tracking
        query_id = cursor.sfqid
        print(f"Query ID: {query_id}")
        
        # Convert rows to dictionaries
        column_names = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        for row in rows:
            record = dict(zip(column_names, row))
            records_list.append(record)
        
        print(f"Found {len(records_list)} records with CONTINUITY_CHECK_PERFORMED = 'NO'")
        return records_list
        
    except Exception as e:
        print(f"Error fetching continuity records: {str(e)}")
        raise
    finally:
        # Reuse existing cleanup pattern
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


def handle_continuity_check_records(records_list: List[Dict], config):
    """Reuse existing continuity handler"""
    
    print(f"Processing {len(records_list)} continuity check records...")
    
    # Reuse existing function directly
    summary = process_continuity_check_no_records_with_database_operations(records_list, config)
    
    print(f"Continuity check processing completed: {summary}")


def calculate_required_target_day(config) -> pendulum.Date:
    """Calculate required target day - reuses existing time parsing"""
    
    x_time_back = config.get('x_time_back', '1d')
    timezone = config.get('timezone', 'UTC')
    
    # Reuse existing granularity parsing function
    time_back_seconds = parse_granularity_to_seconds(x_time_back)
    
    # Calculate required target day
    current_time = pendulum.now(timezone)
    required_datetime = current_time.subtract(seconds=time_back_seconds)
    required_target_day = required_datetime.date()
    
    print(f"Current time: {current_time}")
    print(f"Time back: {x_time_back} ({time_back_seconds} seconds)")
    print(f"Required target day: {required_target_day}")
    
    return required_target_day


def handle_missing_target_days(config, required_target_day: pendulum.Date):
    """Find gaps and create records - reuses existing functions"""
    
    existing_target_days = get_all_existing_target_days(config)
    
    if not existing_target_days:
        print("No existing target days found - skipping gap analysis")
        return
    
    missing_days = find_missing_days_in_range(existing_target_days, required_target_day)
    
    if not missing_days:
        print("No missing days found in sequence")
        return
    
    # Create records for missing days - reuse existing functions
    for missing_day in missing_days:
        print(f"Creating records for missing target day: {missing_day}")
        
        # Reuse existing record creation function
        records = create_all_records_for_target_day(str(missing_day), config)
        
        # Reuse existing insert function
        insert_multiple_records_to_database(records, config)


def handle_required_target_day_creation(config, required_target_day: pendulum.Date):
    """Handle required target day creation - reuses existing functions"""
    
    max_target_day = get_max_target_day(config)
    
    if max_target_day is None:
        print(f"No existing records - creating required target day: {required_target_day}")
        
        # Reuse existing functions
        records = create_all_records_for_target_day(str(required_target_day), config)
        insert_multiple_records_to_database(records, config)
        
    elif max_target_day < required_target_day:
        print(f"Max target day ({max_target_day}) < required ({required_target_day}) - creating required target day")
        
        # Reuse existing functions
        records = create_all_records_for_target_day(str(required_target_day), config)
        insert_multiple_records_to_database(records, config)
        
    else:
        print(f"Up to date - max target day: {max_target_day}, required: {required_target_day}")


def get_all_existing_target_days(config) -> Set[pendulum.Date]:
    """Get all existing target days - reuses existing patterns"""
    
    target_days_set = set()
    
    try:
        # Reuse existing connection pattern
        sf_config = config['sf_drive_config']
        
        conn = snowflake.connector.connect(
            user=sf_config['user'],
            password=sf_config['password'],
            account=sf_config['account'],
            warehouse=sf_config['warehouse'],
            database=sf_config['database'],
            schema=sf_config['schema']
        )
        
        cursor = conn.cursor()
        
        # Reuse existing query pattern
        query = """
        SELECT DISTINCT TARGET_DAY 
        FROM %(table_name)s 
        WHERE PIPELINE_NAME = %(PIPELINE_NAME)s 
        AND SOURCE_COMPLETE_CATEGORY = %(SOURCE_COMPLETE_CATEGORY)s
        AND PIPELINE_PRIORITY = %(PIPELINE_PRIORITY)s        
        AND CONTINUITY_CHECK_PERFORMED = 'YES'
        """
        
        final_query = query % {
            'table_name': sf_config['table_name'],
            'PIPELINE_NAME': sf_config['PIPELINE_NAME'],
            'SOURCE_COMPLETE_CATEGORY': sf_config['SOURCE_COMPLETE_CATEGORY'],
            'PIPELINE_PRIORITY': sf_config['PIPELINE_PRIORITY']
        }
        
        print(f"Executing Snowflake Query:")
        print(f"Query: {final_query}")
        print("-" * 50)
        
        cursor.execute(final_query)
        
        query_id = cursor.sfqid
        print(f"Query ID: {query_id}")
        
        results = cursor.fetchall()
        
        # Reuse existing date conversion function
        for row in results:
            target_day_value = row[0]
            pendulum_date = convert_to_pendulum_date(target_day_value)
            target_days_set.add(pendulum_date)
        
        print(f"Found {len(target_days_set)} existing target days")
        
    except Exception as e:
        print(f"Error fetching existing target days: {str(e)}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
    
    return target_days_set


def get_max_target_day(config) -> Union[pendulum.Date, None]:
    """Get max target day - reuses existing patterns"""
    
    try:
        # Reuse existing connection pattern
        sf_config = config['sf_drive_config']
        
        conn = snowflake.connector.connect(
            user=sf_config['user'],
            password=sf_config['password'],
            account=sf_config['account'],
            warehouse=sf_config['warehouse'],
            database=sf_config['database'],
            schema=sf_config['schema']
        )
        
        cursor = conn.cursor()
        
        # Reuse existing query pattern
        query = """
        SELECT MAX(TARGET_DAY) 
        FROM %(table_name)s 
        WHERE PIPELINE_NAME = %(PIPELINE_NAME)s 
        AND SOURCE_COMPLETE_CATEGORY = %(SOURCE_COMPLETE_CATEGORY)s
        AND PIPELINE_PRIORITY = %(PIPELINE_PRIORITY)s        
        AND CONTINUITY_CHECK_PERFORMED = 'YES'
        """
        
        final_query = query % {
            'table_name': sf_config['table_name'],
            'PIPELINE_NAME': sf_config['PIPELINE_NAME'],
            'SOURCE_COMPLETE_CATEGORY': sf_config['SOURCE_COMPLETE_CATEGORY'],
            'PIPELINE_PRIORITY': sf_config['PIPELINE_PRIORITY']
        }
        
        cursor.execute(final_query)
        result = cursor.fetchone()
        
        if result and result[0]:
            # Reuse existing date conversion function
            max_target_day = convert_to_pendulum_date(result[0])
            print(f"Max target day: {max_target_day}")
            return max_target_day
        else:
            print("No existing target days found")
            return None
        
    except Exception as e:
        print(f"Error fetching max target day: {str(e)}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


def find_missing_days_in_range(existing_days: Set[pendulum.Date], 
                              required_target_day: pendulum.Date) -> List[pendulum.Date]:
    """Find missing days - simple logic"""
    
    if not existing_days:
        return []
    
    missing_days = []
    start_date = min(existing_days)
    current_date = start_date.add(days=1)
    
    while current_date <= required_target_day:
        if current_date not in existing_days:
            missing_days.append(current_date)
        current_date = current_date.add(days=1)
    
    print(f"Existing days: {sorted(existing_days)}")
    print(f"Checking from {start_date} to {required_target_day}")
    print(f"Missing days: {missing_days}")
    
    return missing_days


# # Example usage
# if __name__ == "__main__":
#     config = {
#         'sf_drive_config': {
#             'user': 'your_user',
#             'password': 'your_password',
#             'account': 'your_account',
#             'warehouse': 'your_warehouse',
#             'database': 'your_database',
#             'schema': 'your_schema',
#             'table_name': 'your_table',
#             'PIPELINE_NAME': 'your_pipeline',
#             'SOURCE_COMPLETE_CATEGORY': 'your_source_category',
#             'PIPELINE_PRIORITY': 1.0
#         },
#         'x_time_back': '1d',
#         'timezone': 'UTC',
#         'expected_granularity': '1h'
#     }
    
#     handle_drive_records_creation_task(config)