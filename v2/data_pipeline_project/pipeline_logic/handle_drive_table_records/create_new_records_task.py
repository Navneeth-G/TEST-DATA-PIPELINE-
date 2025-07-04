# pipeline_logic/handle_drive_table_records/create_new_records_task.py

import pendulum
import snowflake.connector
from typing import Set, List, Union, Dict
import logging

# Configure logging for Airflow
logger = logging.getLogger(__name__)

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
    """Main orchestrator - Clean Flow with comprehensive error handling"""
    
    try:
        logger.info("=" * 60)
        logger.info("STARTING DRIVE RECORDS CREATION TASK")
        logger.info("=" * 60)
        
        # Validate config first
        _validate_config(config)
        logger.info(" Configuration validation passed")
        
        # Step 1: Handle continuity check records first
        logger.info("Step 1: Checking for continuity check records...")
        continuity_records = check_continuity_check_performed_no_records_exist(config)
        
        if continuity_records:
            logger.info(f"Found {len(continuity_records)} continuity check records to process")
            handle_continuity_check_records(continuity_records, config)
            logger.info(" Continuity check records processed successfully")
            return
        else:
            logger.info("No continuity check records found, proceeding to target day calculations")
        
        # Step 2: Calculate required target day
        logger.info("Step 2: Calculating required target day...")
        required_target_day = calculate_required_target_day(config)
        logger.info(f"Required target day calculated: {required_target_day}")
        
        # Step 3: Handle missing days in sequence
        logger.info("Step 3: Handling missing target days...")
        handle_missing_target_days(config, required_target_day)
        
        # Step 4: Handle required target day if needed
        logger.info("Step 4: Handling required target day creation...")
        handle_required_target_day_creation(config, required_target_day)
        
        logger.info("=" * 60)
        logger.info(" DRIVE RECORDS CREATION TASK COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error(" DRIVE RECORDS CREATION TASK FAILED")
        logger.error(f"Error: {str(e)}")
        logger.error("=" * 60)
        raise


def _validate_config(config):
    """Validate configuration parameters"""
    try:
        logger.info("Validating configuration...")
        
        if not config:
            raise ValueError("Config is None or empty")
        
        # Check sf_drive_config
        sf_config = config.get('sf_drive_config')
        if not sf_config:
            raise ValueError("sf_drive_config is missing from config")
        
        required_sf_fields = ['user', 'password', 'account', 'warehouse', 'database', 'schema', 'table_name', 'PIPELINE_NAME', 'SOURCE_COMPLETE_CATEGORY']
        for field in required_sf_fields:
            if not sf_config.get(field):
                raise ValueError(f"Required field sf_drive_config.{field} is missing or empty")
        
        # Check other required fields
        if not config.get('x_time_back'):
            config['x_time_back'] = '1d'  # Default value
        
        if not config.get('timezone'):
            config['timezone'] = 'UTC'  # Default value
        
        if not config.get('expected_granularity'):
            config['expected_granularity'] = '1h'  # Default value
        
        logger.info(f"Config validation passed for pipeline: {sf_config['PIPELINE_NAME']}")
        
    except Exception as e:
        logger.error(f"Configuration validation failed: {str(e)}")
        raise


def check_continuity_check_performed_no_records_exist(config) -> List[Dict]:
    """Get all records with CONTINUITY_CHECK_PERFORMED = 'NO' with robust error handling"""
    
    records_list = []
    conn = None
    cursor = None
    
    try:
        logger.info("Connecting to Snowflake to check continuity records...")
        
        sf_config = config['sf_drive_config']
        
        # Test connection parameters
        _test_snowflake_connection_params(sf_config)
        
        conn = snowflake.connector.connect(
            user=sf_config['user'],
            password=sf_config['password'],
            account=sf_config['account'],
            warehouse=sf_config['warehouse'],
            database=sf_config['database'],
            schema=sf_config['schema']
        )
        
        logger.info(" Snowflake connection established")
        
        cursor = conn.cursor()
        
        # Build and validate query
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
            'PIPELINE_NAME': f"'{sf_config['PIPELINE_NAME']}'",
            'SOURCE_COMPLETE_CATEGORY': f"'{sf_config['SOURCE_COMPLETE_CATEGORY']}'",
            'PIPELINE_PRIORITY': sf_config.get('PIPELINE_PRIORITY', 1.0)
        }
        
        logger.info("Executing Snowflake Query:")
        logger.info(f"Query: {final_query}")
        logger.info("-" * 50)
        
        cursor.execute(final_query)
        
        query_id = cursor.sfqid
        logger.info(f"Query ID: {query_id}")
        
        # Convert rows to dictionaries
        column_names = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        if not rows:
            logger.info("No records found with CONTINUITY_CHECK_PERFORMED = 'NO'")
            return []
        
        for row in rows:
            record = dict(zip(column_names, row))
            records_list.append(record)
        
        logger.info(f"Found {len(records_list)} records with CONTINUITY_CHECK_PERFORMED = 'NO'")
        
        # Log sample record for debugging
        if records_list:
            sample_record = {k: v for k, v in list(records_list[0].items())[:5]}  # First 5 fields
            logger.info(f"Sample record: {sample_record}")
        
        return records_list
        
    except snowflake.connector.errors.DatabaseError as e:
        logger.error(f"Snowflake database error: {str(e)}")
        raise
    except snowflake.connector.errors.InterfaceError as e:
        logger.error(f"Snowflake interface error (connection issue): {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching continuity records: {str(e)}")
        raise
    finally:
        # Cleanup connections
        try:
            if cursor:
                cursor.close()
                logger.info("Cursor closed")
        except Exception as e:
            logger.warning(f"Error closing cursor: {str(e)}")
        
        try:
            if conn:
                conn.close()
                logger.info("Connection closed")
        except Exception as e:
            logger.warning(f"Error closing connection: {str(e)}")


def _test_snowflake_connection_params(sf_config):
    """Test Snowflake connection parameters before attempting connection"""
    try:
        logger.info("Testing Snowflake connection parameters...")
        
        # Check if all required fields are present and not template strings
        required_fields = ['user', 'password', 'account', 'warehouse', 'database', 'schema']
        for field in required_fields:
            value = sf_config.get(field)
            if not value:
                raise ValueError(f"Snowflake config field '{field}' is missing")
            if isinstance(value, str) and value.startswith('{{'):
                raise ValueError(f"Snowflake config field '{field}' appears to be an unresolved template: {value}")
        
        logger.info(" Snowflake connection parameters validated")
        
    except Exception as e:
        logger.error(f"Snowflake connection parameters validation failed: {str(e)}")
        raise


def handle_continuity_check_records(records_list: List[Dict], config):
    """Handle continuity check records with error handling"""
    
    try:
        logger.info(f"Processing {len(records_list)} continuity check records...")
        
        if not records_list:
            logger.warning("No records provided for continuity check processing")
            return
        
        # Process records
        summary = process_continuity_check_no_records_with_database_operations(records_list, config)
        
        logger.info("Continuity check processing completed successfully")
        logger.info(f"Processing summary: {summary}")
        
    except Exception as e:
        logger.error(f"Error handling continuity check records: {str(e)}")
        raise


def calculate_required_target_day(config) -> pendulum.Date:
    """Calculate required target day with robust error handling"""
    
    try:
        logger.info("Calculating required target day...")
        
        x_time_back = config.get('x_time_back', '1d')
        timezone = config.get('timezone', 'UTC')
        
        logger.info(f"Time back parameter: {x_time_back}")
        logger.info(f"Timezone: {timezone}")
        
        # Parse granularity
        time_back_seconds = parse_granularity_to_seconds(x_time_back)
        logger.info(f"Time back in seconds: {time_back_seconds}")
        
        # Calculate required target day
        current_time = pendulum.now(timezone)
        required_datetime = current_time.subtract(seconds=time_back_seconds)
        required_target_day = required_datetime.date()
        
        logger.info(f"Current time: {current_time}")
        logger.info(f"Required datetime: {required_datetime}")
        logger.info(f"Required target day: {required_target_day}")
        
        return required_target_day
        
    except Exception as e:
        logger.error(f"Error calculating required target day: {str(e)}")
        logger.error(f"x_time_back: {config.get('x_time_back')}")
        logger.error(f"timezone: {config.get('timezone')}")
        raise


def handle_missing_target_days(config, required_target_day: pendulum.Date):
    """Find gaps and create records with comprehensive error handling"""
    
    try:
        logger.info("Checking for missing target days...")
        
        existing_target_days = get_all_existing_target_days(config)
        
        if not existing_target_days:
            logger.info("No existing target days found - skipping gap analysis")
            return
        
        logger.info(f"Found {len(existing_target_days)} existing target days")
        
        missing_days = find_missing_days_in_range(existing_target_days, required_target_day)
        
        if not missing_days:
            logger.info("No missing days found in sequence")
            return
        
        logger.info(f"Found {len(missing_days)} missing days to create")
        
        # Create records for missing days
        for i, missing_day in enumerate(missing_days, 1):
            try:
                logger.info(f"Creating records for missing target day {i}/{len(missing_days)}: {missing_day}")
                
                records = create_all_records_for_target_day(str(missing_day), config)
                logger.info(f"Created {len(records)} records for {missing_day}")
                
                insert_multiple_records_to_database(records, config)
                logger.info(f" Successfully inserted records for {missing_day}")
                
            except Exception as e:
                logger.error(f" Failed to create records for missing day {missing_day}: {str(e)}")
                # Continue with other days instead of failing completely
                continue
        
        logger.info(" Missing target days processing completed")
        
    except Exception as e:
        logger.error(f"Error handling missing target days: {str(e)}")
        raise


def handle_required_target_day_creation(config, required_target_day: pendulum.Date):
    """Handle required target day creation with error handling"""
    
    try:
        logger.info("Checking required target day creation...")
        
        max_target_day = get_max_target_day(config)
        
        if max_target_day is None:
            logger.info(f"No existing records - creating required target day: {required_target_day}")
            
            records = create_all_records_for_target_day(str(required_target_day), config)
            logger.info(f"Created {len(records)} records for required target day")
            
            insert_multiple_records_to_database(records, config)
            logger.info(" Successfully inserted records for required target day")
            
        elif max_target_day < required_target_day:
            logger.info(f"Max target day ({max_target_day}) < required ({required_target_day}) - creating required target day")
            
            records = create_all_records_for_target_day(str(required_target_day), config)
            logger.info(f"Created {len(records)} records for required target day")
            
            insert_multiple_records_to_database(records, config)
            logger.info(" Successfully inserted records for required target day")
            
        else:
            logger.info(f"Up to date - max target day: {max_target_day}, required: {required_target_day}")
    
    except Exception as e:
        logger.error(f"Error handling required target day creation: {str(e)}")
        raise


def get_all_existing_target_days(config) -> Set[pendulum.Date]:
    """Get all existing target days with robust error handling"""
    
    target_days_set = set()
    conn = None
    cursor = None
    
    try:
        logger.info("Fetching all existing target days...")
        
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
            'PIPELINE_NAME': f"'{sf_config['PIPELINE_NAME']}'",
            'SOURCE_COMPLETE_CATEGORY': f"'{sf_config['SOURCE_COMPLETE_CATEGORY']}'",
            'PIPELINE_PRIORITY': sf_config.get('PIPELINE_PRIORITY', 1.0)
        }
        
        logger.info(f"Query: {final_query}")
        
        cursor.execute(final_query)
        
        query_id = cursor.sfqid
        logger.info(f"Query ID: {query_id}")
        
        results = cursor.fetchall()
        
        if not results:
            logger.info("No existing target days found")
            return set()
        
        # Convert to pendulum dates
        for row in results:
            target_day_value = row[0]
            if target_day_value:  # Check for null values
                try:
                    pendulum_date = convert_to_pendulum_date(target_day_value)
                    target_days_set.add(pendulum_date)
                except Exception as e:
                    logger.warning(f"Failed to convert target day '{target_day_value}': {str(e)}")
                    continue
        
        logger.info(f"Found {len(target_days_set)} valid existing target days")
        if target_days_set:
            logger.info(f"Date range: {min(target_days_set)} to {max(target_days_set)}")
        
        return target_days_set
        
    except Exception as e:
        logger.error(f"Error fetching existing target days: {str(e)}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_max_target_day(config) -> Union[pendulum.Date, None]:
    """Get max target day with error handling"""
    
    conn = None
    cursor = None
    
    try:
        logger.info("Fetching max target day...")
        
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
            'PIPELINE_NAME': f"'{sf_config['PIPELINE_NAME']}'",
            'SOURCE_COMPLETE_CATEGORY': f"'{sf_config['SOURCE_COMPLETE_CATEGORY']}'",
            'PIPELINE_PRIORITY': sf_config.get('PIPELINE_PRIORITY', 1.0)
        }
        
        cursor.execute(final_query)
        result = cursor.fetchone()
        
        if result and result[0]:
            max_target_day = convert_to_pendulum_date(result[0])
            logger.info(f"Max target day: {max_target_day}")
            return max_target_day
        else:
            logger.info("No existing target days found")
            return None
        
    except Exception as e:
        logger.error(f"Error fetching max target day: {str(e)}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def find_missing_days_in_range(existing_days: Set[pendulum.Date], 
                              required_target_day: pendulum.Date) -> List[pendulum.Date]:
    """Find missing days with validation"""
    
    try:
        logger.info("Finding missing days in range...")
        
        if not existing_days:
            logger.info("No existing days provided")
            return []
        
        missing_days = []
        start_date = min(existing_days)
        current_date = start_date.add(days=1)
        
        logger.info(f"Checking from {start_date} to {required_target_day}")
        
        while current_date <= required_target_day:
            if current_date not in existing_days:
                missing_days.append(current_date)
            current_date = current_date.add(days=1)
        
        logger.info(f"Existing days count: {len(existing_days)}")
        logger.info(f"Missing days found: {len(missing_days)}")
        if missing_days:
            logger.info(f"Missing days: {[str(day) for day in missing_days]}")
        
        return missing_days
        
    except Exception as e:
        logger.error(f"Error finding missing days: {str(e)}")
        raise