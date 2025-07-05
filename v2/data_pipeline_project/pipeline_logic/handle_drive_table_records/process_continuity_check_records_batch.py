# pipleine_logic/handle_drive_table_records/process_continuity_check_records_batch.py
import pendulum
import snowflake.connector
from typing import Union, Set, List, Dict
import logging

# Configure logging for Airflow
logger = logging.getLogger(__name__)

from pipleine_logic.handle_drive_table_records.handle_record_with_continuity_check_no import handle_record_with_continuity_check_as_no


def convert_to_pendulum_date(date_value: Union[str, pendulum.Date, pendulum.DateTime, object]) -> pendulum.Date:
    """
    Convert various date formats to pendulum.Date object with comprehensive error handling
    
    Args:
        date_value: Date value in various formats (string, datetime, pendulum, etc.)
        
    Returns:
        pendulum.Date: Converted date as pendulum Date object
        
    Raises:
        ValueError: If conversion fails
    """
    
    try:
        logger.debug(f"Converting date value: {date_value} (type: {type(date_value)})")
        
        # Handle None or empty values
        if date_value is None:
            raise ValueError("Date value is None")
        
        if isinstance(date_value, str) and not date_value.strip():
            raise ValueError("Date value is empty string")
        
        # Step 1: Convert to ISO standard string
        if isinstance(date_value, str):
            # Already a string, parse and convert to ISO
            temp_date = pendulum.parse(date_value.strip())
            iso_string = temp_date.to_date_string()  # YYYY-MM-DD format
        elif hasattr(date_value, 'date'):
            # DateTime object (pendulum or standard datetime)
            if hasattr(date_value, 'to_date_string'):
                # Pendulum datetime
                iso_string = date_value.to_date_string()
            else:
                # Standard datetime
                iso_string = date_value.strftime('%Y-%m-%d')
        elif hasattr(date_value, 'strftime'):
            # Date object
            iso_string = date_value.strftime('%Y-%m-%d')
        else:
            # Try to convert whatever it is to string first
            iso_string = pendulum.parse(str(date_value)).to_date_string()
        
        # Step 2: Convert ISO string to pendulum.Date
        pendulum_date = pendulum.parse(iso_string).date()
        
        logger.debug(f"Successfully converted {date_value} to {pendulum_date}")
        return pendulum_date
        
    except Exception as e:
        error_msg = f"Failed to convert date value '{date_value}' (type: {type(date_value)}) to pendulum.Date: {str(e)}"
        logger.error(error_msg)
        raise ValueError(error_msg)


def get_target_days_with_continuity_check_yes(config) -> Set[pendulum.Date]:
    """
    Get all TARGET_DAY values from database where CONTINUITY_CHECK_PERFORMED = YES
    with comprehensive error handling
    
    Args:
        config: Configuration dictionary containing sf_drive_config for Snowflake connection
        
    Returns:
        Set[pendulum.Date]: Set of TARGET_DAY values as pendulum Date objects
    """
    
    target_days_set = set()
    conn = None
    cursor = None
    
    try:
        logger.info("Fetching target days with CONTINUITY_CHECK_PERFORMED = YES...")
        
        # Validate config
        if not config:
            raise ValueError("Config is None or empty")
        
        sf_config = config.get('sf_drive_config')
        if not sf_config:
            raise ValueError("sf_drive_config is missing from config")
        
        # Validate required fields
        required_fields = ['user', 'password', 'account', 'warehouse', 'database', 'schema', 'table_name', 'PIPELINE_NAME', 'SOURCE_COMPLETE_CATEGORY']
        for field in required_fields:
            if not sf_config.get(field):
                raise ValueError(f"Required field sf_drive_config.{field} is missing or empty")
        
        # Establish Snowflake connection
        logger.info("Establishing Snowflake connection...")
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
        
        # Build SQL query with parameter substitution
        query = """
        SELECT DISTINCT TARGET_DAY 
        FROM %(table_name)s 
        WHERE PIPELINE_NAME = %(PIPELINE_NAME)s 
        AND SOURCE_COMPLETE_CATEGORY = %(SOURCE_COMPLETE_CATEGORY)s
        AND PIPELINE_PRIORITY = %(PIPELINE_PRIORITY)s        
        AND CONTINUITY_CHECK_PERFORMED = 'YES'
        """
        
        # Substitute parameters safely
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
        
        # Get query ID for debugging
        query_id = cursor.sfqid
        logger.info(f"Query ID: {query_id}")
        
        # Fetch all results
        results = cursor.fetchall()
        
        if not results:
            logger.info("No target days found with CONTINUITY_CHECK_PERFORMED = YES")
            return set()
        
        logger.info(f"Found {len(results)} target day records")
        
        # Convert to set of pendulum Date objects
        conversion_errors = 0
        for i, row in enumerate(results):
            try:
                target_day_value = row[0]
                if target_day_value is not None:  # Skip null values
                    pendulum_date = convert_to_pendulum_date(target_day_value)
                    target_days_set.add(pendulum_date)
                else:
                    logger.warning(f"Skipping null TARGET_DAY value in row {i+1}")
                    conversion_errors += 1
            except Exception as e:
                logger.error(f"Error converting TARGET_DAY in row {i+1}: {str(e)}")
                conversion_errors += 1
                continue
        
        logger.info(f"Successfully converted {len(target_days_set)} target days")
        if conversion_errors > 0:
            logger.warning(f"Failed to convert {conversion_errors} target day values")
        
        if target_days_set:
            sorted_dates = sorted([str(date) for date in target_days_set])
            logger.info(f"Target days range: {sorted_dates[0]} to {sorted_dates[-1]}")
            logger.debug(f"All target days: {sorted_dates}")
        
        return target_days_set
        
    except snowflake.connector.errors.DatabaseError as e:
        logger.error(f"Snowflake database error: {str(e)}")
        raise
    except snowflake.connector.errors.InterfaceError as e:
        logger.error(f"Snowflake interface error (connection issue): {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching target days from Snowflake: {str(e)}")
        raise
    
    finally:
        # Close connections safely
        try:
            if cursor:
                cursor.close()
                logger.debug("Cursor closed")
        except Exception as e:
            logger.warning(f"Error closing cursor: {str(e)}")
        
        try:
            if conn:
                conn.close()
                logger.debug("Connection closed")
        except Exception as e:
            logger.warning(f"Error closing connection: {str(e)}")


def delete_exact_record_from_database(record, config):
    """
    Delete the exact record from database using all record fields as WHERE conditions
    with comprehensive error handling
    
    Args:
        record: Record dictionary containing all fields to match
        config: Configuration dictionary with sf_drive_config
    """
    
    conn = None
    cursor = None
    
    try:
        logger.info("Deleting exact record from database...")
        
        # Validate inputs
        if not record:
            raise ValueError("Record is None or empty")
        
        if not config or not config.get('sf_drive_config'):
            raise ValueError("Invalid config or missing sf_drive_config")
        
        sf_config = config['sf_drive_config']
        
        # Log record info for debugging
        pipeline_id = record.get('PIPELINE_ID', 'Unknown')
        target_day = record.get('TARGET_DAY', 'Unknown')
        logger.info(f"Deleting record - PIPELINE_ID: {pipeline_id}, TARGET_DAY: {target_day}")
        
        # Establish Snowflake connection
        conn = snowflake.connector.connect(
            user=sf_config['user'],
            password=sf_config['password'],
            account=sf_config['account'],
            warehouse=sf_config['warehouse'],
            database=sf_config['database'],
            schema=sf_config['schema']
        )
        
        cursor = conn.cursor()
        
        # Build WHERE clause dynamically from all record fields
        where_conditions = []
        query_params = {}
        
        for field_name, field_value in record.items():
            if field_value is not None:
                where_conditions.append(f"{field_name} = %({field_name})s")
                query_params[field_name] = field_value
        
        if not where_conditions:
            raise ValueError("No valid fields found in record for WHERE clause")
        
        # Construct DELETE query
        where_clause = " AND ".join(where_conditions)
        
        query = f"DELETE FROM {sf_config['table_name']} WHERE {where_clause}"
        
        logger.info("Executing Snowflake DELETE Query:")
        logger.info(f"Query: {query}")
        logger.info(f"Parameters count: {len(query_params)}")
        logger.debug(f"Parameters: {query_params}")
        logger.info("-" * 50)
        
        cursor.execute(query, query_params)
        
        # Get query ID and affected rows
        query_id = cursor.sfqid
        affected_rows = cursor.rowcount
        
        logger.info(f"Query ID: {query_id}")
        logger.info(f"Deleted {affected_rows} record(s)")
        
        if affected_rows == 0:
            logger.warning("No records were deleted - record may not exist")
        elif affected_rows > 1:
            logger.warning(f"Multiple records ({affected_rows}) were deleted - this may indicate duplicate records")
        
    except snowflake.connector.errors.DatabaseError as e:
        logger.error(f"Snowflake database error during delete: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error deleting record from Snowflake: {str(e)}")
        logger.error(f"Record PIPELINE_ID: {record.get('PIPELINE_ID', 'Unknown')}")
        raise
    
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception as e:
            logger.warning(f"Error closing cursor: {str(e)}")
        
        try:
            if conn:
                conn.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {str(e)}")


def insert_single_record_to_database(record, config):
    """
    Insert a single processed record into the database with comprehensive error handling
    
    Args:
        record: Single processed record dictionary to insert
        config: Configuration dictionary with sf_drive_config
    """
    
    conn = None
    cursor = None
    
    try:
        logger.info("Inserting single record to database...")
        
        # Validate inputs
        if not record:
            raise ValueError("Record is None or empty")
        
        if not config or not config.get('sf_drive_config'):
            raise ValueError("Invalid config or missing sf_drive_config")
        
        sf_config = config['sf_drive_config']
        
        # Log record info for debugging
        pipeline_id = record.get('PIPELINE_ID', 'Unknown')
        target_day = record.get('TARGET_DAY', 'Unknown')
        logger.info(f"Inserting record - PIPELINE_ID: {pipeline_id}, TARGET_DAY: {target_day}")
        
        # Establish Snowflake connection
        conn = snowflake.connector.connect(
            user=sf_config['user'],
            password=sf_config['password'],
            account=sf_config['account'],
            warehouse=sf_config['warehouse'],
            database=sf_config['database'],
            schema=sf_config['schema']
        )
        
        cursor = conn.cursor()
        
        # Build INSERT query dynamically from record fields
        column_names = list(record.keys())
        if not column_names:
            raise ValueError("Record has no fields to insert")
        
        column_placeholders = [f"%({col})s" for col in column_names]
        
        columns_str = ", ".join(column_names)
        values_str = ", ".join(column_placeholders)
        
        query = f"INSERT INTO {sf_config['table_name']} ({columns_str}) VALUES ({values_str})"
        
        logger.info("Executing Snowflake INSERT Query:")
        logger.info(f"Query: {query}")
        logger.info(f"Columns count: {len(column_names)}")
        logger.debug(f"Columns: {column_names}")
        logger.debug(f"Record values: {record}")
        logger.info("-" * 50)
        
        cursor.execute(query, record)
        
        # Get query ID and affected rows
        query_id = cursor.sfqid
        affected_rows = cursor.rowcount
        
        logger.info(f"Query ID: {query_id}")
        logger.info(f"Inserted {affected_rows} record(s)")
        logger.info(f" Successfully inserted record PIPELINE_ID: {pipeline_id}")
        
        if affected_rows != 1:
            logger.warning(f"Expected to insert 1 record, but inserted {affected_rows}")
        
    except snowflake.connector.errors.DatabaseError as e:
        logger.error(f"Snowflake database error during insert: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error inserting single record to Snowflake: {str(e)}")
        logger.error(f"Record PIPELINE_ID: {record.get('PIPELINE_ID', 'Unknown')}")
        raise
    
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception as e:
            logger.warning(f"Error closing cursor: {str(e)}")
        
        try:
            if conn:
                conn.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {str(e)}")


def insert_multiple_records_to_database(records_list, config):
    """
    Insert multiple processed records into the database using bulk insert
    with comprehensive error handling
    
    Args:
        records_list: List of processed record dictionaries to insert
        config: Configuration dictionary with sf_drive_config
    """
    
    conn = None
    cursor = None
    
    try:
        logger.info("Inserting multiple records to database...")
        
        # Validate inputs
        if not records_list:
            logger.info("No records to insert")
            return
        
        if not isinstance(records_list, list):
            raise ValueError("records_list must be a list")
        
        if not config or not config.get('sf_drive_config'):
            raise ValueError("Invalid config or missing sf_drive_config")
        
        sf_config = config['sf_drive_config']
        
        logger.info(f"Inserting {len(records_list)} records...")
        
        # Validate record structure
        if not records_list[0]:
            raise ValueError("First record is empty")
        
        column_names = list(records_list[0].keys())
        if not column_names:
            raise ValueError("Records have no columns")
        
        # Validate all records have same structure
        for i, record in enumerate(records_list):
            if not record:
                raise ValueError(f"Record {i+1} is empty")
            if list(record.keys()) != column_names:
                logger.warning(f"Record {i+1} has different column structure")
        
        # Establish Snowflake connection
        conn = snowflake.connector.connect(
            user=sf_config['user'],
            password=sf_config['password'],
            account=sf_config['account'],
            warehouse=sf_config['warehouse'],
            database=sf_config['database'],
            schema=sf_config['schema']
        )
        
        cursor = conn.cursor()
        
        # Build query components
        columns_str = ", ".join(column_names)
        placeholders = ", ".join(["?"] * len(column_names))
        query = f"INSERT INTO {sf_config['table_name']} ({columns_str}) VALUES ({placeholders})"
        
        # Prepare data as list of tuples
        data = []
        conversion_errors = 0
        for i, record in enumerate(records_list):
            try:
                tuple_data = tuple(record[col] for col in column_names)
                data.append(tuple_data)
            except Exception as e:
                logger.error(f"Error preparing record {i+1} for insert: {str(e)}")
                conversion_errors += 1
                continue
        
        if conversion_errors > 0:
            logger.warning(f"Failed to prepare {conversion_errors} records for insert")
        
        if not data:
            raise ValueError("No valid records to insert after data preparation")
        
        logger.info("Executing Snowflake BULK INSERT Query:")
        logger.info(f"Query: {query}")
        logger.info(f"Number of records: {len(data)}")
        logger.info(f"Columns count: {len(column_names)}")
        logger.debug(f"Columns: {column_names}")
        logger.debug(f"Sample data (first 2 records): {data[:2]}")
        logger.info("-" * 50)
        
        # Execute bulk insert
        cursor.executemany(query, data)
        
        # Get query ID and affected rows
        query_id = cursor.sfqid
        affected_rows = cursor.rowcount
        
        logger.info(f"Query ID: {query_id}")
        logger.info(f"Inserted {affected_rows} record(s)")
        logger.info(" Successfully completed bulk insert")
        
        if affected_rows != len(data):
            logger.warning(f"Expected to insert {len(data)} records, but inserted {affected_rows}")
        
    except snowflake.connector.errors.DatabaseError as e:
        logger.error(f"Snowflake database error during bulk insert: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error inserting multiple records to Snowflake: {str(e)}")
        logger.error(f"Number of records attempted: {len(records_list) if records_list else 0}")
        raise
    
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception as e:
            logger.warning(f"Error closing cursor: {str(e)}")
        
        try:
            if conn:
                conn.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {str(e)}")


def process_continuity_check_no_records_with_database_operations(records_list, config):
    """
    Main function: Process list of records with CONTINUITY_CHECK_PERFORMED = NO
    Handles database operations (delete + insert) for each processed record
    with comprehensive error handling and progress tracking
    
    Args:
        records_list: List of records with CONTINUITY_CHECK_PERFORMED = NO
        config: Configuration dictionary
        
    Returns:
        dict: Summary of processing results
    """
    
    processing_summary = {
        "total_records_processed": 0,
        "single_record_rebuilds": 0,
        "full_day_recreations_missing_windows": 0,
        "full_day_creations_new_target_day": 0,
        "failed_records": []
    }

    try:
        logger.info("=" * 60)
        logger.info("STARTING CONTINUITY CHECK RECORDS PROCESSING")
        logger.info("=" * 60)
        
        # Validate inputs
        if not records_list:
            logger.info("No records to process")
            return processing_summary
        
        if not isinstance(records_list, list):
            raise ValueError("records_list must be a list")
        
        if not config:
            raise ValueError("Config is None or empty")
        
        logger.info(f"Processing {len(records_list)} records with CONTINUITY_CHECK_PERFORMED = NO")
        
        # Get all processed target days once at the beginning
        logger.info("Fetching existing target days with CONTINUITY_CHECK_PERFORMED = YES...")
        CONTINUITY_CHECK_yes_target_days = get_target_days_with_continuity_check_yes(config)
        logger.info(f"Found {len(CONTINUITY_CHECK_yes_target_days)} existing processed target days")

        # Process each record
        for record_index, record in enumerate(records_list, 1):
            try:
                logger.info(f"\n--- Processing record {record_index}/{len(records_list)} ---")
                
                # Validate record
                if not record:
                    logger.warning(f"Skipping empty record {record_index}")
                    continue
                
                # Extract TARGET_DAY from record
                TARGET_DAY = record.get("TARGET_DAY")
                PIPELINE_ID = record.get("PIPELINE_ID", "Unknown")
                
                if not TARGET_DAY:
                    logger.warning(f"Skipping record {record_index} without TARGET_DAY: PIPELINE_ID {PIPELINE_ID}")
                    continue
                
                logger.info(f"Processing TARGET_DAY: {TARGET_DAY}, PIPELINE_ID: {PIPELINE_ID}")
                
                # Convert TARGET_DAY to pendulum date for comparison
                try:
                    TARGET_DAY_pendulum = convert_to_pendulum_date(TARGET_DAY)
                except Exception as e:
                    logger.error(f"Failed to convert TARGET_DAY '{TARGET_DAY}' for record {record_index}: {str(e)}")
                    processing_summary["failed_records"].append({
                        "record_index": record_index,
                        "TARGET_DAY": TARGET_DAY,
                        "PIPELINE_ID": PIPELINE_ID,
                        "error": f"Date conversion error: {str(e)}"
                    })
                    continue
                
                # Check if TARGET_DAY exists in our collected set
                target_day_exists = TARGET_DAY_pendulum in CONTINUITY_CHECK_yes_target_days
                logger.info(f"TARGET_DAY {TARGET_DAY} exists in processed days: {target_day_exists}")
                
                # Build processed_target_days list for this record
                processed_target_days = [TARGET_DAY] if target_day_exists else []
                
                # Process record through existing decision tree
                logger.info("Processing record through decision tree...")
                result_records = handle_record_with_continuity_check_as_no(record, config, processed_target_days)
                
                if not result_records:
                    logger.error(f"No result records returned for TARGET_DAY {TARGET_DAY}")
                    processing_summary["failed_records"].append({
                        "record_index": record_index,
                        "TARGET_DAY": TARGET_DAY,
                        "PIPELINE_ID": PIPELINE_ID,
                        "error": "No result records returned from processing"
                    })
                    continue
                
                logger.info(f"Generated {len(result_records)} result records")
                
                # Delete existing record
                logger.info("Deleting existing record...")
                delete_exact_record_from_database(record, config)
                
                # Insert based on result type
                if len(result_records) == 1:
                    # Single Record Rebuild: TARGET_DAY exists + complete windows
                    logger.info("Single record rebuild - inserting 1 record")
                    insert_single_record_to_database(result_records[0], config)
                    processing_summary["single_record_rebuilds"] += 1
                    logger.info(f" Single Record Rebuild completed for TARGET_DAY: {TARGET_DAY}")
                else:
                    # Multiple Records: Either missing windows or new target day
                    logger.info(f"Multiple records - inserting {len(result_records)} records")
                    insert_multiple_records_to_database(result_records, config)
                    if target_day_exists:
                        processing_summary["full_day_recreations_missing_windows"] += 1
                        logger.info(f" Full Day Recreation (Missing Windows) completed: {len(result_records)} records for TARGET_DAY: {TARGET_DAY}")
                    else:
                        processing_summary["full_day_creations_new_target_day"] += 1
                        logger.info(f" Full Day Creation (New Target Day) completed: {len(result_records)} records for TARGET_DAY: {TARGET_DAY}")
                
                processing_summary["total_records_processed"] += 1
                logger.info(f" Successfully processed record {record_index}/{len(records_list)}")
                
            except Exception as e:
                logger.error(f" Error processing record {record_index} for TARGET_DAY {TARGET_DAY}: {str(e)}")
                processing_summary["failed_records"].append({
                    "record_index": record_index,
                    "TARGET_DAY": TARGET_DAY,
                    "PIPELINE_ID": record.get("PIPELINE_ID", "Unknown"),
                    "error": str(e)
                })
                # Continue with next record instead of failing completely
                continue
        
        # Print final summary
        logger.info("\n" + "=" * 60)
        logger.info("PROCESSING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total records processed: {processing_summary['total_records_processed']}")
        logger.info(f"Single Record Rebuilds: {processing_summary['single_record_rebuilds']}")
        logger.info(f"Full Day Recreations (Missing Windows): {processing_summary['full_day_recreations_missing_windows']}")
        logger.info(f"Full Day Creations (New Target Day): {processing_summary['full_day_creations_new_target_day']}")
        logger.info(f"Failed records: {len(processing_summary['failed_records'])}")
        
        if processing_summary['failed_records']:
            logger.warning("Failed records details:")
            for failed_record in processing_summary['failed_records']:
                logger.warning(f"  - Record {failed_record['record_index']}: {failed_record['TARGET_DAY']} - {failed_record['error']}")
        
        logger.info("=" * 60)
        
        return processing_summary
        
    except Exception as e:
        logger.error(f"Critical error in continuity check processing: {str(e)}")
        raise








