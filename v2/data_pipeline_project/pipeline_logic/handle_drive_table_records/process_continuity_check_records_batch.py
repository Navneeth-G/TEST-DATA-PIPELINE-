# pipleine_logic/handle_drive_table_records/process_continuity_check_records_batch.py
import pendulum
from typing import Union

from pipleine_logic.handle_drive_table_records.handle_record_with_continuity_check_no import handle_record_with_continuity_check_as_no

def convert_to_pendulum_date(date_value: Union[str, pendulum.Date, pendulum.DateTime, object]) -> pendulum.Date:
    """
    Convert various date formats to pendulum.Date object
    First converts to ISO standard string, then to pendulum
    
    Args:
        date_value: Date value in various formats (string, datetime, pendulum, etc.)
        
    Returns:
        pendulum.Date: Converted date as pendulum Date object
        
    Raises:
        ValueError: If conversion fails
    """
    
    try:
        # Step 1: Convert to ISO standard string
        if isinstance(date_value, str):
            # Already a string, parse and convert to ISO
            temp_date = pendulum.parse(date_value)
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
        
        return pendulum_date
        
    except Exception as e:
        raise ValueError(f"Failed to convert date value '{date_value}' to pendulum.Date: {str(e)}")


def get_target_days_with_continuity_check_yes(config) -> Set[pendulum.Date]:
    """
    Get all TARGET_DAY values from database where CONTINUITY_CHECK_PERFORMED = YES
    
    Args:
        config: Configuration dictionary containing sf_drive_config for Snowflake connection
        
    Returns:
        Set[pendulum.Date]: Set of TARGET_DAY values as pendulum Date objects
    """
    
    target_days_set = set()
    
    try:
        # Extract Snowflake config
        sf_config = config['sf_drive_config']
        
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
        
        # SQL query with parameter substitution
        query = """
        SELECT DISTINCT TARGET_DAY 
        FROM %(table_name)s 
        WHERE PIPELINE_NAME = %(PIPELINE_NAME)s 
        AND SOURCE_COMPLETE_CATEGORY = %(SOURCE_COMPLETE_CATEGORY)s
        AND PIPELINE_PRIORITY = %(PIPELINE_PRIORITY)s        
        AND CONTINUITY_CHECK_PERFORMED = 'YES'
        """
        
        # Substitute parameters
        final_query = query % {
            'table_name': sf_config['table_name'],
            'PIPELINE_NAME': sf_config['PIPELINE_NAME'],
            'SOURCE_COMPLETE_CATEGORY': sf_config['SOURCE_COMPLETE_CATEGORY'],
            'PIPELINE_PRIORITY': sf_config['PIPELINE_PRIORITY']
        }
        
        print(f" Executing Snowflake Query:")
        print(f"Query: {final_query}")
        print("-" * 50)
        
        cursor.execute(final_query)
        
        # Get query ID for debugging
        query_id = cursor.sfqid
        print(f" Query ID: {query_id}")
        
        # Fetch all results
        results = cursor.fetchall()
        
        # Convert to set of pendulum Date objects using our conversion function
        for row in results:
            target_day_value = row[0]
            pendulum_date = convert_to_pendulum_date(target_day_value)
            target_days_set.add(pendulum_date)
        
        print(f" Found {len(target_days_set)} target days with CONTINUITY_CHECK_PERFORMED = YES")
        print(f"Target days: {sorted([str(date) for date in target_days_set])}")
        
    except Exception as e:
        print(f" Error fetching target days from Snowflake: {str(e)}")
        raise
    
    finally:
        # Close connections
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
    
    return target_days_set



def delete_exact_record_from_database(record, config):
    """
    Delete the exact record from database using all record fields as WHERE conditions
    
    Args:
        record: Record dictionary containing all fields to match
        config: Configuration dictionary with sf_drive_config
    """
    
    try:
        # Extract Snowflake config
        sf_config = config['sf_drive_config']
        
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
        
        # Construct DELETE query
        where_clause = " AND ".join(where_conditions)
        
        query = """
        DELETE FROM %(table_name)s 
        WHERE %(where_clause)s
        """
        
        # Substitute parameters
        final_query = query % {
            'table_name': sf_config['table_name'],
            'where_clause': where_clause
        }
        
        print(f" Executing Snowflake DELETE Query:")
        print(f"Query: {final_query}")
        print(f"Parameters: {query_params}")
        print("-" * 50)
        
        cursor.execute(final_query, query_params)
        
        # Get query ID and affected rows
        query_id = cursor.sfqid
        affected_rows = cursor.rowcount
        
        print(f" Query ID: {query_id}")
        print(f" Deleted {affected_rows} record(s)")
        
    except Exception as e:
        print(f" Error deleting record from Snowflake: {str(e)}")
        raise
    
    finally:
        # Close connections
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()



def insert_single_record_to_database(record, config):
    """
    Insert a single processed record into the database
    
    Args:
        record: Single processed record dictionary to insert
        config: Configuration dictionary with sf_drive_config
    """
    
    try:
        # Extract Snowflake config
        sf_config = config['sf_drive_config']
        
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
        column_placeholders = [f"%({col})s" for col in column_names]
        
        columns_str = ", ".join(column_names)
        values_str = ", ".join(column_placeholders)
        
        query = """
        INSERT INTO %(table_name)s (%(columns)s) 
        VALUES (%(values)s)
        """
        
        # Substitute parameters
        final_query = query % {
            'table_name': sf_config['table_name'],
            'columns': columns_str,
            'values': values_str
        }
        
        print(f"Executing Snowflake INSERT Query:")
        print(f"Query: {final_query}")
        print(f"Parameters: {record}")
        print("-" * 50)
        
        cursor.execute(final_query, record)
        
        # Get query ID and affected rows
        query_id = cursor.sfqid
        affected_rows = cursor.rowcount
        
        print(f"Query ID: {query_id}")
        print(f"Inserted {affected_rows} record(s)")
        print(f"Record PIPELINE_ID: {record.get('PIPELINE_ID', 'Unknown')}")
        
    except Exception as e:
        print(f"Error inserting single record to Snowflake: {str(e)}")
        raise
    
    finally:
        # Close connections
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()



def insert_multiple_records_to_database(records_list, config):
    """
    Insert multiple processed records into the database using bulk insert
    
    Args:
        records_list: List of processed record dictionaries to insert
        config: Configuration dictionary with sf_drive_config
    """
    
    try:
        # Extract Snowflake config
        sf_config = config['sf_drive_config']
        
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
        
        # Check if records exist
        if not records_list:
            print("No records to insert")
            return
        
        # Build query components
        column_names = list(records_list[0].keys())
        columns_str = ", ".join(column_names)
        placeholders = ", ".join(["?"] * len(column_names))
        query = f"INSERT INTO {sf_config['table_name']} ({columns_str}) VALUES ({placeholders})"
        
        # Prepare data as list of tuples
        data = [tuple(record[col] for col in column_names) for record in records_list]
        
        print(f"Executing Snowflake BULK INSERT Query:")
        print(f"Query: {query}")
        print(f"Number of records: {len(records_list)}")
        print(f"Sample data: {data[:2]}")  # Show first 2 tuples
        print("-" * 50)
        
        # Execute bulk insert
        cursor.executemany(query, data)
        
        # Get query ID and affected rows
        query_id = cursor.sfqid
        affected_rows = cursor.rowcount
        
        print(f"Query ID: {query_id}")
        print(f"Inserted {affected_rows} record(s)")
        
    except Exception as e:
        print(f"Error inserting multiple records to Snowflake: {str(e)}")
        raise
    
    finally:
        # Close connections
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()




def process_continuity_check_no_records_with_database_operations(records_list, config):
    """
    Main function: Process list of records with CONTINUITY_CHECK_PERFORMED = NO
    Handles database operations (delete + insert) for each processed record
    
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

    # Get all processed target days once at the beginning
    CONTINUITY_CHECK_yes_target_days = get_target_days_with_continuity_check_yes(config)

    for record in records_list:
        try:
            # Extract TARGET_DAY from record
            TARGET_DAY = record.get("TARGET_DAY")
            if not TARGET_DAY:
                print(f"Skipping record without TARGET_DAY: {record.get('PIPELINE_ID', 'Unknown')}")
                continue
            
            # Convert TARGET_DAY to pendulum date for comparison
            TARGET_DAY_pendulum = convert_to_pendulum_date(TARGET_DAY)
            
            # Check if TARGET_DAY exists in our collected set
            target_day_exists = TARGET_DAY_pendulum in CONTINUITY_CHECK_yes_target_days
            
            # Build processed_target_days list for this record
            processed_target_days = [TARGET_DAY] if target_day_exists else []
            
            # Process record through existing decision tree
            result_records = handle_record_with_continuity_check_as_no(record, config, processed_target_days)
            
            # Delete existing records for this TARGET_DAY
            delete_exact_record_from_database(record, config)
            
            # Insert based on result type
            if len(result_records) == 1:
                # Single Record Rebuild: TARGET_DAY exists + complete windows
                insert_single_record_to_database(result_records[0], config)
                processing_summary["single_record_rebuilds"] += 1
                print(f"Single Record Rebuild: Processed 1 record for TARGET_DAY: {TARGET_DAY}")
            else:
                # Multiple Records: Either missing windows or new target day
                insert_multiple_records_to_database(result_records, config)
                if target_day_exists:
                    processing_summary["full_day_recreations_missing_windows"] += 1
                    print(f"Full Day Recreation (Missing Windows): Processed {len(result_records)} records for TARGET_DAY: {TARGET_DAY}")
                else:
                    processing_summary["full_day_creations_new_target_day"] += 1
                    print(f"Full Day Creation (New Target Day): Processed {len(result_records)} records for TARGET_DAY: {TARGET_DAY}")
            
            processing_summary["total_records_processed"] += 1
            
        except Exception as e:
            print(f"Error processing record for TARGET_DAY {TARGET_DAY}: {str(e)}")
            processing_summary["failed_records"].append({
                "TARGET_DAY": TARGET_DAY,
                "PIPELINE_ID": record.get("PIPELINE_ID", "Unknown"),
                "error": str(e)
            })
    
    # Print final summary
    print(f"\nProcessing Summary:")
    print(f"Total records processed: {processing_summary['total_records_processed']}")
    print(f"Single Record Rebuilds: {processing_summary['single_record_rebuilds']}")
    print(f"Full Day Recreations (Missing Windows): {processing_summary['full_day_recreations_missing_windows']}")
    print(f"Full Day Creations (New Target Day): {processing_summary['full_day_creations_new_target_day']}")
    print(f"Failed records: {len(processing_summary['failed_records'])}")
    
    return processing_summary



