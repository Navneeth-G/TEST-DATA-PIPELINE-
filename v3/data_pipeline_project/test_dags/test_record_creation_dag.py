# dags/test_record_creation_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pendulum
import sys
import os
import logging
import snowflake.connector

# Configure logging
logger = logging.getLogger(__name__)

# Add your project path to sys.path
sys.path.append('/opt/airflow/dags/pipeline_logic')

# Import your record creation function
try:
    from pipeline_logic.handle_drive_table_records.create_new_records_task import handle_drive_records_creation_task
    logger.info("Successfully imported handle_drive_records_creation_task")
except ImportError as e:
    logger.error(f"Failed to import handle_drive_records_creation_task: {str(e)}")
    raise

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}

# DAG definition
dag = DAG(
    'test_record_creation_dag',
    default_args=default_args,
    description='Test DAG for record creation task with comprehensive error handling',
    schedule_interval=None,  # Manual trigger for testing
    catchup=False,
    tags=['test', 'record-creation', 'drive-table'],
)


def get_airflow_variable_safely(var_name, default_value=None):
    """
    Safely get Airflow variable with error handling
    
    Args:
        var_name: Variable name to retrieve
        default_value: Default value if variable doesn't exist
        
    Returns:
        Variable value or default value
    """
    try:
        value = Variable.get(var_name, default_var=default_value)
        if value and not str(value).startswith('{{'):
            return value
        else:
            logger.warning(f"Variable {var_name} appears to be unresolved template or empty: {value}")
            return default_value
    except Exception as e:
        logger.warning(f"Failed to get Airflow variable {var_name}: {str(e)}")
        return default_value


def get_test_config():
    """
    Return test configuration with comprehensive error handling
    """
    try:
        logger.info("Building test configuration from Airflow variables...")
        
        # Core Snowflake configuration
        sf_config = {
            'user': get_airflow_variable_safely('snowflake_user'),
            'password': get_airflow_variable_safely('snowflake_password'),
            'account': get_airflow_variable_safely('snowflake_account'),
            'warehouse': get_airflow_variable_safely('snowflake_warehouse'),
            'database': get_airflow_variable_safely('snowflake_database'),
            'schema': get_airflow_variable_safely('snowflake_schema'),
            'table_name': get_airflow_variable_safely('drive_table_name'),
            'PIPELINE_NAME': get_airflow_variable_safely('pipeline_name'),
            'SOURCE_COMPLETE_CATEGORY': get_airflow_variable_safely('source_complete_category'),
            'PIPELINE_PRIORITY': float(get_airflow_variable_safely('pipeline_priority', '1.0'))
        }
        
        # Additional configuration
        config = {
            'sf_drive_config': sf_config,
            'x_time_back': get_airflow_variable_safely('x_time_back', '1d'),
            'timezone': get_airflow_variable_safely('timezone', 'UTC'),
            'expected_granularity': get_airflow_variable_safely('expected_granularity', '1h'),
            
            # Record creation configuration
            'PIPELINE_NAME': get_airflow_variable_safely('pipeline_name'),
            'index_group': get_airflow_variable_safely('index_group'),
            'index_name': get_airflow_variable_safely('index_name'),
            'index_id': get_airflow_variable_safely('index_id'),
            's3_bucket': get_airflow_variable_safely('s3_bucket'),
            's3_prefix_list': [get_airflow_variable_safely('s3_prefix', 'default-prefix')],
            'database.schema.table': get_airflow_variable_safely('target_database_path'),
            'CAN_ACCESS_HISTORICAL_DATA': get_airflow_variable_safely('can_access_historical_data', 'YES')
        }
        
        logger.info("Configuration built successfully")
        return config
        
    except Exception as e:
        logger.error(f"Failed to build test configuration: {str(e)}")
        raise


def validate_config_task(**context):
    """
    Validate configuration before running the main task with comprehensive checks
    """
    
    try:
        logger.info("=" * 60)
        logger.info("VALIDATING CONFIGURATION")
        logger.info("=" * 60)
        
        config = get_test_config()
        
        # Check required Snowflake fields
        required_sf_fields = [
            ('user', 'snowflake_user'),
            ('password', 'snowflake_password'),
            ('account', 'snowflake_account'),
            ('warehouse', 'snowflake_warehouse'),
            ('database', 'snowflake_database'),
            ('schema', 'snowflake_schema'),
            ('table_name', 'drive_table_name'),
            ('PIPELINE_NAME', 'pipeline_name'),
            ('SOURCE_COMPLETE_CATEGORY', 'source_complete_category')
        ]
        
        missing_sf_fields = []
        for field_key, var_name in required_sf_fields:
            value = config['sf_drive_config'].get(field_key)
            if not value or str(value).startswith('{{') or str(value).strip() == '':
                missing_sf_fields.append(var_name)
        
        # Check required record creation fields
        required_record_fields = [
            ('index_group', 'index_group'),
            ('index_name', 'index_name'),
            ('index_id', 'index_id'),
            ('s3_bucket', 's3_bucket'),
            ('database.schema.table', 'target_database_path')
        ]
        
        missing_record_fields = []
        for field_key, var_name in required_record_fields:
            value = config.get(field_key)
            if not value or str(value).startswith('{{') or str(value).strip() == '':
                missing_record_fields.append(var_name)
        
        all_missing_fields = missing_sf_fields + missing_record_fields
        
        if all_missing_fields:
            logger.error("Missing required configuration fields:")
            for field in all_missing_fields:
                logger.error(f"   - {field}")
            
            logger.info("\nPlease set these Airflow variables:")
            logger.info("Core Snowflake variables:")
            logger.info("   - snowflake_user")
            logger.info("   - snowflake_password") 
            logger.info("   - snowflake_account")
            logger.info("   - snowflake_warehouse")
            logger.info("   - snowflake_database")
            logger.info("   - snowflake_schema")
            logger.info("   - drive_table_name")
            logger.info("   - pipeline_name")
            logger.info("   - source_complete_category")
            logger.info("\nRecord creation variables:")
            logger.info("   - index_group")
            logger.info("   - index_name")
            logger.info("   - index_id")
            logger.info("   - s3_bucket")
            logger.info("   - target_database_path")
            logger.info("\nOptional variables (have defaults):")
            logger.info("   - pipeline_priority (default: 1.0)")
            logger.info("   - x_time_back (default: 1d)")
            logger.info("   - timezone (default: UTC)")
            logger.info("   - expected_granularity (default: 1h)")
            logger.info("   - s3_prefix (default: default-prefix)")
            logger.info("   - can_access_historical_data (default: YES)")
            
            raise ValueError(f"Missing required configuration fields: {all_missing_fields}")
        
        # Test Snowflake connection
        logger.info("Testing Snowflake connection...")
        test_snowflake_connection(config['sf_drive_config'])
        
        logger.info("Configuration validation passed successfully!")
        logger.info(f"Pipeline: {config['sf_drive_config']['PIPELINE_NAME']}")
        logger.info(f"Source Category: {config['sf_drive_config']['SOURCE_COMPLETE_CATEGORY']}")
        logger.info(f"Time Back: {config['x_time_back']}")
        logger.info(f"Granularity: {config['expected_granularity']}")
        logger.info(f"Target Table: {config['sf_drive_config']['table_name']}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error("Configuration validation failed!")
        logger.error(f"Error: {str(e)}")
        logger.error("=" * 60)
        raise


def test_snowflake_connection(sf_config):
    """
    Test Snowflake connection with detailed error handling
    
    Args:
        sf_config: Snowflake configuration dictionary
    """
    
    conn = None
    cursor = None
    
    try:
        logger.info("Attempting Snowflake connection...")
        
        # Test connection
        conn = snowflake.connector.connect(
            user=sf_config['user'],
            password=sf_config['password'],
            account=sf_config['account'],
            warehouse=sf_config['warehouse'],
            database=sf_config['database'],
            schema=sf_config['schema']
        )
        
        logger.info("Snowflake connection established successfully")
        
        # Test basic query
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()[0]
        logger.info(f"Snowflake version: {version}")
        
        # Test table access
        table_name = sf_config['table_name']
        test_query = f"SELECT COUNT(*) FROM {table_name} LIMIT 1"
        logger.info(f"Testing table access with query: {test_query}")
        
        cursor.execute(test_query)
        count_result = cursor.fetchone()
        logger.info(f"Table access test successful - query executed")
        
        logger.info("Snowflake connection test passed")
        
    except snowflake.connector.errors.DatabaseError as e:
        logger.error(f"Snowflake database error: {str(e)}")
        logger.error("Check your database, schema, warehouse, or table configuration")
        raise
    except snowflake.connector.errors.ProgrammingError as e:
        logger.error(f"Snowflake programming error: {str(e)}")
        logger.error("Check your SQL syntax or table permissions")
        raise
    except snowflake.connector.errors.InterfaceError as e:
        logger.error(f"Snowflake interface error: {str(e)}")
        logger.error("Check your connection parameters (user, password, account)")
        raise
    except Exception as e:
        logger.error(f"Unexpected error testing Snowflake connection: {str(e)}")
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


def run_record_creation_task(**context):
    """
    Main task to run record creation with comprehensive error handling
    """
    
    execution_summary = {
        "success": False,
        "error": None,
        "records_processed": 0,
        "execution_time": None
    }
    
    start_time = pendulum.now()
    
    try:
        logger.info("=" * 60)
        logger.info("STARTING RECORD CREATION TASK")
        logger.info("=" * 60)
        
        config = get_test_config()
        
        # Log configuration summary
        logger.info("Configuration Summary:")
        logger.info(f"  Pipeline: {config['sf_drive_config']['PIPELINE_NAME']}")
        logger.info(f"  Source Category: {config['sf_drive_config']['SOURCE_COMPLETE_CATEGORY']}")
        logger.info(f"  Target Table: {config['sf_drive_config']['table_name']}")
        logger.info(f"  Time Back: {config['x_time_back']}")
        logger.info(f"  Granularity: {config['expected_granularity']}")
        logger.info(f"  Timezone: {config['timezone']}")
        
        # Run the main record creation task
        logger.info("Executing handle_drive_records_creation_task...")
        result = handle_drive_records_creation_task(config)
        
        execution_summary["success"] = True
        execution_summary["execution_time"] = (pendulum.now() - start_time).total_seconds()
        
        logger.info("=" * 60)
        logger.info("RECORD CREATION TASK COMPLETED SUCCESSFULLY")
        logger.info(f"Execution time: {execution_summary['execution_time']:.2f} seconds")
        logger.info("=" * 60)
        
        # Store summary in XCom for use by summary task
        context['task_instance'].xcom_push(key='execution_summary', value=execution_summary)
        
    except Exception as e:
        execution_summary["success"] = False
        execution_summary["error"] = str(e)
        execution_summary["execution_time"] = (pendulum.now() - start_time).total_seconds()
        
        logger.error("=" * 60)
        logger.error("RECORD CREATION TASK FAILED")
        logger.error(f"Error: {str(e)}")
        logger.error(f"Execution time: {execution_summary['execution_time']:.2f} seconds")
        logger.error("=" * 60)
        
        # Store summary in XCom even for failures
        context['task_instance'].xcom_push(key='execution_summary', value=execution_summary)
        
        # Re-raise to mark task as failed
        raise


def print_summary_task(**context):
    """
    Print summary of the test run with comprehensive reporting
    """
    
    try:
        logger.info("=" * 60)
        logger.info("TEST SUMMARY")
        logger.info("=" * 60)
        
        # Get execution summary from XCom
        execution_summary = context['task_instance'].xcom_pull(
            task_ids='run_record_creation', 
            key='execution_summary'
        )
        
        config = get_test_config()
        
        # Basic configuration info
        logger.info("Configuration:")
        logger.info(f"  Pipeline: {config['sf_drive_config']['PIPELINE_NAME']}")
        logger.info(f"  Source Category: {config['sf_drive_config']['SOURCE_COMPLETE_CATEGORY']}")
        logger.info(f"  Time Back: {config['x_time_back']}")
        logger.info(f"  Granularity: {config['expected_granularity']}")
        logger.info(f"  Target Table: {config['sf_drive_config']['table_name']}")
        logger.info(f"  Timezone: {config['timezone']}")
        
        # Execution results
        if execution_summary:
            logger.info("\nExecution Results:")
            logger.info(f"  Success: {execution_summary['success']}")
            logger.info(f"  Execution Time: {execution_summary.get('execution_time', 'Unknown'):.2f}s")
            
            if not execution_summary['success']:
                logger.error(f"  Error: {execution_summary.get('error', 'Unknown error')}")
            else:
                logger.info("  Status: All tasks completed successfully")
        else:
            logger.warning("No execution summary available from previous task")
        
        # What was tested
        logger.info("\nTest Coverage:")
        logger.info("  1. Configuration validation")
        logger.info("  2. Snowflake connectivity test") 
        logger.info("  3. Continuity check record handling")
        logger.info("  4. Required target day calculation")
        logger.info("  5. Missing target day gap filling")
        logger.info("  6. Required target day record creation")
        logger.info("  7. Database operations (insert/delete)")
        
        # Next steps
        logger.info("\nNext Steps:")
        logger.info("  1. Check your Snowflake table for new records:")
        logger.info(f"     SELECT * FROM {config['sf_drive_config']['table_name']}")
        logger.info("     WHERE CONTINUITY_CHECK_PERFORMED = 'YES'")
        logger.info("     ORDER BY RECORD_FIRST_CREATED_TIME DESC")
        logger.info("  2. Verify records for missing target days were created")
        logger.info("  3. Confirm records for required target day exist")
        logger.info("  4. Check PIPELINE_STATUS and other field values")
        
        # Troubleshooting info
        if execution_summary and not execution_summary['success']:
            logger.info("\nTroubleshooting:")
            logger.info("  1. Check Airflow logs for detailed error messages")
            logger.info("  2. Verify Snowflake table permissions")
            logger.info("  3. Validate configuration values in Airflow Variables")
            logger.info("  4. Test Snowflake connection manually")
            logger.info("  5. Check for data type mismatches in table schema")
        
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error generating test summary: {str(e)}")
        logger.error("Test completed but summary generation failed")


# Task definitions with error handling
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

validate_config = PythonOperator(
    task_id='validate_config',
    python_callable=validate_config_task,
    provide_context=True,
    dag=dag,
)

run_record_creation = PythonOperator(
    task_id='run_record_creation',
    python_callable=run_record_creation_task,
    provide_context=True,
    dag=dag,
)

print_summary = PythonOperator(
    task_id='print_summary',
    python_callable=print_summary_task,
    provide_context=True,
    dag=dag,
    # Continue even if previous task failed
    trigger_rule='all_done'
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
    # Continue even if previous tasks failed
    trigger_rule='all_done'
)

# Task dependencies
start_task >> validate_config >> run_record_creation >> print_summary >> end_task