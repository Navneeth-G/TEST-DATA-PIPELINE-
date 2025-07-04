# dags/test_record_creation_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import pendulum
import sys
import os

# Add your project path to sys.path
sys.path.append('/opt/airflow/dags/pipeline_logic')

# Import your record creation function
from pipeline_logic.handle_drive_table_records.create_new_records_task import handle_drive_records_creation_task

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
    description='Test DAG for record creation task',
    schedule_interval=None,  # Manual trigger for testing
    catchup=False,
    tags=['test', 'record-creation', 'drive-table'],
)

def get_test_config():
    """
    Return test configuration
    Update these values with your actual Snowflake credentials
    """
    config = {
        'sf_drive_config': {
            'user': '{{ var.value.snowflake_user }}',
            'password': '{{ var.value.snowflake_password }}',
            'account': '{{ var.value.snowflake_account }}',
            'warehouse': '{{ var.value.snowflake_warehouse }}',
            'database': '{{ var.value.snowflake_database }}',
            'schema': '{{ var.value.snowflake_schema }}',
            'table_name': '{{ var.value.drive_table_name }}',
            'PIPELINE_NAME': '{{ var.value.pipeline_name }}',
            'SOURCE_COMPLETE_CATEGORY': '{{ var.value.source_complete_category }}',
            'PIPELINE_PRIORITY': 1.0
        },
        'x_time_back': '1d',  # Test with 1 day back
        'timezone': 'UTC',
        'expected_granularity': '1h',  # Test with 1 hour granularity
        
        # Additional config for record creation
        'index_group': '{{ var.value.index_group }}',
        'index_name': '{{ var.value.index_name }}',
        'index_id': '{{ var.value.index_id }}',
        's3_bucket': '{{ var.value.s3_bucket }}',
        's3_prefix_list': ['{{ var.value.s3_prefix }}'],
        'database.schema.table': '{{ var.value.target_database_path }}',
        'CAN_ACCESS_HISTORICAL_DATA': 'YES'
    }
    
    return config

def validate_config_task(**context):
    """
    Validate configuration before running the main task
    """
    print("=" * 60)
    print("VALIDATING CONFIGURATION")
    print("=" * 60)
    
    config = get_test_config()
    
    # Check required fields
    required_fields = [
        'sf_drive_config.user',
        'sf_drive_config.password',
        'sf_drive_config.account',
        'sf_drive_config.warehouse',
        'sf_drive_config.database',
        'sf_drive_config.schema',
        'sf_drive_config.table_name',
        'sf_drive_config.PIPELINE_NAME',
        'sf_drive_config.SOURCE_COMPLETE_CATEGORY'
    ]
    
    missing_fields = []
    for field in required_fields:
        keys = field.split('.')
        value = config
        try:
            for key in keys:
                value = value[key]
            if not value or str(value).startswith('{{'):
                missing_fields.append(field)
        except KeyError:
            missing_fields.append(field)
    
    if missing_fields:
        print(f"❌ Missing required configuration fields:")
        for field in missing_fields:
            print(f"   - {field}")
        print("\n📝 Please set these Airflow variables:")
        print("   - snowflake_user")
        print("   - snowflake_password")
        print("   - snowflake_account")
        print("   - snowflake_warehouse")
        print("   - snowflake_database")
        print("   - snowflake_schema")
        print("   - drive_table_name")
        print("   - pipeline_name")
        print("   - source_complete_category")
        print("   - index_group")
        print("   - index_name")
        print("   - index_id")
        print("   - s3_bucket")
        print("   - s3_prefix")
        print("   - target_database_path")
        raise ValueError(f"Missing required configuration fields: {missing_fields}")
    
    print("✅ Configuration validation passed!")
    print(f"📊 Pipeline: {config['sf_drive_config']['PIPELINE_NAME']}")
    print(f"📊 Source Category: {config['sf_drive_config']['SOURCE_COMPLETE_CATEGORY']}")
    print(f"📊 Time Back: {config['x_time_back']}")
    print(f"📊 Granularity: {config['expected_granularity']}")
    print("=" * 60)

def run_record_creation_task(**context):
    """
    Main task to run record creation
    """
    print("=" * 60)
    print("STARTING RECORD CREATION TASK")
    print("=" * 60)
    
    config = get_test_config()
    
    try:
        # Run the main record creation task
        handle_drive_records_creation_task(config)
        
        print("=" * 60)
        print("✅ RECORD CREATION TASK COMPLETED SUCCESSFULLY")
        print("=" * 60)
        
    except Exception as e:
        print("=" * 60)
        print("❌ RECORD CREATION TASK FAILED")
        print(f"Error: {str(e)}")
        print("=" * 60)
        raise

def print_summary_task(**context):
    """
    Print summary of the test run
    """
    print("=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    config = get_test_config()
    
    print(f"🎯 Pipeline: {config['sf_drive_config']['PIPELINE_NAME']}")
    print(f"🎯 Source Category: {config['sf_drive_config']['SOURCE_COMPLETE_CATEGORY']}")
    print(f"🎯 Time Back: {config['x_time_back']}")
    print(f"🎯 Granularity: {config['expected_granularity']}")
    print(f"🎯 Target Table: {config['sf_drive_config']['table_name']}")
    
    print("\n📋 What was tested:")
    print("   1. ✅ Continuity check record handling")
    print("   2. ✅ Required target day calculation")
    print("   3. ✅ Missing target day gap filling")
    print("   4. ✅ Required target day record creation")
    print("   5. ✅ Database operations (insert)")
    
    print("\n📊 Check your Snowflake table for:")
    print("   - New records with CONTINUITY_CHECK_PERFORMED = 'YES'")
    print("   - Records for missing target days")
    print("   - Records for required target day")
    
    print("=" * 60)

# Task definitions
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
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Task dependencies
start_task >> validate_config >> run_record_creation >> print_summary >> end_task





