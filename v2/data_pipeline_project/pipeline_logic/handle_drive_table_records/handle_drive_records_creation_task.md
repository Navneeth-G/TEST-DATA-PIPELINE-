# Records Creation Decision Tree Flow - Complete Documentation

## Overview
This document describes the complete decision tree flow for `handle_drive_records_creation_task()` which decides whether to create new records for the drive table based on continuity checks and target day analysis.

## Decision Tree Flow

### Flow Diagram
```
START: handle_drive_records_creation_task(config)
│
├── STEP 1: Check CONTINUITY_CHECK_PERFORMED = 'NO' records
│   ├── Query: SELECT * FROM table WHERE CONTINUITY_CHECK_PERFORMED = 'NO'
│   ├── Result: List of records OR empty list
│   │
│   ├── IF records found (list not empty):
│   │   ├── Action: Call handle_continuity_check_records(records_list, config)
│   │   ├── Process: Use existing process_continuity_check_no_records_with_database_operations()
│   │   └── EXIT: Task complete (continuity takes priority)
│   │
│   └── IF no records found (empty list):
│       └── Continue to STEP 2
│
├── STEP 2: Calculate required_target_day
│   ├── Extract: x_time_back from config (e.g., '1d', '1h', '2d')
│   ├── Parse: Use parse_granularity_to_seconds() to convert to seconds
│   ├── Calculate: current_time(timezone) - x_time_back_seconds
│   └── Result: required_target_day as pendulum.Date
│
├── STEP 3: Handle missing target days (fill gaps)
│   ├── Query: Get all existing target days with CONTINUITY_CHECK_PERFORMED = 'YES'
│   ├── Result: Set of existing target days OR empty set
│   │
│   ├── IF no existing target days:
│   │   └── Skip gap analysis (no reference point)
│   │
│   ├── IF existing target days found:
│   │   ├── Auto-detect: start_date = min(existing_target_days)
│   │   ├── Find gaps: From start_date+1 to required_target_day
│   │   │
│   │   └── FOR each missing day:
│   │       ├── Create: create_all_records_for_target_day(missing_day, config)
│   │       └── Insert: insert_multiple_records_to_database(records, config)
│   │
│   └── Continue to STEP 4
│
├── STEP 4: Handle required target day creation
│   ├── Query: Get MAX(TARGET_DAY) from existing records
│   ├── Result: max_target_day OR None
│   │
│   ├── IF max_target_day = None (no existing records):
│   │   ├── Create: create_all_records_for_target_day(required_target_day, config)
│   │   └── Insert: insert_multiple_records_to_database(records, config)
│   │
│   ├── IF max_target_day < required_target_day:
│   │   ├── Create: create_all_records_for_target_day(required_target_day, config)
│   │   └── Insert: insert_multiple_records_to_database(records, config)
│   │
│   └── IF max_target_day >= required_target_day:
│       └── EXIT: Do nothing (up to date or ahead)
│
END: Task complete
```

## What We Create

### 1. Continuity Check Records (STEP 1)
**When**: Records with `CONTINUITY_CHECK_PERFORMED = 'NO'` exist
**What we create**: 
- Process existing records through decision tree (Branch 1A, 1B, 1C)
- Single records or multiple records per target day
- Set `CONTINUITY_CHECK_PERFORMED = 'YES'` after processing

**Example**:
```
Input: Record with CONTINUITY_CHECK_PERFORMED = 'NO'
Output: 1 or more records with CONTINUITY_CHECK_PERFORMED = 'YES'
```

### 2. Gap Filling Records (STEP 3)
**When**: Missing target days found between existing days
**What we create**: All records for each missing target day based on `expected_granularity`

**Example**:
```
Existing target days: [2025-01-01, 2025-01-03, 2025-01-05]
Required target day: 2025-01-06
Missing days: [2025-01-02, 2025-01-04, 2025-01-06]

For each missing day (e.g., 2025-01-02):
- If expected_granularity = '1h': Create 24 records (one per hour)
- If expected_granularity = '30m': Create 48 records (one per 30 minutes)
- All records have CONTINUITY_CHECK_PERFORMED = 'YES'
```

### 3. Required Target Day Records (STEP 4)
**When**: `max_target_day` is behind `required_target_day` OR no existing records
**What we create**: All records for `required_target_day` based on `expected_granularity`

**Example**:
```
Case A: No existing records (max_target_day = None)
- Create all records for required_target_day only
- Don't create historical records (no reference point)

Case B: Behind schedule (max_target_day < required_target_day)
- Create all records for required_target_day
- Bridge the gap to current requirements
```

## Record Creation Details

### Single Target Day Record Creation
**Function**: `create_all_records_for_target_day(target_day, config)`
**Process**:
1. Parse `expected_granularity` (e.g., '1h' → 3600 seconds)
2. Generate day boundaries (00:00:00 to 23:59:59)
3. Create window records based on granularity
4. Each record includes:
   - `TARGET_DAY`: The target day
   - `WINDOW_START_TIME`: Start of time window
   - `WINDOW_END_TIME`: End of time window
   - `TIME_INTERVAL`: Duration in HH:MM:SS format
   - `CONTINUITY_CHECK_PERFORMED`: 'YES'
   - All other required pipeline fields

**Example Output (1h granularity)**:
```
Record 1: 2025-01-15 00:00:00 to 2025-01-15 01:00:00 (01:00:00)
Record 2: 2025-01-15 01:00:00 to 2025-01-15 02:00:00 (01:00:00)
...
Record 24: 2025-01-15 23:00:00 to 2025-01-16 00:00:00 (01:00:00)
```

### Database Operations
**Insert Function**: `insert_multiple_records_to_database(records, config)`
**Process**:
1. Bulk insert using `cursor.executemany()`
2. Proper error handling and logging
3. Query ID tracking for debugging
4. Affected rows reporting

## Configuration Requirements

### Required Config Structure
```python
config = {
    'sf_drive_config': {
        'user': 'snowflake_username',
        'password': 'snowflake_password',
        'account': 'snowflake_account',
        'warehouse': 'snowflake_warehouse',
        'database': 'snowflake_database',
        'schema': 'snowflake_schema',
        'table_name': 'target_table_name',
        'PIPELINE_NAME': 'pipeline_identifier',
        'SOURCE_COMPLETE_CATEGORY': 'source_category',
        'PIPELINE_PRIORITY': 1.0
    },
    'x_time_back': '1d',  # Time to look back for required target day
    'timezone': 'UTC',
    'expected_granularity': '1h',  # Granularity for record creation
    # ... other pipeline configuration
}
```

## Exit Conditions (Early Returns)

### 1. Continuity Check Priority Exit
**Condition**: `CONTINUITY_CHECK_PERFORMED = 'NO'` records found
**Action**: Process continuity records and EXIT
**Reason**: Continuity check takes priority over new record creation

### 2. Up to Date Exit
**Condition**: `max_target_day >= required_target_day`
**Action**: EXIT with no action
**Reason**: Already have records up to or beyond required date

### 3. No Work Needed Exit
**Condition**: No missing days found and max_target_day is current
**Action**: EXIT with no action
**Reason**: Database is complete and up to date

## Performance Optimizations

### Database Efficiency
- **Minimal Queries**: Only essential database calls
- **Bulk Operations**: Use `executemany()` for multiple record inserts
- **Early Exits**: Stop processing as soon as possible
- **Set Operations**: Use sets for efficient day lookups

### Memory Efficiency
- **Streaming Processing**: Process records one target day at a time
- **Date Conversion**: Reuse existing `convert_to_pendulum_date()` function
- **Connection Management**: Proper connection cleanup

## Error Handling

### Database Errors
- **Connection Failures**: Proper exception propagation
- **Query Errors**: Log query and parameters for debugging
- **Transaction Safety**: Each target day processed independently

### Data Validation
- **Config Validation**: Check required configuration fields
- **Date Validation**: Ensure valid date formats and ranges
- **Granularity Validation**: Validate time format parsing

## Monitoring and Logging

### Processing Statistics
- Number of continuity records processed
- Number of missing days filled
- Number of required target day records created
- Total execution time per step

### Query Logging
- **Query Text**: Complete SQL with parameters
- **Query ID**: Snowflake query identifier
- **Affected Rows**: Number of records inserted
- **Error Details**: Full exception information

## Status: COMPLETE

The decision tree flow is fully implemented and integrated with existing components:

- ✅ **Step 1**: Continuity check priority handling
- ✅ **Step 2**: Required target day calculation
- ✅ **Step 3**: Gap filling with auto-detection
- ✅ **Step 4**: Required target day creation
- ✅ **Early Exits**: Optimized for efficiency
- ✅ **Error Handling**: Comprehensive exception management
- ✅ **Integration**: Uses existing functions and patterns
- ✅ **Documentation**: Complete flow documentation

The implementation is ready for production use with robust error handling, efficient database operations, and comprehensive monitoring capabilities.