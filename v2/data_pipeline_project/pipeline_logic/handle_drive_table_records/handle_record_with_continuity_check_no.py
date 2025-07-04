# pipleine_logic/handle_drive_table_records/handle_record_with_continuity_check_no.py
from pipleine_logic.handle_drive_table_records.record_creation.create_single_drive_record import rebuild_single_record_for_given_window
from pipleine_logic.handle_drive_table_records.record_creation.create_multiple_drive_record import rebuild_all_records_for_given_target_day, create_all_records_for_target_day


def handle_record_with_continuity_check_as_no(record, config, target_days_list_with_continuity_check_as_yes):
    """
    Main function: Handle record with CONTINUITY_CHECK_PERFORMED = NO
    Routes through the complete decision tree flow
    
    Args:
        record: Single record dictionary with CONTINUITY_CHECK_PERFORMED = NO
        config: Configuration dictionary
        target_days_list_with_continuity_check_as_yes: Set/List of target days with CONTINUITY_CHECK_PERFORMED = YES
        
    Returns:
        List of records (single record for Branch 1A, multiple for Branch 1B/1C)
    """
    
    # Validate input
    if record.get("CONTINUITY_CHECK_PERFORMED") != "NO":
        raise ValueError("This function only handles records with CONTINUITY_CHECK_PERFORMED = NO")
    
    TARGET_DAY = record.get("TARGET_DAY")
    
    # Step 1: Check TARGET_DAY Status
    if TARGET_DAY in target_days_list_with_continuity_check_as_yes:
        print(f"TARGET_DAY {TARGET_DAY} already processed - checking windows")
        
        # Step 2: Check Window Times
        WINDOW_START_TIME = record.get("WINDOW_START_TIME")
        WINDOW_END_TIME = record.get("WINDOW_END_TIME")
        
        if WINDOW_START_TIME is not None and WINDOW_END_TIME is not None:
            # Branch 1A: Rebuild single record
            print("Branch 1A: Rebuilding single record with windows")
            return [rebuild_single_record_for_given_window(record, config)]
        else:
            # Branch 1B: Missing window data
            print("Branch 1B: Missing window data - creating all records for day")
            return rebuild_all_records_for_given_target_day(record, config)
    
    else:
        # Branch 1C: New target day
        print(f"Branch 1C: New TARGET_DAY {TARGET_DAY} - creating all records for day")
        return create_all_records_for_target_day(TARGET_DAY, config)



