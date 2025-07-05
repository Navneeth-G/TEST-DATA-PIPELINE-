import pendulum



def get_start_of_day_relative_to_now(timezone: str, ago_seconds: int) -> pendulum.DateTime:
    """
    Return the start of the day for the timestamp obtained by subtracting ago_seconds from current time.

    Args:
        timezone (str): Timezone (e.g., 'UTC', 'Asia/Kolkata').
        ago_seconds (int): Seconds to subtract from now.

    Returns:
        pendulum.DateTime: Start of the day in the given timezone.
    """
    current_time = pendulum.now(timezone)
    adjusted_time = current_time.subtract(seconds=ago_seconds)
    return adjusted_time.start_of('day')


def calculate_window_end_time_within_day(WINDOW_START_TIME: pendulum.DateTime, excess_seconds: int) -> pendulum.DateTime:
    """
    Calculate the end timestamp for a window starting at WINDOW_START_TIME.
    Caps the end time to the start of the next day.

    Args:
        WINDOW_START_TIME (pendulum.DateTime): Start of the window.
        excess_seconds (int): Duration in seconds to add to the start time.

    Returns:
        pendulum.DateTime: Computed end timestamp, capped at next day's start.
    """
    # Start of the next day in the same timezone
    next_day_start = WINDOW_START_TIME.start_of('day').add(days=1)

    # Add excess seconds to window start time
    proposed_end_time = WINDOW_START_TIME.add(seconds=excess_seconds)

    # Return the smaller of the two times
    return min(proposed_end_time, next_day_start)



def parse_granularity_to_seconds(granularity: str) -> int:
    """
    Parse granularity string to seconds - supports complex formats with comprehensive error handling
    
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
    
    try:
        logger.info(f"Parsing granularity: '{granularity}'")
        
        # Validate input
        if not granularity:
            raise ValueError("Granularity cannot be None or empty")
        
        if not isinstance(granularity, str):
            raise ValueError(f"Granularity must be a string, got {type(granularity)}")
        
        # Remove spaces and convert to lowercase
        granularity = granularity.strip().lower()
        
        if not granularity:
            raise ValueError("Granularity cannot be empty after stripping whitespace")
        
        logger.debug(f"Cleaned granularity: '{granularity}'")
        
        # Regex pattern to find all number+unit combinations
        pattern = r'(\d+)([dhms])'
        matches = re.findall(pattern, granularity)
        
        if not matches:
            raise ValueError(f"Invalid granularity format: '{granularity}'. Expected format: '1h', '30m', '1d2h30m40s', etc.")
        
        logger.debug(f"Regex matches: {matches}")
        
        # Check for duplicate units
        units_found = [match[1] for match in matches]
        if len(units_found) != len(set(units_found)):
            duplicate_units = [unit for unit in set(units_found) if units_found.count(unit) > 1]
            raise ValueError(f"Duplicate units found in granularity '{granularity}': {duplicate_units}")
        
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
            try:
                value = int(value_str)
                if value < 0:
                    raise ValueError(f"Negative values not allowed: {value}{unit}")
                if value > 365:  # Reasonable upper limit for days
                    logger.warning(f"Large value detected: {value}{unit} - this may be unintentional")
                
                seconds_for_unit = value * multipliers[unit]
                total_seconds += seconds_for_unit
                logger.debug(f"Unit {value}{unit} = {seconds_for_unit} seconds")
                
            except ValueError as e:
                raise ValueError(f"Invalid numeric value in granularity '{granularity}': {value_str}{unit}")
        
        # Validate that we parsed the entire string (no leftover characters)
        reconstructed = ''.join([f"{value}{unit}" for value, unit in matches])
        if reconstructed != granularity:
            leftover = granularity.replace(reconstructed, '')
            raise ValueError(f"Invalid characters in granularity '{granularity}': '{leftover}'. Only numbers and units (d,h,m,s) allowed.")
        
        # Validate reasonable bounds
        if total_seconds <= 0:
            raise ValueError(f"Granularity must result in positive seconds, got {total_seconds}")
        
        if total_seconds > 86400 * 365:  # More than a year
            logger.warning(f"Very large granularity detected: {total_seconds} seconds ({total_seconds/86400:.1f} days)")
        
        logger.info(f" Granularity '{granularity}' parsed to {total_seconds} seconds")
        return total_seconds
        
    except Exception as e:
        logger.error(f" Failed to parse granularity '{granularity}': {str(e)}")
        raise


def convert_seconds_to_granularity(seconds: int) -> str:
    """
    Convert total seconds to a compact granularity string like '1d2h30m40s'.
    Skips units with zero values unless total seconds is zero.

    Args:
        seconds (int): Total seconds to convert.

    Returns:
        str: Granularity string like '1h30m', '2d', or '0s' if zero.
    """
    if not isinstance(seconds, (int, float)):
        raise ValueError(f"Seconds must be numeric, got {type(seconds)}")
    if seconds < 0:
        raise ValueError(f"Seconds cannot be negative, got {seconds}")

    seconds = int(seconds)
    if seconds == 0:
        return "0s"

    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)

    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if secs:
        parts.append(f"{secs}s")

    return ''.join(parts)



def generate_SOURCE_COMPLETE_CATEGORY(config: Dict, WINDOW_START_TIME: pendulum.DateTime, WINDOW_END_TIME: pendulum.DateTime) -> str:
    """
    Generate source complete category based on business logic with error handling
    
    Args:
        config: Pipeline configuration
        WINDOW_START_TIME: Window start time
        WINDOW_END_TIME: Window end time
        
    Returns:
        Source complete category string: "index_group|index_name"
    """
    
    try:
        logger.debug("Generating SOURCE_COMPLETE_CATEGORY...")
        
        # Validate inputs
        if not config:
            raise ValueError("Config is required")
        
        index_group = config.get("index_group")
        index_name = config.get("index_name")

        result = f"{index_group}|{index_name}"
        logger.debug(f"SOURCE_COMPLETE_CATEGORY generated: {result}")
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to generate SOURCE_COMPLETE_CATEGORY: {str(e)}")
        raise


def generate_TARGET_COMPLETE_CATEGORY(config: Dict, WINDOW_START_TIME: pendulum.DateTime, WINDOW_END_TIME: pendulum.DateTime) -> str:
    """
    Generate target complete category based on business logic with error handling
    
    Args:
        config: Pipeline configuration
        WINDOW_START_TIME: Window start time
        WINDOW_END_TIME: Window end time
        
    Returns:
        Target complete category string: "database.schema.table|prefix/YYYY-MM-DD/HH-mm/"
    """
    
    try:
        logger.debug("Generating TARGET_COMPLETE_CATEGORY...")
        
        # Get required config fields
        s3_prefix_list = config.get("s3_prefix_list")
        database_path = config.get("database.schema.table")
        
        # Build s3_prefix_sub from list
        s3_prefix_sub = '/'.join(s3_prefix_list)
        
        # Extract datetime parts from WINDOW_START_TIME
        date_part = WINDOW_START_TIME.format('YYYY-MM-DD')  # e.g., "2025-01-01"
        time_part = WINDOW_START_TIME.format('HH-mm')       # e.g., "14-30"

        
        # Construct target path
        target_path = f"{s3_prefix_sub}/{date_part}/{time_part}/"
        
        result = f"{database_path}|{target_path}"
        logger.debug(f"TARGET_COMPLETE_CATEGORY generated: {result}")
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to generate TARGET_COMPLETE_CATEGORY: {str(e)}")
        raise


def generate_STAGE_COMPLETE_CATEGORY(config: Dict, WINDOW_START_TIME: pendulum.DateTime, WINDOW_END_TIME: pendulum.DateTime) -> str:
    """
    Generate stage complete category based on business logic with error handling
    
    Args:
        config: Pipeline configuration
        WINDOW_START_TIME: Window start time
        WINDOW_END_TIME: Window end time
        
    Returns:
        Stage complete category string: "s3_bucket|s3://bucket/prefix/YYYY-MM-DD/HH-mm/indexid_epochtime.json"
    """
    
    try:
        logger.debug("Generating STAGE_COMPLETE_CATEGORY...")
                
        # Get required config fields
        s3_prefix_list = config.get("s3_prefix_list")
        s3_bucket = config.get("s3_bucket")
        index_id = config.get("index_id")
        
        # Build s3_prefix_sub from list
        s3_prefix_sub = '/'.join(s3_prefix_list)
        
        # Extract datetime parts from WINDOW_START_TIME

        date_part = WINDOW_START_TIME.format('YYYY-MM-DD')  # e.g., "2025-01-01"
        time_part = WINDOW_START_TIME.format('HH-mm')       # e.g., "14-30"

        
        # Get current epoch time in config timezone
        timezone = config.get('timezone')

        current_time = pendulum.now(timezone)
        epoch_timestamp = int(current_time.timestamp())
        
        # Construct S3 path with actual epoch timestamp
        s3_path = f"s3://{s3_bucket}/{s3_prefix_sub}/{date_part}/{time_part}/{index_id}_{epoch_timestamp}.json"
        
        result = f"{s3_bucket}|{s3_path}"
        logger.debug(f"STAGE_COMPLETE_CATEGORY generated: {result}")
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to generate STAGE_COMPLETE_CATEGORY: {str(e)}")
        raise


def generate_PIPELINE_ID(PIPELINE_NAME: str, SOURCE_COMPLETE_CATEGORY: str, STAGE_COMPLETE_CATEGORY: str, 
                        TARGET_COMPLETE_CATEGORY: str, WINDOW_START_TIME: pendulum.DateTime, WINDOW_END_TIME: pendulum.DateTime) -> str:
    """
    Generate unique pipeline ID by hashing all components with error handling
    
    Args:
        PIPELINE_NAME: Name of the pipeline
        SOURCE_COMPLETE_CATEGORY: Source complete category
        STAGE_COMPLETE_CATEGORY: Stage complete category
        TARGET_COMPLETE_CATEGORY: Target complete category
        WINDOW_START_TIME: Window start time
        WINDOW_END_TIME: Window end time
        
    Returns:
        SHA256 hash as hexadecimal string
    """
    
    try:
        logger.debug("Generating PIPELINE_ID...")
              
        # Create string to hash from all components
        try:
            start_time_str = WINDOW_START_TIME.to_iso8601_string()
            end_time_str = WINDOW_END_TIME.to_iso8601_string()
        except Exception as e:
            raise ValueError(f"Failed to convert datetime objects to ISO strings: {str(e)}")
        
        components = [
            PIPELINE_NAME,
            SOURCE_COMPLETE_CATEGORY,
            STAGE_COMPLETE_CATEGORY,
            TARGET_COMPLETE_CATEGORY,
            start_time_str,
            end_time_str
        ]
        
        # Join with separator and encode
        hash_input = "|".join(components)
        
        try:
            hash_input_encoded = hash_input.encode('utf-8')
        except Exception as e:
            raise ValueError(f"Failed to encode hash input string: {str(e)}")
        
        # Generate SHA256 hash
        try:
            hash_object = hashlib.sha256(hash_input_encoded)
            pipeline_id = hash_object.hexdigest()
        except Exception as e:
            raise ValueError(f"Failed to generate SHA256 hash: {str(e)}")
        
        logger.debug(f"PIPELINE_ID generated: {pipeline_id}")
        return pipeline_id
        
    except Exception as e:
        logger.error(f"Failed to generate PIPELINE_ID: {str(e)}")
        raise



def single_record_creation_for_given_time_windows(config: Dict, TARGET_DAY:pendulum.DateTime, WINDOW_START_TIME: pendulum.DateTime,  WINDOW_END_TIME: pendulum.DateTime, TIME_INTERVAL: str):











