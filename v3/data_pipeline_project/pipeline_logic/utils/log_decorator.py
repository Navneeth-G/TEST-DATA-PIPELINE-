# log_decorator.py

import functools
import time
from framework.utils.pipeline_logger import PipelineLogger


def log_block(subject: str, log_key: str = "GENERAL"):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = PipelineLogger()
            start_time = time.time()

            logger.info(
                subject=subject,
                message=f"Started function: {func.__name__}",
                log_key=log_key,
                keywords=["start", func.__name__]
            )

            result = func(*args, **kwargs)

            duration = round(time.time() - start_time, 2)

            logger.info(
                subject=subject,
                message=f"Ended function: {func.__name__}",
                log_key=log_key,
                keywords=["end", func.__name__],
                duration=f"{duration} sec"
            )

            return result

        return wrapper
    return decorator
