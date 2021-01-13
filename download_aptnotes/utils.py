import functools
import logging
from timeit import default_timer
from typing import Callable

logger = logging.getLogger(__name__)


def log_duration(level: str, msg: str):
    def decorator_log_duration(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper_log_duration(*args, **kwargs):
            start_time = default_timer()
            value = func(*args, **kwargs)
            duration = default_timer() - start_time
            logger.log(getattr(logging, level), f"{msg} {duration}")
            return value

        return wrapper_log_duration

    return decorator_log_duration
