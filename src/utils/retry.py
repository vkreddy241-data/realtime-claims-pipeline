import time, functools, logging
from typing import Callable, Tuple, Type

logger = logging.getLogger(__name__)

def with_retry(max_attempts: int = 3, backoff_seconds: float = 5.0,
               backoff_multiplier: float = 2.0,
               exceptions: Tuple[Type[Exception], ...] = (Exception,)):
    def decorator(fn: Callable):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            delay = backoff_seconds
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:
                    if attempt == max_attempts:
                        raise
                    logger.warning("Attempt %d/%d failed — retrying in %.1fs. Error: %s",
                                   attempt, max_attempts, delay, exc)
                    time.sleep(delay)
                    delay *= backoff_multiplier
        return wrapper
    return decorator
