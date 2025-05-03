# src/core/rate_limiting.py
import time
import logging
from threading import Lock

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Ensures that requests do not exceed a specified rate limit.

    This class is thread-safe.
    """

    def __init__(self, min_interval_seconds: float):
        """
        Initializes the RateLimiter.

        Args:
            min_interval_seconds: The minimum time interval (in seconds)
                                  required between consecutive actions.
                                  e.g., 0.11 for ~9 requests/second.
        """
        if min_interval_seconds <= 0:
            raise ValueError("Minimum interval must be positive.")

        self.min_interval = min_interval_seconds
        self.last_request_time: float = 0.0
        self._lock = Lock()
        logger.info(
            f"RateLimiter initialized with min interval: {self.min_interval:.3f} seconds"
        )

    def wait(self) -> None:
        """
        Blocks execution if necessary to maintain the minimum interval
        since the last call to wait().
        """
        with self._lock:  # Ensure thread safety
            current_time = time.monotonic(
            )  # Use monotonic clock for intervals
            time_since_last = current_time - self.last_request_time

            if time_since_last < self.min_interval:
                wait_time = self.min_interval - time_since_last
                if wait_time > 0.001:  # Only log if wait is noticeable
                    logger.debug(
                        f"Rate limit enforcing wait: {wait_time:.3f} seconds")
                time.sleep(wait_time)
                # Update last request time after waiting
                self.last_request_time = time.monotonic()
            else:
                # No wait needed, just update last request time
                self.last_request_time = current_time


# Optional: Create a default instance based on settings for convenience
# This might be better placed where the pipeline/client is initialized though.
# from finlens.config.settings import get_settings
# settings = get_settings()
# default_rate_limiter = RateLimiter(settings.sec_api.rate_limit)
