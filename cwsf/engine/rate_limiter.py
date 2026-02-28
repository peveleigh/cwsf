import asyncio
import time
import logging
from typing import Any, Callable, Awaitable, Optional, Dict
from urllib.parse import urlparse
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

class DomainRateLimiter:
    """
    Unified rate limiter and retry handler for a specific domain.
    Encapsulates delay, concurrency, and retry logic.
    """
    def __init__(
        self,
        delay_seconds: float = 1.0,
        max_concurrent: int = 1,
        max_retries: int = 3,
        backoff_factor: float = 2.0
    ):
        self.delay_seconds = delay_seconds
        self.max_concurrent = max_concurrent
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._last_request_time = 0.0
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def acquire(self):
        """
        Async context manager to enforce concurrency and per-request delay.
        """
        async with self._semaphore:
            async with self._lock:
                now = time.perf_counter()
                wait_time = self.delay_seconds - (now - self._last_request_time)
                if wait_time > 0:
                    logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
            
            try:
                yield
            finally:
                async with self._lock:
                    self._last_request_time = time.perf_counter()

    async def execute(self, url: str, request_callable: Callable[[], Awaitable[Any]], site_name: str = "unknown") -> Any:
        """
        Execute a request with retry logic and exponential backoff.
        The request_callable should perform the actual network request.
        """
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                if attempt > 0:
                    # Exponential backoff: backoff_factor ^ attempt_number
                    wait_time = self.backoff_factor ** attempt
                    logger.warning(
                        f"Retry attempt {attempt}/{self.max_retries} for {url} "
                        f"(site: {site_name}) after {wait_time}s backoff"
                    )
                    await asyncio.sleep(wait_time)

                # Each attempt must acquire the rate limiter (delay + concurrency)
                async with self.acquire():
                    result = await request_callable()
                
                # Check if result has status_code (like FetchResult or httpx.Response)
                status_code = getattr(result, "status_code", None)
                
                if status_code in RETRYABLE_STATUS_CODES:
                    if attempt < self.max_retries:
                        logger.warning(
                            f"Retryable status {status_code} for {url} (site: {site_name}). "
                            f"Attempt {attempt + 1}/{self.max_retries}"
                        )
                        continue
                    else:
                        # Exhausted retries with a retryable status code
                        # Story 7.3: Log final failure with full context
                        logger.error(
                            f"Exhausted retries for {url} (site: {site_name}). "
                            f"Final status: {status_code}, retries attempted: {attempt}, "
                            f"timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}"
                        )
                        return result
                
                # Success recovery log
                if attempt > 0:
                    logger.info(f"Recovered on attempt {attempt} for {url} (site: {site_name})")

                # Success or non-retryable error
                return result

            except Exception as exc:
                last_exception = exc
                if attempt < self.max_retries:
                    logger.warning(
                        f"Retryable error for {url} (site: {site_name}): {exc}. "
                        f"Attempt {attempt + 1}/{self.max_retries}"
                    )
                    continue
                else:
                    # Story 7.3: Log final failure with full context
                    logger.error(
                        f"Exhausted retries for {url} (site: {site_name}). "
                        f"Final error: {type(exc).__name__}: {exc}, retries attempted: {attempt}, "
                        f"timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}"
                    )
                    raise

        # Should not reach here if max_retries >= 0
        if last_exception:
            raise last_exception
