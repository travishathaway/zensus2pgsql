"""
Retry logic with exponential backoff for handling transient errors.

This module provides utilities for retrying async operations with exponential backoff
and jitter, specifically designed for handling HTTP errors in the fetch_worker.
"""

import asyncio
import logging
import random
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import httpx

logger = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    """Configuration for retry behavior with exponential backoff.

    Parameters
    ----------
    max_attempts : int
        Maximum number of attempts including the initial try (default: 4)
    base_delay : float
        Base delay in seconds for exponential backoff (default: 1.0)
    max_delay : float
        Maximum delay in seconds between retries (default: 60.0)
    backoff_multiplier : float
        Multiplier for exponential backoff (default: 2.0)
    jitter_range : float
        Jitter range as a fraction (e.g., 0.2 = ±20% jitter) (default: 0.2)
    """

    max_attempts: int = 4
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_multiplier: float = 2.0
    jitter_range: float = 0.2


def is_retryable_error(exc: Exception) -> bool:
    """Determine if an error should trigger a retry attempt.

    Retryable errors include:
    - HTTP 5xx server errors (500-599)
    - HTTP 429 (Too Many Requests / rate limiting)
    - Network errors (connection failures, timeouts)

    Non-retryable errors include:
    - HTTP 4xx client errors (except 429)
    - Authentication errors (401, 403)

    Parameters
    ----------
    exc : Exception
        The exception to evaluate

    Returns
    -------
    bool
        True if the error should trigger a retry, False otherwise
    """
    # HTTP status errors
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        # Retry on 5xx server errors or 429 rate limiting
        if 500 <= status_code < 600 or status_code == 429:
            return True
        # Don't retry on 4xx client errors (except 429)
        return False

    # Network and timeout errors
    if isinstance(
        exc,
        (
            httpx.TimeoutException,
            httpx.ConnectTimeout,
            httpx.ReadTimeout,
            httpx.ConnectError,
            httpx.NetworkError,
        ),
    ):
        return True

    # Don't retry other exceptions
    return False


def calculate_backoff(attempt: int, config: RetryConfig) -> float:
    """Calculate delay for exponential backoff with jitter.

    Formula: min(base_delay * (multiplier ** attempt) * random_factor, max_delay)
    Random factor is between (1 - jitter_range) and (1 + jitter_range)

    Parameters
    ----------
    attempt : int
        Current retry attempt number (0-indexed)
    config : RetryConfig
        Retry configuration

    Returns
    -------
    float
        Delay in seconds before next retry attempt
    """
    # Calculate base exponential backoff
    delay = config.base_delay * (config.backoff_multiplier**attempt)

    # Apply jitter (random factor)
    jitter_min = 1.0 - config.jitter_range
    jitter_max = 1.0 + config.jitter_range
    jitter_factor = random.uniform(jitter_min, jitter_max)

    delay *= jitter_factor

    # Cap at maximum delay
    return min(delay, config.max_delay)


async def retry_async(
    func: Callable, *args: Any, config: RetryConfig, filename: str = "", **kwargs: Any
) -> Any:
    """Execute an async function with retry logic and exponential backoff.

    Attempts the function up to config.max_attempts times. Logs each retry
    attempt at DEBUG level and final failures at ERROR level.

    Parameters
    ----------
    func : Callable
        Async function to execute
    *args : Any
        Positional arguments to pass to func
    config : RetryConfig
        Retry configuration
    filename : str
        Filename for logging purposes (optional)
    **kwargs : Any
        Keyword arguments to pass to func

    Returns
    -------
    Any
        Return value from func

    Raises
    ------
    Exception
        The last exception if all retry attempts are exhausted
    """
    last_exception = None

    for attempt in range(config.max_attempts):
        try:
            result = await func(*args, **kwargs)
            # Success - return result
            if attempt > 0:
                logger.debug(f"Successfully completed after {attempt + 1} attempts")
            return result

        except Exception as exc:
            last_exception = exc

            # Check if we should retry this error
            if not is_retryable_error(exc):
                logger.debug(
                    f"Non-retryable error encountered{f' for {filename}' if filename else ''}: "
                    f"{type(exc).__name__}"
                )
                raise

            # Check if we have retries left
            is_last_attempt = attempt >= config.max_attempts - 1
            if is_last_attempt:
                # No more retries - will raise below
                break

            # Calculate backoff delay
            delay = calculate_backoff(attempt, config)

            # Log retry attempt at DEBUG level
            error_type = type(exc).__name__
            logger.debug(
                f"Retrying download{f' for {filename}' if filename else ''} "
                f"(attempt {attempt + 2}/{config.max_attempts}) after {error_type}. "
                f"Waiting {delay:.1f}s (exponential backoff with jitter)"
            )

            # Wait before retrying
            await asyncio.sleep(delay)

    # All retries exhausted - log and raise
    if last_exception is not None:
        error_msg = (
            f"Failed{f' to download {filename}' if filename else ''} "
            f"after {config.max_attempts} attempts: {last_exception}"
        )
        logger.error(error_msg)
        raise last_exception

    # Should not reach here, but just in case
    raise RuntimeError("Unexpected state in retry_async")
