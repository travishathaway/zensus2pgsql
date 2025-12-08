"""
Unit tests for retry logic with exponential backoff.
"""

from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest

from zensus2pgsql.retry import RetryConfig, calculate_backoff, is_retryable_error, retry_async


class TestCalculateBackoff:
    """Tests for calculate_backoff function."""

    def test_calculate_backoff_basic(self):
        """Verify basic exponential backoff formula."""
        config = RetryConfig(
            base_delay=1.0, backoff_multiplier=2.0, max_delay=60.0, jitter_range=0.0
        )

        # Without jitter, should follow exact exponential pattern
        assert calculate_backoff(0, config) == 1.0  # 1 * 2^0 = 1
        assert calculate_backoff(1, config) == 2.0  # 1 * 2^1 = 2
        assert calculate_backoff(2, config) == 4.0  # 1 * 2^2 = 4
        assert calculate_backoff(3, config) == 8.0  # 1 * 2^3 = 8

    def test_calculate_backoff_with_jitter(self):
        """Verify jitter is applied within expected range."""
        config = RetryConfig(
            base_delay=10.0, backoff_multiplier=2.0, max_delay=60.0, jitter_range=0.2
        )

        # Run multiple times to test jitter randomness
        for _ in range(20):
            delay = calculate_backoff(1, config)
            expected_base = 20.0  # 10 * 2^1 = 20
            min_delay = expected_base * 0.8  # 20% less
            max_delay = expected_base * 1.2  # 20% more
            assert min_delay <= delay <= max_delay, f"Delay {delay} outside jitter range"

    def test_calculate_backoff_respects_max_delay(self):
        """Verify max_delay cap is enforced."""
        config = RetryConfig(
            base_delay=10.0, backoff_multiplier=2.0, max_delay=30.0, jitter_range=0.0
        )

        # Without jitter, attempt 2 would be 40 (10 * 2^2), but capped at 30
        assert calculate_backoff(2, config) == 30.0
        assert calculate_backoff(5, config) == 30.0  # Much larger, still capped

    def test_calculate_backoff_with_large_multiplier(self):
        """Verify behavior with large backoff multiplier."""
        config = RetryConfig(
            base_delay=1.0, backoff_multiplier=3.0, max_delay=100.0, jitter_range=0.0
        )

        assert calculate_backoff(0, config) == 1.0  # 1 * 3^0 = 1
        assert calculate_backoff(1, config) == 3.0  # 1 * 3^1 = 3
        assert calculate_backoff(2, config) == 9.0  # 1 * 3^2 = 9


class TestIsRetryableError:
    """Tests for is_retryable_error function."""

    def test_is_retryable_error_5xx(self):
        """HTTPStatusError with 5xx should be retryable."""
        response = Mock(spec=httpx.Response)
        response.status_code = 500
        exc = httpx.HTTPStatusError("Server error", request=Mock(), response=response)
        assert is_retryable_error(exc) is True

        response.status_code = 502
        exc = httpx.HTTPStatusError("Bad gateway", request=Mock(), response=response)
        assert is_retryable_error(exc) is True

        response.status_code = 503
        exc = httpx.HTTPStatusError("Service unavailable", request=Mock(), response=response)
        assert is_retryable_error(exc) is True

    def test_is_retryable_error_429(self):
        """HTTPStatusError with 429 (rate limiting) should be retryable."""
        response = Mock(spec=httpx.Response)
        response.status_code = 429
        exc = httpx.HTTPStatusError("Too many requests", request=Mock(), response=response)
        assert is_retryable_error(exc) is True

    def test_is_retryable_error_404(self):
        """HTTPStatusError with 404 should NOT be retryable."""
        response = Mock(spec=httpx.Response)
        response.status_code = 404
        exc = httpx.HTTPStatusError("Not found", request=Mock(), response=response)
        assert is_retryable_error(exc) is False

    def test_is_retryable_error_401(self):
        """HTTPStatusError with 401 (auth error) should NOT be retryable."""
        response = Mock(spec=httpx.Response)
        response.status_code = 401
        exc = httpx.HTTPStatusError("Unauthorized", request=Mock(), response=response)
        assert is_retryable_error(exc) is False

    def test_is_retryable_error_403(self):
        """HTTPStatusError with 403 (forbidden) should NOT be retryable."""
        response = Mock(spec=httpx.Response)
        response.status_code = 403
        exc = httpx.HTTPStatusError("Forbidden", request=Mock(), response=response)
        assert is_retryable_error(exc) is False

    def test_is_retryable_error_timeout(self):
        """TimeoutException should be retryable."""
        exc = httpx.TimeoutException("Request timeout")
        assert is_retryable_error(exc) is True

        exc = httpx.ConnectTimeout("Connect timeout")
        assert is_retryable_error(exc) is True

        exc = httpx.ReadTimeout("Read timeout")
        assert is_retryable_error(exc) is True

    def test_is_retryable_error_network(self):
        """Network errors should be retryable."""
        exc = httpx.ConnectError("Connection failed")
        assert is_retryable_error(exc) is True

        exc = httpx.NetworkError("Network error")
        assert is_retryable_error(exc) is True

    def test_is_retryable_error_other_exceptions(self):
        """Other exceptions should NOT be retryable."""
        exc = ValueError("Some value error")
        assert is_retryable_error(exc) is False

        exc = RuntimeError("Some runtime error")
        assert is_retryable_error(exc) is False


class TestRetryAsync:
    """Tests for retry_async function."""

    @pytest.mark.asyncio
    async def test_retry_async_success_first_try(self):
        """Function succeeds on first try - no retries needed."""
        mock_func = AsyncMock(return_value="success")
        config = RetryConfig(max_attempts=3)

        result = await retry_async(mock_func, config=config)

        assert result == "success"
        assert mock_func.call_count == 1  # Called only once

    @pytest.mark.asyncio
    async def test_retry_async_success_after_retry(self):
        """Function succeeds after one retry."""
        # Fail first, then succeed
        mock_func = AsyncMock(side_effect=[httpx.TimeoutException("Timeout"), "success"])
        config = RetryConfig(max_attempts=3, base_delay=0.01)  # Fast retry for testing

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            result = await retry_async(mock_func, config=config, filename="test.zip")

            assert result == "success"
            assert mock_func.call_count == 2  # Initial + 1 retry
            assert mock_sleep.call_count == 1  # Slept once before retry

    @pytest.mark.asyncio
    async def test_retry_async_exhausts_retries(self):
        """All retry attempts fail - exception is raised."""
        # Always fail
        mock_func = AsyncMock(side_effect=httpx.TimeoutException("Persistent timeout"))
        config = RetryConfig(max_attempts=3, base_delay=0.01)

        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(httpx.TimeoutException, match="Persistent timeout"):
                await retry_async(mock_func, config=config)

            assert mock_func.call_count == 3  # All attempts exhausted

    @pytest.mark.asyncio
    async def test_retry_async_non_retryable_error(self):
        """Non-retryable error fails immediately without retry."""
        response = Mock(spec=httpx.Response)
        response.status_code = 404
        mock_func = AsyncMock(
            side_effect=httpx.HTTPStatusError("Not found", request=Mock(), response=response)
        )
        config = RetryConfig(max_attempts=3)

        with pytest.raises(httpx.HTTPStatusError, match="Not found"):
            await retry_async(mock_func, config=config)

        assert mock_func.call_count == 1  # Only called once, no retries

    @pytest.mark.asyncio
    async def test_retry_async_with_args_and_kwargs(self):
        """Arguments and keyword arguments are passed correctly to function."""
        mock_func = AsyncMock(return_value="success")
        config = RetryConfig(max_attempts=3)

        result = await retry_async(
            mock_func, "arg1", "arg2", config=config, kwarg1="value1", kwarg2="value2"
        )

        assert result == "success"
        mock_func.assert_called_once_with("arg1", "arg2", kwarg1="value1", kwarg2="value2")

    @pytest.mark.asyncio
    async def test_retry_async_respects_max_attempts(self):
        """Verify retry logic respects max_attempts configuration."""
        mock_func = AsyncMock(side_effect=httpx.TimeoutException("Timeout"))
        config = RetryConfig(max_attempts=5, base_delay=0.01)

        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(httpx.TimeoutException):
                await retry_async(mock_func, config=config)

            assert mock_func.call_count == 5  # Exactly max_attempts

    @pytest.mark.asyncio
    async def test_retry_async_backoff_increases(self):
        """Verify backoff delay increases with each retry."""
        mock_func = AsyncMock(side_effect=httpx.TimeoutException("Timeout"))
        config = RetryConfig(
            max_attempts=4, base_delay=1.0, backoff_multiplier=2.0, jitter_range=0.0
        )

        sleep_delays = []

        async def capture_sleep(delay):
            sleep_delays.append(delay)

        with patch("asyncio.sleep", side_effect=capture_sleep):
            with pytest.raises(httpx.TimeoutException):
                await retry_async(mock_func, config=config)

            # Should have slept 3 times (between 4 attempts)
            assert len(sleep_delays) == 3
            # Without jitter, delays should be exactly 1, 2, 4
            assert sleep_delays[0] == 1.0
            assert sleep_delays[1] == 2.0
            assert sleep_delays[2] == 4.0

    @pytest.mark.asyncio
    async def test_retry_async_mixed_errors(self):
        """Test with a mix of retryable and non-retryable errors."""
        response_500 = Mock(spec=httpx.Response)
        response_500.status_code = 500
        response_404 = Mock(spec=httpx.Response)
        response_404.status_code = 404

        # First two retryable, third non-retryable
        mock_func = AsyncMock(
            side_effect=[
                httpx.HTTPStatusError("Server error", request=Mock(), response=response_500),
                httpx.TimeoutException("Timeout"),
                httpx.HTTPStatusError("Not found", request=Mock(), response=response_404),
            ]
        )
        config = RetryConfig(max_attempts=5, base_delay=0.01)

        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(httpx.HTTPStatusError, match="Not found"):
                await retry_async(mock_func, config=config)

            # Should have retried twice (for 500 and timeout), then failed on 404
            assert mock_func.call_count == 3
