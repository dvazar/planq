"""Comprehensive tests for DeadlineMiddleware implementation."""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock

import pytest
import time_machine
from hypothesis import given

from qanat.enums import Header, JsonRpcError
from qanat.middleware import DeadlineMiddleware, Middleware

from .conftest import valid_expire_at_values

# === Layer 1: Instantiation and Inheritance ===


class TestDeadlineMiddlewareInit:
    """Test DeadlineMiddleware instantiation and inheritance."""

    def test_deadline_middleware_can_be_instantiated(self):
        """DeadlineMiddleware can be instantiated."""
        middleware = DeadlineMiddleware()
        assert isinstance(middleware, DeadlineMiddleware)

    def test_deadline_middleware_inherits_from_base_middleware(self):
        """DeadlineMiddleware is a subclass of Middleware."""
        assert issubclass(DeadlineMiddleware, Middleware)

    def test_deadline_middleware_instance_is_base_middleware(self):
        """DeadlineMiddleware instance is an instance of Middleware."""
        middleware = DeadlineMiddleware()
        assert isinstance(middleware, Middleware)

    def test_deadline_middleware_default_leeway_is_zero(self):
        """DeadlineMiddleware defaults to leeway=0.0."""
        middleware = DeadlineMiddleware()
        assert middleware.leeway == 0.0

    def test_multiple_instances_are_independent(self):
        """Multiple DeadlineMiddleware instances are independent."""
        m1 = DeadlineMiddleware()
        m2 = DeadlineMiddleware()
        assert m1 is not m2


# === Layer 1.5: Leeway Parameter Validation ===


class TestDeadlineMiddlewareLeewayValidation:
    """Test leeway parameter validation and initialization."""

    def test_default_leeway_is_zero(self):
        """Default leeway is 0.0."""
        middleware = DeadlineMiddleware()
        assert middleware.leeway == 0.0

    def test_leeway_can_be_set_to_positive_float(self):
        """Leeway can be set to a positive float."""
        middleware = DeadlineMiddleware(leeway=2.5)
        assert middleware.leeway == 2.5

    def test_leeway_can_be_set_to_positive_int(self):
        """Leeway can be set to a positive integer."""
        middleware = DeadlineMiddleware(leeway=5)
        assert middleware.leeway == 5

    def test_leeway_accepts_zero(self):
        """Leeway accepts 0.0 explicitly."""
        middleware = DeadlineMiddleware(leeway=0.0)
        assert middleware.leeway == 0.0

    def test_negative_leeway_raises_value_error(self):
        """Negative leeway raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            DeadlineMiddleware(leeway=-1.0)
        assert "non-negative" in str(exc_info.value)

    def test_negative_leeway_error_message(self):
        """Negative leeway error message mentions "must be non-negative"."""
        with pytest.raises(ValueError) as exc_info:
            DeadlineMiddleware(leeway=-5.0)
        assert "must be non-negative" in str(exc_info.value)

    def test_large_leeway_logs_warning(self, caplog):
        """Leeway >60 seconds logs a warning."""
        import logging

        with caplog.at_level(logging.WARNING):
            middleware = DeadlineMiddleware(leeway=120.0)

        assert middleware.leeway == 120.0
        assert len(caplog.records) == 1
        assert caplog.records[0].levelname == "WARNING"

    def test_warning_message_mentions_misconfiguration(self, caplog):
        """Warning message mentions misconfiguration."""
        import logging

        with caplog.at_level(logging.WARNING):
            DeadlineMiddleware(leeway=90.0)

        assert "misconfiguration" in caplog.text.lower()

    def test_leeway_exactly_60_does_not_warn(self, caplog):
        """Leeway of exactly 60 does not warn."""
        import logging

        with caplog.at_level(logging.WARNING):
            middleware = DeadlineMiddleware(leeway=60.0)

        assert middleware.leeway == 60.0
        assert len(caplog.records) == 0

    def test_leeway_60_point_1_warns(self, caplog):
        """Leeway of 60.1 triggers warning."""
        import logging

        with caplog.at_level(logging.WARNING):
            DeadlineMiddleware(leeway=60.1)

        assert len(caplog.records) == 1
        assert caplog.records[0].levelname == "WARNING"


# === Layer 2: Edge Cases - Header Presence ===


class TestDeadlineMiddlewareHeaderPresence:
    """Test DeadlineMiddleware behavior with different header scenarios."""

    @pytest.mark.asyncio
    async def test_message_without_expire_at_header(
        self, mock_broker_message, mock_call_next
    ):
        """Message without x-expire-at header delegates to call_next."""
        middleware = DeadlineMiddleware()
        mock_broker_message.headers = {}

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is None
        mock_call_next.assert_called_once_with(mock_broker_message)

    @pytest.mark.asyncio
    async def test_message_with_empty_headers_dict(
        self, mock_broker_message, mock_call_next
    ):
        """Message with empty headers dict delegates to call_next."""
        middleware = DeadlineMiddleware()
        mock_broker_message.headers = {}

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is None
        mock_call_next.assert_called_once_with(mock_broker_message)

    @pytest.mark.asyncio
    async def test_message_with_other_headers_no_ttl(
        self, mock_broker_message, mock_call_next
    ):
        """Message with other headers but no x-expire-at delegates."""
        middleware = DeadlineMiddleware()
        mock_broker_message.headers = {
            "x-max-retries": "3",
            "x-correlation-id": "abc-123",
        }

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is None
        mock_call_next.assert_called_once_with(mock_broker_message)


# === Layer 3: Boundary Cases - Time Comparison ===


class TestDeadlineMiddlewareBoundaryCases:
    """Test DeadlineMiddleware time comparison boundary conditions."""

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_message_with_exactly_current_time(
        self, mock_broker_message, mock_call_next
    ):
        """Message expiring at exactly current time is NOT dropped."""
        middleware = DeadlineMiddleware()
        current_time = time.time()
        mock_broker_message.headers = {Header.EXPIRE_AT: str(current_time)}

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is None
        mock_call_next.assert_called_once()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_expired_request_returns_error_response(
        self, mock_broker_message, mock_call_next
    ):
        """Expired request returns JsonRpcResponse with error."""
        middleware = DeadlineMiddleware()
        current_time = time.time()
        expire_at = current_time - 1.0
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}
        mock_broker_message.correlation_id = "test-123"

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is not None
        assert result.error is not None
        assert result.error.code == JsonRpcError.DEADLINE_EXCEEDED
        assert result.id == "test-123"
        mock_call_next.assert_not_called()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_expired_notification_returns_none(
        self, mock_broker_message, mock_call_next
    ):
        """Expired notification returns None."""
        middleware = DeadlineMiddleware()
        current_time = time.time()
        expire_at = current_time - 1.0
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}
        mock_broker_message.correlation_id = None

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is None
        mock_call_next.assert_not_called()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_message_expires_in_one_second(
        self, mock_broker_message, mock_call_next
    ):
        """Message expiring in 1 second delegates to call_next."""
        middleware = DeadlineMiddleware()
        current_time = time.time()
        expire_at = current_time + 1.0
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is None
        mock_call_next.assert_called_once()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_message_expires_in_far_future(
        self, mock_broker_message, mock_call_next
    ):
        """Message expiring far in the future delegates."""
        middleware = DeadlineMiddleware()
        current_time = time.time()
        expire_at = current_time + 86400 * 365
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is None
        mock_call_next.assert_called_once()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_message_expired_in_far_past(
        self, mock_broker_message, mock_call_next
    ):
        """Message expired long ago is dropped."""
        middleware = DeadlineMiddleware()
        current_time = time.time()
        expire_at = current_time - 86400 * 365
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}
        mock_broker_message.correlation_id = "test-123"

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is not None
        assert result.error is not None
        mock_call_next.assert_not_called()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_message_expired_by_milliseconds(
        self, mock_broker_message, mock_call_next
    ):
        """Message expired by milliseconds is dropped."""
        middleware = DeadlineMiddleware()
        current_time = time.time()
        expire_at = current_time - 0.001
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}
        mock_broker_message.correlation_id = "test-123"

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is not None
        assert result.error is not None
        mock_call_next.assert_not_called()


# === Layer 3.5: Boundary Cases with Leeway ===


class TestDeadlineMiddlewareLeewayBoundary:
    """Test deadline checking with leeway tolerance."""

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_expired_by_less_than_leeway_allows(
        self, mock_broker_message, mock_call_next
    ):
        """Message expired by less than leeway delegates."""
        middleware = DeadlineMiddleware(leeway=2.0)
        current_time = time.time()
        expire_at = current_time - 1.5
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is None
        mock_call_next.assert_called_once()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_expired_exactly_by_leeway_allows(
        self, mock_broker_message, mock_call_next
    ):
        """Message expired exactly by leeway amount delegates."""
        middleware = DeadlineMiddleware(leeway=2.0)
        current_time = time.time()
        expire_at = current_time - 2.0
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is None
        mock_call_next.assert_called_once()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_expired_by_more_than_leeway_drops(
        self, mock_broker_message, mock_call_next
    ):
        """Message expired by more than leeway is dropped."""
        middleware = DeadlineMiddleware(leeway=2.0)
        current_time = time.time()
        expire_at = current_time - 2.1
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}
        mock_broker_message.correlation_id = "test-123"

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is not None
        assert result.error is not None
        mock_call_next.assert_not_called()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_leeway_does_not_affect_future_expiry(
        self, mock_broker_message, mock_call_next
    ):
        """Leeway does not affect messages with future expiry."""
        middleware = DeadlineMiddleware(leeway=2.0)
        current_time = time.time()
        expire_at = current_time + 100
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is None
        mock_call_next.assert_called_once()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_zero_leeway_preserves_strict_behavior(
        self, mock_broker_message, mock_call_next
    ):
        """Zero leeway preserves strict deadline enforcement."""
        middleware = DeadlineMiddleware(leeway=0.0)
        current_time = time.time()
        expire_at = current_time - 0.001
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}
        mock_broker_message.correlation_id = "test-123"

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is not None
        assert result.error is not None
        mock_call_next.assert_not_called()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_large_leeway_allows_very_old_messages(
        self, mock_broker_message, mock_call_next
    ):
        """Large leeway allows messages far past deadline."""
        middleware = DeadlineMiddleware(leeway=200.0)
        current_time = time.time()
        expire_at = current_time - 100
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is None
        mock_call_next.assert_called_once()


# === Layer 4: No Direct Broker Operations ===


class TestDeadlineMiddlewareNoBrokerOps:
    """Test that DeadlineMiddleware never calls broker operations."""

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_does_not_call_reject_when_expired(
        self, mock_broker_message, mock_call_next
    ):
        """Does not call msg.reject() when expired."""
        middleware = DeadlineMiddleware()
        current_time = time.time()
        expire_at = current_time - 10.0
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        await middleware(mock_broker_message, mock_call_next)

        mock_broker_message.reject.assert_not_called()
        mock_broker_message.nack.assert_not_called()
        mock_broker_message.ack.assert_not_called()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_does_not_call_any_broker_op_when_valid(
        self, mock_broker_message, mock_call_next
    ):
        """Does not call any broker op when message is valid."""
        middleware = DeadlineMiddleware()
        current_time = time.time()
        expire_at = current_time + 10.0
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        await middleware(mock_broker_message, mock_call_next)

        mock_broker_message.reject.assert_not_called()
        mock_broker_message.nack.assert_not_called()
        mock_broker_message.ack.assert_not_called()


# === Layer 5: Header Parsing ===


class TestDeadlineMiddlewareHeaderParsing:
    """Test DeadlineMiddleware header value parsing."""

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_expire_at_as_integer_timestamp(
        self, mock_broker_message, mock_call_next
    ):
        """Parses integer timestamp correctly."""
        middleware = DeadlineMiddleware()
        current_time = int(time.time())
        expire_at = current_time + 100
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is None
        mock_call_next.assert_called_once()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_expire_at_as_float_timestamp(
        self, mock_broker_message, mock_call_next
    ):
        """Parses float timestamp correctly."""
        middleware = DeadlineMiddleware()
        current_time = time.time()
        expire_at = current_time + 100.5
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is None
        mock_call_next.assert_called_once()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_expire_at_with_decimal_precision(
        self, mock_broker_message, mock_call_next
    ):
        """Handles timestamps with high decimal precision."""
        middleware = DeadlineMiddleware()
        current_time = time.time()
        expire_at = f"{current_time + 100:.6f}"
        mock_broker_message.headers = {Header.EXPIRE_AT: expire_at}

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is None
        mock_call_next.assert_called_once()


# === Layer 6: Error Handling ===


class TestDeadlineMiddlewareErrorHandling:
    """Test DeadlineMiddleware error handling for malformed headers."""

    @pytest.mark.asyncio
    async def test_invalid_expire_at_format_raises_value_error(
        self, mock_broker_message, mock_call_next
    ):
        """Invalid expire_at format raises ValueError."""
        middleware = DeadlineMiddleware()
        mock_broker_message.headers = {Header.EXPIRE_AT: "not-a-number"}

        with pytest.raises(ValueError):
            await middleware(mock_broker_message, mock_call_next)

    @pytest.mark.asyncio
    async def test_expire_at_non_numeric_raises_value_error(
        self, mock_broker_message, mock_call_next
    ):
        """Non-numeric expire_at raises ValueError."""
        middleware = DeadlineMiddleware()
        mock_broker_message.headers = {Header.EXPIRE_AT: "invalid"}

        with pytest.raises(ValueError):
            await middleware(mock_broker_message, mock_call_next)

    @pytest.mark.asyncio
    async def test_expire_at_empty_string_raises_value_error(
        self, mock_broker_message, mock_call_next
    ):
        """Empty string expire_at raises ValueError."""
        middleware = DeadlineMiddleware()
        mock_broker_message.headers = {Header.EXPIRE_AT: ""}

        with pytest.raises(ValueError):
            await middleware(mock_broker_message, mock_call_next)

    @pytest.mark.asyncio
    async def test_expire_at_with_whitespace_raises_value_error(
        self, mock_broker_message, mock_call_next
    ):
        """Whitespace-only expire_at raises ValueError."""
        middleware = DeadlineMiddleware()
        mock_broker_message.headers = {Header.EXPIRE_AT: "   "}

        with pytest.raises(ValueError):
            await middleware(mock_broker_message, mock_call_next)


# === Layer 7: Time Mocking Integration ===


class TestDeadlineMiddlewareTimeMocking:
    """Test DeadlineMiddleware with time-machine for time control."""

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_deadline_check_with_frozen_time(
        self, mock_broker_message, mock_call_next
    ):
        """Deadline check works correctly with frozen time."""
        middleware = DeadlineMiddleware()
        current_time = time.time()
        expire_at = current_time - 1.0
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}
        mock_broker_message.correlation_id = "test-123"

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is not None
        assert result.error is not None
        mock_call_next.assert_not_called()

    @pytest.mark.asyncio
    async def test_deadline_check_with_time_progression(
        self, mock_broker_message, mock_call_next
    ):
        """Deadline check works as time progresses."""
        middleware = DeadlineMiddleware()

        with time_machine.travel("2025-01-15 12:00:00", tick=False):
            current_time = time.time()
            expire_at = current_time + 1.0
            mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

            result = await middleware(mock_broker_message, mock_call_next)
            assert result is None
            mock_call_next.assert_called_once()

        mock_call_next.reset_mock()
        mock_broker_message.correlation_id = "test-123"

        with time_machine.travel("2025-01-15 12:00:02", tick=False):
            result = await middleware(mock_broker_message, mock_call_next)
            assert result is not None
            assert result.error is not None
            mock_call_next.assert_not_called()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_multiple_checks_with_same_frozen_time(
        self, mock_broker_message, mock_call_next
    ):
        """Multiple deadline checks at same frozen time are consistent."""
        middleware = DeadlineMiddleware()
        current_time = time.time()
        expire_at = current_time + 100.0
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        for _ in range(3):
            mock_call_next.reset_mock()
            result = await middleware(mock_broker_message, mock_call_next)
            assert result is None
            mock_call_next.assert_called_once()


# === Layer 8: Integration Scenarios ===


class TestDeadlineMiddlewareIntegration:
    """Test DeadlineMiddleware in realistic integration scenarios."""

    @pytest.mark.asyncio
    async def test_deadline_middleware_with_real_time(
        self, mock_broker_message, mock_call_next
    ):
        """DeadlineMiddleware works with real time.time()."""
        middleware = DeadlineMiddleware()
        expire_at = time.time() + 3600
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is None
        mock_call_next.assert_called_once()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_multiple_messages_with_different_ttls(self):
        """Process multiple messages with different deadlines."""
        from qanat.message import BrokerMessage
        from qanat.models import JsonRpcRequest

        middleware = DeadlineMiddleware()
        current_time = time.time()
        call_next = AsyncMock(return_value=None)

        # Message 1: Not expired
        msg1 = MagicMock(spec=BrokerMessage)
        msg1.headers = {Header.EXPIRE_AT: str(current_time + 100)}
        msg1.body = JsonRpcRequest(method="test.method", params={}, id="msg1")
        msg1.correlation_id = "msg1"

        # Message 2: Expired (request)
        msg2 = MagicMock(spec=BrokerMessage)
        msg2.headers = {Header.EXPIRE_AT: str(current_time - 10)}
        msg2.body = JsonRpcRequest(method="test.method", params={}, id="msg2")
        msg2.correlation_id = "msg2"

        # Message 3: No deadline
        msg3 = MagicMock(spec=BrokerMessage)
        msg3.headers = {}
        msg3.body = JsonRpcRequest(method="test.method", params={}, id="msg3")
        msg3.correlation_id = "msg3"

        # Process msg1: should delegate
        call_next.reset_mock()
        result1 = await middleware(msg1, call_next)
        assert result1 is None
        call_next.assert_called_once()

        # Process msg2: should return error
        call_next.reset_mock()
        result2 = await middleware(msg2, call_next)
        assert result2 is not None
        assert result2.error is not None
        assert result2.error.code == JsonRpcError.DEADLINE_EXCEEDED
        call_next.assert_not_called()

        # Process msg3: should delegate
        call_next.reset_mock()
        result3 = await middleware(msg3, call_next)
        assert result3 is None
        call_next.assert_called_once()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_deadline_middleware_doesnt_mutate_message(
        self, mock_broker_message, mock_call_next
    ):
        """DeadlineMiddleware doesn't mutate message on success."""
        middleware = DeadlineMiddleware()
        current_time = time.time()
        expire_at = current_time + 100.0
        original_headers = {Header.EXPIRE_AT: str(expire_at)}
        mock_broker_message.headers = original_headers.copy()
        original_params = mock_broker_message.body.params.copy()

        await middleware(mock_broker_message, mock_call_next)

        assert mock_broker_message.headers == original_headers
        assert mock_broker_message.body.params == original_params


# === Layer 8.5: Integration with Leeway ===


class TestDeadlineMiddlewareLeewayIntegration:
    """Test realistic scenarios with different leeway configurations."""

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_multiple_messages_with_same_leeway(self):
        """Process multiple messages with same leeway configuration."""
        from qanat.message import BrokerMessage
        from qanat.models import JsonRpcRequest

        middleware = DeadlineMiddleware(leeway=2.0)
        current_time = time.time()
        call_next = AsyncMock(return_value=None)

        # Message 1: Expired by 1s (within leeway)
        msg1 = MagicMock(spec=BrokerMessage)
        msg1.headers = {Header.EXPIRE_AT: str(current_time - 1.0)}
        msg1.body = JsonRpcRequest(method="test.method", params={}, id="msg1")
        msg1.correlation_id = "msg1"

        # Message 2: Expired by 3s (beyond leeway)
        msg2 = MagicMock(spec=BrokerMessage)
        msg2.headers = {Header.EXPIRE_AT: str(current_time - 3.0)}
        msg2.body = JsonRpcRequest(method="test.method", params={}, id="msg2")
        msg2.correlation_id = "msg2"

        # Message 3: Not expired
        msg3 = MagicMock(spec=BrokerMessage)
        msg3.headers = {Header.EXPIRE_AT: str(current_time + 100)}
        msg3.body = JsonRpcRequest(method="test.method", params={}, id="msg3")
        msg3.correlation_id = "msg3"

        # Process msg1: should delegate (within leeway)
        call_next.reset_mock()
        result1 = await middleware(msg1, call_next)
        assert result1 is None
        call_next.assert_called_once()

        # Process msg2: should return error (beyond leeway)
        call_next.reset_mock()
        result2 = await middleware(msg2, call_next)
        assert result2 is not None
        assert result2.error is not None
        call_next.assert_not_called()

        # Process msg3: should delegate (not expired)
        call_next.reset_mock()
        result3 = await middleware(msg3, call_next)
        assert result3 is None
        call_next.assert_called_once()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_leeway_stored_as_instance_variable(self):
        """Leeway is accessible as instance variable."""
        middleware1 = DeadlineMiddleware(leeway=1.5)
        assert middleware1.leeway == 1.5

        middleware2 = DeadlineMiddleware(leeway=5.0)
        assert middleware2.leeway == 5.0

        middleware3 = DeadlineMiddleware()
        assert middleware3.leeway == 0.0


# === Layer 9: Property-Based Testing ===


class TestDeadlineMiddlewarePropertyBased:
    """Property-based tests with hypothesis."""

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    @given(expire_at=valid_expire_at_values())
    async def test_deadline_check_with_random_timestamps(self, expire_at):
        """Deadline check handles random expire_at values correctly."""
        from qanat.message import BrokerMessage
        from qanat.models import JsonRpcRequest

        mock_msg = MagicMock(spec=BrokerMessage)
        mock_msg.headers = {Header.EXPIRE_AT: str(expire_at)}
        mock_msg.body = JsonRpcRequest(
            method="test.method", params={}, id="test-123"
        )
        mock_msg.correlation_id = "test-123"

        call_next = AsyncMock(return_value=None)
        middleware = DeadlineMiddleware()
        current_time = time.time()

        result = await middleware(mock_msg, call_next)

        if current_time > expire_at:
            assert result is not None
            assert result.error is not None
            assert result.error.code == JsonRpcError.DEADLINE_EXCEEDED
            call_next.assert_not_called()
        else:
            assert result is None
            call_next.assert_called_once()


# === Layer 10: Documentation ===


class TestDeadlineMiddlewareDocumentation:
    """Test DeadlineMiddleware documentation."""

    def test_deadline_middleware_has_docstring(self):
        """DeadlineMiddleware class has docstring."""
        assert DeadlineMiddleware.__doc__ is not None
        assert len(DeadlineMiddleware.__doc__) > 0

    def test_docstring_mentions_deadline(self):
        """Docstring mentions deadline or expiration."""
        docstring = DeadlineMiddleware.__doc__.lower()
        assert "deadline" in docstring or "expir" in docstring

    def test_call_has_docstring(self):
        """__call__ override has docstring."""
        assert DeadlineMiddleware.__call__.__doc__ is not None


# === Layer 11: Header Enum Usage ===


class TestDeadlineMiddlewareHeaderEnumUsage:
    """Test that DeadlineMiddleware uses Header enum correctly."""

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_uses_header_enum_constant(
        self, mock_broker_message, mock_call_next
    ):
        """DeadlineMiddleware uses Header.EXPIRE_AT enum."""
        middleware = DeadlineMiddleware()
        current_time = time.time()
        mock_broker_message.headers = {
            Header.EXPIRE_AT: str(current_time + 100)
        }

        result = await middleware(mock_broker_message, mock_call_next)

        assert result is None

    @pytest.mark.asyncio
    async def test_header_enum_value_is_x_expire_at(self):
        """Header.EXPIRE_AT has correct string value."""
        assert Header.EXPIRE_AT == "x-expire-at"
