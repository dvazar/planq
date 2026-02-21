"""Comprehensive tests for TtlMiddleware implementation."""

from __future__ import annotations

import time

import pytest
import time_machine
from hypothesis import given

from qanat.enums import Header
from qanat.middleware import Middleware, SkipMessage, TtlMiddleware
from qanat.models import JsonRpcResponse

from .conftest import valid_expire_at_values

# === Layer 1: Instantiation and Inheritance ===


class TestTtlMiddlewareInit:
    """Test TtlMiddleware instantiation and inheritance."""

    def test_ttl_middleware_can_be_instantiated(self):
        """TtlMiddleware can be instantiated."""
        middleware = TtlMiddleware()
        assert isinstance(middleware, TtlMiddleware)

    def test_ttl_middleware_inherits_from_middleware(self):
        """TtlMiddleware is a subclass of Middleware."""
        assert issubclass(TtlMiddleware, Middleware)

    def test_ttl_middleware_instance_is_middleware(self):
        """TtlMiddleware instance is an instance of Middleware."""
        middleware = TtlMiddleware()
        assert isinstance(middleware, Middleware)

    def test_ttl_middleware_has_no_init_parameters(self):
        """TtlMiddleware __init__ takes no parameters."""
        middleware = TtlMiddleware()
        assert middleware is not None

    def test_multiple_instances_are_independent(self):
        """Multiple TtlMiddleware instances are independent."""
        m1 = TtlMiddleware()
        m2 = TtlMiddleware()
        assert m1 is not m2


# === Layer 2: Edge Cases - TTL Header Presence ===


class TestTtlMiddlewareHeaderPresence:
    """Test TtlMiddleware behavior with different header scenarios."""

    @pytest.mark.asyncio
    async def test_message_without_expire_at_header(
        self, mock_consumer, mock_broker_message
    ):
        """Message without x-expire-at header continues processing."""
        middleware = TtlMiddleware()
        mock_broker_message.headers = {}

        # Should not raise SkipMessage
        result = await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert result is None
        mock_broker_message.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_message_with_empty_headers_dict(
        self, mock_consumer, mock_broker_message
    ):
        """Message with empty headers dict continues processing."""
        middleware = TtlMiddleware()
        mock_broker_message.headers = {}

        result = await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert result is None
        mock_broker_message.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_message_with_other_headers_no_ttl(
        self, mock_consumer, mock_broker_message
    ):
        """Message with other headers but no x-expire-at continues."""
        middleware = TtlMiddleware()
        mock_broker_message.headers = {
            "x-max-retries": "3",
            "x-correlation-id": "abc-123",
        }

        result = await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert result is None
        mock_broker_message.reject.assert_not_called()


# === Layer 3: Boundary Cases - Time Comparison ===


class TestTtlMiddlewareBoundaryCases:
    """Test TtlMiddleware time comparison boundary conditions."""

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_message_with_exactly_current_time(
        self, mock_consumer, mock_broker_message
    ):
        """Message expiring at exactly current time is NOT rejected."""
        middleware = TtlMiddleware()
        current_time = time.time()
        mock_broker_message.headers = {Header.EXPIRE_AT: str(current_time)}

        # time.time() > float(expire_at) is False when equal
        result = await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert result is None
        mock_broker_message.reject.assert_not_called()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_message_expired_by_one_second(
        self, mock_consumer, mock_broker_message
    ):
        """Message expired by 1 second is rejected."""
        middleware = TtlMiddleware()
        current_time = time.time()
        expire_at = current_time - 1.0
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        with pytest.raises(SkipMessage):
            await middleware.before_process_message(
                mock_consumer, mock_broker_message
            )

        mock_broker_message.reject.assert_called_once()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_message_expires_in_one_second(
        self, mock_consumer, mock_broker_message
    ):
        """Message expiring in 1 second continues processing."""
        middleware = TtlMiddleware()
        current_time = time.time()
        expire_at = current_time + 1.0
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        result = await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert result is None
        mock_broker_message.reject.assert_not_called()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_message_expires_in_far_future(
        self, mock_consumer, mock_broker_message
    ):
        """Message expiring far in the future continues."""
        middleware = TtlMiddleware()
        current_time = time.time()
        expire_at = current_time + 86400 * 365  # 1 year from now
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        result = await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert result is None
        mock_broker_message.reject.assert_not_called()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_message_expired_in_far_past(
        self, mock_consumer, mock_broker_message
    ):
        """Message expired long ago is rejected."""
        middleware = TtlMiddleware()
        current_time = time.time()
        expire_at = current_time - 86400 * 365  # 1 year ago
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        with pytest.raises(SkipMessage):
            await middleware.before_process_message(
                mock_consumer, mock_broker_message
            )

        mock_broker_message.reject.assert_called_once()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_message_expired_by_milliseconds(
        self, mock_consumer, mock_broker_message
    ):
        """Message expired by milliseconds is rejected."""
        middleware = TtlMiddleware()
        current_time = time.time()
        expire_at = current_time - 0.001  # 1ms ago
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        with pytest.raises(SkipMessage):
            await middleware.before_process_message(
                mock_consumer, mock_broker_message
            )

        mock_broker_message.reject.assert_called_once()


# === Layer 4: Rejection Behavior ===


class TestTtlMiddlewareRejectionBehavior:
    """Test TtlMiddleware rejection and SkipMessage raising."""

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_calls_msg_reject_when_expired(
        self, mock_consumer, mock_broker_message
    ):
        """Calls msg.reject() when message is expired."""
        middleware = TtlMiddleware()
        current_time = time.time()
        expire_at = current_time - 10.0
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        with pytest.raises(SkipMessage):
            await middleware.before_process_message(
                mock_consumer, mock_broker_message
            )

        mock_broker_message.reject.assert_called_once()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_raises_skip_message_after_reject(
        self, mock_consumer, mock_broker_message
    ):
        """Raises SkipMessage after calling reject()."""
        middleware = TtlMiddleware()
        current_time = time.time()
        expire_at = current_time - 10.0
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        with pytest.raises(SkipMessage):
            await middleware.before_process_message(
                mock_consumer, mock_broker_message
            )

        # reject() must be called before SkipMessage is raised
        assert mock_broker_message.reject.called

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_does_not_call_reject_when_not_expired(
        self, mock_consumer, mock_broker_message
    ):
        """Does not call reject() when message is not expired."""
        middleware = TtlMiddleware()
        current_time = time.time()
        expire_at = current_time + 10.0
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        mock_broker_message.reject.assert_not_called()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_does_not_raise_skip_message_when_not_expired(
        self, mock_consumer, mock_broker_message
    ):
        """Does not raise SkipMessage when message is not expired."""
        middleware = TtlMiddleware()
        current_time = time.time()
        expire_at = current_time + 10.0
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        # Should not raise
        result = await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert result is None

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_does_not_call_nack_or_ack(
        self, mock_consumer, mock_broker_message
    ):
        """TtlMiddleware only calls reject(), never nack() or ack()."""
        middleware = TtlMiddleware()
        current_time = time.time()
        expire_at = current_time - 10.0
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        with pytest.raises(SkipMessage):
            await middleware.before_process_message(
                mock_consumer, mock_broker_message
            )

        mock_broker_message.reject.assert_called_once()
        mock_broker_message.nack.assert_not_called()
        mock_broker_message.ack.assert_not_called()


# === Layer 5: Header Parsing ===


class TestTtlMiddlewareHeaderParsing:
    """Test TtlMiddleware header value parsing."""

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_expire_at_as_integer_timestamp(
        self, mock_consumer, mock_broker_message
    ):
        """Parses integer timestamp correctly."""
        middleware = TtlMiddleware()
        current_time = int(time.time())
        expire_at = current_time + 100
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        result = await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert result is None
        mock_broker_message.reject.assert_not_called()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_expire_at_as_float_timestamp(
        self, mock_consumer, mock_broker_message
    ):
        """Parses float timestamp correctly."""
        middleware = TtlMiddleware()
        current_time = time.time()
        expire_at = current_time + 100.5
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        result = await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert result is None
        mock_broker_message.reject.assert_not_called()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_expire_at_as_string_timestamp(
        self, mock_consumer, mock_broker_message
    ):
        """Parses string representation of timestamp."""
        middleware = TtlMiddleware()
        current_time = time.time()
        expire_at = str(current_time + 100)
        mock_broker_message.headers = {Header.EXPIRE_AT: expire_at}

        result = await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert result is None
        mock_broker_message.reject.assert_not_called()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_expire_at_with_decimal_precision(
        self, mock_consumer, mock_broker_message
    ):
        """Handles timestamps with high decimal precision."""
        middleware = TtlMiddleware()
        current_time = time.time()
        expire_at = f"{current_time + 100:.6f}"
        mock_broker_message.headers = {Header.EXPIRE_AT: expire_at}

        result = await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert result is None
        mock_broker_message.reject.assert_not_called()


# === Layer 6: Error Handling ===


class TestTtlMiddlewareErrorHandling:
    """Test TtlMiddleware error handling for malformed headers."""

    @pytest.mark.asyncio
    async def test_invalid_expire_at_format_raises_value_error(
        self, mock_consumer, mock_broker_message
    ):
        """Invalid expire_at format raises ValueError."""
        middleware = TtlMiddleware()
        mock_broker_message.headers = {Header.EXPIRE_AT: "not-a-number"}

        with pytest.raises(ValueError):
            await middleware.before_process_message(
                mock_consumer, mock_broker_message
            )

    @pytest.mark.asyncio
    async def test_expire_at_non_numeric_raises_value_error(
        self, mock_consumer, mock_broker_message
    ):
        """Non-numeric expire_at raises ValueError."""
        middleware = TtlMiddleware()
        mock_broker_message.headers = {Header.EXPIRE_AT: "invalid"}

        with pytest.raises(ValueError):
            await middleware.before_process_message(
                mock_consumer, mock_broker_message
            )

    @pytest.mark.asyncio
    async def test_expire_at_empty_string_raises_value_error(
        self, mock_consumer, mock_broker_message
    ):
        """Empty string expire_at raises ValueError."""
        middleware = TtlMiddleware()
        mock_broker_message.headers = {Header.EXPIRE_AT: ""}

        with pytest.raises(ValueError):
            await middleware.before_process_message(
                mock_consumer, mock_broker_message
            )

    @pytest.mark.asyncio
    async def test_expire_at_with_whitespace_raises_value_error(
        self, mock_consumer, mock_broker_message
    ):
        """Whitespace-only expire_at raises ValueError."""
        middleware = TtlMiddleware()
        mock_broker_message.headers = {Header.EXPIRE_AT: "   "}

        with pytest.raises(ValueError):
            await middleware.before_process_message(
                mock_consumer, mock_broker_message
            )


# === Layer 7: Other Hooks Remain No-ops ===


class TestTtlMiddlewareOtherHooks:
    """Test that TtlMiddleware doesn't override other hooks."""

    @pytest.mark.asyncio
    async def test_after_process_message_is_noop(
        self, mock_consumer, mock_broker_message
    ):
        """after_process_message is inherited no-op."""
        middleware = TtlMiddleware()

        result = await middleware.after_process_message(
            mock_consumer,
            mock_broker_message,
            result={"status": "ok"},
            exception=None,
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_after_skip_message_is_noop(
        self, mock_consumer, mock_broker_message
    ):
        """after_skip_message is inherited no-op."""
        middleware = TtlMiddleware()

        result = await middleware.after_skip_message(
            mock_consumer, mock_broker_message
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_before_publish_response_is_noop(
        self, mock_consumer, mock_broker_message
    ):
        """before_publish_response is inherited no-op."""
        middleware = TtlMiddleware()
        response = JsonRpcResponse(jsonrpc="2.0", id="123", result={"ok": True})
        headers = {}

        result = await middleware.before_publish_response(
            mock_consumer, mock_broker_message, response, headers
        )

        assert result is None
        assert headers == {}

    @pytest.mark.asyncio
    async def test_other_hooks_dont_call_reject(
        self, mock_consumer, mock_broker_message
    ):
        """Other hooks don't call msg.reject()."""
        middleware = TtlMiddleware()
        response = JsonRpcResponse(jsonrpc="2.0", id="123", result={})
        headers = {}

        await middleware.after_process_message(
            mock_consumer, mock_broker_message, result=None, exception=None
        )
        await middleware.after_skip_message(mock_consumer, mock_broker_message)
        await middleware.before_publish_response(
            mock_consumer, mock_broker_message, response, headers
        )

        mock_broker_message.reject.assert_not_called()


# === Layer 8: Time Mocking Integration ===


class TestTtlMiddlewareTimeMocking:
    """Test TtlMiddleware with time-machine for time control."""

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_ttl_check_with_frozen_time(
        self, mock_consumer, mock_broker_message
    ):
        """TTL check works correctly with frozen time."""
        middleware = TtlMiddleware()
        # Current time is 1736942400.0 (2025-01-15 12:00:00 UTC)
        current_time = time.time()
        expire_at = current_time - 1.0
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        with pytest.raises(SkipMessage):
            await middleware.before_process_message(
                mock_consumer, mock_broker_message
            )

        mock_broker_message.reject.assert_called_once()

    @pytest.mark.asyncio
    async def test_ttl_check_with_time_progression(
        self, mock_consumer, mock_broker_message
    ):
        """TTL check works as time progresses."""
        middleware = TtlMiddleware()

        # Set expire_at to 1 second from now
        with time_machine.travel("2025-01-15 12:00:00", tick=False):
            current_time = time.time()
            expire_at = current_time + 1.0
            mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

            # Should not be expired yet
            result = await middleware.before_process_message(
                mock_consumer, mock_broker_message
            )
            assert result is None

        # Move time forward by 2 seconds
        with time_machine.travel("2025-01-15 12:00:02", tick=False):
            # Now it should be expired
            with pytest.raises(SkipMessage):
                await middleware.before_process_message(
                    mock_consumer, mock_broker_message
                )

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_multiple_checks_with_same_frozen_time(
        self, mock_consumer, mock_broker_message
    ):
        """Multiple TTL checks at same frozen time are consistent."""
        middleware = TtlMiddleware()
        current_time = time.time()
        expire_at = current_time + 100.0
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        # All checks should pass
        for _ in range(3):
            result = await middleware.before_process_message(
                mock_consumer, mock_broker_message
            )
            assert result is None

        mock_broker_message.reject.assert_not_called()


# === Layer 9: Integration Scenarios ===


class TestTtlMiddlewareIntegration:
    """Test TtlMiddleware in realistic integration scenarios."""

    @pytest.mark.asyncio
    async def test_ttl_middleware_with_real_time(
        self, mock_consumer, mock_broker_message
    ):
        """TtlMiddleware works with real time.time()."""
        middleware = TtlMiddleware()
        # Set expire_at to far future
        expire_at = time.time() + 3600  # 1 hour from now
        mock_broker_message.headers = {Header.EXPIRE_AT: str(expire_at)}

        result = await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert result is None
        mock_broker_message.reject.assert_not_called()

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_multiple_messages_with_different_ttls(self, mock_consumer):
        """Process multiple messages with different TTLs."""
        from unittest.mock import AsyncMock, MagicMock

        from qanat.message import BrokerMessage
        from qanat.models import JsonRpcRequest

        middleware = TtlMiddleware()
        current_time = time.time()

        # Message 1: Not expired
        msg1 = MagicMock(spec=BrokerMessage)
        msg1.headers = {Header.EXPIRE_AT: str(current_time + 100)}
        msg1.body = JsonRpcRequest(method="test.method", params={}, id="msg1")
        msg1.reject = AsyncMock()

        # Message 2: Expired
        msg2 = MagicMock(spec=BrokerMessage)
        msg2.headers = {Header.EXPIRE_AT: str(current_time - 10)}
        msg2.body = JsonRpcRequest(method="test.method", params={}, id="msg2")
        msg2.reject = AsyncMock()

        # Message 3: No TTL
        msg3 = MagicMock(spec=BrokerMessage)
        msg3.headers = {}
        msg3.body = JsonRpcRequest(method="test.method", params={}, id="msg3")
        msg3.reject = AsyncMock()

        # Process msg1: should succeed
        result1 = await middleware.before_process_message(mock_consumer, msg1)
        assert result1 is None
        assert not msg1.reject.called

        # Process msg2: should reject
        with pytest.raises(SkipMessage):
            await middleware.before_process_message(mock_consumer, msg2)
        assert msg2.reject.called

        # Process msg3: should succeed
        result3 = await middleware.before_process_message(mock_consumer, msg3)
        assert result3 is None
        assert not msg3.reject.called

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_ttl_middleware_doesnt_mutate_message(
        self, mock_consumer, mock_broker_message
    ):
        """TtlMiddleware doesn't mutate message on success."""
        middleware = TtlMiddleware()
        current_time = time.time()
        expire_at = current_time + 100.0
        original_headers = {Header.EXPIRE_AT: str(expire_at)}
        mock_broker_message.headers = original_headers.copy()
        original_params = mock_broker_message.body.params.copy()

        await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        # Headers and params should be unchanged
        assert mock_broker_message.headers == original_headers
        assert mock_broker_message.body.params == original_params


# === Layer 10: Property-Based Testing ===


class TestTtlMiddlewarePropertyBased:
    """Property-based tests with hypothesis."""

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    @given(expire_at=valid_expire_at_values())
    async def test_ttl_check_with_random_timestamps(self, expire_at):
        """TTL check handles random expire_at values correctly."""
        from unittest.mock import AsyncMock, MagicMock

        from qanat.consumer import QanatConsumer
        from qanat.message import BrokerMessage
        from qanat.models import JsonRpcRequest

        # Create fresh mocks for each hypothesis example
        mock_consumer = MagicMock(spec=QanatConsumer)
        mock_msg = MagicMock(spec=BrokerMessage)
        mock_msg.headers = {Header.EXPIRE_AT: str(expire_at)}
        mock_msg.body = JsonRpcRequest(
            method="test.method", params={}, id="test-123"
        )
        mock_msg.reject = AsyncMock()

        middleware = TtlMiddleware()
        current_time = time.time()

        if current_time > expire_at:
            # Should reject expired messages
            with pytest.raises(SkipMessage):
                await middleware.before_process_message(mock_consumer, mock_msg)
            mock_msg.reject.assert_called_once()
        else:
            # Should allow non-expired messages
            result = await middleware.before_process_message(
                mock_consumer, mock_msg
            )
            assert result is None
            mock_msg.reject.assert_not_called()


# === Layer 11: Documentation ===


class TestTtlMiddlewareDocumentation:
    """Test TtlMiddleware documentation."""

    def test_ttl_middleware_has_docstring(self):
        """TtlMiddleware class has docstring."""
        assert TtlMiddleware.__doc__ is not None
        assert len(TtlMiddleware.__doc__) > 0

    def test_docstring_mentions_ttl(self):
        """Docstring mentions TTL or expiration."""
        docstring = TtlMiddleware.__doc__.lower()
        assert "ttl" in docstring or "expir" in docstring

    def test_docstring_mentions_reject(self):
        """Docstring mentions message rejection."""
        docstring = TtlMiddleware.__doc__.lower()
        assert "reject" in docstring

    def test_before_process_message_has_docstring(self):
        """before_process_message override has docstring."""
        assert TtlMiddleware.before_process_message.__doc__ is not None

    def test_before_process_docstring_mentions_skip_message(self):
        """before_process_message docstring mentions SkipMessage."""
        docstring = TtlMiddleware.before_process_message.__doc__
        assert "SkipMessage" in docstring


# === Layer 12: Header Enum Usage ===


class TestTtlMiddlewareHeaderEnumUsage:
    """Test that TtlMiddleware uses Header enum correctly."""

    @pytest.mark.asyncio
    @time_machine.travel("2025-01-15 12:00:00", tick=False)
    async def test_uses_header_enum_constant(
        self, mock_consumer, mock_broker_message
    ):
        """TtlMiddleware uses Header.EXPIRE_AT enum."""
        middleware = TtlMiddleware()
        current_time = time.time()

        # Use the actual Header.EXPIRE_AT value
        mock_broker_message.headers = {
            Header.EXPIRE_AT: str(current_time + 100)
        }

        result = await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_header_enum_value_is_x_expire_at(self):
        """Header.EXPIRE_AT has correct string value."""
        assert Header.EXPIRE_AT == "x-expire-at"
