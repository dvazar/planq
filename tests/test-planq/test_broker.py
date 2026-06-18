"""Comprehensive tests for BaseBroker abstract base class."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from hypothesis import given
from hypothesis import strategies as st

from planq.broker import BaseBroker

# === Test Implementation Classes ===


class ConcreteBroker(BaseBroker):
    """Concrete implementation for context manager testing."""

    def __init__(self, dsn: str) -> None:
        """Initialize with tracking flags."""
        super().__init__(dsn)
        self.connect_called = False
        self.disconnect_called = False

    async def connect(self) -> None:
        """Track connect() calls."""
        self.connect_called = True

    async def disconnect(self) -> None:
        """Track disconnect() calls."""
        self.disconnect_called = True


class MinimalBroker(BaseBroker):
    """Minimal implementation without overriding abstract methods."""

    pass


class CustomOnPoisonBroker(BaseBroker):
    """Broker with custom on_poison_message implementation."""

    def __init__(self, dsn: str) -> None:
        """Initialize with tracking flag."""
        super().__init__(dsn)
        self.on_poison_called = False
        self.last_message_id = None
        self.last_raw_body = None
        self.last_queue = None
        self.last_error = None

    async def on_poison_message(
        self,
        message_id: str,
        raw_body: str | bytes,
        queue: str,
        error: Exception,
    ) -> None:
        """Custom implementation that tracks calls."""
        self.on_poison_called = True
        self.last_message_id = message_id
        self.last_raw_body = raw_body
        self.last_queue = queue
        self.last_error = error


# === Layer 1: Explicit Edge Cases ===


class TestBaseBrokerInit:
    """Test BaseBroker constructor and DSN storage."""

    def test_init_with_simple_dsn(self):
        """BaseBroker stores the provided DSN."""
        broker = MinimalBroker("sqs://queue-url")

        assert broker.dsn == "sqs://queue-url"

    def test_init_with_empty_string(self):
        """BaseBroker accepts empty string as DSN."""
        broker = MinimalBroker("")

        assert broker.dsn == ""

    def test_init_with_complex_dsn(self):
        """BaseBroker stores complex connection strings."""
        dsn = (
            "amqp://user:pass@localhost:5672/vhost"
            "?heartbeat=30&connection_timeout=10"
        )
        broker = MinimalBroker(dsn)

        assert broker.dsn == dsn

    def test_init_with_unicode_dsn(self):
        """BaseBroker handles unicode in DSN."""
        dsn = "amqp://пользователь:пароль@localhost/очередь"
        broker = MinimalBroker(dsn)

        assert broker.dsn == dsn
        assert isinstance(broker.dsn, str)

    def test_init_with_special_characters(self):
        """BaseBroker handles special characters in DSN."""
        dsn = "sqs://queue-!@#$%^&*()_+=[]{}|;:,.<>?"
        broker = MinimalBroker(dsn)

        assert broker.dsn == dsn

    def test_multiple_instances_are_independent(self):
        """Multiple BaseBroker instances don't share DSN state."""
        broker1 = MinimalBroker("dsn-one")
        broker2 = MinimalBroker("dsn-two")

        assert broker1.dsn == "dsn-one"
        assert broker2.dsn == "dsn-two"
        assert broker1.dsn != broker2.dsn


class TestGetQueueName:
    """Test get_queue_name method."""

    def test_get_queue_name_returns_stripped_identifier(self):
        """get_queue_name strips whitespace from identifier."""
        broker = MinimalBroker("test-dsn")

        result = broker.get_queue_name("  queue-name  ")

        assert result == "queue-name"

    def test_get_queue_name_with_no_whitespace(self):
        """get_queue_name returns identifier unchanged if no whitespace."""
        broker = MinimalBroker("test-dsn")

        result = broker.get_queue_name("queue-name")

        assert result == "queue-name"

    def test_get_queue_name_with_leading_whitespace(self):
        """get_queue_name strips leading whitespace."""
        broker = MinimalBroker("test-dsn")

        result = broker.get_queue_name("   queue-name")

        assert result == "queue-name"

    def test_get_queue_name_with_trailing_whitespace(self):
        """get_queue_name strips trailing whitespace."""
        broker = MinimalBroker("test-dsn")

        result = broker.get_queue_name("queue-name   ")

        assert result == "queue-name"

    def test_get_queue_name_with_tabs_and_newlines(self):
        """get_queue_name strips tabs and newlines."""
        broker = MinimalBroker("test-dsn")

        result = broker.get_queue_name("\t\nqueue-name\n\t")

        assert result == "queue-name"

    def test_get_queue_name_with_empty_string(self):
        """get_queue_name returns empty string for whitespace-only input."""
        broker = MinimalBroker("test-dsn")

        result = broker.get_queue_name("   ")

        assert result == ""

    def test_get_queue_name_with_url(self):
        """get_queue_name handles queue URLs."""
        broker = MinimalBroker("test-dsn")

        result = broker.get_queue_name(
            "  https://sqs.us-east-1.amazonaws.com/123/queue  "
        )

        assert result == "https://sqs.us-east-1.amazonaws.com/123/queue"

    def test_get_queue_name_with_arn(self):
        """get_queue_name handles ARN identifiers."""
        broker = MinimalBroker("test-dsn")

        result = broker.get_queue_name(
            "  arn:aws:sqs:us-east-1:123456789012:queue-name  "
        )

        assert result == "arn:aws:sqs:us-east-1:123456789012:queue-name"

    def test_get_queue_name_with_unicode_whitespace(self):
        """get_queue_name strips unicode whitespace characters."""
        broker = MinimalBroker("test-dsn")
        # Non-breaking space (U+00A0)
        result = broker.get_queue_name("\u00a0queue\u00a0")

        # strip() should handle unicode whitespace
        assert result == "queue"

    def test_get_queue_name_with_unicode_identifier(self):
        """get_queue_name handles unicode queue names."""
        broker = MinimalBroker("test-dsn")

        result = broker.get_queue_name("  очередь-задач  ")

        assert result == "очередь-задач"

    @pytest.mark.parametrize(
        "identifier,expected",
        [
            ("simple-queue", "simple-queue"),
            ("  spaces  ", "spaces"),
            ("\ttabs\t", "tabs"),
            ("\nnewlines\n", "newlines"),
            ("  \t\n  mixed  \n\t  ", "mixed"),
            ("", ""),
            ("   ", ""),
        ],
    )
    def test_get_queue_name_with_various_inputs(self, identifier, expected):
        """get_queue_name handles various whitespace combinations."""
        broker = MinimalBroker("test-dsn")

        result = broker.get_queue_name(identifier)

        assert result == expected


class TestBaseBrokerAbstractMethods:
    """Test that abstract methods raise NotImplementedError."""

    @pytest.mark.asyncio
    async def test_connect_not_implemented(self):
        """BaseBroker.connect() raises NotImplementedError."""
        broker = MinimalBroker("test-dsn")

        with pytest.raises(NotImplementedError):
            await broker.connect()

    @pytest.mark.asyncio
    async def test_connect_exception_type(self):
        """BaseBroker.connect() raises exactly NotImplementedError."""
        broker = MinimalBroker("test-dsn")

        with pytest.raises(NotImplementedError) as exc_info:
            await broker.connect()

        assert type(exc_info.value) is NotImplementedError

    @pytest.mark.asyncio
    async def test_disconnect_not_implemented(self):
        """BaseBroker.disconnect() raises NotImplementedError."""
        broker = MinimalBroker("test-dsn")

        with pytest.raises(NotImplementedError):
            await broker.disconnect()

    @pytest.mark.asyncio
    async def test_disconnect_exception_type(self):
        """BaseBroker.disconnect() raises exactly NotImplementedError."""
        broker = MinimalBroker("test-dsn")

        with pytest.raises(NotImplementedError) as exc_info:
            await broker.disconnect()

        assert type(exc_info.value) is NotImplementedError

    @pytest.mark.asyncio
    async def test_get_queue_depth_not_implemented(self):
        """BaseBroker.get_queue_depth() raises NotImplementedError."""
        broker = MinimalBroker("test-dsn")

        with pytest.raises(NotImplementedError):
            await broker.get_queue_depth("q")

    @pytest.mark.asyncio
    async def test_publish_not_implemented(self, json_rpc_notification):
        """BaseBroker.publish() raises NotImplementedError."""
        broker = MinimalBroker("test-dsn")

        with pytest.raises(NotImplementedError):
            await broker.publish("queue-name", json_rpc_notification)

    @pytest.mark.asyncio
    async def test_publish_with_all_params_not_implemented(
        self,
        json_rpc_request_string_id,
    ):
        """BaseBroker.publish() with all parameters raises."""
        broker = MinimalBroker("test-dsn")
        headers = {"x-custom": "value"}

        with pytest.raises(NotImplementedError):
            await broker.publish(
                queue="queue-name",
                rpc=json_rpc_request_string_id,
                delay=30.0,
                expire_at=1234567890.5,
                reply_to="reply-queue",
                headers=headers,
            )

    @pytest.mark.asyncio
    async def test_publish_with_only_delay(self, json_rpc_notification):
        """BaseBroker.publish() with only delay parameter."""
        broker = MinimalBroker("test-dsn")

        with pytest.raises(NotImplementedError):
            await broker.publish("queue", json_rpc_notification, delay=10.0)

    @pytest.mark.asyncio
    async def test_publish_with_only_expire_at(self, json_rpc_notification):
        """BaseBroker.publish() with only expire_at parameter."""
        broker = MinimalBroker("test-dsn")

        with pytest.raises(NotImplementedError):
            await broker.publish(
                "queue", json_rpc_notification, expire_at=1234567890.0
            )

    @pytest.mark.asyncio
    async def test_publish_with_only_reply_to(self, json_rpc_request_string_id):
        """BaseBroker.publish() with only reply_to parameter."""
        broker = MinimalBroker("test-dsn")

        with pytest.raises(NotImplementedError):
            await broker.publish(
                "queue", json_rpc_request_string_id, reply_to="reply-queue"
            )

    @pytest.mark.asyncio
    async def test_publish_with_only_headers(self, json_rpc_notification):
        """BaseBroker.publish() with only headers parameter."""
        broker = MinimalBroker("test-dsn")

        with pytest.raises(NotImplementedError):
            await broker.publish(
                "queue", json_rpc_notification, headers={"x-key": "value"}
            )

    @pytest.mark.asyncio
    async def test_consume_not_implemented(self):
        """BaseBroker.consume() raises NotImplementedError."""
        broker = MinimalBroker("test-dsn")

        with pytest.raises(NotImplementedError):
            async for _ in broker.consume("queue-name"):
                pass

    @pytest.mark.asyncio
    async def test_consume_is_async_generator(self):
        """BaseBroker.consume() returns async iterator."""
        broker = MinimalBroker("test-dsn")

        # consume() should return an async generator
        result = broker.consume("queue-name")

        # Check that it's an async iterator
        assert hasattr(result, "__aiter__")
        assert hasattr(result, "__anext__")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("prefetch", [1, 10, 100, 1000])
    async def test_consume_with_various_prefetch(self, prefetch):
        """BaseBroker.consume() with different prefetch values."""
        broker = MinimalBroker("test-dsn")

        with pytest.raises(NotImplementedError):
            async for _ in broker.consume("queue-name", prefetch=prefetch):
                pass


class TestOnPoisonMessage:
    """Test on_poison_message default implementation."""

    @pytest.mark.asyncio
    async def test_on_poison_message_logs_error(self):
        """on_poison_message logs error via logger.error()."""
        broker = MinimalBroker("test-dsn")
        raw_body = "invalid json"
        queue = "test-queue"
        error = ValueError("JSON decode failed")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            mock_logger.error.assert_called_once()
            args, kwargs = mock_logger.error.call_args
            assert "Poison message" in args[0]
            assert "%(queue_name)r" in args[0]
            assert kwargs["exc_info"] is error

    @pytest.mark.asyncio
    async def test_on_poison_message_includes_queue_name(self):
        """on_poison_message includes queue name in log_ctx."""
        broker = MinimalBroker("test-dsn")
        raw_body = "test"
        queue = "my-queue"
        error = ValueError("test")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            args, kwargs = mock_logger.error.call_args
            log_ctx = args[1]
            assert log_ctx["queue_name"] == "my-queue"

    @pytest.mark.asyncio
    async def test_on_poison_message_calls_get_queue_name(self):
        """on_poison_message uses get_queue_name for queue."""
        broker = MinimalBroker("test-dsn")
        raw_body = "test"
        queue = "  my-queue  "  # With whitespace
        error = ValueError("test")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            args, kwargs = mock_logger.error.call_args
            log_ctx = args[1]
            # Should be stripped via get_queue_name
            assert log_ctx["queue_name"] == "my-queue"

    @pytest.mark.asyncio
    async def test_on_poison_message_truncates_long_body(self):
        """on_poison_message truncates body longer than 1000 chars."""
        broker = MinimalBroker("test-dsn")
        raw_body = "x" * 1500  # 1500 characters
        queue = "test-queue"
        error = ValueError("test error")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            _, kwargs = mock_logger.error.call_args
            extra = kwargs["extra"]
            snippet = extra["raw_body_snippet"]
            assert snippet.startswith("x" * 1000)
            assert "truncated, total size: 1500 bytes" in snippet
            assert extra["body_size"] == 1500
            assert extra["is_truncated"] is True

    @pytest.mark.asyncio
    async def test_on_poison_message_does_not_truncate_short_body(self):
        """on_poison_message preserves body of 1000 chars or less."""
        broker = MinimalBroker("test-dsn")
        raw_body = "x" * 500  # 500 characters
        queue = "test-queue"
        error = ValueError("test error")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            _, kwargs = mock_logger.error.call_args
            extra = kwargs["extra"]
            snippet = extra["raw_body_snippet"]
            assert snippet == raw_body
            assert "truncated" not in snippet
            assert extra["body_size"] == 500
            assert extra["is_truncated"] is False

    @pytest.mark.asyncio
    async def test_on_poison_message_exactly_1000_chars(self):
        """on_poison_message at boundary (1000 chars) is not truncated."""
        broker = MinimalBroker("test-dsn")
        raw_body = "x" * 1000  # Exactly 1000 characters
        queue = "test-queue"
        error = ValueError("test error")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            _, kwargs = mock_logger.error.call_args
            extra = kwargs["extra"]
            snippet = extra["raw_body_snippet"]
            assert snippet == raw_body
            assert "truncated" not in snippet
            assert extra["body_size"] == 1000
            assert extra["is_truncated"] is False

    @pytest.mark.asyncio
    async def test_on_poison_message_exactly_1001_chars(self):
        """on_poison_message truncates at 1001 chars (first over limit)."""
        broker = MinimalBroker("test-dsn")
        raw_body = "x" * 1001  # 1001 characters
        queue = "test-queue"
        error = ValueError("test error")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            _, kwargs = mock_logger.error.call_args
            extra = kwargs["extra"]
            snippet = extra["raw_body_snippet"]
            assert snippet.startswith("x" * 1000)
            assert "truncated, total size: 1001 bytes" in snippet
            assert extra["body_size"] == 1001
            assert extra["is_truncated"] is True

    @pytest.mark.asyncio
    async def test_on_poison_message_with_bytes_body(self):
        """on_poison_message handles bytes raw_body."""
        broker = MinimalBroker("test-dsn")
        raw_body = b"invalid json bytes"
        queue = "test-queue"
        error = ValueError("test error")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            mock_logger.error.assert_called_once()
            _, kwargs = mock_logger.error.call_args
            extra = kwargs["extra"]
            # Should be decoded to string
            assert isinstance(extra["raw_body_snippet"], str)
            assert extra["raw_body_snippet"] == "invalid json bytes"
            assert extra["body_size"] == 18  # len(b"invalid json bytes")

    @pytest.mark.asyncio
    async def test_on_poison_message_with_bytes_truncation(self):
        """on_poison_message truncates and decodes long bytes."""
        broker = MinimalBroker("test-dsn")
        raw_body = b"x" * 1500
        queue = "test-queue"
        error = ValueError("test error")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            _, kwargs = mock_logger.error.call_args
            extra = kwargs["extra"]
            snippet = extra["raw_body_snippet"]
            assert isinstance(snippet, str)
            assert snippet.startswith("x" * 1000)
            assert "truncated, total size: 1500 bytes" in snippet
            assert extra["body_size"] == 1500

    @pytest.mark.asyncio
    async def test_on_poison_message_with_invalid_utf8_bytes(self):
        """on_poison_message handles invalid UTF-8 bytes gracefully."""
        broker = MinimalBroker("test-dsn")
        # Invalid UTF-8 sequence
        raw_body = b"\xff\xfe invalid utf8"
        queue = "test-queue"
        error = ValueError("test error")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            mock_logger.error.assert_called_once()
            _, kwargs = mock_logger.error.call_args
            extra = kwargs["extra"]
            # Should use replacement character for invalid bytes
            assert isinstance(extra["raw_body_snippet"], str)
            # The exact replacement depends on decode(errors="replace")
            assert "invalid utf8" in extra["raw_body_snippet"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error",
        [
            ValueError("json error"),
            TypeError("type mismatch"),
            RuntimeError("runtime failure"),
            Exception("generic error"),
        ],
    )
    async def test_on_poison_message_with_different_exceptions(self, error):
        """on_poison_message handles different exception types."""
        broker = MinimalBroker("test-dsn")
        raw_body = "test body"
        queue = "test-queue"

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            _, kwargs = mock_logger.error.call_args
            assert kwargs["exc_info"] is error

    @pytest.mark.asyncio
    async def test_on_poison_message_with_empty_string_body(self):
        """on_poison_message handles empty string raw body."""
        broker = MinimalBroker("test-dsn")
        raw_body = ""
        queue = "test-queue"
        error = ValueError("empty message")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            mock_logger.error.assert_called_once()
            _, kwargs = mock_logger.error.call_args
            extra = kwargs["extra"]
            assert extra["raw_body_snippet"] == ""
            assert extra["body_size"] == 0
            assert extra["is_truncated"] is False

    @pytest.mark.asyncio
    async def test_on_poison_message_with_empty_bytes_body(self):
        """on_poison_message handles empty bytes raw body."""
        broker = MinimalBroker("test-dsn")
        raw_body = b""
        queue = "test-queue"
        error = ValueError("empty message")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            mock_logger.error.assert_called_once()
            _, kwargs = mock_logger.error.call_args
            extra = kwargs["extra"]
            assert extra["raw_body_snippet"] == ""
            assert extra["body_size"] == 0
            assert extra["is_truncated"] is False

    @pytest.mark.asyncio
    async def test_on_poison_message_with_unicode_string(self):
        """on_poison_message handles unicode strings correctly."""
        broker = MinimalBroker("test-dsn")
        raw_body = "Привет мир! 你好世界! 🌍"
        queue = "test-queue"
        error = ValueError("test")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            _, kwargs = mock_logger.error.call_args
            extra = kwargs["extra"]
            assert extra["raw_body_snippet"] == raw_body
            assert extra["body_size"] == len(raw_body)

    @pytest.mark.asyncio
    async def test_on_poison_message_with_emoji_truncation(self):
        """on_poison_message truncates strings with emoji correctly."""
        broker = MinimalBroker("test-dsn")
        # Create string with emoji that goes over limit
        raw_body = "😀" * 600  # Each emoji is 1 char but multiple bytes
        queue = "test-queue"
        error = ValueError("test")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            _, kwargs = mock_logger.error.call_args
            extra = kwargs["extra"]
            # Should truncate at 1000 characters, not bytes
            assert extra["body_size"] == 600  # Character count
            assert extra["is_truncated"] is False

    @pytest.mark.asyncio
    async def test_on_poison_message_with_emoji_over_limit(self):
        """on_poison_message with emoji string over 1000 chars."""
        broker = MinimalBroker("test-dsn")
        raw_body = "😀" * 1500  # 1500 emoji characters
        queue = "test-queue"
        error = ValueError("test")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            _, kwargs = mock_logger.error.call_args
            extra = kwargs["extra"]
            snippet = extra["raw_body_snippet"]
            assert snippet.startswith("😀" * 1000)
            assert "truncated, total size: 1500 bytes" in snippet
            assert extra["body_size"] == 1500
            assert extra["is_truncated"] is True

    @pytest.mark.asyncio
    async def test_on_poison_message_bytes_truncated_mid_utf8(self):
        """on_poison_message handles truncation in middle of UTF-8."""
        broker = MinimalBroker("test-dsn")
        # Create bytes that will be truncated in middle of multi-byte char
        emoji = "😀"  # 4 bytes per emoji
        raw_body = (emoji * 300).encode("utf-8")  # 1200 bytes total
        queue = "test-queue"
        error = ValueError("test")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            _, kwargs = mock_logger.error.call_args
            extra = kwargs["extra"]
            snippet = extra["raw_body_snippet"]
            # Should decode with replacement character for partial sequence
            assert isinstance(snippet, str)
            assert "truncated, total size: 1200 bytes" in snippet
            # Verify it was decoded (may contain replacement char)
            assert len(snippet) > 0

    @pytest.mark.asyncio
    async def test_on_poison_message_bytes_with_null_bytes(self):
        """on_poison_message handles bytes with null characters."""
        broker = MinimalBroker("test-dsn")
        raw_body = b"data\x00with\x00nulls"
        queue = "test-queue"
        error = ValueError("test")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            mock_logger.error.assert_called_once()
            _, kwargs = mock_logger.error.call_args
            extra = kwargs["extra"]
            # Should decode null bytes as unicode null
            assert isinstance(extra["raw_body_snippet"], str)
            assert extra["body_size"] == 15

    @pytest.mark.asyncio
    async def test_on_poison_message_string_with_control_chars(self):
        """on_poison_message handles control characters in strings."""
        broker = MinimalBroker("test-dsn")
        raw_body = "line1\nline2\rline3\tline4"
        queue = "test-queue"
        error = ValueError("test")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            _, kwargs = mock_logger.error.call_args
            extra = kwargs["extra"]
            # Should preserve control characters
            assert extra["raw_body_snippet"] == raw_body

    @pytest.mark.asyncio
    async def test_on_poison_message_truncation_message_format(self):
        """on_poison_message uses correct truncation message format."""
        broker = MinimalBroker("test-dsn")
        raw_body = "x" * 1500
        queue = "test-queue"
        error = ValueError("test")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            _, kwargs = mock_logger.error.call_args
            extra = kwargs["extra"]
            snippet = extra["raw_body_snippet"]
            # Verify exact format
            expected_suffix = "... [truncated, total size: 1500 bytes]"
            assert snippet.endswith(expected_suffix)
            assert snippet == ("x" * 1000) + expected_suffix

    @pytest.mark.asyncio
    async def test_on_poison_message_extra_context_structure(self):
        """on_poison_message provides structured extra context."""
        broker = MinimalBroker("test-dsn")
        raw_body = "test body"
        queue = "test-queue"
        error = ValueError("test")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            args, kwargs = mock_logger.error.call_args
            log_ctx = args[1]
            extra = kwargs["extra"]
            # log_ctx contains message_id & queue_name
            assert isinstance(log_ctx["message_id"], str)
            assert isinstance(log_ctx["queue_name"], str)
            # Non-filter keys preserved in extra
            assert "body_size" in extra
            assert "is_truncated" in extra
            assert "raw_body_snippet" in extra
            # Verify types
            assert isinstance(extra["body_size"], int)
            assert isinstance(extra["is_truncated"], bool)
            assert isinstance(extra["raw_body_snippet"], str)

    @pytest.mark.asyncio
    async def test_on_poison_message_logs_message_id(self):
        """on_poison_message includes message_id in log_ctx."""
        broker = MinimalBroker("test-dsn")
        message_id = "poison-msg-12345"
        raw_body = "test body"
        queue = "test-queue"
        error = ValueError("test")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(message_id, raw_body, queue, error)

            args, kwargs = mock_logger.error.call_args
            log_ctx = args[1]
            assert log_ctx["message_id"] == "poison-msg-12345"


class TestBaseBrokerContextManager:
    """Test async context manager protocol."""

    @pytest.mark.asyncio
    async def test_aenter_calls_connect(self):
        """__aenter__ calls connect()."""
        broker = ConcreteBroker("test-dsn")

        assert not broker.connect_called

        await broker.__aenter__()

        assert broker.connect_called

    @pytest.mark.asyncio
    async def test_aenter_returns_self(self):
        """__aenter__ returns self."""
        broker = ConcreteBroker("test-dsn")

        result = await broker.__aenter__()

        assert result is broker

    @pytest.mark.asyncio
    async def test_aexit_calls_disconnect(self):
        """__aexit__ calls disconnect()."""
        broker = ConcreteBroker("test-dsn")
        await broker.__aenter__()

        assert not broker.disconnect_called

        await broker.__aexit__(None, None, None)

        assert broker.disconnect_called

    @pytest.mark.asyncio
    async def test_aexit_with_exception_info(self):
        """__aexit__ calls disconnect even with exception info."""
        broker = ConcreteBroker("test-dsn")
        await broker.__aenter__()

        exc_type = ValueError
        exc_val = ValueError("test error")
        exc_tb = None

        await broker.__aexit__(exc_type, exc_val, exc_tb)

        assert broker.disconnect_called

    @pytest.mark.asyncio
    async def test_aexit_returns_none(self):
        """__aexit__ returns None (does not suppress exceptions)."""
        broker = ConcreteBroker("test-dsn")
        await broker.__aenter__()

        result = await broker.__aexit__(None, None, None)

        # Should return None, allowing exceptions to propagate
        assert result is None

    @pytest.mark.asyncio
    async def test_aexit_does_not_suppress_exceptions(self):
        """__aexit__ does not suppress exceptions."""
        broker = ConcreteBroker("test-dsn")

        # Exception should propagate
        with pytest.raises(ValueError, match="test error"):
            async with broker:
                raise ValueError("test error")

        # But disconnect should still be called
        assert broker.disconnect_called

    @pytest.mark.asyncio
    async def test_context_manager_full_flow(self):
        """Full context manager flow connects and disconnects."""
        broker = ConcreteBroker("test-dsn")

        assert not broker.connect_called
        assert not broker.disconnect_called

        async with broker:
            assert broker.connect_called
            assert not broker.disconnect_called

        assert broker.connect_called
        assert broker.disconnect_called

    @pytest.mark.asyncio
    async def test_context_manager_disconnect_on_exception(self):
        """Context manager disconnects even when exception occurs."""
        broker = ConcreteBroker("test-dsn")

        try:
            async with broker:
                assert broker.connect_called
                raise ValueError("test error")
        except ValueError:
            pass

        assert broker.disconnect_called

    @pytest.mark.asyncio
    async def test_multiple_context_manager_entries(self):
        """Broker can be used as context manager multiple times."""
        broker = ConcreteBroker("test-dsn")

        # First use
        async with broker:
            pass
        assert broker.connect_called
        assert broker.disconnect_called

        # Reset flags
        broker.connect_called = False
        broker.disconnect_called = False

        # Second use
        async with broker:
            pass
        assert broker.connect_called
        assert broker.disconnect_called


class TestBaseBrokerSubclassing:
    """Test subclassing behavior and method overriding."""

    @pytest.mark.asyncio
    async def test_subclass_can_override_connect(self):
        """Subclass can override connect() method."""
        broker = ConcreteBroker("test-dsn")

        await broker.connect()

        assert broker.connect_called

    @pytest.mark.asyncio
    async def test_subclass_can_override_disconnect(self):
        """Subclass can override disconnect() method."""
        broker = ConcreteBroker("test-dsn")

        await broker.disconnect()

        assert broker.disconnect_called

    def test_subclass_can_override_get_queue_name(self):
        """Subclass can override get_queue_name() method."""

        class CustomBroker(BaseBroker):
            def get_queue_name(self, identifier: str) -> str:
                return f"custom-{identifier.strip()}"

        broker = CustomBroker("test-dsn")

        result = broker.get_queue_name("  queue  ")

        assert result == "custom-queue"

    @pytest.mark.asyncio
    async def test_subclass_inherits_on_poison_message(self):
        """Subclass inherits default on_poison_message implementation."""
        broker = MinimalBroker("test-dsn")
        raw_body = "test body"
        queue = "test-queue"
        error = ValueError("test error")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            # Should use inherited implementation
            mock_logger.error.assert_called_once()

    @pytest.mark.asyncio
    async def test_subclass_can_override_on_poison_message(self):
        """Subclass can override on_poison_message implementation."""
        broker = CustomOnPoisonBroker("test-dsn")
        raw_body = "test body"
        queue = "test-queue"
        error = ValueError("test error")

        assert not broker.on_poison_called

        await broker.on_poison_message("test-msg-id", raw_body, queue, error)

        assert broker.on_poison_called
        assert broker.last_message_id == "test-msg-id"
        assert broker.last_raw_body == raw_body
        assert broker.last_queue == queue
        assert broker.last_error is error


# === Layer 2: Property-Based Tests ===


@pytest.mark.hypothesis
class TestBaseBrokerPropertyBased:
    """Property-based tests for BaseBroker with generated inputs."""

    @given(st.text(min_size=0, max_size=1000))
    def test_dsn_storage_property(self, dsn: str):
        """Any string DSN is stored correctly."""
        broker = MinimalBroker(dsn)

        assert broker.dsn == dsn
        assert isinstance(broker.dsn, str)

    @given(st.text(min_size=0, max_size=2000))
    def test_get_queue_name_always_strips(self, identifier: str):
        """get_queue_name always returns stripped identifier."""
        broker = MinimalBroker("test-dsn")

        result = broker.get_queue_name(identifier)

        assert result == identifier.strip()
        # No leading/trailing whitespace
        assert result == result.strip()

    @given(
        st.text(min_size=1, max_size=50),
        st.one_of(
            st.text(min_size=0, max_size=2000),
            st.binary(min_size=0, max_size=2000),
        ),
        st.text(min_size=1, max_size=100),
        st.one_of(
            st.builds(ValueError),
            st.builds(TypeError),
            st.builds(RuntimeError),
            st.builds(KeyError),
            st.builds(Exception, st.text()),
        ),
    )
    @pytest.mark.asyncio
    async def test_on_poison_message_handles_any_input(
        self,
        message_id: str,
        raw_body: str | bytes,
        queue: str,
        error: Exception,
    ):
        """on_poison_message handles any error/body/queue combination."""
        broker = MinimalBroker("test-dsn")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(message_id, raw_body, queue, error)

            # Should always log
            mock_logger.error.assert_called_once()
            args, kwargs = mock_logger.error.call_args

            # Verify exc_info is the error
            assert kwargs["exc_info"] is error

            # Verify context structure
            log_ctx = args[1]
            extra = kwargs["extra"]
            # log_ctx contains message_id & queue_name
            assert isinstance(log_ctx["queue_name"], str)
            assert isinstance(log_ctx["message_id"], str)
            assert isinstance(extra["body_size"], int)
            assert isinstance(extra["is_truncated"], bool)
            assert isinstance(extra["raw_body_snippet"], str)

            # Verify truncation logic
            body_size = len(raw_body)
            if body_size > 1000:
                assert extra["is_truncated"] is True
                assert "truncated" in extra["raw_body_snippet"]
            else:
                assert extra["is_truncated"] is False

    @given(st.integers(min_value=0, max_value=2500))
    @pytest.mark.asyncio
    async def test_truncation_boundary_property(self, length: int):
        """Truncation behavior is consistent across all lengths."""
        broker = MinimalBroker("test-dsn")
        raw_body = "x" * length
        queue = "test-queue"
        error = ValueError("test")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            _, kwargs = mock_logger.error.call_args
            extra = kwargs["extra"]
            snippet = extra["raw_body_snippet"]

            # Verify truncation invariant
            if length > 1000:
                assert snippet.startswith("x" * 1000)
                assert f"truncated, total size: {length} bytes" in snippet
                assert extra["is_truncated"] is True
            else:
                assert snippet == raw_body
                assert "truncated" not in snippet
                assert extra["is_truncated"] is False
            assert extra["body_size"] == length

    @given(st.integers(min_value=0, max_value=2500))
    @pytest.mark.asyncio
    async def test_truncation_boundary_property_bytes(self, length: int):
        """Truncation behavior with bytes is consistent."""
        broker = MinimalBroker("test-dsn")
        raw_body = b"x" * length
        queue = "test-queue"
        error = ValueError("test")

        with patch("planq.broker.logger") as mock_logger:
            await broker.on_poison_message(
                "test-msg-id", raw_body, queue, error
            )

            _, kwargs = mock_logger.error.call_args
            extra = kwargs["extra"]
            snippet = extra["raw_body_snippet"]

            # Should be decoded to string
            assert isinstance(snippet, str)

            # Verify truncation invariant
            if length > 1000:
                assert snippet.startswith("x" * 1000)
                assert f"truncated, total size: {length} bytes" in snippet
                assert extra["is_truncated"] is True
            else:
                assert snippet == "x" * length
                assert "truncated" not in snippet
                assert extra["is_truncated"] is False
            assert extra["body_size"] == length
