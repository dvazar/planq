"""Comprehensive tests for planq.log module."""

from __future__ import annotations

import logging
from typing import override

import pytest

from planq.context import PlanqContextFilter, get_planq_context
from planq.enums import ExecutionMode
from planq.log import instrument_logging, logger
from planq.message import BrokerMessage
from planq.models import JsonRpcRequest, TaskRoute

# === Test Helper ===


class _TestBrokerMessage(BrokerMessage):
    """Concrete BrokerMessage for testing."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._broker_message_id = "test-msg-id"
        self._delivery_count = 1
        self._reply_to: str | None = None

    @property
    @override
    def broker_message_id(self) -> str:
        return self._broker_message_id

    @broker_message_id.setter
    def broker_message_id(self, value: str):
        self._broker_message_id = value

    @property
    @override
    def delivery_count(self) -> int:
        return self._delivery_count

    @delivery_count.setter
    def delivery_count(self, value: int):
        self._delivery_count = value

    @property
    @override
    def reply_to(self) -> str | None:
        return self._reply_to

    @reply_to.setter
    def reply_to(self, value: str | None):
        self._reply_to = value

    @override
    async def ack(self) -> None:
        pass

    @override
    async def reject(self) -> None:
        pass

    @override
    async def nack(self, delay: float) -> None:
        pass


# === Test Helper ===


@pytest.fixture(autouse=True)
def restore_log_record_factory():
    """Restore original LogRecordFactory after each test."""
    original_factory = logging.getLogRecordFactory()
    yield
    logging.setLogRecordFactory(original_factory)


@pytest.fixture(autouse=True)
def reset_planq_context():
    """Reset PlanqContext before each test to avoid pollution."""
    # Import here to avoid circular dependency
    from planq.context import _planq_context

    # Clear the context before each test
    _planq_context.set(None)
    yield
    # Clear after test as well
    _planq_context.set(None)


# === Layer 1: Module-Level Setup ===


class TestModuleSetup:
    """Test planq.log module setup."""

    def test_logger_has_planq_context_filter(self):
        """Module logger has PlanqContextFilter attached."""
        # Check that at least one filter is a PlanqContextFilter
        has_context_filter = any(
            isinstance(f, PlanqContextFilter) for f in logger.filters
        )

        assert has_context_filter is True

    def test_logger_has_null_handler(self):
        """Module logger has NullHandler to prevent warnings."""
        # Check that at least one handler is a NullHandler
        has_null_handler = any(
            isinstance(h, logging.NullHandler) for h in logger.handlers
        )

        assert has_null_handler is True


# === Layer 2: instrument_logging() Tests ===


class TestInstrumentLogging:
    """Test instrument_logging() function."""

    def test_replaces_log_record_factory(self):
        """instrument_logging() replaces global LogRecordFactory."""
        original_factory = logging.getLogRecordFactory()

        instrument_logging(default_value="TEST")

        new_factory = logging.getLogRecordFactory()

        assert new_factory is not original_factory

    def test_created_records_include_context_fields(self, caplog):
        """Records created after instrumentation include context fields."""
        instrument_logging(default_value="NONE")

        # Set up context
        ctx = get_planq_context()
        ctx.broker_message_id = "test-msg-123"

        # Create a log record
        with caplog.at_level(logging.INFO):
            logger.info("Test message")

        # Verify record has context attribute (will use default_value)
        record = caplog.records[0]
        # The filter will set broker_message_id based on ctx.msg, which is
        # None, so it should use default_value
        assert hasattr(record, "broker_message_id")

    def test_custom_default_value_is_respected(self):
        """instrument_logging() uses custom default_value."""
        instrument_logging(default_value="CUSTOM")

        # Don't set any context (msg is None)
        ctx = get_planq_context()
        ctx.msg = None

        # The custom factory should have been set by instrument_logging
        # It will apply the filter with CUSTOM default
        factory = logging.getLogRecordFactory()
        new_record = factory(
            "test",
            logging.INFO,
            "test.py",
            1,
            "test message",
            (),
            None,
        )

        # All context fields should use the custom default
        assert new_record.broker_message_id == "CUSTOM"
        assert new_record.queue_name == "CUSTOM"
        assert new_record.method == "CUSTOM"

    def test_works_with_existing_logging_configuration(self, caplog):
        """instrument_logging() works with existing logging setup."""
        # Set up some logging configuration
        test_logger = logging.getLogger("test_custom")
        test_logger.setLevel(logging.DEBUG)

        # Instrument logging
        instrument_logging(default_value="-")

        # Create log records
        with caplog.at_level(logging.DEBUG, logger="test_custom"):
            test_logger.debug("Debug message")
            test_logger.info("Info message")

        # Both records should have context fields
        assert len(caplog.records) >= 2
        for record in caplog.records:
            assert hasattr(record, "broker_message_id")
            assert hasattr(record, "queue_name")


# === Layer 3: Integration Tests ===


class TestLoggingIntegration:
    """Test logging integration with PlanqContext."""

    def test_log_outside_handler_uses_default_value(self):
        """Log records outside handler context use default_value."""
        instrument_logging(default_value="OUTSIDE")

        # Don't set up any context
        ctx = get_planq_context()
        ctx.msg = None
        ctx.route = None

        # Create record via factory
        factory = logging.getLogRecordFactory()
        record = factory(
            "test",
            logging.INFO,
            "test.py",
            1,
            "test message",
            (),
            None,
        )

        assert record.broker_message_id == "OUTSIDE"
        assert record.queue_name == "OUTSIDE"
        assert record.method == "OUTSIDE"
        assert record.handler == "OUTSIDE"
        assert record.execution_mode == "OUTSIDE"

    def test_log_with_context_uses_real_values(self):
        """Log records with context use actual values."""
        instrument_logging(default_value="DEFAULT")

        # Set up full context
        ctx = get_planq_context()

        body = JsonRpcRequest(method="test.method", id="req-123")
        msg = _TestBrokerMessage(
            raw=None,
            body=body,
            headers={},
            received_at=1234567890.0,
            queue_name="real-queue",
        )
        msg.broker_message_id = "real-msg-id"
        msg.delivery_count = 3

        def test_handler():
            pass

        route = TaskRoute(
            handler=test_handler,
            mode=ExecutionMode.ASYNC,
            max_retries=5,
            time_limit=60.0,
        )

        ctx.msg = msg
        ctx.route = route
        ctx.max_attempts = 6

        # Create a log record manually
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )

        # Apply filter
        planq_filter = PlanqContextFilter(default_value="DEFAULT")
        planq_filter.filter(record)

        # Verify real values are used
        assert record.queue_name == "real-queue"
        assert record.broker_message_id == "real-msg-id"
        assert record.method == "test.method"
        assert record.correlation_id == "req-123"
        assert record.attempt == 3
        assert record.max_attempts == 6

    def test_all_expected_fields_present(self, caplog):
        """All expected context fields are present on log records."""
        instrument_logging(default_value="-")

        with caplog.at_level(logging.INFO):
            logger.info("Test message")

        record = caplog.records[0]

        # Verify all expected fields exist
        expected_fields = [
            "queue_name",
            "broker_message_id",
            "correlation_id",
            "method",
            "attempt",
            "reply_to",
            "planq_headers",
            "handler",
            "execution_mode",
            "time_limit",
            "max_attempts",
            "broker_latency_sec",
            "internal_latency_sec",
        ]

        for field in expected_fields:
            assert hasattr(record, field), f"Missing field: {field}"

    @pytest.mark.parametrize(
        "log_level,level_name",
        [
            (logging.DEBUG, "DEBUG"),
            (logging.INFO, "INFO"),
            (logging.WARNING, "WARNING"),
            (logging.ERROR, "ERROR"),
        ],
    )
    def test_works_with_different_log_levels(self, log_level, level_name):
        """Context fields are added for all log levels."""
        instrument_logging(default_value="LEVEL")

        # Create record via factory for different levels
        factory = logging.getLogRecordFactory()
        record = factory(
            "test",
            log_level,
            "test.py",
            1,
            f"{level_name} message",
            (),
            None,
        )

        assert record.levelname == level_name
        assert hasattr(record, "broker_message_id")
        assert record.broker_message_id == "LEVEL"
