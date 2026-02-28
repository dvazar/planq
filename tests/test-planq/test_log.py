"""Comprehensive tests for planq.log module."""

from __future__ import annotations

import logging
from collections import ChainMap
from typing import override

import pytest

from planq.context import PlanqContextFilter, get_planq_context
from planq.enums import ExecutionMode
from planq.log import get_planq_logger, instrument_logging, logger
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
    def message_id(self) -> str:
        return self._broker_message_id

    @message_id.setter
    def message_id(self, value: str):
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


# === Fixtures ===


@pytest.fixture(autouse=True)
def restore_log_state():
    """Restore planq log module state after each test."""
    import planq.log as log_module

    original_factory = logging.getLogRecordFactory()
    original_global_filter = log_module._global_filter
    yield
    logging.setLogRecordFactory(original_factory)
    log_module._global_filter = original_global_filter


@pytest.fixture(autouse=True)
def reset_planq_context():
    """Reset PlanqContext before each test to avoid pollution."""
    from planq.context import _planq_context

    _planq_context.set(None)
    yield
    _planq_context.set(None)


# === Layer 1: Module-Level Setup ===


class TestModuleSetup:
    """Test planq.log module setup."""

    def test_factory_not_planq_wrapper_at_import_time(self):
        """Global LogRecordFactory is NOT a planq wrapper at import."""
        factory = logging.getLogRecordFactory()
        # Without instrument_logging(), the factory should be the
        # stdlib default (not from planq.log)
        assert factory.__module__ != "planq.log"

    def test_logger_has_null_handler(self):
        """Module logger has NullHandler to prevent warnings."""
        has_null_handler = any(
            isinstance(h, logging.NullHandler) for h in logger.handlers
        )

        assert has_null_handler is True

    def test_non_planq_logger_not_enriched(self):
        """Non-planq loggers are NOT enriched without instrument_logging."""
        factory = logging.getLogRecordFactory()
        record = factory(
            "myapp.views",
            logging.INFO,
            "test.py",
            1,
            "test message",
            (),
            None,
        )

        assert not hasattr(record, "process_id")
        assert not hasattr(record, "queue_name")


# === Layer 2: get_planq_logger() Tests ===


class TestGetPlanqLogger:
    """Test get_planq_logger() function."""

    def test_returns_logger_with_filter(self):
        """get_planq_logger() returns Logger with PlanqContextFilter."""
        log = get_planq_logger("test.module")

        assert isinstance(log, logging.Logger)
        has_filter = any(isinstance(f, PlanqContextFilter) for f in log.filters)
        assert has_filter is True

    def test_idempotent_no_duplicate_filters(self):
        """Calling get_planq_logger() twice doesn't add duplicate."""
        log1 = get_planq_logger("test.idem")
        log2 = get_planq_logger("test.idem")

        assert log1 is log2
        filter_count = sum(
            1 for f in log1.filters if isinstance(f, PlanqContextFilter)
        )
        assert filter_count == 1

    def test_records_enriched_with_context_fields(self, caplog):
        """Records from get_planq_logger() are enriched."""
        log = get_planq_logger("test.enriched")
        log.setLevel(logging.DEBUG)

        ctx = get_planq_context()
        body = JsonRpcRequest(method="test.method", id="req-1")
        msg = _TestBrokerMessage(
            raw=None,
            body=body,
            headers={},
            received_at=1234567890.0,
            queue_name="test-queue",
        )
        ctx.msg = msg

        with caplog.at_level(logging.INFO, logger="test.enriched"):
            log.info("test message")

        record = caplog.records[0]
        assert hasattr(record, "process_id")
        assert hasattr(record, "thread_id")
        assert record.queue_name == "test-queue"

    def test_uses_global_filter_when_instrumented(self):
        """get_planq_logger() uses _global_filter after instrument_logging()."""
        instrument_logging(default_value="GLOBAL")

        log = get_planq_logger("test.after_instrument")

        # The attached filter should be the global one with default_value
        planq_filters = [
            f for f in log.filters if isinstance(f, PlanqContextFilter)
        ]
        assert len(planq_filters) == 1
        assert planq_filters[0].default_value == "GLOBAL"

    def test_chainmap_resolves_context_fields_in_format(self, caplog):
        """ChainMap enables %(field)s to resolve context attrs."""
        log = get_planq_logger("test.chainmap")
        log.setLevel(logging.DEBUG)

        ctx = get_planq_context()
        body = JsonRpcRequest(method="test.method", id="req-1")
        msg = _TestBrokerMessage(
            raw=None,
            body=body,
            headers={},
            received_at=1234567890.0,
            queue_name="my-queue",
        )
        ctx.msg = msg

        log_ctx = {"event": "test_event"}
        with caplog.at_level(logging.INFO, logger="test.chainmap"):
            log.info(
                "Queue: %(queue_name)s, Event: %(event)s",
                log_ctx,
            )

        record = caplog.records[0]
        assert isinstance(record.args, ChainMap)
        formatted = record.msg % record.args
        assert "Queue: my-queue" in formatted
        assert "Event: test_event" in formatted


# === Layer 3: instrument_logging() Tests ===


class TestInstrumentLogging:
    """Test instrument_logging() function."""

    def test_factory_set_after_call(self):
        """instrument_logging() installs a custom LogRecordFactory."""
        original = logging.getLogRecordFactory()
        instrument_logging()
        new_factory = logging.getLogRecordFactory()

        assert new_factory is not original

    def test_enables_global_enrichment(self):
        """instrument_logging() enables enrichment for all loggers."""
        instrument_logging()

        factory = logging.getLogRecordFactory()
        record = factory(
            "myapp.views",
            logging.INFO,
            "test.py",
            1,
            "test message",
            (),
            None,
        )

        assert hasattr(record, "process_id")
        assert hasattr(record, "thread_id")

    def test_created_records_include_context_fields(self, caplog):
        """Records created after instrumentation include context."""
        instrument_logging()

        with caplog.at_level(logging.INFO):
            logger.info("Test message")

        record = caplog.records[0]
        assert hasattr(record, "process_id")
        assert hasattr(record, "thread_id")

    def test_custom_default_value_is_respected(self):
        """instrument_logging() uses custom default_value."""
        instrument_logging(default_value="CUSTOM")

        ctx = get_planq_context()
        body = JsonRpcRequest(method="test.method", id=None)
        msg = _TestBrokerMessage(
            raw=None,
            body=body,
            headers={},
            received_at=1234567890.0,
            queue_name="test-queue",
        )
        msg.reply_to = None
        ctx.msg = msg

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

        assert new_record.correlation_id == "CUSTOM"
        assert new_record.reply_to == "CUSTOM"

    def test_works_with_existing_logging_configuration(self, caplog):
        """instrument_logging() works with existing logging setup."""
        test_logger = logging.getLogger("test_custom")
        test_logger.setLevel(logging.DEBUG)

        instrument_logging()

        with caplog.at_level(logging.DEBUG, logger="test_custom"):
            test_logger.debug("Debug message")
            test_logger.info("Info message")

        assert len(caplog.records) >= 2
        for record in caplog.records:
            assert hasattr(record, "process_id")
            assert hasattr(record, "thread_id")

    def test_instrument_logging_idempotent(self):
        """Calling instrument_logging() multiple times is safe."""
        instrument_logging(default_value="FIRST")
        instrument_logging(default_value="SECOND")

        ctx = get_planq_context()
        body = JsonRpcRequest(method="test.method", id=None)
        msg = _TestBrokerMessage(
            raw=None,
            body=body,
            headers={},
            received_at=1234567890.0,
            queue_name="test-queue",
        )
        ctx.msg = msg

        factory = logging.getLogRecordFactory()
        record = factory(
            "myapp",
            logging.INFO,
            "test.py",
            1,
            "test message",
            (),
            None,
        )

        # Should use the latest default_value
        assert record.correlation_id == "FIRST"


# === Layer 4: Integration Tests ===


class TestLoggingIntegration:
    """Test logging integration with PlanqContext."""

    def test_log_outside_handler_uses_default_value(self):
        """Log records outside handler context omit msg/route fields."""
        instrument_logging(default_value="OUTSIDE")

        ctx = get_planq_context()
        ctx.msg = None
        ctx.route = None

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

        assert not hasattr(record, "message_id")
        assert not hasattr(record, "queue_name")
        assert not hasattr(record, "method")
        assert not hasattr(record, "handler")
        assert not hasattr(record, "execution_mode")
        # Always-present fields still exist
        assert hasattr(record, "process_id")
        assert hasattr(record, "thread_id")

    def test_log_with_context_uses_real_values(self):
        """Log records with context use actual values."""
        instrument_logging(default_value="DEFAULT")

        ctx = get_planq_context()

        body = JsonRpcRequest(method="test.method", id="req-123")
        msg = _TestBrokerMessage(
            raw=None,
            body=body,
            headers={},
            received_at=1234567890.0,
            queue_name="real-queue",
        )
        msg.message_id = "real-msg-id"
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
        assert record.message_id == "real-msg-id"
        assert record.method == "test.method"
        assert record.correlation_id == "req-123"
        assert record.current_attempt == 3
        assert record.max_attempts == 6

    def test_all_expected_fields_present(self, caplog):
        """All expected context fields are present on log records."""
        instrument_logging()

        ctx = get_planq_context()

        body = JsonRpcRequest(method="test.method", id="req-123")
        msg = _TestBrokerMessage(
            raw=None,
            body=body,
            headers={"x-custom": "value"},
            received_at=1234567890.0,
            queue_name="test-queue",
        )
        msg.message_id = "test-msg-id"
        msg.delivery_count = 2
        msg.reply_to = "reply-queue"

        def dummy_handler():
            pass

        route = TaskRoute(
            handler=dummy_handler,
            mode=ExecutionMode.ASYNC,
            max_retries=3,
            time_limit=30.0,
        )

        ctx.msg = msg
        ctx.route = route
        ctx.max_attempts = 4
        ctx.broker_latency = 1.5
        ctx.internal_latency = 0.25

        with caplog.at_level(logging.INFO):
            logger.info("Test message")

        record = caplog.records[0]

        expected_fields = [
            "process_id",
            "thread_id",
            "queue_name",
            "message_id",
            "correlation_id",
            "method",
            "current_attempt",
            "reply_to",
            "headers",
            "handler",
            "execution_mode",
            "time_limit_seconds",
            "max_attempts",
            "broker_latency_seconds",
            "internal_latency_seconds",
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
        assert hasattr(record, "process_id")
        assert hasattr(record, "thread_id")
