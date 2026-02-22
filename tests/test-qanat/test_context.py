"""Comprehensive tests for QanatContext and related functionality."""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import override

import pytest

from qanat import types as qanat_types
from qanat.context import QanatContext, QanatContextFilter, get_qanat_context
from qanat.enums import ExecutionMode
from qanat.exceptions import HandlerTimeout
from qanat.message import BrokerMessage
from qanat.models import JsonRpcRequest, TaskRoute

# Rebuild models with proper type namespace
JsonRpcRequest.model_rebuild(_types_namespace=qanat_types.__dict__)
TaskRoute.model_rebuild(
    _types_namespace={**qanat_types.__dict__, "ExecutionMode": ExecutionMode}
)


# === Test Helper: Concrete BrokerMessage Implementation ===


class _TestBrokerMessage(BrokerMessage):
    """Concrete BrokerMessage implementation for testing."""

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


# === Layer 1: QanatContext Tests ===


class TestQanatContextConstruction:
    """Test QanatContext construction and default values."""

    def test_construction_with_defaults(self):
        """QanatContext initializes all fields to None."""
        ctx = QanatContext()

        assert ctx.broker_message_id is None
        assert ctx.msg is None
        assert ctx.route is None
        assert ctx.max_attempts is None
        assert ctx.broker_latency is None
        assert ctx.internal_latency is None

    def test_is_cancelled_initially_false(self):
        """is_cancelled property is False initially."""
        ctx = QanatContext()

        assert ctx.is_cancelled is False

    def test_attribute_assignment_and_retrieval(self):
        """QanatContext stores assigned attribute values."""
        ctx = QanatContext()

        ctx.broker_message_id = "msg-123"
        ctx.max_attempts = 3
        ctx.broker_latency = 1.5
        ctx.internal_latency = 0.25

        assert ctx.broker_message_id == "msg-123"
        assert ctx.max_attempts == 3
        assert ctx.broker_latency == 1.5
        assert ctx.internal_latency == 0.25


class TestQanatContextCancellation:
    """Test QanatContext cancellation primitives."""

    def test_cancel_sets_event(self):
        """cancel() method sets internal event."""
        ctx = QanatContext()

        assert ctx.is_cancelled is False

        ctx.cancel()

        assert ctx.is_cancelled is True

    def test_check_cancellation_raises_when_cancelled(self):
        """check_cancellation() raises HandlerTimeout when cancelled."""
        ctx = QanatContext()
        ctx.cancel()

        with pytest.raises(HandlerTimeout):
            ctx.check_cancellation()

    def test_check_cancellation_noop_when_not_cancelled(self):
        """check_cancellation() is no-op when not cancelled."""
        ctx = QanatContext()

        # Should not raise
        ctx.check_cancellation()

        assert ctx.is_cancelled is False

    def test_multiple_cancel_calls_are_safe(self):
        """Calling cancel() multiple times is safe."""
        ctx = QanatContext()

        ctx.cancel()
        ctx.cancel()
        ctx.cancel()

        assert ctx.is_cancelled is True


# === Layer 2: get_qanat_context() Tests ===


class TestGetQanatContext:
    """Test get_qanat_context() function behavior."""

    def test_returns_new_context_when_not_set(self):
        """get_qanat_context() returns new QanatContext when not set."""
        # Note: This test may fail if context is already set from previous
        # tests. In practice, contextvars are isolated per test.
        ctx = get_qanat_context()

        assert isinstance(ctx, QanatContext)
        assert ctx.broker_message_id is None

    def test_returns_existing_context_in_same_scope(self):
        """get_qanat_context() returns same context within scope."""
        ctx1 = get_qanat_context()
        ctx1.broker_message_id = "test-id"

        ctx2 = get_qanat_context()

        assert ctx1 is ctx2
        assert ctx2.broker_message_id == "test-id"

    @pytest.mark.asyncio
    async def test_context_shared_across_async_tasks(self):
        """Async tasks created with create_task share parent context.

        This is expected Python behavior: ContextVars inherit from the
        parent task but modifications in child tasks don't affect parent.
        """
        # Set context in parent
        parent_ctx = get_qanat_context()
        parent_ctx.broker_message_id = "parent-id"

        async def child_task():
            # Child sees parent's context
            ctx = get_qanat_context()
            return ctx is parent_ctx

        task = asyncio.create_task(child_task())
        same_context = await task

        # Child task sees the same context object
        assert same_context is True

    def test_isolation_across_threads(self):
        """Different threads see different QanatContext instances."""
        results = {}

        def thread_func(name: str):
            ctx = get_qanat_context()
            ctx.broker_message_id = f"{name}-id"
            results[name] = ctx

        thread_a = threading.Thread(target=thread_func, args=("thread-a",))
        thread_b = threading.Thread(target=thread_func, args=("thread-b",))

        thread_a.start()
        thread_b.start()

        thread_a.join()
        thread_b.join()

        assert results["thread-a"].broker_message_id == "thread-a-id"
        assert results["thread-b"].broker_message_id == "thread-b-id"
        assert results["thread-a"] is not results["thread-b"]


# === Layer 3: QanatContextFilter Tests ===


class TestQanatContextFilterConstruction:
    """Test QanatContextFilter construction."""

    def test_construction_with_default_value(self):
        """QanatContextFilter stores default_value parameter."""
        filter_instance = QanatContextFilter(default_value="NONE")

        assert filter_instance.default_value == "NONE"

    def test_construction_with_default_default(self):
        """QanatContextFilter uses '-' as default_value by default."""
        filter_instance = QanatContextFilter()

        assert filter_instance.default_value == "-"


class TestQanatContextFilterWithFullContext:
    """Test QanatContextFilter with fully populated context."""

    def test_filter_with_full_context(self):
        """filter() populates all record attributes from full context."""
        # Create context with all fields populated
        ctx = get_qanat_context()

        # Create a test message
        body = JsonRpcRequest(
            method="test.method", params={"key": "value"}, id="req-123"
        )
        msg = _TestBrokerMessage(
            raw={"native": "data"},
            body=body,
            headers={"x-custom": "header-value"},
            received_at=1234567890.0,
            queue_name="test-queue",
        )
        msg.broker_message_id = "broker-msg-123"
        msg.delivery_count = 2
        msg.reply_to = "reply-queue"

        # Create a mock route
        def dummy_handler():
            pass

        route = TaskRoute(
            handler=dummy_handler,
            mode=ExecutionMode.ASYNC,
            max_retries=3,
            time_limit=30.0,
        )

        # Populate context
        ctx.broker_message_id = msg.broker_message_id
        ctx.msg = msg
        ctx.route = route
        ctx.max_attempts = 4
        ctx.broker_latency = 1.5
        ctx.internal_latency = 0.25

        # Create filter and apply to record
        filter_instance = QanatContextFilter(default_value="N/A")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )

        result = filter_instance.filter(record)

        # Verify all attributes are set
        assert result is True
        assert record.queue_name == "test-queue"
        assert record.broker_message_id == "broker-msg-123"
        assert record.correlation_id == "req-123"
        assert record.method == "test.method"
        assert record.attempt == 2
        assert record.reply_to == "reply-queue"
        assert record.qanat_headers == {"x-custom": "header-value"}
        # Handler qualname includes the enclosing method
        assert record.handler.endswith("dummy_handler")
        assert record.execution_mode == "async"
        assert record.time_limit == 30.0
        assert record.max_attempts == 4
        assert record.broker_latency_sec == 1.5
        assert record.internal_latency_sec == 0.25


class TestQanatContextFilterWithPartialContext:
    """Test QanatContextFilter with partially populated context."""

    def test_filter_with_no_message(self):
        """filter() uses default_value when msg is None."""
        ctx = get_qanat_context()
        ctx.msg = None

        filter_instance = QanatContextFilter(default_value="MISSING")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )

        filter_instance.filter(record)

        assert record.queue_name == "MISSING"
        assert record.broker_message_id == "MISSING"
        assert record.correlation_id == "MISSING"
        assert record.method == "MISSING"
        assert record.attempt == "MISSING"
        assert record.reply_to == "MISSING"
        assert record.qanat_headers == {}

    def test_filter_with_no_route(self):
        """filter() uses default_value when route is None."""
        ctx = get_qanat_context()
        ctx.route = None

        filter_instance = QanatContextFilter(default_value="NONE")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )

        filter_instance.filter(record)

        assert record.handler == "NONE"
        assert record.execution_mode == "NONE"
        assert record.time_limit == "NONE"

    def test_filter_with_none_correlation_id(self):
        """filter() uses default_value when correlation_id is None."""
        ctx = get_qanat_context()

        body = JsonRpcRequest(method="test.notification", id=None)
        msg = _TestBrokerMessage(
            raw=None,
            body=body,
            headers={},
            received_at=1234567890.0,
            queue_name="test-queue",
        )

        ctx.msg = msg

        filter_instance = QanatContextFilter(default_value="NO-ID")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )

        filter_instance.filter(record)

        assert record.correlation_id == "NO-ID"

    def test_filter_with_none_reply_to(self):
        """filter() uses default_value when reply_to is None."""
        ctx = get_qanat_context()

        body = JsonRpcRequest(method="test.method", id="req-1")
        msg = _TestBrokerMessage(
            raw=None,
            body=body,
            headers={},
            received_at=1234567890.0,
            queue_name="test-queue",
        )
        msg.reply_to = None

        ctx.msg = msg

        filter_instance = QanatContextFilter(default_value="NO-REPLY")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )

        filter_instance.filter(record)

        assert record.reply_to == "NO-REPLY"

    def test_filter_with_none_time_limit(self):
        """filter() uses default_value when time_limit is None."""
        ctx = get_qanat_context()

        def dummy_handler():
            pass

        route = TaskRoute(
            handler=dummy_handler,
            mode=ExecutionMode.ASYNC,
            max_retries=None,
            time_limit=None,
        )

        ctx.route = route

        filter_instance = QanatContextFilter(default_value="UNLIMITED")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )

        filter_instance.filter(record)

        assert record.time_limit == "UNLIMITED"


class TestQanatContextFilterEdgeCases:
    """Test QanatContextFilter edge cases."""

    def test_filter_always_returns_true(self):
        """filter() always returns True to allow record propagation."""
        filter_instance = QanatContextFilter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )

        result = filter_instance.filter(record)

        assert result is True

    def test_filter_with_empty_headers(self):
        """filter() handles empty headers dict."""
        ctx = get_qanat_context()

        body = JsonRpcRequest(method="test.method", id="req-1")
        msg = _TestBrokerMessage(
            raw=None,
            body=body,
            headers={},
            received_at=1234567890.0,
            queue_name="test-queue",
        )

        ctx.msg = msg

        filter_instance = QanatContextFilter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )

        filter_instance.filter(record)

        assert record.qanat_headers == {}
