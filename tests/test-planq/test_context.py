"""Comprehensive tests for PlanqContext and related functionality."""

from __future__ import annotations

import asyncio
import logging
import threading
from collections import ChainMap
from typing import override

import pytest

from planq.context import (
    PlanqContext,
    PlanqContextFilter,
    get_planq_context,
    request_shutdown,
    reset_shutdown,
)
from planq.enums import ExecutionMode
from planq.exceptions import HandlerTimeout, Shutdown
from planq.message import BrokerMessage
from planq.models import JsonRpcRequest, TaskRoute
from planq.tracing import TraceContext

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


# === Layer 1: PlanqContext Tests ===


class TestPlanqContextConstruction:
    """Test PlanqContext construction and default values."""

    def test_construction_with_defaults(self):
        """PlanqContext initializes all fields to None."""
        ctx = PlanqContext()

        assert ctx.msg is None
        assert ctx.route is None
        assert ctx.max_attempts is None
        assert ctx.broker_latency is None
        assert ctx.internal_latency is None
        assert ctx.trace is None

    def test_is_cancelled_initially_false(self):
        """is_cancelled property is False initially."""
        ctx = PlanqContext()

        assert ctx.is_cancelled is False

    def test_attribute_assignment_and_retrieval(self):
        """PlanqContext stores assigned attribute values."""
        ctx = PlanqContext()

        ctx.max_attempts = 3
        ctx.broker_latency = 1.5
        ctx.internal_latency = 0.25

        assert ctx.max_attempts == 3
        assert ctx.broker_latency == 1.5
        assert ctx.internal_latency == 0.25


class TestPlanqContextCancellation:
    """Test PlanqContext cancellation primitives."""

    def test_cancel_sets_is_cancelled(self):
        """cancel(reason) flips is_cancelled to True."""
        ctx = PlanqContext()

        assert ctx.is_cancelled is False

        ctx.cancel(HandlerTimeout())

        assert ctx.is_cancelled is True

    def test_check_cancellation_raises_the_given_reason(self):
        """check_cancellation() raises the exact reason passed to cancel()."""
        ctx = PlanqContext()
        reason = Shutdown()
        ctx.cancel(reason)

        with pytest.raises(Shutdown) as exc_info:
            ctx.check_cancellation()

        assert exc_info.value is reason

    def test_check_cancellation_preserves_reason_payload(self):
        """A HandlerTimeout reason keeps its time_limit when re-raised."""
        ctx = PlanqContext()
        ctx.cancel(HandlerTimeout(5.0))

        with pytest.raises(HandlerTimeout) as exc_info:
            ctx.check_cancellation()

        assert exc_info.value.time_limit == 5.0

    def test_check_cancellation_noop_when_not_cancelled(self):
        """check_cancellation() is no-op when not cancelled."""
        ctx = PlanqContext()

        # Should not raise
        ctx.check_cancellation()

        assert ctx.is_cancelled is False

    def test_multiple_cancel_calls_are_safe(self):
        """Calling cancel() multiple times is safe."""
        ctx = PlanqContext()

        ctx.cancel(HandlerTimeout())
        ctx.cancel(HandlerTimeout())
        ctx.cancel(HandlerTimeout())

        assert ctx.is_cancelled is True


class TestPlanqContextGlobalShutdown:
    """Test process-wide shutdown broadcast via the global flag."""

    def test_global_shutdown_cancels_fresh_context(self):
        """A context created after request_shutdown() reports cancelled."""
        request_shutdown(Shutdown())

        ctx = PlanqContext()

        assert ctx.is_cancelled is True

    def test_check_cancellation_raises_global_reason(self):
        """check_cancellation() raises the global reason for any context."""
        reason = Shutdown("draining")
        request_shutdown(reason)

        ctx = PlanqContext()

        with pytest.raises(Shutdown) as exc_info:
            ctx.check_cancellation()

        assert exc_info.value is reason

    def test_per_context_reason_takes_precedence_over_global(self):
        """A per-context reason wins over the global shutdown reason."""
        request_shutdown(Shutdown())
        ctx = PlanqContext()
        timeout = HandlerTimeout(10.0)
        ctx.cancel(timeout)

        with pytest.raises(HandlerTimeout) as exc_info:
            ctx.check_cancellation()

        assert exc_info.value is timeout

    def test_reset_shutdown_clears_global(self):
        """reset_shutdown() restores the no-cancellation state."""
        request_shutdown(Shutdown())
        reset_shutdown()

        ctx = PlanqContext()

        assert ctx.is_cancelled is False
        ctx.check_cancellation()  # must not raise


# === Layer 2: get_planq_context() Tests ===


class TestGetPlanqContext:
    """Test get_planq_context() function behavior."""

    def test_returns_new_context_when_not_set(self):
        """get_planq_context() returns new PlanqContext when not set."""
        # Note: This test may fail if context is already set from previous
        # tests. In practice, contextvars are isolated per test.
        ctx = get_planq_context()

        assert isinstance(ctx, PlanqContext)
        assert ctx.msg is None

    def test_returns_existing_context_in_same_scope(self):
        """get_planq_context() returns same context within scope."""
        ctx1 = get_planq_context()
        ctx1.max_attempts = 3

        ctx2 = get_planq_context()

        assert ctx1 is ctx2
        assert ctx2.max_attempts == 3

    @pytest.mark.asyncio
    async def test_context_shared_across_async_tasks(self):
        """Async tasks created with create_task share parent context.

        This is expected Python behavior: ContextVars inherit from the
        parent task but modifications in child tasks don't affect parent.
        """
        # Set context in parent
        parent_ctx = get_planq_context()
        parent_ctx.max_attempts = 5

        async def child_task():
            # Child sees parent's context
            ctx = get_planq_context()
            return ctx is parent_ctx

        task = asyncio.create_task(child_task())
        same_context = await task

        # Child task sees the same context object
        assert same_context is True

    def test_isolation_across_threads(self):
        """Different threads see different PlanqContext instances."""
        results = {}

        def thread_func(name: str):
            ctx = get_planq_context()
            ctx.max_attempts = hash(name)
            results[name] = ctx

        thread_a = threading.Thread(target=thread_func, args=("thread-a",))
        thread_b = threading.Thread(target=thread_func, args=("thread-b",))

        thread_a.start()
        thread_b.start()

        thread_a.join()
        thread_b.join()

        assert results["thread-a"].max_attempts == hash("thread-a")
        assert results["thread-b"].max_attempts == hash("thread-b")
        assert results["thread-a"] is not results["thread-b"]


# === Layer 3: PlanqContextFilter Tests ===


class TestPlanqContextFilterConstruction:
    """Test PlanqContextFilter construction."""

    def test_construction_with_default_value(self):
        """PlanqContextFilter stores default_value parameter."""
        filter_instance = PlanqContextFilter(default_value="NONE")

        assert filter_instance.default_value == "NONE"

    def test_construction_with_default_default(self):
        """PlanqContextFilter uses None as default_value by default."""
        filter_instance = PlanqContextFilter()

        assert filter_instance.default_value is None


class TestPlanqContextFilterWithFullContext:
    """Test PlanqContextFilter with fully populated context."""

    def test_filter_with_full_context(self):
        """filter() populates all record attributes from full context."""
        # Create context with all fields populated
        ctx = get_planq_context()

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
        msg.message_id = "broker-msg-123"
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
        ctx.msg = msg
        ctx.route = route
        ctx.max_attempts = 4
        ctx.broker_latency = 1.5
        ctx.internal_latency = 0.25

        # Create filter and apply to record
        filter_instance = PlanqContextFilter(default_value="N/A")
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
        assert record.message_id == "broker-msg-123"
        assert record.correlation_id == "req-123"
        assert record.method == "test.method"
        assert record.current_attempt == 2
        assert record.reply_to == "reply-queue"
        assert record.headers == {"x-custom": "header-value"}
        # Handler qualname includes the enclosing method
        assert record.handler.endswith("dummy_handler")
        assert record.execution_mode == "async"
        assert record.time_limit_seconds == 30.0
        assert record.max_attempts == 4
        assert record.broker_latency_seconds == 1.5
        assert record.internal_latency_seconds == 0.25


class TestPlanqContextFilterWithPartialContext:
    """Test PlanqContextFilter with partially populated context."""

    def test_filter_with_no_message(self):
        """filter() omits message fields when ctx.msg is None."""
        ctx = get_planq_context()
        ctx.msg = None

        filter_instance = PlanqContextFilter(default_value="MISSING")
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

        assert not hasattr(record, "queue_name")
        assert not hasattr(record, "message_id")
        assert not hasattr(record, "correlation_id")
        assert not hasattr(record, "method")
        assert not hasattr(record, "current_attempt")
        assert not hasattr(record, "reply_to")
        assert not hasattr(record, "headers")

    def test_filter_with_no_route(self):
        """filter() omits route fields when ctx.route is None."""
        ctx = get_planq_context()
        ctx.route = None

        filter_instance = PlanqContextFilter(default_value="NONE")
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

        assert not hasattr(record, "handler")
        assert not hasattr(record, "execution_mode")
        assert not hasattr(record, "time_limit_seconds")

    def test_filter_with_none_correlation_id(self):
        """filter() uses default_value when correlation_id is None."""
        ctx = get_planq_context()

        body = JsonRpcRequest(method="test.notification", id=None)
        msg = _TestBrokerMessage(
            raw=None,
            body=body,
            headers={},
            received_at=1234567890.0,
            queue_name="test-queue",
        )

        ctx.msg = msg

        filter_instance = PlanqContextFilter(default_value="NO-ID")
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
        ctx = get_planq_context()

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

        filter_instance = PlanqContextFilter(default_value="NO-REPLY")
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
        """filter() uses None when time_limit is None."""
        ctx = get_planq_context()

        def dummy_handler():
            pass

        route = TaskRoute(
            handler=dummy_handler,
            mode=ExecutionMode.ASYNC,
            max_retries=None,
            time_limit=None,
        )

        ctx.route = route

        filter_instance = PlanqContextFilter(default_value="UNLIMITED")
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

        assert record.time_limit_seconds is None


class TestPlanqContextFilterEdgeCases:
    """Test PlanqContextFilter edge cases."""

    def test_filter_always_returns_true(self):
        """filter() always returns True to allow record propagation."""
        filter_instance = PlanqContextFilter()
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

        filter_instance = PlanqContextFilter()
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

        assert record.headers == {}


class TestPlanqContextFilterChainMap:
    """Test ChainMap wrapping of record.args."""

    def test_filter_applies_chainmap_for_dict_args(self):
        """filter() wraps dict args in ChainMap with record attrs."""
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

        filter_instance = PlanqContextFilter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="%(method)s called",
            args=(),
            exc_info=None,
        )
        # Simulate what logging does: set args to a dict
        record.args = {"method": "explicit"}

        filter_instance.filter(record)

        assert isinstance(record.args, ChainMap)
        # Explicit dict value takes priority
        assert record.args["method"] == "explicit"
        # Record attrs are accessible as fallback
        assert record.args["queue_name"] == "test-queue"

    def test_filter_skips_chainmap_for_non_dict_args(self):
        """filter() does not wrap tuple args in ChainMap."""
        filter_instance = PlanqContextFilter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="value is %s",
            args=("hello",),
            exc_info=None,
        )

        filter_instance.filter(record)

        assert record.args == ("hello",)
        assert not isinstance(record.args, ChainMap)

    def test_filter_skips_chainmap_for_none_args(self):
        """filter() does not wrap None args in ChainMap."""
        filter_instance = PlanqContextFilter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="no args",
            args=None,
            exc_info=None,
        )

        filter_instance.filter(record)

        assert record.args is None

    def test_filter_chainmap_idempotent(self):
        """Double-applying filter does not double-wrap ChainMap."""
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

        filter_instance = PlanqContextFilter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="%(method)s called",
            args=(),
            exc_info=None,
        )
        # Simulate what logging does: set args to a dict
        record.args = {"method": "explicit"}

        filter_instance.filter(record)
        filter_instance.filter(record)

        assert isinstance(record.args, ChainMap)
        # Should still have exactly 2 maps, not 3
        assert len(record.args.maps) == 2


# === Layer 4: PlanqContextFilter Trace Enrichment ===


class TestPlanqContextFilterTraceEnrichment:
    """Test trace field injection into log records."""

    def test_trace_fields_set_when_trace_exists(self):
        """filter() sets trace_id, span_id, parent_span_id."""
        ctx = get_planq_context()
        ctx.trace = TraceContext(
            trace_id="a" * 32,
            span_id="b" * 16,
            parent_span_id="c" * 16,
            trace_flags="01",
        )

        filter_instance = PlanqContextFilter()
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

        assert record.trace_id == "a" * 32
        assert record.span_id == "b" * 16
        assert record.parent_span_id == "c" * 16

    def test_parent_span_id_none_for_root_trace(self):
        """filter() sets parent_span_id=None for root traces."""
        ctx = get_planq_context()
        ctx.trace = TraceContext(
            trace_id="a" * 32,
            span_id="b" * 16,
        )

        filter_instance = PlanqContextFilter()
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

        assert record.trace_id == "a" * 32
        assert record.span_id == "b" * 16
        assert record.parent_span_id is None

    def test_trace_fields_absent_when_trace_is_none(self):
        """filter() does not set trace fields when ctx.trace is None."""
        ctx = get_planq_context()
        ctx.trace = None

        filter_instance = PlanqContextFilter()
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

        assert not hasattr(record, "trace_id")
        assert not hasattr(record, "span_id")
        assert not hasattr(record, "parent_span_id")
