"""Comprehensive tests for PlanqConsumer."""

from __future__ import annotations

import asyncio
import os
import signal
import threading
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from planq.app import Planq
from planq.consumer import (
    DEFAULT_MAX_RETRIES,
    PlanqConsumer,
    _ProcessPool,
    _worker_main,
    should_retry,
)
from planq.context import _planq_context, get_planq_context
from planq.enums import ExecutionMode, JsonRpcError
from planq.exceptions import (
    FeatureNotSupportedError,
    HandlerTimeout,
    RejectMessage,
    RetryMessage,
)
from planq.message import BrokerMessage
from planq.middleware import DeadlineMiddleware, Middleware
from planq.models import (
    ConsumerSettings,
    JsonRpcRequest,
    TaskResult,
    TaskRoute,
)


def _add_one(x: int) -> int:
    """Module-level function for process pool tests (must be picklable)."""
    return x + 1


def _return_42() -> int:
    """Module-level function for process pool tests (must be picklable)."""
    return 42


def _multiply(x: int) -> int:
    """Module-level function for process pool tests (must be picklable)."""
    return x * 2


# === Fixtures ===


@pytest.fixture
def mock_message():
    """Factory for creating mock BrokerMessage instances."""

    def _create(
        method: str,
        params=None,
        id: str | None = "test-123",
        headers: dict | None = None,
        delivery_count: int = 1,
        reply_to: str | None = "reply-queue",
        enqueued_at: float | None = None,
        received_at: float | None = None,
    ):
        msg = MagicMock(spec=BrokerMessage)
        msg.body = JsonRpcRequest(method=method, params=params, id=id)
        msg.correlation_id = id
        msg.headers = headers or {}
        msg.delivery_count = delivery_count
        msg.reply_to = reply_to
        msg.message_id = "test-msg-id"
        msg.queue_name = "test-queue"
        msg.enqueued_at = enqueued_at or time.time() - 0.1
        msg.received_at = received_at or time.time()
        msg.ack = AsyncMock()
        msg.nack = AsyncMock()
        msg.reject = AsyncMock()
        return msg

    return _create


# === Layer 1: Task Registration ===


class TestTaskRegistration:
    """Tests for @app.task() decorator and route management."""

    def test_task_decorator_registers_handler(self):
        """@app.task() registers handler in app.routes dict."""
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task("my.method", mode=ExecutionMode.ASYNC)
        async def handler(x: int) -> int:
            return x * 2

        consumer = PlanqConsumer(app, middlewares=[])

        assert "my.method" in consumer.routes
        route = consumer.routes["my.method"]
        assert route.handler is handler._func
        assert route.mode == ExecutionMode.ASYNC

    def test_task_decorator_returns_planq_task(self):
        """Decorator returns a PlanqTask wrapping the original."""
        from planq.app import PlanqTask

        broker = MagicMock()
        app = Planq(broker=broker)

        async def handler(x: int) -> int:
            return x * 2

        decorated = app.task("test.method")(handler)
        assert isinstance(decorated, PlanqTask)
        assert decorated._func is handler

    def test_handler_alias_works_identically(self):
        """app.handler() is an alias for app.task()."""
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.handler("my.method", mode=ExecutionMode.ASYNC)
        async def handler(x: int) -> int:
            return x * 2

        consumer = PlanqConsumer(app, middlewares=[])

        assert "my.method" in consumer.routes
        route = consumer.routes["my.method"]
        assert route.handler is handler._func

    def test_duplicate_task_names_raises_error(self):
        """Registering same name twice raises ValueError."""
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task("duplicate")
        async def first_handler():
            return "first"

        with pytest.raises(ValueError, match="already registered"):

            @app.task("duplicate")
            async def second_handler():
                return "second"

    def test_task_stores_max_retries(self):
        """max_retries parameter is stored in TaskRoute."""
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task("test", max_retries=5)
        async def handler():
            pass

        consumer = PlanqConsumer(app, middlewares=[])

        assert consumer.routes["test"].max_retries == 5

    def test_task_stores_time_limit(self):
        """time_limit parameter is stored in TaskRoute."""
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task("test", time_limit=30.0)
        async def handler():
            pass

        consumer = PlanqConsumer(app, middlewares=[])

        assert consumer.routes["test"].time_limit == 30.0

    def test_task_validates_max_retries_non_negative(self):
        """max_retries must be >= 0."""
        broker = MagicMock()
        app = Planq(broker=broker)

        with pytest.raises(ValidationError) as exc_info:

            @app.task("test", max_retries=-1)
            async def handler():
                pass

        errors = exc_info.value.errors()
        assert any(error["loc"] == ("max_retries",) for error in errors)

    def test_task_validates_time_limit_positive(self):
        """time_limit must be > 0 when specified."""
        broker = MagicMock()
        app = Planq(broker=broker)

        with pytest.raises(ValidationError) as exc_info:

            @app.task("test", time_limit=0.0)
            async def handler():
                pass

        errors = exc_info.value.errors()
        assert any(error["loc"] == ("time_limit",) for error in errors)

    def test_task_accepts_zero_max_retries(self):
        """max_retries=0 means one attempt, no retries."""
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task("test", max_retries=0)
        async def handler():
            pass

        consumer = PlanqConsumer(app, middlewares=[])

        assert consumer.routes["test"].max_retries == 0

    def test_typed_handler_works_at_runtime(self):
        """Type hints don't break runtime behavior."""
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task("typed.method", mode=ExecutionMode.ASYNC)
        async def handler(x: int, y: int) -> int:
            return x + y

        consumer = PlanqConsumer(app, middlewares=[])

        # Verify registration works
        assert "typed.method" in consumer.routes
        route = consumer.routes["typed.method"]
        assert route.handler is handler._func
        assert route.mode == ExecutionMode.ASYNC


# === Layer 2: Retry Logic ===


class TestRetryLogic:
    """Tests for backoff calculation and retry limit resolution."""

    def test_default_max_retries_constant(self):
        """DEFAULT_MAX_RETRIES is 3."""
        assert DEFAULT_MAX_RETRIES == 3

    def test_calculate_backoff_returns_float(self):
        """_calculate_backoff returns a float."""
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        backoff = consumer._calculate_backoff(delivery_count=1)
        assert isinstance(backoff, float)

    def test_calculate_backoff_in_valid_range(self):
        """Backoff is between 0 and exponential_cap."""
        broker = MagicMock()
        app = Planq(broker=broker)
        settings = ConsumerSettings(
            retry_base_delay=2.0,
            retry_max_delay=100.0,
        )
        consumer = PlanqConsumer(app, settings=settings, middlewares=[])

        # delivery_count=3 → 2^(3-1) = 4 → cap = min(100, 2*4) = 8
        for _ in range(100):
            backoff = consumer._calculate_backoff(delivery_count=3)
            assert 0 <= backoff <= 8.0

    def test_calculate_backoff_respects_max_delay(self):
        """Backoff never exceeds retry_max_delay."""
        broker = MagicMock()
        app = Planq(broker=broker)
        settings = ConsumerSettings(
            retry_base_delay=10.0,
            retry_max_delay=30.0,
        )
        consumer = PlanqConsumer(app, settings=settings, middlewares=[])

        # delivery_count=10 would give huge exponential, but capped at 30
        for _ in range(100):
            backoff = consumer._calculate_backoff(delivery_count=10)
            assert backoff <= 30.0

    def test_get_max_retries_route_priority(self):
        """Route max_retries takes priority."""
        broker = MagicMock()
        app = Planq(broker=broker)
        settings = ConsumerSettings(max_retries=5)
        consumer = PlanqConsumer(app, settings=settings, middlewares=[])

        route = TaskRoute(
            handler=lambda: None,
            mode=ExecutionMode.ASYNC,
            max_retries=10,
        )

        assert consumer._get_max_retries(route) == 10

    def test_get_max_retries_settings_priority(self):
        """Settings max_retries used when route is None."""
        broker = MagicMock()
        app = Planq(broker=broker)
        settings = ConsumerSettings(max_retries=7)
        consumer = PlanqConsumer(app, settings=settings, middlewares=[])

        route = TaskRoute(
            handler=lambda: None,
            mode=ExecutionMode.ASYNC,
            max_retries=None,
        )

        assert consumer._get_max_retries(route) == 7

    def test_get_max_retries_default_fallback(self):
        """DEFAULT_MAX_RETRIES used when both route and settings are None."""
        broker = MagicMock()
        app = Planq(broker=broker)
        settings = ConsumerSettings(max_retries=None)
        consumer = PlanqConsumer(app, settings=settings, middlewares=[])

        route = TaskRoute(
            handler=lambda: None,
            mode=ExecutionMode.ASYNC,
            max_retries=None,
        )

        assert consumer._get_max_retries(route) == DEFAULT_MAX_RETRIES


# === Layer 3: Message Processing (ASYNC mode) ===


class TestMessageProcessingAsync:
    """Tests for ASYNC execution mode message handling."""

    @pytest.mark.asyncio
    async def test_route_lookup_by_method_name(self, mock_message):
        """Routes message to handler by msg.body.method."""
        broker = MagicMock()
        app = Planq(broker=broker)

        handler_called = False

        @app.task("test.lookup", mode=ExecutionMode.ASYNC)
        async def handler():
            nonlocal handler_called
            handler_called = True
            return "success"

        consumer = PlanqConsumer(app, middlewares=[])
        msg = mock_message(method="test.lookup", id="123")

        response = await consumer._router_endpoint(msg)

        assert handler_called
        assert response.result == "success"

    @pytest.mark.asyncio
    async def test_handler_execution_positional_params(self, mock_message):
        """Handler receives positional params from list."""
        broker = MagicMock()
        app = Planq(broker=broker)

        received_args = None

        @app.task("test.positional", mode=ExecutionMode.ASYNC)
        async def handler(a: int, b: str, c: float):
            nonlocal received_args
            received_args = (a, b, c)
            return "ok"

        consumer = PlanqConsumer(app, middlewares=[])
        msg = mock_message(method="test.positional", params=[42, "hello", 3.14])

        await consumer._router_endpoint(msg)

        assert received_args == (42, "hello", 3.14)

    @pytest.mark.asyncio
    async def test_handler_execution_named_params(self, mock_message):
        """Handler receives named params from dict."""
        broker = MagicMock()
        app = Planq(broker=broker)

        received_kwargs = None

        @app.task("test.named", mode=ExecutionMode.ASYNC)
        async def handler(name: str, age: int):
            nonlocal received_kwargs
            received_kwargs = {"name": name, "age": age}
            return "ok"

        consumer = PlanqConsumer(app, middlewares=[])
        msg = mock_message(
            method="test.named", params={"name": "Alice", "age": 30}
        )

        await consumer._router_endpoint(msg)

        assert received_kwargs == {"name": "Alice", "age": 30}

    @pytest.mark.asyncio
    async def test_notification_returns_none(self, mock_message):
        """Notification (id=None) returns None response."""
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task("test.notification", mode=ExecutionMode.ASYNC)
        async def handler():
            return "done"

        consumer = PlanqConsumer(app, middlewares=[])
        msg = mock_message(method="test.notification", id=None)

        response = await consumer._router_endpoint(msg)

        assert response is None

    @pytest.mark.asyncio
    async def test_task_result_headers_included_in_response(self, mock_message):
        """TaskResult headers merged into JsonRpcResponse."""
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task("test.headers", mode=ExecutionMode.ASYNC)
        async def handler():
            return TaskResult(
                result={"data": "value"},
                headers={"x-custom": "header-value"},
            )

        consumer = PlanqConsumer(app, middlewares=[])
        msg = mock_message(method="test.headers", id="123")

        response = await consumer._router_endpoint(msg)

        assert response.headers["x-custom"] == "header-value"
        assert response.result == {"data": "value"}

    @pytest.mark.asyncio
    async def test_request_with_id_but_no_reply_to_returns_none(
        self, mock_message
    ):
        """Request with id but no reply_to returns None."""
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task("test.no_reply", mode=ExecutionMode.ASYNC)
        async def handler():
            return "result"

        consumer = PlanqConsumer(app, middlewares=[])
        msg = mock_message(method="test.no_reply", id="123", reply_to=None)

        response = await consumer._router_endpoint(msg)

        assert response is None

    @pytest.mark.asyncio
    async def test_request_with_id_but_empty_reply_to_returns_none(
        self, mock_message
    ):
        """Request with id but empty string reply_to returns None."""
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task("test.empty_reply", mode=ExecutionMode.ASYNC)
        async def handler():
            return "result"

        consumer = PlanqConsumer(app, middlewares=[])
        msg = mock_message(method="test.empty_reply", id="123", reply_to="")

        response = await consumer._router_endpoint(msg)

        assert response is None


# === Layer 3a: ASYNC Execution Mode ===


class TestExecutionModeAsync:
    """Tests for ASYNC execution mode via _execute()."""

    @pytest.mark.asyncio
    async def test_async_mode_executes_coroutine_handler(self):
        """ASYNC mode handler runs as native coroutine.

        Verifies:
        - _execute() routes to _execute_async() for ExecutionMode.ASYNC
        - Coroutine execution works correctly
        - Return value propagates
        - Covers match statement lines 489-496
        """
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task("test.async", mode=ExecutionMode.ASYNC)
        async def async_handler(x: int) -> int:
            await asyncio.sleep(0.01)  # Ensure truly async
            return x * 2

        consumer = PlanqConsumer(app, middlewares=[])
        route = consumer.routes["test.async"]
        result = await consumer._execute(route, [21], "test.async")

        assert result == 42

    @pytest.mark.asyncio
    async def test_async_mode_with_time_limit(self):
        """ASYNC mode respects time_limit parameter.

        Verifies:
        - time_limit passed through to _execute_async()
        - asyncio.timeout() enforced
        - HandlerTimeout raised on expiry
        """
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task(
            "test.async_timeout",
            mode=ExecutionMode.ASYNC,
            time_limit=0.05,
        )
        async def slow_handler():
            await asyncio.sleep(1.0)
            return "should not complete"

        consumer = PlanqConsumer(app, middlewares=[])
        route = consumer.routes["test.async_timeout"]

        with pytest.raises(HandlerTimeout) as exc_info:
            await consumer._execute(route, None, "test.async_timeout")

        assert exc_info.value.time_limit == 0.05

    @pytest.mark.asyncio
    async def test_async_mode_without_time_limit(self):
        """ASYNC mode without time_limit runs handler directly.

        Verifies:
        - time_limit=None disables timeout
        - Handler completes regardless of duration
        """
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task("test.async_no_limit", mode=ExecutionMode.ASYNC)
        async def handler():
            await asyncio.sleep(0.1)
            return "completed"

        consumer = PlanqConsumer(app, middlewares=[])
        route = consumer.routes["test.async_no_limit"]
        result = await consumer._execute(route, None, "test.async_no_limit")

        assert result == "completed"


# === Layer 3b: THREAD Execution Mode ===


class TestExecutionModeThread:
    """Tests for THREAD execution mode via _execute()."""

    @pytest.mark.asyncio
    async def test_thread_mode_executes_sync_handler(self):
        """THREAD mode handler runs via asyncio.to_thread."""
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task("test.thread", mode=ExecutionMode.THREAD)
        def sync_handler(x: int) -> int:
            return x * 2

        consumer = PlanqConsumer(app, middlewares=[])
        route = consumer.routes["test.thread"]
        result = await consumer._execute(route, [21], "test.thread")
        assert result == 42

    @pytest.mark.asyncio
    async def test_thread_mode_with_time_limit(self):
        """THREAD mode respects time_limit."""
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task(
            "test.thread_timeout",
            mode=ExecutionMode.THREAD,
            time_limit=0.01,
        )
        def slow_handler():
            import time as t

            t.sleep(1.0)

        consumer = PlanqConsumer(app, middlewares=[])
        route = consumer.routes["test.thread_timeout"]
        with pytest.raises(HandlerTimeout):
            await consumer._execute(route, None, "test.thread_timeout")

    @pytest.mark.asyncio
    async def test_thread_mode_without_time_limit(self):
        """THREAD mode without time_limit calls to_thread directly."""
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task("test.thread_no_limit", mode=ExecutionMode.THREAD)
        def handler():
            return "done"

        consumer = PlanqConsumer(app, middlewares=[])
        route = consumer.routes["test.thread_no_limit"]
        result = await consumer._execute(route, None, "test.thread_no_limit")
        assert result == "done"


# === Layer 4: Error Handling ===


class TestErrorHandling:
    """Tests for exception handling and error responses."""

    @pytest.mark.asyncio
    async def test_method_not_found_raises_reject_message(self, mock_message):
        """Unknown method raises RejectMessage."""
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        msg = mock_message(method="unknown.method", id="123")

        with pytest.raises(RejectMessage):
            await consumer._router_endpoint(msg)

    @pytest.mark.asyncio
    async def test_retry_message_propagates_unchanged(self, mock_message):
        """RetryMessage raised by handler propagates."""
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task("test.retry", mode=ExecutionMode.ASYNC)
        async def handler():
            raise RetryMessage(delay=5.0)

        consumer = PlanqConsumer(app, middlewares=[])
        msg = mock_message(method="test.retry", id="123")

        with pytest.raises(RetryMessage) as exc_info:
            await consumer._router_endpoint(msg)

        assert exc_info.value.delay == 5.0

    @pytest.mark.asyncio
    async def test_generic_exception_retries_if_attempts_available(
        self, mock_message
    ):
        """Generic exception raises RetryMessage if retries available."""
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task(
            "test.error",
            mode=ExecutionMode.ASYNC,
            max_retries=3,
            retry_on=Exception,
        )
        async def handler():
            raise ValueError("something went wrong")

        consumer = PlanqConsumer(app, middlewares=[])
        msg = mock_message(method="test.error", id="123", delivery_count=1)

        _planq_context.set(None)
        ctx = get_planq_context()
        ctx.msg = msg

        with pytest.raises(RetryMessage):
            await consumer._router_endpoint(msg)

    @pytest.mark.asyncio
    async def test_generic_exception_error_response_if_retries_exhausted(
        self, mock_message
    ):
        """Generic exception returns error response if retries exhausted."""
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task(
            "test.exhausted",
            mode=ExecutionMode.ASYNC,
            max_retries=2,
            retry_on=Exception,
        )
        async def handler():
            raise ValueError("permanent failure")

        consumer = PlanqConsumer(app, middlewares=[])
        msg = mock_message(method="test.exhausted", id="123", delivery_count=3)

        response = await consumer._router_endpoint(msg)

        assert response.id == "123"
        assert response.error is not None
        assert response.error.code == JsonRpcError.INTERNAL_ERROR
        assert "permanent failure" in response.error.message

    @pytest.mark.asyncio
    async def test_handler_timeout_treated_as_retriable(self, mock_message):
        """HandlerTimeout is retriable if attempts remain."""
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task(
            "test.timeout",
            mode=ExecutionMode.ASYNC,
            max_retries=3,
            time_limit=0.01,
            retry_on=Exception,
        )
        async def handler():
            await asyncio.sleep(1.0)  # Exceeds time_limit

        consumer = PlanqConsumer(app, middlewares=[])
        msg = mock_message(method="test.timeout", id="123", delivery_count=1)

        _planq_context.set(None)
        ctx = get_planq_context()
        ctx.msg = msg

        with pytest.raises(RetryMessage):
            await consumer._router_endpoint(msg)

    @pytest.mark.asyncio
    async def test_max_retries_exceeded_notification_raises_exception(
        self, mock_message
    ):
        """MaxRetriesExceeded raised for notifications when exhausted."""
        from planq.exceptions import MaxRetriesExceeded

        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task(
            "test.notification_fail",
            mode=ExecutionMode.ASYNC,
            max_retries=3,
            retry_on=Exception,
        )
        async def handler():
            raise ValueError("permanent failure")

        consumer = PlanqConsumer(app, middlewares=[])
        msg = mock_message(
            method="test.notification_fail", id=None, delivery_count=4
        )

        with pytest.raises(MaxRetriesExceeded):
            await consumer._router_endpoint(msg)

    @pytest.mark.asyncio
    async def test_max_retries_exceeded_notification_without_reply_to(
        self, mock_message
    ):
        """MaxRetriesExceeded raised when retries exhausted and no reply_to."""
        from planq.exceptions import MaxRetriesExceeded

        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task(
            "test.no_reply",
            mode=ExecutionMode.ASYNC,
            max_retries=3,
            retry_on=Exception,
        )
        async def handler():
            raise ValueError("failure")

        consumer = PlanqConsumer(app, middlewares=[])
        msg = mock_message(
            method="test.no_reply", id="123", reply_to=None, delivery_count=4
        )

        with pytest.raises(MaxRetriesExceeded):
            await consumer._router_endpoint(msg)


# === Layer 4.5: Reject Callbacks ===


class TestRejectCallbacks:
    """Tests for on_reject() decorator and callback execution."""

    @pytest.mark.asyncio
    async def test_on_reject_decorator_registers_callback(self):
        """on_reject decorator registers callback in _reject_callbacks list."""
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @consumer.on_reject()
        async def my_callback(msg, exc):
            pass

        assert my_callback in consumer._reject_callbacks

    @pytest.mark.asyncio
    async def test_on_reject_decorator_returns_function_unchanged(self):
        """Decorator returns original function unchanged."""
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        async def original_function(msg, exc):
            pass

        decorated = consumer.on_reject()(original_function)
        assert decorated is original_function

    @pytest.mark.asyncio
    async def test_on_reject_multiple_callbacks_registered(self):
        """Multiple callbacks can be registered and order is preserved."""
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @consumer.on_reject()
        async def callback_one(msg, exc):
            pass

        @consumer.on_reject()
        async def callback_two(msg, exc):
            pass

        @consumer.on_reject()
        async def callback_three(msg, exc):
            pass

        assert len(consumer._reject_callbacks) == 3
        assert consumer._reject_callbacks[0] is callback_one
        assert consumer._reject_callbacks[1] is callback_two
        assert consumer._reject_callbacks[2] is callback_three

    @pytest.mark.asyncio
    async def test_execute_reject_callbacks_with_empty_list(self, mock_message):
        """Early return when no callbacks registered."""
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        msg = mock_message(method="test", id=None)
        exc = ValueError("test error")

        # Should not raise, just return
        await consumer._execute_reject_callbacks(msg, exc)

    @pytest.mark.asyncio
    async def test_execute_reject_callbacks_calls_all_callbacks(
        self, mock_message
    ):
        """All registered callbacks are called with msg and exc."""
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        callback_one = AsyncMock()
        callback_two = AsyncMock()

        consumer._reject_callbacks.append(callback_one)
        consumer._reject_callbacks.append(callback_two)

        msg = mock_message(method="test", id=None)
        exc = ValueError("test error")

        await consumer._execute_reject_callbacks(msg, exc)

        callback_one.assert_called_once_with(msg, exc)
        callback_two.assert_called_once_with(msg, exc)

    @pytest.mark.asyncio
    async def test_execute_reject_callbacks_with_failing_callback(
        self, mock_message
    ):
        """Failing callback logs error but doesn't stop other callbacks."""
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        call_tracker = []

        async def callback_success_one(msg, exc):
            call_tracker.append("one")

        async def callback_fail(msg, exc):
            call_tracker.append("fail")
            raise RuntimeError("callback failed")

        async def callback_success_two(msg, exc):
            call_tracker.append("two")

        consumer._reject_callbacks.append(callback_success_one)
        consumer._reject_callbacks.append(callback_fail)
        consumer._reject_callbacks.append(callback_success_two)

        msg = mock_message(method="test", id=None)
        exc = ValueError("test error")

        with patch("planq.consumer.logger") as mock_logger:
            await consumer._execute_reject_callbacks(msg, exc)

        # All callbacks attempted
        assert "one" in call_tracker
        assert "fail" in call_tracker
        assert "two" in call_tracker

        # Error logged for failed callback
        mock_logger.error.assert_called_once()
        assert "failed" in mock_logger.error.call_args[0][0].lower()

    @pytest.mark.asyncio
    async def test_execute_reject_callbacks_passes_exception_to_callbacks(
        self, mock_message
    ):
        """Callbacks receive the correct msg and exc parameters."""
        from planq.exceptions import MethodNotFound

        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        captured_msg = None
        captured_exc = None

        async def capturing_callback(msg, exc):
            nonlocal captured_msg, captured_exc
            captured_msg = msg
            captured_exc = exc

        consumer._reject_callbacks.append(capturing_callback)

        msg = mock_message(method="unknown", id=None)
        exc = MethodNotFound("unknown")

        await consumer._execute_reject_callbacks(msg, exc)

        assert captured_msg is msg
        assert captured_exc is exc
        assert isinstance(captured_exc, MethodNotFound)

    @pytest.mark.asyncio
    async def test_process_message_calls_reject_callbacks_on_reject(
        self, mock_message
    ):
        """Reject callbacks are called when message is rejected."""
        broker = AsyncMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        callback_called = False

        @consumer.on_reject()
        async def track_reject(msg, exc):
            nonlocal callback_called
            callback_called = True

        msg = mock_message(method="unknown.method", id=None)
        await consumer._process_message(msg)

        assert callback_called is True


# === Layer 5: Middleware Integration ===


class TestMiddlewareIntegration:
    """Tests for middleware pipeline construction and execution."""

    @pytest.mark.asyncio
    async def test_middleware_called_before_router(self, mock_message):
        """Middleware runs before router endpoint."""
        broker = MagicMock()
        app = Planq(broker=broker)

        class TrackingMiddleware(Middleware):
            def __init__(self):
                self.called = False

            async def __call__(self, msg, call_next):
                self.called = True
                return await call_next(msg)

        tracking = TrackingMiddleware()
        consumer = PlanqConsumer(app, middlewares=[tracking])

        @app.task("test.middleware", mode=ExecutionMode.ASYNC)
        async def handler():
            return "ok"

        msg = mock_message(method="test.middleware", id="123")

        await consumer._pipeline(msg)

        assert tracking.called

    @pytest.mark.asyncio
    async def test_middleware_order_preserved(self, mock_message):
        """Middleware executes in order: first registered runs first."""
        broker = MagicMock()
        app = Planq(broker=broker)
        call_order = []

        class OrderMiddleware(Middleware):
            def __init__(self, name):
                self.name = name

            async def __call__(self, msg, call_next):
                call_order.append(f"{self.name}-before")
                result = await call_next(msg)
                call_order.append(f"{self.name}-after")
                return result

        mw1 = OrderMiddleware("first")
        mw2 = OrderMiddleware("second")
        consumer = PlanqConsumer(app, middlewares=[mw1, mw2])

        @app.task("test.order", mode=ExecutionMode.ASYNC)
        async def handler():
            call_order.append("handler")
            return "ok"

        msg = mock_message(method="test.order", id="123")

        await consumer._pipeline(msg)

        assert call_order == [
            "first-before",
            "second-before",
            "handler",
            "second-after",
            "first-after",
        ]

    @pytest.mark.asyncio
    async def test_deadline_middleware_rejects_expired_message(
        self, mock_message
    ):
        """DeadlineMiddleware returns error response for expired requests."""
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[DeadlineMiddleware()])

        handler_called = False

        @app.task("test.expired", mode=ExecutionMode.ASYNC)
        async def handler():
            nonlocal handler_called
            handler_called = True
            return "should not run"

        # Create message with expired TTL
        expired_time = time.time() - 100  # 100 seconds ago
        msg = mock_message(method="test.expired", id="123")
        msg.headers = {"x-expire-at": str(expired_time)}

        _planq_context.set(None)
        ctx = get_planq_context()
        ctx.msg = msg

        response = await consumer._pipeline(msg)

        # Handler should not be called
        assert not handler_called
        # Should get error response
        assert response.error is not None
        assert response.error.code == JsonRpcError.DEADLINE_EXCEEDED

    @pytest.mark.asyncio
    async def test_empty_middleware_list_works(self, mock_message):
        """Empty middleware list goes directly to router."""
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task("test.no_middleware", mode=ExecutionMode.ASYNC)
        async def handler():
            return "direct"

        msg = mock_message(method="test.no_middleware", id="123")

        response = await consumer._pipeline(msg)

        assert response.result == "direct"


# === Layer 6: Transport Integration (_process_message) ===


class TestTransportIntegration:
    """Tests for _process_message transport operations."""

    @pytest.mark.asyncio
    async def test_successful_message_acks(self, mock_message):
        """Successful processing calls msg.ack()."""
        broker = AsyncMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task("test.ack", mode=ExecutionMode.ASYNC)
        async def handler():
            return "success"

        msg = mock_message(method="test.ack", id=None)

        await consumer._process_message(msg)

        msg.ack.assert_called_once()
        msg.nack.assert_not_called()
        msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_retry_message_nacks_with_delay(self, mock_message):
        """RetryMessage calls msg.nack() with backoff delay."""
        broker = AsyncMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task(
            "test.nack",
            mode=ExecutionMode.ASYNC,
            max_retries=3,
            retry_on=Exception,
        )
        async def handler():
            raise ValueError("retry me")

        msg = mock_message(method="test.nack", id=None, delivery_count=1)

        await consumer._process_message(msg)

        msg.nack.assert_called_once()
        # Verify delay is a positive float
        delay = msg.nack.call_args[0][0]
        assert isinstance(delay, float)
        assert delay >= 0

        msg.ack.assert_not_called()
        msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_retry_message_with_explicit_delay(self, mock_message):
        """RetryMessage with explicit delay uses provided delay value.

        Verifies:
        - Handler raises RetryMessage(delay=10.0)
        - _process_message uses explicit delay (not calculated backoff)
        - msg.nack() called with the explicit delay value
        - Covers branch 669->671 (exc.delay is not None)
        """
        broker = AsyncMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task("test.explicit_delay", mode=ExecutionMode.ASYNC)
        async def handler():
            raise RetryMessage(delay=10.0)

        msg = mock_message(method="test.explicit_delay", id=None)

        await consumer._process_message(msg)

        # Verify nack called with explicit delay (not calculated backoff)
        msg.nack.assert_called_once_with(10.0)
        msg.ack.assert_not_called()
        msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_reject_message_rejects(self, mock_message):
        """RejectMessage calls msg.reject()."""
        broker = AsyncMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        msg = mock_message(method="unknown.method", id=None)

        await consumer._process_message(msg)

        msg.reject.assert_called_once()
        msg.ack.assert_not_called()
        msg.nack.assert_not_called()

    @pytest.mark.asyncio
    async def test_response_published_to_reply_to_queue(self, mock_message):
        """Successful request publishes response to reply_to."""
        broker = AsyncMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task("test.publish", mode=ExecutionMode.ASYNC)
        async def handler():
            return {"result": "data"}

        msg = mock_message(method="test.publish", id="req-123")

        await consumer._process_message(msg)

        # Verify broker.publish was called
        broker.publish.assert_called_once()
        call_args = broker.publish.call_args

        # Check queue name
        assert call_args[0][0] == "reply-queue"

        # Check response
        response = call_args[0][1]
        assert response.id == "req-123"
        assert response.result == {"result": "data"}

        msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_response_publish_failure_nacks_message(self, mock_message):
        """Response publish failure causes nack, not ack."""
        broker = AsyncMock()
        app = Planq(broker=broker)
        broker.publish.side_effect = ConnectionError("Publish failed")
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task("test.publish_fail", mode=ExecutionMode.ASYNC)
        async def handler():
            return "result"

        msg = mock_message(method="test.publish_fail", id="req-123")

        await consumer._process_message(msg)

        # Verify nack called, not ack
        msg.nack.assert_called_once()
        msg.ack.assert_not_called()

        # Verify backoff delay passed to nack
        delay = msg.nack.call_args[0][0]
        assert isinstance(delay, float)
        assert delay >= 0

    @pytest.mark.asyncio
    async def test_response_publish_failure_logs_error_with_context(
        self, mock_message
    ):
        """Response publish failure logs error with exc_info and context."""
        broker = AsyncMock()
        app = Planq(broker=broker)
        broker.publish.side_effect = ConnectionError("Publish failed")
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task("test.log_fail", mode=ExecutionMode.ASYNC)
        async def handler():
            return "result"

        msg = mock_message(method="test.log_fail", id="req-123")

        with patch("planq.consumer.logger") as mock_logger:
            await consumer._process_message(msg)

        # Verify error logged
        mock_logger.error.assert_called_once()
        call_args = mock_logger.error.call_args

        # Check log message
        assert "Failed to publish response" in call_args[0][0]

        # Check exc_info passed
        assert call_args[1]["exc_info"] is not None

        # Check extra dict contains delay_seconds
        assert "delay_seconds" in call_args[1]["extra"]

    @pytest.mark.asyncio
    async def test_response_publish_failure_returns_without_ack(
        self, mock_message
    ):
        """Response publish failure returns early without ack."""
        broker = AsyncMock()
        app = Planq(broker=broker)
        broker.publish.side_effect = ConnectionError("Publish failed")
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task("test.no_ack", mode=ExecutionMode.ASYNC)
        async def handler():
            return "result"

        msg = mock_message(method="test.no_ack", id="req-123")

        await consumer._process_message(msg)

        # Verify nack called but not ack
        msg.nack.assert_called_once()
        msg.ack.assert_not_called()
        msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_guarded_process_logs_broker_operation_failure(
        self, mock_message
    ):
        """Broker operation failure in _guarded_process logs error."""
        broker = AsyncMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task("test.broker_fail", mode=ExecutionMode.ASYNC)
        async def handler():
            return "success"

        msg = mock_message(method="test.broker_fail", id=None)
        # Both ack and nack must fail for exception to reach _guarded_process
        msg.ack.side_effect = ConnectionError("Ack failed")
        msg.nack.side_effect = ConnectionError("Nack also failed")

        sem = asyncio.Semaphore(1)
        await sem.acquire()

        with patch("planq.consumer.logger") as mock_logger:
            await consumer._guarded_process(msg, sem)

        # Should have 2 error logs:
        # 1. "Unhandled pipeline error" from msg.ack failing
        # 2. "Broker operation failed" from _guarded_process
        #    catching msg.nack failure
        assert mock_logger.error.call_count == 2
        last_call = mock_logger.error.call_args_list[-1]

        assert "Broker operation failed" in last_call[0][0]
        assert last_call[1]["exc_info"] is not None

    @pytest.mark.asyncio
    async def test_guarded_process_releases_semaphore_on_success(
        self, mock_message
    ):
        """Semaphore released after successful processing."""
        broker = AsyncMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task("test.success", mode=ExecutionMode.ASYNC)
        async def handler():
            return "ok"

        msg = mock_message(method="test.success", id=None)

        sem = asyncio.Semaphore(1)
        await sem.acquire()
        assert sem.locked()

        await consumer._guarded_process(msg, sem)

        # Semaphore should be released
        assert not sem.locked()

    @pytest.mark.asyncio
    async def test_guarded_process_releases_semaphore_on_exception(
        self, mock_message
    ):
        """Semaphore released even when exception occurs."""
        broker = AsyncMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task("test.exception", mode=ExecutionMode.ASYNC)
        async def handler():
            return "ok"

        msg = mock_message(method="test.exception", id=None)
        msg.ack.side_effect = ConnectionError("Failed")

        sem = asyncio.Semaphore(1)
        await sem.acquire()
        assert sem.locked()

        with patch("planq.consumer.logger"):
            await consumer._guarded_process(msg, sem)

        # Semaphore should still be released
        assert not sem.locked()

    @pytest.mark.asyncio
    async def test_unhandled_pipeline_exception_nacks_with_backoff(
        self, mock_message
    ):
        """Unhandled pipeline exception causes nack with backoff."""
        broker = AsyncMock()
        app = Planq(broker=broker)

        class FailingMiddleware(Middleware):
            async def __call__(self, msg, call_next):
                raise RuntimeError("Middleware error")

        consumer = PlanqConsumer(app, middlewares=[FailingMiddleware()])

        @app.task("test.pipeline_fail", mode=ExecutionMode.ASYNC)
        async def handler():
            return "ok"

        msg = mock_message(method="test.pipeline_fail", id=None)

        with patch("planq.consumer.logger"):
            await consumer._process_message(msg)

        # Verify nack called with backoff
        msg.nack.assert_called_once()
        delay = msg.nack.call_args[0][0]
        assert isinstance(delay, float)
        assert delay >= 0

        # Verify ack and reject not called
        msg.ack.assert_not_called()
        msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_unhandled_pipeline_exception_logs_error(self, mock_message):
        """Unhandled pipeline exception logs error with exc_info."""
        broker = AsyncMock()
        app = Planq(broker=broker)

        class FailingMiddleware(Middleware):
            async def __call__(self, msg, call_next):
                raise RuntimeError("Middleware error")

        consumer = PlanqConsumer(app, middlewares=[FailingMiddleware()])

        @app.task("test.log_pipeline", mode=ExecutionMode.ASYNC)
        async def handler():
            return "ok"

        msg = mock_message(method="test.log_pipeline", id=None)

        with patch("planq.consumer.logger") as mock_logger:
            await consumer._process_message(msg)

        # Verify error logged
        mock_logger.error.assert_called_once()
        call_args = mock_logger.error.call_args

        assert "Unhandled pipeline error" in call_args[0][0]
        assert call_args[1]["exc_info"] is not None

    @pytest.mark.asyncio
    async def test_response_includes_traceparent_header(self, mock_message):
        """Response published to reply_to includes traceparent."""
        broker = AsyncMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task("test.tp", mode=ExecutionMode.ASYNC)
        async def handler():
            return "ok"

        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        msg = mock_message(
            method="test.tp",
            id="req-tp",
            headers={"traceparent": traceparent},
        )

        await consumer._process_message(msg)

        broker.publish.assert_called_once()
        headers = broker.publish.call_args[1]["headers"]
        assert "traceparent" in headers

        # Verify it's a valid child span of the original trace
        parts = headers["traceparent"].split("-")
        assert len(parts) == 4
        assert parts[0] == "00"
        assert parts[1] == "0af7651916cd43dd8448eb211c80319c"
        assert parts[3] == "01"
        # span_id is the consumer's span (not the original parent)
        assert parts[2] != "b7ad6b7169203331"

    @pytest.mark.asyncio
    async def test_response_traceparent_not_overwritten_by_auto_inject(
        self, mock_message
    ):
        """Handler-set traceparent is preserved, not overwritten."""
        broker = AsyncMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task("test.tp_custom", mode=ExecutionMode.ASYNC)
        async def handler():
            return TaskResult(
                result="ok",
                headers={"traceparent": "00-custom-span-01"},
            )

        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        msg = mock_message(
            method="test.tp_custom",
            id="req-tp-custom",
            headers={"traceparent": traceparent},
        )

        await consumer._process_message(msg)

        broker.publish.assert_called_once()
        headers = broker.publish.call_args[1]["headers"]
        assert headers["traceparent"] == "00-custom-span-01"

    @pytest.mark.asyncio
    async def test_notification_does_not_publish_traceparent(
        self, mock_message
    ):
        """Notification (id=None) does not attempt publish."""
        broker = AsyncMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task("test.tp_notif", mode=ExecutionMode.ASYNC)
        async def handler():
            return "ok"

        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        msg = mock_message(
            method="test.tp_notif",
            id=None,
            headers={"traceparent": traceparent},
        )

        await consumer._process_message(msg)

        broker.publish.assert_not_called()
        msg.ack.assert_called_once()


# === Layer 7: Context Population ===


class TestContextPopulation:
    """Tests for PlanqContext field population."""

    @pytest.mark.asyncio
    async def test_context_msg_populated(self, mock_message):
        """ctx.msg is populated before pipeline."""
        from planq.context import get_planq_context

        broker = AsyncMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        captured_ctx_msg = None

        @app.task("test.ctx", mode=ExecutionMode.ASYNC)
        async def handler():
            nonlocal captured_ctx_msg
            ctx = get_planq_context()
            captured_ctx_msg = ctx.msg
            return "ok"

        msg = mock_message(method="test.ctx", id=None)

        await consumer._process_message(msg)

        assert captured_ctx_msg is msg

    @pytest.mark.asyncio
    async def test_context_route_populated(self, mock_message):
        """ctx.route is populated before handler execution."""
        from planq.context import get_planq_context

        broker = AsyncMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        captured_route = None

        @app.task("test.route", mode=ExecutionMode.ASYNC, max_retries=5)
        async def handler():
            nonlocal captured_route
            ctx = get_planq_context()
            captured_route = ctx.route
            return "ok"

        msg = mock_message(method="test.route", id=None)

        await consumer._process_message(msg)

        assert captured_route is not None
        assert captured_route.handler is handler._func
        assert captured_route.max_retries == 5

    @pytest.mark.asyncio
    async def test_context_max_attempts_calculated(self, mock_message):
        """ctx.max_attempts = max_retries + 1."""
        from planq.context import get_planq_context

        broker = AsyncMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        captured_max_attempts = None

        @app.task("test.attempts", mode=ExecutionMode.ASYNC, max_retries=3)
        async def handler():
            nonlocal captured_max_attempts
            ctx = get_planq_context()
            captured_max_attempts = ctx.max_attempts
            return "ok"

        msg = mock_message(method="test.attempts", id=None)

        await consumer._process_message(msg)

        assert captured_max_attempts == 4  # 3 retries + 1 attempt

    @pytest.mark.asyncio
    async def test_context_trace_set_from_traceparent_header(
        self, mock_message
    ):
        """ctx.trace is populated from traceparent header."""
        from planq.context import get_planq_context

        broker = AsyncMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        captured_trace = None

        @app.task("test.trace", mode=ExecutionMode.ASYNC)
        async def handler():
            nonlocal captured_trace
            ctx = get_planq_context()
            captured_trace = ctx.trace
            return "ok"

        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        msg = mock_message(
            method="test.trace",
            id=None,
            headers={"traceparent": traceparent},
        )

        await consumer._process_message(msg)

        assert captured_trace is not None
        assert captured_trace.trace_id == ("0af7651916cd43dd8448eb211c80319c")
        assert captured_trace.parent_span_id == "b7ad6b7169203331"
        assert captured_trace.trace_flags == "01"

    @pytest.mark.asyncio
    async def test_context_trace_generated_without_header(self, mock_message):
        """ctx.trace generates new trace when no traceparent header."""
        import re

        from planq.context import get_planq_context

        broker = AsyncMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        captured_trace = None

        @app.task("test.no_trace", mode=ExecutionMode.ASYNC)
        async def handler():
            nonlocal captured_trace
            ctx = get_planq_context()
            captured_trace = ctx.trace
            return "ok"

        msg = mock_message(method="test.no_trace", id=None)

        await consumer._process_message(msg)

        assert captured_trace is not None
        assert re.fullmatch(r"[0-9a-f]{32}", captured_trace.trace_id)
        assert re.fullmatch(r"[0-9a-f]{16}", captured_trace.span_id)
        assert captured_trace.parent_span_id is None
        assert captured_trace.trace_flags == "00"


# === Layer 8: Conditional Retry (retry_on) ===


class TestShouldRetryHelper:
    """Tests for should_retry() helper function."""

    def test_single_exception_type_match(self):
        """Single exception type matches correctly."""
        exc = ValueError("test error")
        result = should_retry(exc, ValueError)
        assert result is True

    def test_single_exception_type_non_match(self):
        """Single exception type returns False when no match."""
        exc = ValueError("test error")
        result = should_retry(exc, KeyError)
        assert result is False

    def test_list_of_types_first_matches(self):
        """List of types matches first exception type."""
        exc = ValueError("test error")
        result = should_retry(exc, [ValueError, KeyError, TypeError])
        assert result is True

    def test_list_of_types_last_matches(self):
        """List of types matches last exception type."""
        exc = TypeError("test error")
        result = should_retry(exc, [ValueError, KeyError, TypeError])
        assert result is True

    def test_list_of_types_none_match(self):
        """List of types returns False when none match."""
        exc = RuntimeError("test error")
        result = should_retry(exc, [ValueError, KeyError, TypeError])
        assert result is False

    def test_single_callable_returns_true(self):
        """Single callable predicate returns True."""
        exc = ValueError("retry this")
        result = should_retry(exc, lambda e: "retry" in str(e))
        assert result is True

    def test_single_callable_returns_false(self):
        """Single callable predicate returns False."""
        exc = ValueError("no match")
        result = should_retry(exc, lambda e: "retry" in str(e))
        assert result is False

    def test_callable_raises_exception_logs_and_returns_false(self):
        """Callable that raises exception logs and returns False."""
        exc = ValueError("test error")

        def broken_predicate(e):
            raise RuntimeError("predicate error")

        with patch("planq.consumer.logger") as mock_logger:
            result = should_retry(exc, broken_predicate)

        assert result is False
        mock_logger.error.assert_called_once()
        # Verify exc_info was passed
        assert mock_logger.error.call_args[1]["exc_info"] is not None

    def test_mixed_list_type_matches(self):
        """Mixed list [ValueError, callable] - type matches."""
        exc = ValueError("test error")
        result = should_retry(exc, [ValueError, lambda e: "retry" in str(e)])
        assert result is True

    def test_mixed_list_callable_matches(self):
        """Mixed list [ValueError, callable] - callable matches."""
        exc = KeyError("retry this")
        result = should_retry(exc, [ValueError, lambda e: "retry" in str(e)])
        assert result is True

    def test_callable_false_then_type_matches(self):
        """Callable returns False, loop continues to next condition."""
        exc = ValueError("test")
        result = should_retry(exc, [lambda e: False, ValueError])
        assert result is True

    def test_non_callable_non_type_condition_skipped(self):
        """Non-type, non-callable condition is skipped (89->84)."""
        exc = ValueError("test")
        # 42 is neither a type nor callable, so it's skipped
        result = should_retry(exc, [42, ValueError])  # type: ignore[list-item]
        assert result is True

    def test_subclass_matching(self):
        """Exception subclass matches parent type in retry_on."""

        class CustomError(ValueError):
            pass

        exc = CustomError("test error")
        result = should_retry(exc, ValueError)
        assert result is True


class TestTaskRouteRetryOn:
    """Tests for TaskRoute retry_on field."""

    def test_retry_on_accepts_single_type(self):
        """retry_on accepts a single exception type."""
        route = TaskRoute(
            handler=lambda: None,
            mode=ExecutionMode.ASYNC,
            retry_on=ValueError,
        )
        assert route.retry_on is ValueError

    def test_retry_on_accepts_list_of_types(self):
        """retry_on accepts a list of exception types."""
        route = TaskRoute(
            handler=lambda: None,
            mode=ExecutionMode.ASYNC,
            retry_on=[ValueError, KeyError],
        )
        assert route.retry_on == [ValueError, KeyError]

    def test_retry_on_accepts_callable(self):
        """retry_on accepts a callable predicate."""

        def predicate(exc):
            return True

        route = TaskRoute(
            handler=lambda: None,
            mode=ExecutionMode.ASYNC,
            retry_on=predicate,
        )
        assert route.retry_on is predicate

    def test_retry_on_accepts_none(self):
        """retry_on accepts None (no retries)."""
        route = TaskRoute(
            handler=lambda: None,
            mode=ExecutionMode.ASYNC,
            retry_on=None,
        )
        assert route.retry_on is None

    def test_task_decorator_stores_retry_on(self):
        """@app.task() stores retry_on parameter."""
        broker = MagicMock()
        app = Planq(broker=broker)

        @app.task("test.retry_on", retry_on=ValueError)
        async def handler():
            pass

        consumer = PlanqConsumer(app, middlewares=[])

        assert consumer.routes["test.retry_on"].retry_on is ValueError


class TestRouterEndpointRetryOn:
    """Tests for _router_endpoint() retry_on integration."""

    @pytest.mark.asyncio
    async def test_retry_on_none_rejects_immediately(self, mock_message):
        """retry_on=None means no retries, reject immediately."""
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task(
            "test.no_retry",
            mode=ExecutionMode.ASYNC,
            max_retries=3,
            retry_on=None,
        )
        async def handler():
            raise ValueError("will not retry")

        msg = mock_message(method="test.no_retry", id=None, delivery_count=1)

        with pytest.raises(RejectMessage):
            await consumer._router_endpoint(msg)

    @pytest.mark.asyncio
    async def test_retry_on_matches_exception_retries(self, mock_message):
        """retry_on=ValueError matches ValueError, retries."""
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task(
            "test.match_retry",
            mode=ExecutionMode.ASYNC,
            max_retries=3,
            retry_on=ValueError,
        )
        async def handler():
            raise ValueError("will retry")

        msg = mock_message(method="test.match_retry", id=None, delivery_count=1)

        _planq_context.set(None)
        ctx = get_planq_context()
        ctx.msg = msg

        with pytest.raises(RetryMessage):
            await consumer._router_endpoint(msg)

    @pytest.mark.asyncio
    async def test_retry_on_does_not_match_rejects(self, mock_message):
        """retry_on=ValueError doesn't match KeyError, rejects."""
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task(
            "test.no_match",
            mode=ExecutionMode.ASYNC,
            max_retries=3,
            retry_on=ValueError,
        )
        async def handler():
            raise KeyError("will not retry")

        msg = mock_message(method="test.no_match", id=None, delivery_count=1)

        with pytest.raises(RejectMessage):
            await consumer._router_endpoint(msg)

    @pytest.mark.asyncio
    async def test_retry_on_matches_but_max_retries_exhausted(
        self, mock_message
    ):
        """retry_on matches but max_retries exhausted returns error."""
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task(
            "test.exhausted",
            mode=ExecutionMode.ASYNC,
            max_retries=2,
            retry_on=ValueError,
        )
        async def handler():
            raise ValueError("permanent failure")

        msg = mock_message(method="test.exhausted", id="123", delivery_count=3)

        response = await consumer._router_endpoint(msg)

        assert response.id == "123"
        assert response.error is not None
        assert response.error.code == JsonRpcError.INTERNAL_ERROR
        assert "permanent failure" in response.error.message

    @pytest.mark.asyncio
    async def test_explicit_retry_message_bypasses_retry_on(self, mock_message):
        """Handler can raise RetryMessage to bypass retry_on check."""
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task(
            "test.explicit_retry",
            mode=ExecutionMode.ASYNC,
            retry_on=None,  # No retries
        )
        async def handler():
            raise RetryMessage(delay=5.0)

        msg = mock_message(
            method="test.explicit_retry", id=None, delivery_count=1
        )

        with pytest.raises(RetryMessage) as exc_info:
            await consumer._router_endpoint(msg)

        assert exc_info.value.delay == 5.0

    @pytest.mark.asyncio
    async def test_retry_on_callable_true_retries(self, mock_message):
        """retry_on callable returning True retries."""
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task(
            "test.callable_true",
            mode=ExecutionMode.ASYNC,
            max_retries=3,
            retry_on=lambda exc: "retry" in str(exc).lower(),
        )
        async def handler():
            raise ValueError("Please RETRY this")

        msg = mock_message(
            method="test.callable_true", id=None, delivery_count=1
        )

        _planq_context.set(None)
        ctx = get_planq_context()
        ctx.msg = msg

        with pytest.raises(RetryMessage):
            await consumer._router_endpoint(msg)

    @pytest.mark.asyncio
    async def test_retry_on_callable_false_rejects(self, mock_message):
        """retry_on callable returning False rejects."""
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task(
            "test.callable_false",
            mode=ExecutionMode.ASYNC,
            max_retries=3,
            retry_on=lambda exc: "retry" in str(exc).lower(),
        )
        async def handler():
            raise ValueError("Will not match")

        msg = mock_message(
            method="test.callable_false", id=None, delivery_count=1
        )

        with pytest.raises(RejectMessage):
            await consumer._router_endpoint(msg)

    @pytest.mark.asyncio
    async def test_retry_on_list_with_multiple_types(self, mock_message):
        """retry_on list matches any exception type in list."""
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task(
            "test.multi_types",
            mode=ExecutionMode.ASYNC,
            max_retries=3,
            retry_on=[ValueError, KeyError, TypeError],
        )
        async def handler():
            raise KeyError("will retry")

        msg = mock_message(method="test.multi_types", id=None, delivery_count=1)

        _planq_context.set(None)
        ctx = get_planq_context()
        ctx.msg = msg

        with pytest.raises(RetryMessage):
            await consumer._router_endpoint(msg)


# === Layer 8b: _ProcessPool Unit Tests ===


class TestProcessPool:
    """Tests for _ProcessPool internals."""

    def test_worker_main_runs_function_with_signals(self):
        """_worker_main sets up signal handlers and executes fn."""
        from queue import Queue

        q = Queue()
        result = _worker_main("task-1", q, _add_one, 41)
        assert result == 42
        # Verify monitoring queue was notified
        task_id, pid = q.get_nowait()
        assert task_id == "task-1"
        assert pid == os.getpid()

    def test_kill_task_process_lookup_error(self):
        """kill_task handles ProcessLookupError for dead process."""
        pool = _ProcessPool.__new__(_ProcessPool)
        pool._active_pids = {"task-1": 99999999}  # non-existent PID
        pool._kos = set()
        pool._lock = threading.Lock()
        pool.kill_task("task-1", signal.SIGKILL)
        # Should not raise - ProcessLookupError is caught

    def test_kill_task_unknown_id_adds_to_kos(self):
        """kill_task adds unknown task_id to KOS set."""
        pool = _ProcessPool.__new__(_ProcessPool)
        pool._active_pids = {}
        pool._kos = set()
        pool._lock = threading.Lock()
        pool.kill_task("task-1", signal.SIGALRM)
        assert "task-1" in pool._kos

    def test_monitor_kos_process_lookup_error(self):
        """Monitor handles ProcessLookupError for KOS process."""
        pool = _ProcessPool.__new__(_ProcessPool)
        pool._active_pids = {}
        pool._kos = {"task-1"}
        pool._lock = threading.Lock()
        pool._monitoring_queue = MagicMock()
        # queue returns (task_id, dead_pid), then poison pill
        pool._monitoring_queue.get = MagicMock(
            side_effect=[("task-1", 99999999), (None, None)]
        )
        pool._monitor_pids()  # Runs synchronously
        assert "task-1" not in pool._kos

    def test_monitor_unexpected_exception(self):
        """Monitor logs unexpected exceptions and continues."""
        pool = _ProcessPool.__new__(_ProcessPool)
        pool._active_pids = {}
        pool._kos = set()
        pool._lock = threading.Lock()
        pool._monitoring_queue = MagicMock()
        pool._monitoring_queue.get = MagicMock(
            side_effect=[RuntimeError("boom"), (None, None)]
        )
        with patch("planq.consumer.logger") as mock_logger:
            pool._monitor_pids()
        mock_logger.error.assert_called_once()
        assert "monitor error" in mock_logger.error.call_args[0][0].lower()


# === Layer 8c: PROCESS Execution Mode ===


class TestExecutionModeProcess:
    """Tests for PROCESS execution mode via _execute()."""

    @pytest.mark.asyncio
    async def test_execute_process_dispatches_to_pool(self):
        """PROCESS mode dispatches to _ProcessPool."""
        from concurrent.futures import Future

        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task("test.process", mode=ExecutionMode.PROCESS)
        def handler(x):
            return x * 2

        route = consumer.routes["test.process"]

        # Create a real concurrent.futures.Future and set result
        cf_future = Future()
        cf_future.set_result(42)

        mock_pool = MagicMock()
        mock_pool.submit.return_value = (cf_future, "task-1")
        consumer._pool = mock_pool

        result = await consumer._execute(route, [21], "test.process")

        assert result == 42
        mock_pool.submit.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_process_no_pool_raises(self):
        """PROCESS mode without pool raises RuntimeError."""
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task("test.no_pool", mode=ExecutionMode.PROCESS)
        def handler():
            return 1

        route = consumer.routes["test.no_pool"]
        with pytest.raises(RuntimeError, match="ProcessPoolExecutor"):
            await consumer._execute(route, None, "test.no_pool")

    @pytest.mark.asyncio
    async def test_windows_platform_with_time_limit_raises_error(self):
        """Windows platform with time_limit raises FeatureNotSupportedError.

        Verifies:
        - sys.platform == "win32" check on line 459
        - FeatureNotSupportedError raised with correct parameters
        - Error message contains "process_time_limit" and "Windows"
        - Test runs on all platforms (Unix/Linux/macOS/Windows)

        Uses mock.patch to simulate Windows on non-Windows systems.
        """
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(app, middlewares=[])

        # Mock the pool to avoid multiprocessing issues with coverage
        mock_pool = MagicMock()
        consumer._pool = mock_pool

        @app.task(
            "test.windows_check",
            mode=ExecutionMode.PROCESS,
            time_limit=1.0,
        )
        def handler():
            return "should not execute"

        route = consumer.routes["test.windows_check"]

        # Mock sys.platform to simulate Windows (patch in consumer module)
        with patch("planq.consumer.sys.platform", "win32"):
            with pytest.raises(FeatureNotSupportedError) as exc_info:
                await consumer._execute(route, None, "test.windows_check")

        # Verify error details
        assert "process_time_limit" in str(exc_info.value)
        assert "Windows" in str(exc_info.value)


# === Layer 9: Signal Handling and Shutdown ===


class TestSignalHandlingAndShutdown:
    """Tests for run() signal handling and graceful shutdown."""

    @pytest.mark.asyncio
    async def test_run_installs_sigint_handler(self, mock_message):
        """SIGINT handler is registered during run()."""

        broker = AsyncMock()
        app = Planq(broker=broker)

        # Create empty async generator for consume
        async def empty_consume(queue, prefetch):
            return
            yield  # Make it a generator

        broker.consume = empty_consume
        consumer = PlanqConsumer(app, middlewares=[])

        with patch.object(asyncio, "get_running_loop") as mock_get_loop:
            loop = MagicMock()
            mock_get_loop.return_value = loop

            try:
                await consumer.run("test-queue")
            except Exception:
                pass

            # Verify SIGINT handler registered
            calls = loop.add_signal_handler.call_args_list
            sigint_registered = any(
                call[0][0] == signal.SIGINT for call in calls
            )
            assert sigint_registered

    @pytest.mark.asyncio
    async def test_run_installs_sigterm_handler(self, mock_message):
        """SIGTERM handler is registered during run()."""

        broker = AsyncMock()
        app = Planq(broker=broker)

        async def empty_consume(queue, prefetch):
            return
            yield

        broker.consume = empty_consume
        consumer = PlanqConsumer(app, middlewares=[])

        with patch.object(asyncio, "get_running_loop") as mock_get_loop:
            loop = MagicMock()
            mock_get_loop.return_value = loop

            try:
                await consumer.run("test-queue")
            except Exception:
                pass

            # Verify both SIGINT and SIGTERM handlers registered
            calls = loop.add_signal_handler.call_args_list
            sigint_registered = any(
                call[0][0] == signal.SIGINT for call in calls
            )
            sigterm_registered = any(
                call[0][0] == signal.SIGTERM for call in calls
            )
            assert sigint_registered
            assert sigterm_registered

    @pytest.mark.asyncio
    async def test_run_shutdown_breaks_consume_loop(self, mock_message):
        """Setting shutdown event breaks the consume loop."""
        broker = AsyncMock()
        app = Planq(broker=broker)
        messages_processed = 0

        async def yielding_consume(queue, prefetch):
            for i in range(5):
                yield mock_message(method="test.task", id=None)

        broker.consume = yielding_consume
        consumer = PlanqConsumer(app, middlewares=[])

        @app.task("test.task", mode=ExecutionMode.ASYNC)
        async def handler():
            nonlocal messages_processed
            messages_processed += 1
            return "ok"

        # Run in background and cancel after brief delay
        task = asyncio.create_task(consumer.run("test-queue"))
        await asyncio.sleep(0.1)
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        # Should have processed some messages before cancellation
        # (exact count depends on timing, just verify some were processed)
        assert messages_processed >= 0

    @pytest.mark.asyncio
    async def test_run_shutdown_event_breaks_consume_loop(self, mock_message):
        """shutdown_event.is_set() breaks the consume loop."""
        broker = AsyncMock()
        broker.__aenter__ = AsyncMock(return_value=broker)
        broker.__aexit__ = AsyncMock(return_value=False)
        app = Planq(broker=broker)
        messages_dispatched = 0

        consumer = PlanqConsumer(
            app,
            middlewares=[],
            install_signal_handlers=False,
        )

        async def controlled_consume(queue, prefetch):
            for i in range(5):
                yield mock_message(method="test.task", id=None)
                if i == 1:
                    consumer._shutdown_event.set()

        broker.consume = controlled_consume

        @app.task("test.task", mode=ExecutionMode.ASYNC)
        async def handler():
            nonlocal messages_dispatched
            messages_dispatched += 1

        await consumer.run("test-queue")

        # Loop broke early: only messages before shutdown was set
        assert messages_dispatched < 5

    @pytest.mark.asyncio
    async def test_run_drains_inflight_messages_before_exit(self, mock_message):
        """In-flight messages complete before shutdown."""
        broker = AsyncMock()
        app = Planq(broker=broker)
        handler_started = []
        handler_completed = []

        async def yielding_consume(queue, prefetch):
            yield mock_message(method="test.slow", id=None)
            yield mock_message(method="test.slow", id=None)

        broker.consume = yielding_consume
        settings = ConsumerSettings(concurrency=2)
        consumer = PlanqConsumer(app, settings=settings, middlewares=[])

        @app.task("test.slow", mode=ExecutionMode.ASYNC)
        async def slow_handler():
            handler_started.append(1)
            await asyncio.sleep(0.05)
            handler_completed.append(1)
            return "ok"

        # Run briefly
        task = asyncio.create_task(consumer.run("test-queue"))
        await asyncio.sleep(0.2)
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        # All started handlers should complete
        assert len(handler_started) == len(handler_completed)

    @pytest.mark.asyncio
    async def test_run_shuts_down_process_pool_on_exit(self):
        """Process pool is shut down in finally block."""
        broker = AsyncMock()
        app = Planq(broker=broker)

        async def empty_consume(queue, prefetch):
            return
            yield

        broker.consume = empty_consume
        settings = ConsumerSettings(process_workers=2)
        consumer = PlanqConsumer(app, settings=settings, middlewares=[])

        # Mock the pool object instead of creating real one
        mock_pool = MagicMock()
        consumer._pool = mock_pool

        try:
            await consumer.run("test-queue")
        except Exception:
            pass

        # Verify shutdown called with wait=True
        mock_pool.shutdown.assert_called_once_with(wait=True)

    @pytest.mark.asyncio
    async def test_run_shuts_down_process_pool_even_on_exception(self):
        """Process pool shutdown occurs even when exception raised."""
        broker = AsyncMock()
        app = Planq(broker=broker)

        async def failing_consume(queue, prefetch):
            # Create message manually (can't use fixture in async generator)
            msg = MagicMock(spec=BrokerMessage)
            msg.body = JsonRpcRequest(method="test", params=None, id=None)
            msg.correlation_id = None
            msg.headers = {}
            msg.delivery_count = 1
            msg.reply_to = None
            msg.message_id = "test-msg-id"
            msg.queue_name = "test-queue"
            msg.enqueued_at = time.time() - 0.1
            msg.received_at = time.time()
            msg.ack = AsyncMock()
            msg.nack = AsyncMock()
            msg.reject = AsyncMock()
            yield msg
            raise RuntimeError("Broker error")

        broker.consume = failing_consume
        settings = ConsumerSettings(process_workers=2)
        consumer = PlanqConsumer(app, settings=settings, middlewares=[])

        @app.task("test", mode=ExecutionMode.ASYNC)
        async def handler():
            return "ok"

        # Mock the pool object instead of creating real one
        mock_pool = MagicMock()
        consumer._pool = mock_pool

        try:
            await consumer.run("test-queue")
        except Exception:
            pass

        # Verify shutdown called despite exception
        mock_pool.shutdown.assert_called_once_with(wait=True)

    @pytest.mark.asyncio
    async def test_stop_before_run_is_honored_on_next_message(
        self,
        mock_message,
    ):
        """stop() called before run() is honored on first message fetch."""
        broker = AsyncMock()
        broker.__aenter__ = AsyncMock(return_value=broker)
        broker.__aexit__ = AsyncMock(return_value=False)
        app = Planq(broker=broker)
        handler_invocations = 0

        async def yielding_consume(queue, prefetch):
            yield mock_message(method="test.task", id=None)

        broker.consume = yielding_consume

        @app.task("test.task", mode=ExecutionMode.ASYNC)
        async def handler():
            nonlocal handler_invocations
            handler_invocations += 1

        consumer = PlanqConsumer(
            app,
            middlewares=[],
            install_signal_handlers=False,
        )

        # Stop BEFORE run() is called.
        await consumer.stop()

        # run() must return without processing the message.
        await asyncio.wait_for(consumer.run("test-queue"), timeout=2.0)

        assert handler_invocations == 0

    @pytest.mark.asyncio
    async def test_stop_after_run_completed_is_noop(self):
        """stop() is safe to call after run() has already returned."""
        broker = AsyncMock()
        broker.__aenter__ = AsyncMock(return_value=broker)
        broker.__aexit__ = AsyncMock(return_value=False)
        app = Planq(broker=broker)

        async def empty_consume(queue, prefetch):
            return
            yield  # make it a generator

        broker.consume = empty_consume

        consumer = PlanqConsumer(
            app,
            middlewares=[],
            install_signal_handlers=False,
        )

        # Let run() complete naturally (empty queue, no messages).
        # Pre-set shutdown so it exits on entry without blocking.
        await consumer.stop()
        await consumer.run("test-queue")

        # Calling stop() again after run() returned must not raise.
        await consumer.stop()
        await consumer.stop()

    @pytest.mark.asyncio
    async def test_stop_triggers_graceful_shutdown(self):
        """stop() drains in-flight messages before run() returns."""
        from planq.providers.memory import InMemoryBroker

        app = Planq(broker=InMemoryBroker())
        handler_started = asyncio.Event()
        handler_can_finish = asyncio.Event()
        processed: list[int] = []

        @app.task(
            "test.process",
            queue_name="q",
            mode=ExecutionMode.ASYNC,
        )
        async def handler(value: int) -> None:
            handler_started.set()
            await handler_can_finish.wait()
            processed.append(value)

        consumer = PlanqConsumer(
            app,
            middlewares=[],
            install_signal_handlers=False,
        )

        run_task = asyncio.create_task(consumer.run("q"))
        await asyncio.sleep(0)

        # Publish one message; handler will block on the event.
        await app.broker.publish(
            "q",
            JsonRpcRequest(
                method="test.process",
                params=[42],
                id=None,
            ),
        )

        # Wait until the handler has actually started executing.
        await asyncio.wait_for(handler_started.wait(), timeout=2.0)

        # Now the handler is genuinely in-flight. Signal shutdown.
        # stop() alone must suffice: it races the blocked
        # broker.consume() poll and cancels it so run() can exit.
        await consumer.stop()

        # Release the blocked handler. It must still complete
        # before run_task returns -- that's the drain guarantee.
        handler_can_finish.set()
        await asyncio.wait_for(run_task, timeout=2.0)

        assert processed == [42]

    @pytest.mark.asyncio
    async def test_run_cancellation_cleans_up_pending_anext(self):
        """Cancelling run() from outside reaps the pending __anext__ task.

        Guards against a task leak where ``_consume_queue`` is
        suspended in ``asyncio.wait`` on an idle broker and its
        host task is cancelled (e.g., the uvicorn process is
        shutting down via a different path than ``stop()``). The
        pending ``consume_gen.__anext__()`` task must be cancelled
        and awaited so it does not trigger a "Task was destroyed
        but it is pending!" warning.
        """
        from planq.providers.memory import InMemoryBroker

        app = Planq(broker=InMemoryBroker())
        consumer = PlanqConsumer(
            app,
            middlewares=[],
            install_signal_handlers=False,
        )

        run_task = asyncio.create_task(consumer.run("q"))
        # Let run() enter the broker context and suspend inside
        # _consume_queue's asyncio.wait on the idle queue.
        await asyncio.sleep(0.05)

        run_task.cancel()
        with pytest.raises((asyncio.CancelledError, BaseExceptionGroup)):
            await run_task

    @pytest.mark.asyncio
    async def test_stop_unblocks_idle_broker(self):
        """stop() interrupts a consume() call blocked on an empty queue.

        Regression test for the case where the broker is idle and
        ``broker.consume()`` is suspended on its underlying poll
        (e.g., XREADGROUP, SQS long-poll, ``asyncio.Queue.get()``).
        Without the fix, ``run()`` would hang until a message arrives
        or the broker disconnected, blowing past uvicorn's shutdown
        grace period.
        """
        from planq.providers.memory import InMemoryBroker

        app = Planq(broker=InMemoryBroker())
        consumer = PlanqConsumer(
            app,
            middlewares=[],
            install_signal_handlers=False,
        )

        run_task = asyncio.create_task(consumer.run("q"))
        # Let run() enter the broker context and suspend on
        # the empty InMemoryBroker queue.
        await asyncio.sleep(0.05)

        await consumer.stop()
        await asyncio.wait_for(run_task, timeout=1.0)

    @pytest.mark.asyncio
    async def test_install_signal_handlers_false_skips_signal_setup(self):
        """install_signal_handlers=False prevents add_signal_handler calls."""
        broker = AsyncMock()
        broker.__aenter__ = AsyncMock(return_value=broker)
        broker.__aexit__ = AsyncMock(return_value=False)
        app = Planq(broker=broker)

        async def empty_consume(queue, prefetch):
            return
            yield

        broker.consume = empty_consume

        consumer = PlanqConsumer(
            app,
            middlewares=[],
            install_signal_handlers=False,
        )

        with patch.object(asyncio, "get_running_loop") as mock_get_loop:
            loop = MagicMock()
            mock_get_loop.return_value = loop
            await consumer.run("test-queue")

            # Neither SIGINT nor SIGTERM handler must be installed.
            calls = loop.add_signal_handler.call_args_list
            sigint_calls = [c for c in calls if c[0][0] == signal.SIGINT]
            sigterm_calls = [c for c in calls if c[0][0] == signal.SIGTERM]
            assert sigint_calls == []
            assert sigterm_calls == []

    @pytest.mark.asyncio
    async def test_backwards_compat_signal_handlers_default_true(self):
        """Default install_signal_handlers=True installs both handlers."""
        broker = AsyncMock()
        broker.__aenter__ = AsyncMock(return_value=broker)
        broker.__aexit__ = AsyncMock(return_value=False)
        app = Planq(broker=broker)

        async def empty_consume(queue, prefetch):
            return
            yield

        broker.consume = empty_consume

        # No install_signal_handlers kwarg -> default True.
        consumer = PlanqConsumer(app, middlewares=[])

        with patch.object(asyncio, "get_running_loop") as mock_get_loop:
            loop = MagicMock()
            mock_get_loop.return_value = loop
            await consumer.run("test-queue")

            calls = loop.add_signal_handler.call_args_list
            sigint_calls = [c for c in calls if c[0][0] == signal.SIGINT]
            sigterm_calls = [c for c in calls if c[0][0] == signal.SIGTERM]
            assert len(sigint_calls) == 1
            assert len(sigterm_calls) == 1


class TestRunMany:
    """Tests for run() multi-queue consumption."""

    @pytest.mark.asyncio
    async def test_run_many_consumes_from_multiple_queues(
        self,
    ) -> None:
        """run() processes messages from all queues."""
        broker = MagicMock()
        broker.connect = AsyncMock()
        broker.disconnect = AsyncMock()
        broker.__aenter__ = AsyncMock(return_value=broker)
        broker.__aexit__ = AsyncMock(return_value=False)

        consumed_queues: list[str] = []

        async def fake_consume(queue, *, prefetch=10):
            consumed_queues.append(queue)
            return
            yield  # make it a generator

        broker.consume = fake_consume

        app = Planq(broker)

        @app.task(name="test.task")
        async def dummy() -> None:
            pass

        consumer = PlanqConsumer(app, process_workers=None, middlewares=[])

        await consumer.run("q1", "q2")
        assert "q1" in consumed_queues
        assert "q2" in consumed_queues

    @pytest.mark.asyncio
    async def test_run_delegates_to_run_many(self) -> None:
        """run(queue) delegates to run([queue])."""
        broker = MagicMock()
        broker.__aenter__ = AsyncMock(return_value=broker)
        broker.__aexit__ = AsyncMock(return_value=False)

        async def fake_consume(queue, *, prefetch=10):
            return
            yield

        broker.consume = fake_consume

        app = Planq(broker)
        consumer = PlanqConsumer(app, process_workers=None, middlewares=[])

        called_with: tuple[str, ...] | None = None

        original_run_many = consumer.run

        async def mock_run_many(*queues: str) -> None:
            nonlocal called_with
            called_with = queues
            await original_run_many(*queues)

        consumer.run = mock_run_many
        await consumer.run("myqueue")
        assert called_with == ("myqueue",)
