"""Comprehensive tests for QanatConsumer (ASYNC mode only)."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from qanat import types as qanat_types
from qanat.consumer import DEFAULT_MAX_RETRIES, QanatConsumer
from qanat.enums import ExecutionMode, JsonRpcError
from qanat.exceptions import RejectMessage, RetryMessage
from qanat.message import BrokerMessage
from qanat.middleware import DeadlineMiddleware, Middleware
from qanat.models import (
    ConsumerSettings,
    JsonRpcRequest,
    JsonRpcResponse,
    TaskResult,
    TaskRoute,
)

# Rebuild models with proper type namespace
JsonRpcRequest.model_rebuild(_types_namespace=qanat_types.__dict__)
JsonRpcResponse.model_rebuild(_types_namespace=qanat_types.__dict__)
TaskRoute.model_rebuild(
    _types_namespace={**qanat_types.__dict__, "ExecutionMode": ExecutionMode}
)


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
        msg.enqueued_at = enqueued_at or time.time() - 0.1
        msg.received_at = received_at or time.time()
        msg.ack = AsyncMock()
        msg.nack = AsyncMock()
        msg.reject = AsyncMock()
        return msg

    return _create


# === Layer 1: Task Registration ===


class TestTaskRegistration:
    """Tests for @consumer.task() decorator and route management."""

    def test_task_decorator_registers_handler(self):
        """@consumer.task() registers handler in routes dict."""
        broker = MagicMock()
        consumer = QanatConsumer(broker)

        @consumer.task("my.method", mode=ExecutionMode.ASYNC)
        async def handler(x: int) -> int:
            return x * 2

        assert "my.method" in consumer.routes
        route = consumer.routes["my.method"]
        assert route.handler is handler
        assert route.mode == ExecutionMode.ASYNC

    def test_task_decorator_returns_function_unchanged(self):
        """Decorator returns the original function."""
        broker = MagicMock()
        consumer = QanatConsumer(broker)

        async def handler(x: int) -> int:
            return x * 2

        decorated = consumer.task("test.method")(handler)
        assert decorated is handler

    def test_handler_alias_works_identically(self):
        """consumer.handler() is an alias for consumer.task()."""
        broker = MagicMock()
        consumer = QanatConsumer(broker)

        @consumer.handler("my.method", mode=ExecutionMode.ASYNC)
        async def handler(x: int) -> int:
            return x * 2

        assert "my.method" in consumer.routes
        route = consumer.routes["my.method"]
        assert route.handler is handler

    def test_duplicate_task_names_overwrite(self):
        """Registering same name twice overwrites previous handler."""
        broker = MagicMock()
        consumer = QanatConsumer(broker)

        @consumer.task("duplicate")
        async def first_handler():
            return "first"

        @consumer.task("duplicate")
        async def second_handler():
            return "second"

        assert consumer.routes["duplicate"].handler is second_handler

    def test_task_stores_max_retries(self):
        """max_retries parameter is stored in TaskRoute."""
        broker = MagicMock()
        consumer = QanatConsumer(broker)

        @consumer.task("test", max_retries=5)
        async def handler():
            pass

        assert consumer.routes["test"].max_retries == 5

    def test_task_stores_time_limit(self):
        """time_limit parameter is stored in TaskRoute."""
        broker = MagicMock()
        consumer = QanatConsumer(broker)

        @consumer.task("test", time_limit=30.0)
        async def handler():
            pass

        assert consumer.routes["test"].time_limit == 30.0

    def test_task_validates_max_retries_non_negative(self):
        """max_retries must be >= 0."""
        broker = MagicMock()
        consumer = QanatConsumer(broker)

        with pytest.raises(ValidationError) as exc_info:

            @consumer.task("test", max_retries=-1)
            async def handler():
                pass

        errors = exc_info.value.errors()
        assert any(error["loc"] == ("max_retries",) for error in errors)

    def test_task_validates_time_limit_positive(self):
        """time_limit must be > 0 when specified."""
        broker = MagicMock()
        consumer = QanatConsumer(broker)

        with pytest.raises(ValidationError) as exc_info:

            @consumer.task("test", time_limit=0.0)
            async def handler():
                pass

        errors = exc_info.value.errors()
        assert any(error["loc"] == ("time_limit",) for error in errors)

    def test_task_accepts_zero_max_retries(self):
        """max_retries=0 means one attempt, no retries."""
        broker = MagicMock()
        consumer = QanatConsumer(broker)

        @consumer.task("test", max_retries=0)
        async def handler():
            pass

        assert consumer.routes["test"].max_retries == 0


# === Layer 2: Retry Logic ===


class TestRetryLogic:
    """Tests for backoff calculation and retry limit resolution."""

    def test_default_max_retries_constant(self):
        """DEFAULT_MAX_RETRIES is 3."""
        assert DEFAULT_MAX_RETRIES == 3

    def test_calculate_backoff_returns_float(self):
        """_calculate_backoff returns a float."""
        broker = MagicMock()
        consumer = QanatConsumer(broker)

        backoff = consumer._calculate_backoff(delivery_count=1)
        assert isinstance(backoff, float)

    def test_calculate_backoff_in_valid_range(self):
        """Backoff is between 0 and exponential_cap."""
        broker = MagicMock()
        settings = ConsumerSettings(
            retry_base_delay=2.0,
            retry_max_delay=100.0,
        )
        consumer = QanatConsumer(broker, settings=settings)

        # delivery_count=3 → 2^(3-1) = 4 → cap = min(100, 2*4) = 8
        for _ in range(100):
            backoff = consumer._calculate_backoff(delivery_count=3)
            assert 0 <= backoff <= 8.0

    def test_calculate_backoff_respects_max_delay(self):
        """Backoff never exceeds retry_max_delay."""
        broker = MagicMock()
        settings = ConsumerSettings(
            retry_base_delay=10.0,
            retry_max_delay=30.0,
        )
        consumer = QanatConsumer(broker, settings=settings)

        # delivery_count=10 would give huge exponential, but capped at 30
        for _ in range(100):
            backoff = consumer._calculate_backoff(delivery_count=10)
            assert backoff <= 30.0

    def test_get_max_retries_route_priority(self):
        """Route max_retries takes priority."""
        broker = MagicMock()
        settings = ConsumerSettings(max_retries=5)
        consumer = QanatConsumer(broker, settings=settings)

        route = TaskRoute(
            handler=lambda: None,
            mode=ExecutionMode.ASYNC,
            max_retries=10,
        )

        assert consumer._get_max_retries(route) == 10

    def test_get_max_retries_settings_priority(self):
        """Settings max_retries used when route is None."""
        broker = MagicMock()
        settings = ConsumerSettings(max_retries=7)
        consumer = QanatConsumer(broker, settings=settings)

        route = TaskRoute(
            handler=lambda: None,
            mode=ExecutionMode.ASYNC,
            max_retries=None,
        )

        assert consumer._get_max_retries(route) == 7

    def test_get_max_retries_default_fallback(self):
        """DEFAULT_MAX_RETRIES used when both route and settings are None."""
        broker = MagicMock()
        settings = ConsumerSettings(max_retries=None)
        consumer = QanatConsumer(broker, settings=settings)

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
        consumer = QanatConsumer(broker, middlewares=[])

        handler_called = False

        @consumer.task("test.lookup", mode=ExecutionMode.ASYNC)
        async def handler():
            nonlocal handler_called
            handler_called = True
            return "success"

        msg = mock_message(method="test.lookup", id="123")

        response = await consumer._router_endpoint(msg)

        assert handler_called
        assert response.result == "success"

    @pytest.mark.asyncio
    async def test_handler_execution_positional_params(self, mock_message):
        """Handler receives positional params from list."""
        broker = MagicMock()
        consumer = QanatConsumer(broker, middlewares=[])

        received_args = None

        @consumer.task("test.positional", mode=ExecutionMode.ASYNC)
        async def handler(a: int, b: str, c: float):
            nonlocal received_args
            received_args = (a, b, c)
            return "ok"

        msg = mock_message(method="test.positional", params=[42, "hello", 3.14])

        await consumer._router_endpoint(msg)

        assert received_args == (42, "hello", 3.14)

    @pytest.mark.asyncio
    async def test_handler_execution_named_params(self, mock_message):
        """Handler receives named params from dict."""
        broker = MagicMock()
        consumer = QanatConsumer(broker, middlewares=[])

        received_kwargs = None

        @consumer.task("test.named", mode=ExecutionMode.ASYNC)
        async def handler(name: str, age: int):
            nonlocal received_kwargs
            received_kwargs = {"name": name, "age": age}
            return "ok"

        msg = mock_message(
            method="test.named", params={"name": "Alice", "age": 30}
        )

        await consumer._router_endpoint(msg)

        assert received_kwargs == {"name": "Alice", "age": 30}

    @pytest.mark.asyncio
    async def test_notification_returns_none(self, mock_message):
        """Notification (id=None) returns None response."""
        broker = MagicMock()
        consumer = QanatConsumer(broker, middlewares=[])

        @consumer.task("test.notification", mode=ExecutionMode.ASYNC)
        async def handler():
            return "done"

        msg = mock_message(method="test.notification", id=None)

        response = await consumer._router_endpoint(msg)

        assert response is None

    @pytest.mark.asyncio
    async def test_task_result_headers_included_in_response(self, mock_message):
        """TaskResult headers merged into JsonRpcResponse."""
        broker = MagicMock()
        consumer = QanatConsumer(broker, middlewares=[])

        @consumer.task("test.headers", mode=ExecutionMode.ASYNC)
        async def handler():
            return TaskResult(
                result={"data": "value"},
                headers={"x-custom": "header-value"},
            )

        msg = mock_message(method="test.headers", id="123")

        response = await consumer._router_endpoint(msg)

        assert response.headers["x-custom"] == "header-value"
        assert response.result == {"data": "value"}


# === Layer 4: Error Handling ===


class TestErrorHandling:
    """Tests for exception handling and error responses."""

    @pytest.mark.asyncio
    async def test_method_not_found_raises_reject_message(self, mock_message):
        """Unknown method raises RejectMessage."""
        broker = MagicMock()
        consumer = QanatConsumer(broker, middlewares=[])

        msg = mock_message(method="unknown.method", id="123")

        with pytest.raises(RejectMessage):
            await consumer._router_endpoint(msg)

    @pytest.mark.asyncio
    async def test_retry_message_propagates_unchanged(self, mock_message):
        """RetryMessage raised by handler propagates."""
        broker = MagicMock()
        consumer = QanatConsumer(broker, middlewares=[])

        @consumer.task("test.retry", mode=ExecutionMode.ASYNC)
        async def handler():
            raise RetryMessage(delay=5.0)

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
        consumer = QanatConsumer(broker, middlewares=[])

        @consumer.task("test.error", mode=ExecutionMode.ASYNC, max_retries=3)
        async def handler():
            raise ValueError("something went wrong")

        msg = mock_message(method="test.error", id="123", delivery_count=1)

        with pytest.raises(RetryMessage):
            await consumer._router_endpoint(msg)

    @pytest.mark.asyncio
    async def test_generic_exception_error_response_if_retries_exhausted(
        self, mock_message
    ):
        """Generic exception returns error response if retries exhausted."""
        broker = MagicMock()
        consumer = QanatConsumer(broker, middlewares=[])

        @consumer.task(
            "test.exhausted", mode=ExecutionMode.ASYNC, max_retries=2
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
    async def test_handler_timeout_treated_as_retriable(self, mock_message):
        """HandlerTimeout is retriable if attempts remain."""
        broker = MagicMock()
        consumer = QanatConsumer(broker, middlewares=[])

        @consumer.task(
            "test.timeout",
            mode=ExecutionMode.ASYNC,
            max_retries=3,
            time_limit=0.01,
        )
        async def handler():
            await asyncio.sleep(1.0)  # Exceeds time_limit

        msg = mock_message(method="test.timeout", id="123", delivery_count=1)

        with pytest.raises(RetryMessage):
            await consumer._router_endpoint(msg)


# === Layer 5: Middleware Integration ===


class TestMiddlewareIntegration:
    """Tests for middleware pipeline construction and execution."""

    @pytest.mark.asyncio
    async def test_middleware_called_before_router(self, mock_message):
        """Middleware runs before router endpoint."""
        broker = MagicMock()

        class TrackingMiddleware(Middleware):
            def __init__(self):
                self.called = False

            async def __call__(self, msg, call_next):
                self.called = True
                return await call_next(msg)

        tracking = TrackingMiddleware()
        consumer = QanatConsumer(broker, middlewares=[tracking])

        @consumer.task("test.middleware", mode=ExecutionMode.ASYNC)
        async def handler():
            return "ok"

        msg = mock_message(method="test.middleware", id="123")

        await consumer._pipeline(msg)

        assert tracking.called

    @pytest.mark.asyncio
    async def test_middleware_order_preserved(self, mock_message):
        """Middleware executes in order: first registered runs first."""
        broker = MagicMock()
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
        consumer = QanatConsumer(broker, middlewares=[mw1, mw2])

        @consumer.task("test.order", mode=ExecutionMode.ASYNC)
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
        consumer = QanatConsumer(broker, middlewares=[DeadlineMiddleware()])

        handler_called = False

        @consumer.task("test.expired", mode=ExecutionMode.ASYNC)
        async def handler():
            nonlocal handler_called
            handler_called = True
            return "should not run"

        # Create message with expired TTL
        expired_time = time.time() - 100  # 100 seconds ago
        msg = mock_message(method="test.expired", id="123")
        msg.headers = {"x-expire-at": str(expired_time)}

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
        consumer = QanatConsumer(broker, middlewares=[])

        @consumer.task("test.no_middleware", mode=ExecutionMode.ASYNC)
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
        consumer = QanatConsumer(broker, middlewares=[])

        @consumer.task("test.ack", mode=ExecutionMode.ASYNC)
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
        consumer = QanatConsumer(broker, middlewares=[])

        @consumer.task("test.nack", mode=ExecutionMode.ASYNC, max_retries=3)
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
    async def test_reject_message_rejects(self, mock_message):
        """RejectMessage calls msg.reject()."""
        broker = AsyncMock()
        consumer = QanatConsumer(broker, middlewares=[])

        msg = mock_message(method="unknown.method", id=None)

        await consumer._process_message(msg)

        msg.reject.assert_called_once()
        msg.ack.assert_not_called()
        msg.nack.assert_not_called()

    @pytest.mark.asyncio
    async def test_response_published_to_reply_to_queue(self, mock_message):
        """Successful request publishes response to reply_to."""
        broker = AsyncMock()
        consumer = QanatConsumer(broker, middlewares=[])

        @consumer.task("test.publish", mode=ExecutionMode.ASYNC)
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


# === Layer 7: Context Population ===


class TestContextPopulation:
    """Tests for QanatContext field population."""

    @pytest.mark.asyncio
    async def test_context_msg_populated(self, mock_message):
        """ctx.msg is populated before pipeline."""
        from qanat.context import get_qanat_context

        broker = AsyncMock()
        consumer = QanatConsumer(broker, middlewares=[])

        captured_ctx_msg = None

        @consumer.task("test.ctx", mode=ExecutionMode.ASYNC)
        async def handler():
            nonlocal captured_ctx_msg
            ctx = get_qanat_context()
            captured_ctx_msg = ctx.msg
            return "ok"

        msg = mock_message(method="test.ctx", id=None)

        await consumer._process_message(msg)

        assert captured_ctx_msg is msg

    @pytest.mark.asyncio
    async def test_context_route_populated(self, mock_message):
        """ctx.route is populated before handler execution."""
        from qanat.context import get_qanat_context

        broker = AsyncMock()
        consumer = QanatConsumer(broker, middlewares=[])

        captured_route = None

        @consumer.task("test.route", mode=ExecutionMode.ASYNC, max_retries=5)
        async def handler():
            nonlocal captured_route
            ctx = get_qanat_context()
            captured_route = ctx.route
            return "ok"

        msg = mock_message(method="test.route", id=None)

        await consumer._process_message(msg)

        assert captured_route is not None
        assert captured_route.handler is handler
        assert captured_route.max_retries == 5

    @pytest.mark.asyncio
    async def test_context_max_attempts_calculated(self, mock_message):
        """ctx.max_attempts = max_retries + 1."""
        from qanat.context import get_qanat_context

        broker = AsyncMock()
        consumer = QanatConsumer(broker, middlewares=[])

        captured_max_attempts = None

        @consumer.task("test.attempts", mode=ExecutionMode.ASYNC, max_retries=3)
        async def handler():
            nonlocal captured_max_attempts
            ctx = get_qanat_context()
            captured_max_attempts = ctx.max_attempts
            return "ok"

        msg = mock_message(method="test.attempts", id=None)

        await consumer._process_message(msg)

        assert captured_max_attempts == 4  # 3 retries + 1 attempt
