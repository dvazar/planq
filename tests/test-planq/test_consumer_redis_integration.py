"""Integration tests for PlanqConsumer with Redis provider."""

from __future__ import annotations

import asyncio
import json
import time

import pytest
import pytest_asyncio

from planq.app import Planq
from planq.consumer import PlanqConsumer
from planq.enums import ExecutionMode, JsonRpcError
from planq.exceptions import RejectMessage, RetryMessage
from planq.middleware import DeadlineMiddleware
from planq.models import (
    ConsumerSettings,
    JsonRpcRequest,
    JsonRpcResponse,
    TaskResult,
)
from planq.providers.redis import RedisBroker, RedisConsumerConfig


@pytest.fixture(scope="module")
def redis_endpoint():
    """Redis endpoint URL."""
    return "redis://localhost:16379"


@pytest_asyncio.fixture
async def redis_broker(redis_endpoint):
    """Connected RedisBroker instance."""
    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="consumer-test-group",
            consumer_name="consumer-test-consumer",
            scheduler_interval=0.5,
        ),
    )
    await broker.connect()
    yield broker
    await broker.disconnect()


@pytest_asyncio.fixture(autouse=True)
async def cleanup_streams(redis_broker):
    """Delete test streams and delayed ZSETs before/after each test."""
    streams = [
        "test-stream",
        "test-stream:delayed",
        "results-stream",
        "results-stream:delayed",
    ]
    for key in streams:
        try:
            await redis_broker._client.delete(key)
        except Exception:
            pass

    yield

    for key in streams:
        try:
            await redis_broker._client.delete(key)
        except Exception:
            pass


async def consume_response(
    broker: RedisBroker, queue: str, timeout: float = 2.0
) -> JsonRpcResponse | None:
    """Consume a single response from results stream."""
    try:
        async with asyncio.timeout(timeout):
            while True:
                entries = await broker._client.xrange(queue, count=1)
                if entries:
                    entry_id, fields = entries[0]
                    response = JsonRpcResponse.model_validate_json(
                        fields["body"]
                    )
                    await broker._client.xdel(queue, entry_id)
                    return response
                await asyncio.sleep(0.1)
    except TimeoutError:
        pass
    return None


async def process_one_message(
    consumer: PlanqConsumer, broker: RedisBroker, queue: str
) -> None:
    """Process one message with timeout."""
    async with asyncio.timeout(3.0):
        async for msg in broker.consume(queue, block_ms=0):
            await consumer._process_message(msg)
            break


# === Layer 1: Basic Handler Execution ===


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_executes_async_handler_with_positional_params(
    redis_broker,
):
    """Consumer executes async handler with positional parameters."""
    app = Planq(broker=redis_broker)
    consumer = PlanqConsumer(app)

    @app.task("math.add", mode=ExecutionMode.ASYNC)
    async def add_numbers(a: int, b: int) -> int:
        return a + b

    request = JsonRpcRequest(method="math.add", params=[10, 32], id="req-1")
    await redis_broker.publish(
        "test-stream", request, reply_to="results-stream"
    )

    await process_one_message(consumer, redis_broker, "test-stream")

    response = await consume_response(redis_broker, "results-stream")
    assert response is not None
    assert response.id == "req-1"
    assert response.result == 42
    assert response.error is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_executes_async_handler_with_named_params(
    redis_broker,
):
    """Consumer executes async handler with named parameters."""
    app = Planq(broker=redis_broker)
    consumer = PlanqConsumer(app)

    @app.task("user.greet", mode=ExecutionMode.ASYNC)
    async def greet(name: str, title: str = "Mr.") -> str:
        return f"Hello, {title} {name}!"

    request = JsonRpcRequest(
        method="user.greet",
        params={"name": "Smith", "title": "Dr."},
        id="req-2",
    )
    await redis_broker.publish(
        "test-stream", request, reply_to="results-stream"
    )

    await process_one_message(consumer, redis_broker, "test-stream")

    response = await consume_response(redis_broker, "results-stream")
    assert response is not None
    assert response.id == "req-2"
    assert response.result == "Hello, Dr. Smith!"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_executes_async_handler_with_no_params(
    redis_broker,
):
    """Consumer executes async handler with no parameters."""
    app = Planq(broker=redis_broker)
    consumer = PlanqConsumer(app)

    @app.task("ping", mode=ExecutionMode.ASYNC)
    async def ping() -> str:
        return "pong"

    request = JsonRpcRequest(method="ping", params=None, id="req-3")
    await redis_broker.publish(
        "test-stream", request, reply_to="results-stream"
    )

    await process_one_message(consumer, redis_broker, "test-stream")

    response = await consume_response(redis_broker, "results-stream")
    assert response is not None
    assert response.id == "req-3"
    assert response.result == "pong"


# === Layer 2: Request/Response Patterns ===


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_notification_does_not_send_response(
    redis_broker,
):
    """Consumer does not send response for notifications (id=None)."""
    app = Planq(broker=redis_broker)
    consumer = PlanqConsumer(app)

    executed = False

    @app.task("notify.log", mode=ExecutionMode.ASYNC)
    async def log_event(message: str) -> None:
        nonlocal executed
        executed = True

    notification = JsonRpcRequest(
        method="notify.log",
        params={"message": "test event"},
        id=None,
    )
    await redis_broker.publish(
        "test-stream",
        notification,
        reply_to="results-stream",
    )

    await process_one_message(consumer, redis_broker, "test-stream")

    assert executed is True

    response = await consume_response(
        redis_broker, "results-stream", timeout=0.5
    )
    assert response is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_returns_task_result_with_headers(
    redis_broker,
):
    """Consumer handles TaskResult with custom headers."""
    app = Planq(broker=redis_broker)
    consumer = PlanqConsumer(app)

    @app.task("api.fetch", mode=ExecutionMode.ASYNC)
    async def fetch_data() -> TaskResult:
        return TaskResult(
            result={"data": "value"},
            headers={
                "x-rate-limit": "100",
                "x-trace-id": "abc123",
            },
        )

    request = JsonRpcRequest(method="api.fetch", id="req-4")
    await redis_broker.publish(
        "test-stream", request, reply_to="results-stream"
    )

    await process_one_message(consumer, redis_broker, "test-stream")

    # Read raw stream entry to verify headers
    async with asyncio.timeout(2.0):
        while True:
            entries = await redis_broker._client.xrange(
                "results-stream", count=1
            )
            if entries:
                break
            await asyncio.sleep(0.1)

    entry_id, fields = entries[0]

    # Verify body
    response = JsonRpcResponse.model_validate_json(fields["body"])
    assert response.result == {"data": "value"}

    # Verify headers in stream entry
    headers = json.loads(fields.get("headers", "{}"))
    assert headers.get("x-rate-limit") == "100"
    assert headers.get("x-trace-id") == "abc123"

    await redis_broker._client.xdel("results-stream", entry_id)


# === Layer 3: Error Handling and Retries ===


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_retries_on_handler_failure(redis_broker):
    """Consumer retries handler on failure and tracks delivery count."""
    app = Planq(broker=redis_broker)
    consumer = PlanqConsumer(app)
    attempts = []

    @app.task(
        "task.flaky",
        mode=ExecutionMode.ASYNC,
        max_retries=2,
        retry_on=Exception,
    )
    async def flaky_handler(value: int) -> int:
        attempts.append(1)
        if len(attempts) < 2:
            raise ValueError("Simulated failure")
        return value * 2

    request = JsonRpcRequest(
        method="task.flaky", params={"value": 21}, id="req-5"
    )
    await redis_broker.publish(
        "test-stream", request, reply_to="results-stream"
    )

    # First attempt (will fail and nack to delayed ZSET)
    await process_one_message(consumer, redis_broker, "test-stream")
    assert len(attempts) == 1

    # Wait for scheduler to migrate from delayed ZSET
    await asyncio.sleep(2.5)

    # Second attempt (will succeed)
    await process_one_message(consumer, redis_broker, "test-stream")
    assert len(attempts) == 2

    response = await consume_response(redis_broker, "results-stream")
    assert response is not None
    assert response.id == "req-5"
    assert response.result == 42


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_returns_error_when_retries_exhausted(
    redis_broker,
):
    """Consumer returns error response when max retries exhausted."""
    settings = ConsumerSettings(max_retries=1)
    app = Planq(broker=redis_broker)
    consumer = PlanqConsumer(app, settings=settings)

    @app.task(
        "task.always_fails",
        mode=ExecutionMode.ASYNC,
        retry_on=Exception,
    )
    async def always_fails() -> None:
        raise ValueError("Permanent failure")

    request = JsonRpcRequest(method="task.always_fails", id="req-6")
    await redis_broker.publish(
        "test-stream", request, reply_to="results-stream"
    )

    # First attempt (nacks to delayed ZSET)
    await process_one_message(consumer, redis_broker, "test-stream")

    # Wait for scheduler to migrate
    await asyncio.sleep(2.5)

    # Second attempt (retries exhausted, delivery_count=2)
    async with asyncio.timeout(3.0):
        async for msg in redis_broker.consume("test-stream", block_ms=0):
            assert msg.delivery_count == 2
            await consumer._process_message(msg)
            break

    response = await consume_response(redis_broker, "results-stream")
    assert response is not None
    assert response.id == "req-6"
    assert response.error is not None
    assert response.error.code == JsonRpcError.INTERNAL_ERROR
    assert "Permanent failure" in response.error.message


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_handles_retry_message_exception(
    redis_broker,
):
    """Consumer nacks message when handler raises RetryMessage."""
    app = Planq(broker=redis_broker)
    consumer = PlanqConsumer(app)
    attempts = []

    @app.task("task.retry_explicit", mode=ExecutionMode.ASYNC)
    async def retry_handler() -> str:
        attempts.append(1)
        if len(attempts) < 2:
            raise RetryMessage(delay=1.0)
        return "success"

    request = JsonRpcRequest(method="task.retry_explicit", id="req-7")
    await redis_broker.publish(
        "test-stream", request, reply_to="results-stream"
    )

    # First attempt (nacks with delay=1.0)
    await process_one_message(consumer, redis_broker, "test-stream")
    assert len(attempts) == 1

    # Wait for delay + scheduler
    await asyncio.sleep(2.5)

    # Second attempt
    await process_one_message(consumer, redis_broker, "test-stream")

    response = await consume_response(redis_broker, "results-stream")
    assert response is not None
    assert response.result == "success"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_handles_reject_message_exception(
    redis_broker,
):
    """Consumer rejects message when handler raises RejectMessage."""
    app = Planq(broker=redis_broker)
    consumer = PlanqConsumer(app)

    @app.task("task.invalid", mode=ExecutionMode.ASYNC)
    async def reject_handler() -> None:
        raise RejectMessage

    request = JsonRpcRequest(method="task.invalid", id="req-8")
    await redis_broker.publish(
        "test-stream", request, reply_to="results-stream"
    )

    await process_one_message(consumer, redis_broker, "test-stream")

    response = await consume_response(
        redis_broker, "results-stream", timeout=0.5
    )
    assert response is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_rejects_unregistered_method(redis_broker):
    """Consumer rejects messages for unregistered methods."""
    app = Planq(broker=redis_broker)
    consumer = PlanqConsumer(app)

    request = JsonRpcRequest(method="unknown.method", id="req-9")
    await redis_broker.publish(
        "test-stream", request, reply_to="results-stream"
    )

    await process_one_message(consumer, redis_broker, "test-stream")

    response = await consume_response(
        redis_broker, "results-stream", timeout=0.5
    )
    assert response is None


# === Layer 4: Middleware Integration ===


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_deadline_middleware_rejects_expired_message(
    redis_broker,
):
    """DeadlineMiddleware rejects messages with expired TTL."""
    app = Planq(broker=redis_broker)
    consumer = PlanqConsumer(app, middlewares=[DeadlineMiddleware()])

    @app.task("task.delayed", mode=ExecutionMode.ASYNC)
    async def delayed_handler() -> str:
        return "should not execute"

    request = JsonRpcRequest(method="task.delayed", id="req-10")
    expire_at = time.time() - 10.0  # Already expired
    await redis_broker.publish(
        "test-stream",
        request,
        expire_at=expire_at,
        reply_to="results-stream",
    )

    await process_one_message(consumer, redis_broker, "test-stream")

    response = await consume_response(redis_broker, "results-stream")
    assert response is not None
    assert response.id == "req-10"
    assert response.error is not None
    assert response.error.code == -32001
    assert "deadline" in response.error.message.lower()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_deadline_middleware_allows_valid_ttl(
    redis_broker,
):
    """DeadlineMiddleware allows messages with valid TTL."""
    app = Planq(broker=redis_broker)
    consumer = PlanqConsumer(app, middlewares=[DeadlineMiddleware()])

    @app.task("task.valid_ttl", mode=ExecutionMode.ASYNC)
    async def valid_handler() -> str:
        return "executed"

    request = JsonRpcRequest(method="task.valid_ttl", id="req-11")
    expire_at = time.time() + 3600.0
    await redis_broker.publish(
        "test-stream",
        request,
        expire_at=expire_at,
        reply_to="results-stream",
    )

    await process_one_message(consumer, redis_broker, "test-stream")

    response = await consume_response(redis_broker, "results-stream")
    assert response is not None
    assert response.id == "req-11"
    assert response.result == "executed"


# === Layer 5: Edge Cases ===


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_handles_multiple_messages_concurrently(
    redis_broker,
):
    """Consumer processes multiple messages concurrently."""
    settings = ConsumerSettings(concurrency=3)
    app = Planq(broker=redis_broker)
    consumer = PlanqConsumer(app, settings=settings)

    executed = []

    @app.task("task.concurrent", mode=ExecutionMode.ASYNC)
    async def concurrent_handler(value: int) -> int:
        await asyncio.sleep(0.1)
        executed.append(value)
        return value * 2

    for i in range(3):
        request = JsonRpcRequest(
            method="task.concurrent",
            params={"value": i},
            id=f"req-{i}",
        )
        await redis_broker.publish(
            "test-stream",
            request,
            reply_to="results-stream",
        )

    processed = 0
    async with asyncio.timeout(5.0):
        async for msg in redis_broker.consume("test-stream", block_ms=0):
            asyncio.create_task(consumer._process_message(msg))
            processed += 1
            if processed >= 3:
                break

    await asyncio.sleep(1.0)

    assert len(executed) == 3
    assert set(executed) == {0, 1, 2}

    responses = []
    for _ in range(3):
        resp = await consume_response(
            redis_broker, "results-stream", timeout=1.0
        )
        if resp:
            responses.append(resp)

    assert len(responses) == 3


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_handler_with_complex_return_type(
    redis_broker,
):
    """Consumer handles complex return types (dict, list, nested)."""
    app = Planq(broker=redis_broker)
    consumer = PlanqConsumer(app)

    @app.task("data.complex", mode=ExecutionMode.ASYNC)
    async def complex_handler() -> dict:
        return {
            "users": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ],
            "meta": {"count": 2, "page": 1},
        }

    request = JsonRpcRequest(method="data.complex", id="req-12")
    await redis_broker.publish(
        "test-stream", request, reply_to="results-stream"
    )

    await process_one_message(consumer, redis_broker, "test-stream")

    response = await consume_response(redis_broker, "results-stream")
    assert response is not None
    assert response.result == {
        "users": [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ],
        "meta": {"count": 2, "page": 1},
    }
