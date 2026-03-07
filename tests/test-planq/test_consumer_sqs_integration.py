"""Integration tests for PlanqConsumer with SQS provider and ElasticMQ."""

from __future__ import annotations

import asyncio
import os
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
from planq.providers.sqs import SqsBroker


@pytest.fixture(scope="module", autouse=True)
def aws_credentials():
    """Set AWS credentials for aiobotocore to connect to ElasticMQ."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    yield
    os.environ.pop("AWS_ACCESS_KEY_ID", None)
    os.environ.pop("AWS_SECRET_ACCESS_KEY", None)
    os.environ.pop("AWS_DEFAULT_REGION", None)


@pytest.fixture(scope="module")
def sqs_endpoint():
    """ElasticMQ endpoint URL."""
    return "http://localhost:19324"


@pytest.fixture(scope="module")
def test_queue_url(sqs_endpoint):
    """Test queue URL for consumer."""
    return f"{sqs_endpoint}/000000000000/test-queue"


@pytest.fixture(scope="module")
def results_queue_url(sqs_endpoint):
    """Results queue URL for responses."""
    return f"{sqs_endpoint}/000000000000/test-queue-results"


@pytest_asyncio.fixture
async def sqs_broker(sqs_endpoint):
    """Connected SqsBroker instance."""
    broker = SqsBroker(dsn=sqs_endpoint)
    await broker.connect()
    yield broker
    await broker.disconnect()


@pytest_asyncio.fixture(autouse=True)
async def purge_queues(sqs_broker, test_queue_url, results_queue_url):
    """Purge test queues before/after each test."""
    for queue_url in [test_queue_url, results_queue_url]:
        try:
            await sqs_broker._client.purge_queue(QueueUrl=queue_url)
        except Exception:
            pass

    yield

    for queue_url in [test_queue_url, results_queue_url]:
        try:
            await sqs_broker._client.purge_queue(QueueUrl=queue_url)
        except Exception:
            pass


async def consume_response(
    broker: SqsBroker, queue_url: str, timeout: float = 2.0
) -> JsonRpcResponse | None:
    """Consume a single response message from results queue."""
    try:
        async with asyncio.timeout(timeout):
            resp = await broker._client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=0,
                AttributeNames=["All"],
                MessageAttributeNames=["All"],
            )
            if "Messages" in resp and resp["Messages"]:
                raw_msg = resp["Messages"][0]
                response = JsonRpcResponse.model_validate_json(raw_msg["Body"])
                # Clean up
                await broker._client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=raw_msg["ReceiptHandle"],
                )
                return response
    except TimeoutError:
        pass
    return None


async def process_one_message(
    consumer: PlanqConsumer, broker: SqsBroker, queue_url: str
) -> None:
    """Helper to process one message with timeout."""
    async with asyncio.timeout(3.0):
        async for msg in broker.consume(queue_url, wait_time_seconds=0):
            await consumer._process_message(msg)
            break


# === Layer 1: Basic Handler Execution ===


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_executes_async_handler_with_positional_params(
    sqs_broker, test_queue_url, results_queue_url
):
    """Consumer executes async handler with positional parameters."""
    app = Planq(broker=sqs_broker)
    consumer = PlanqConsumer(app)

    @app.task("math.add", mode=ExecutionMode.ASYNC)
    async def add_numbers(a: int, b: int) -> int:
        return a + b

    # Publish request
    request = JsonRpcRequest(method="math.add", params=[10, 32], id="req-1")
    await sqs_broker.publish(
        test_queue_url, request, reply_to=results_queue_url
    )

    # Process one message
    await process_one_message(consumer, sqs_broker, test_queue_url)

    # Verify response
    response = await consume_response(sqs_broker, results_queue_url)
    assert response is not None
    assert response.id == "req-1"
    assert response.result == 42
    assert response.error is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_executes_async_handler_with_named_params(
    sqs_broker, test_queue_url, results_queue_url
):
    """Consumer executes async handler with named parameters."""
    app = Planq(broker=sqs_broker)
    consumer = PlanqConsumer(app)

    @app.task("user.greet", mode=ExecutionMode.ASYNC)
    async def greet(name: str, title: str = "Mr.") -> str:
        return f"Hello, {title} {name}!"

    # Publish request
    request = JsonRpcRequest(
        method="user.greet",
        params={"name": "Smith", "title": "Dr."},
        id="req-2",
    )
    await sqs_broker.publish(
        test_queue_url, request, reply_to=results_queue_url
    )

    # Process one message
    await process_one_message(consumer, sqs_broker, test_queue_url)

    # Verify response
    response = await consume_response(sqs_broker, results_queue_url)
    assert response is not None
    assert response.id == "req-2"
    assert response.result == "Hello, Dr. Smith!"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_executes_async_handler_with_no_params(
    sqs_broker, test_queue_url, results_queue_url
):
    """Consumer executes async handler with no parameters."""
    app = Planq(broker=sqs_broker)
    consumer = PlanqConsumer(app)

    @app.task("ping", mode=ExecutionMode.ASYNC)
    async def ping() -> str:
        return "pong"

    # Publish request
    request = JsonRpcRequest(method="ping", params=None, id="req-3")
    await sqs_broker.publish(
        test_queue_url, request, reply_to=results_queue_url
    )

    # Process one message
    await process_one_message(consumer, sqs_broker, test_queue_url)

    # Verify response
    response = await consume_response(sqs_broker, results_queue_url)
    assert response is not None
    assert response.id == "req-3"
    assert response.result == "pong"


# === Layer 2: Request/Response Patterns ===


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_notification_does_not_send_response(
    sqs_broker, test_queue_url, results_queue_url
):
    """Consumer does not send response for notifications (id=None)."""
    app = Planq(broker=sqs_broker)
    consumer = PlanqConsumer(app)

    executed = False

    @app.task("notify.log", mode=ExecutionMode.ASYNC)
    async def log_event(message: str) -> None:
        nonlocal executed
        executed = True

    # Publish notification (id=None)
    notification = JsonRpcRequest(
        method="notify.log", params={"message": "test event"}, id=None
    )
    await sqs_broker.publish(
        test_queue_url, notification, reply_to=results_queue_url
    )

    # Process message
    await process_one_message(consumer, sqs_broker, test_queue_url)

    # Verify handler was executed
    assert executed is True

    # Verify no response was sent
    response = await consume_response(
        sqs_broker, results_queue_url, timeout=0.5
    )
    assert response is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_returns_task_result_with_headers(
    sqs_broker, test_queue_url, results_queue_url
):
    """Consumer handles TaskResult with custom headers."""
    app = Planq(broker=sqs_broker)
    consumer = PlanqConsumer(app)

    @app.task("api.fetch", mode=ExecutionMode.ASYNC)
    async def fetch_data() -> TaskResult:
        return TaskResult(
            result={"data": "value"},
            headers={"x-rate-limit": "100", "x-trace-id": "abc123"},
        )

    # Publish request
    request = JsonRpcRequest(method="api.fetch", id="req-4")
    await sqs_broker.publish(
        test_queue_url, request, reply_to=results_queue_url
    )

    # Process message
    await process_one_message(consumer, sqs_broker, test_queue_url)

    # Verify response - check raw SQS message for headers in MessageAttributes
    async with asyncio.timeout(2.0):
        resp = await sqs_broker._client.receive_message(
            QueueUrl=results_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=0,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        )
        assert "Messages" in resp
        raw_msg = resp["Messages"][0]

        # Verify body
        response = JsonRpcResponse.model_validate_json(raw_msg["Body"])
        assert response.result == {"data": "value"}

        # Verify headers in MessageAttributes
        msg_attrs = raw_msg.get("MessageAttributes", {})
        assert msg_attrs.get("x-rate-limit", {}).get("StringValue") == "100"
        assert msg_attrs.get("x-trace-id", {}).get("StringValue") == "abc123"

        # Clean up
        await sqs_broker._client.delete_message(
            QueueUrl=results_queue_url,
            ReceiptHandle=raw_msg["ReceiptHandle"],
        )


# === Layer 3: Error Handling and Retries ===


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_retries_on_handler_failure(
    sqs_broker, test_queue_url, results_queue_url
):
    """Consumer retries handler on failure and tracks delivery count."""
    app = Planq(broker=sqs_broker)
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

    # Publish request
    request = JsonRpcRequest(
        method="task.flaky", params={"value": 21}, id="req-5"
    )
    await sqs_broker.publish(
        test_queue_url, request, reply_to=results_queue_url
    )

    # Process first attempt (will fail and nack)
    await process_one_message(consumer, sqs_broker, test_queue_url)

    assert len(attempts) == 1

    # Wait for message to become visible again
    await asyncio.sleep(1.5)

    # Process second attempt (will succeed)
    await process_one_message(consumer, sqs_broker, test_queue_url)

    assert len(attempts) == 2

    # Verify success response
    response = await consume_response(sqs_broker, results_queue_url)
    assert response is not None
    assert response.id == "req-5"
    assert response.result == 42


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_returns_error_when_retries_exhausted(
    sqs_broker, test_queue_url, results_queue_url
):
    """Consumer returns error response when max retries exhausted."""
    settings = ConsumerSettings(max_retries=1)
    app = Planq(broker=sqs_broker)
    consumer = PlanqConsumer(app, settings=settings)

    @app.task(
        "task.always_fails",
        mode=ExecutionMode.ASYNC,
        retry_on=Exception,
    )
    async def always_fails() -> None:
        raise ValueError("Permanent failure")

    # Publish request
    request = JsonRpcRequest(method="task.always_fails", id="req-6")
    await sqs_broker.publish(
        test_queue_url, request, reply_to=results_queue_url
    )

    # Process first attempt
    await process_one_message(consumer, sqs_broker, test_queue_url)

    await asyncio.sleep(1.5)

    # Process second attempt (retries exhausted)
    async with asyncio.timeout(3.0):
        async for msg in sqs_broker.consume(
            test_queue_url, wait_time_seconds=0
        ):
            # Delivery count should be 2
            assert msg.delivery_count == 2
            await consumer._process_message(msg)
            break

    # Verify error response
    response = await consume_response(sqs_broker, results_queue_url)
    assert response is not None
    assert response.id == "req-6"
    assert response.error is not None
    assert response.error.code == JsonRpcError.INTERNAL_ERROR
    assert "Permanent failure" in response.error.message


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_handles_retry_message_exception(
    sqs_broker, test_queue_url, results_queue_url
):
    """Consumer nacks message when handler raises RetryMessage."""
    app = Planq(broker=sqs_broker)
    consumer = PlanqConsumer(app)
    attempts = []

    @app.task("task.retry_explicit", mode=ExecutionMode.ASYNC)
    async def retry_handler() -> str:
        attempts.append(1)
        if len(attempts) < 2:
            raise RetryMessage(delay=1.0)
        return "success"

    # Publish request
    request = JsonRpcRequest(method="task.retry_explicit", id="req-7")
    await sqs_broker.publish(
        test_queue_url, request, reply_to=results_queue_url
    )

    # Process first attempt
    await process_one_message(consumer, sqs_broker, test_queue_url)

    assert len(attempts) == 1

    # Wait for delay
    await asyncio.sleep(1.5)

    # Process second attempt
    await process_one_message(consumer, sqs_broker, test_queue_url)

    # Verify success response
    response = await consume_response(sqs_broker, results_queue_url)
    assert response is not None
    assert response.result == "success"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_handles_reject_message_exception(
    sqs_broker, test_queue_url, results_queue_url
):
    """Consumer rejects message when handler raises RejectMessage."""
    app = Planq(broker=sqs_broker)
    consumer = PlanqConsumer(app)

    @app.task("task.invalid", mode=ExecutionMode.ASYNC)
    async def reject_handler() -> None:
        raise RejectMessage

    # Publish request
    request = JsonRpcRequest(method="task.invalid", id="req-8")
    await sqs_broker.publish(
        test_queue_url, request, reply_to=results_queue_url
    )

    # Process message
    await process_one_message(consumer, sqs_broker, test_queue_url)

    # Verify message was rejected (no response sent)
    response = await consume_response(
        sqs_broker, results_queue_url, timeout=0.5
    )
    assert response is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_rejects_unregistered_method(
    sqs_broker, test_queue_url, results_queue_url
):
    """Consumer rejects messages for unregistered methods without response."""
    app = Planq(broker=sqs_broker)
    consumer = PlanqConsumer(app)

    # Publish request for unknown method
    request = JsonRpcRequest(method="unknown.method", id="req-9")
    await sqs_broker.publish(
        test_queue_url, request, reply_to=results_queue_url
    )

    # Process message
    await process_one_message(consumer, sqs_broker, test_queue_url)

    # Verify no response sent (consumer rejects without reply)
    response = await consume_response(
        sqs_broker, results_queue_url, timeout=0.5
    )
    assert response is None


# === Layer 4: Middleware Integration ===


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_deadline_middleware_rejects_expired_message(
    sqs_broker, test_queue_url, results_queue_url
):
    """DeadlineMiddleware rejects messages with expired TTL."""
    app = Planq(broker=sqs_broker)
    consumer = PlanqConsumer(app, middlewares=[DeadlineMiddleware()])

    @app.task("task.delayed", mode=ExecutionMode.ASYNC)
    async def delayed_handler() -> str:
        return "should not execute"

    # Publish request with expired TTL
    request = JsonRpcRequest(method="task.delayed", id="req-10")
    expire_at = time.time() - 10.0  # Already expired
    await sqs_broker.publish(
        test_queue_url, request, expire_at=expire_at, reply_to=results_queue_url
    )

    # Process message
    await process_one_message(consumer, sqs_broker, test_queue_url)

    # Verify error response with custom deadline error code
    response = await consume_response(sqs_broker, results_queue_url)
    assert response is not None
    assert response.id == "req-10"
    assert response.error is not None
    assert response.error.code == -32001  # DeadlineMiddleware custom code
    assert "deadline" in response.error.message.lower()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_deadline_middleware_allows_valid_ttl(
    sqs_broker, test_queue_url, results_queue_url
):
    """DeadlineMiddleware allows messages with valid TTL."""
    app = Planq(broker=sqs_broker)
    consumer = PlanqConsumer(app, middlewares=[DeadlineMiddleware()])

    @app.task("task.valid_ttl", mode=ExecutionMode.ASYNC)
    async def valid_handler() -> str:
        return "executed"

    # Publish request with valid TTL
    request = JsonRpcRequest(method="task.valid_ttl", id="req-11")
    expire_at = time.time() + 3600.0  # Valid for 1 hour
    await sqs_broker.publish(
        test_queue_url, request, expire_at=expire_at, reply_to=results_queue_url
    )

    # Process message
    await process_one_message(consumer, sqs_broker, test_queue_url)

    # Verify success response
    response = await consume_response(sqs_broker, results_queue_url)
    assert response is not None
    assert response.id == "req-11"
    assert response.result == "executed"


# === Layer 5: Edge Cases ===


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_handles_multiple_messages_concurrently(
    sqs_broker, test_queue_url, results_queue_url
):
    """Consumer processes multiple messages concurrently."""
    settings = ConsumerSettings(concurrency=3)
    app = Planq(broker=sqs_broker)
    consumer = PlanqConsumer(app, settings=settings)

    executed = []

    @app.task("task.concurrent", mode=ExecutionMode.ASYNC)
    async def concurrent_handler(value: int) -> int:
        await asyncio.sleep(0.1)  # Simulate work
        executed.append(value)
        return value * 2

    # Publish multiple requests
    for i in range(3):
        request = JsonRpcRequest(
            method="task.concurrent", params={"value": i}, id=f"req-{i}"
        )
        await sqs_broker.publish(
            test_queue_url, request, reply_to=results_queue_url
        )

    # Process all messages
    processed = 0
    async with asyncio.timeout(5.0):
        async for msg in sqs_broker.consume(
            test_queue_url, wait_time_seconds=0
        ):
            # Process without awaiting to allow concurrency
            asyncio.create_task(consumer._process_message(msg))
            processed += 1
            if processed >= 3:
                break

    # Wait for all tasks to complete
    await asyncio.sleep(1.0)

    # Verify all handlers executed
    assert len(executed) == 3
    assert set(executed) == {0, 1, 2}

    # Verify all responses received
    responses = []
    for _ in range(3):
        resp = await consume_response(
            sqs_broker, results_queue_url, timeout=1.0
        )
        if resp:
            responses.append(resp)

    assert len(responses) == 3


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_handler_with_complex_return_type(
    sqs_broker, test_queue_url, results_queue_url
):
    """Consumer handles complex return types (dict, list, nested)."""
    app = Planq(broker=sqs_broker)
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

    # Publish request
    request = JsonRpcRequest(method="data.complex", id="req-12")
    await sqs_broker.publish(
        test_queue_url, request, reply_to=results_queue_url
    )

    # Process message
    await process_one_message(consumer, sqs_broker, test_queue_url)

    # Verify response
    response = await consume_response(sqs_broker, results_queue_url)
    assert response is not None
    assert response.result == {
        "users": [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ],
        "meta": {"count": 2, "page": 1},
    }
