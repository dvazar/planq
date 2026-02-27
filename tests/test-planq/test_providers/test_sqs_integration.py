"""Integration tests for SQS provider with real ElasticMQ."""

from __future__ import annotations

import asyncio
import os
import time

import pytest
import pytest_asyncio

from planq.enums import Header
from planq.message import BrokerMessage
from planq.models import JsonRpcRequest, JsonRpcResponse
from planq.providers.sqs import SqsBroker


async def consume_one(
    broker: SqsBroker, queue_url: str, timeout: float = 1.0
) -> BrokerMessage | None:
    """Consume a single message with timeout, return None if no message."""
    try:
        async with asyncio.timeout(timeout):
            async for msg in broker.consume(queue_url, wait_time_seconds=0):
                return msg
    except TimeoutError:
        return None
    return None


async def consume_all(
    broker: SqsBroker, queue_url: str, timeout: float = 1.0
) -> list[BrokerMessage]:
    """Consume all available messages with timeout."""
    messages = []
    try:
        async with asyncio.timeout(timeout):
            async for msg in broker.consume(queue_url, wait_time_seconds=0):
                messages.append(msg)
    except TimeoutError:
        pass
    return messages


@pytest.fixture(scope="module", autouse=True)
def aws_credentials():
    """Set AWS credentials for aiobotocore to connect to ElasticMQ."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    yield
    # Cleanup
    os.environ.pop("AWS_ACCESS_KEY_ID", None)
    os.environ.pop("AWS_SECRET_ACCESS_KEY", None)
    os.environ.pop("AWS_DEFAULT_REGION", None)


@pytest.fixture(scope="module")
def sqs_endpoint():
    """ElasticMQ endpoint URL."""
    return "http://localhost:19324"


@pytest.fixture(scope="module")
def test_queue_url(sqs_endpoint):
    """Test queue URL."""
    return f"{sqs_endpoint}/000000000000/test-queue"


@pytest.fixture(scope="module")
def results_queue_url(sqs_endpoint):
    """Results queue URL."""
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
    try:
        await sqs_broker._client.purge_queue(QueueUrl=test_queue_url)
    except Exception:
        pass
    try:
        await sqs_broker._client.purge_queue(QueueUrl=results_queue_url)
    except Exception:
        pass

    yield

    try:
        await sqs_broker._client.purge_queue(QueueUrl=test_queue_url)
    except Exception:
        pass
    try:
        await sqs_broker._client.purge_queue(QueueUrl=results_queue_url)
    except Exception:
        pass


@pytest.mark.integration
@pytest.mark.asyncio
async def test_publish_and_consume(sqs_broker, test_queue_url):
    """Full round-trip: publish → consume → ack."""
    request = JsonRpcRequest(
        method="test.method", params={"key": "value"}, id="123"
    )
    message_id = await sqs_broker.publish(test_queue_url, request)
    assert message_id is not None

    msg = await consume_one(sqs_broker, test_queue_url)
    assert msg is not None
    assert msg.body.method == "test.method"
    assert msg.body.params == {"key": "value"}
    assert msg.message_id == message_id
    await msg.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_message_with_delay(sqs_broker, test_queue_url):
    """Message with delay is not immediately visible."""
    request = JsonRpcRequest(method="delayed.task", id="delay-123")
    await sqs_broker.publish(test_queue_url, request, delay=2)

    # Message should not be visible immediately
    msg = await consume_one(sqs_broker, test_queue_url, timeout=0.5)
    assert msg is None

    # Wait for delay to expire
    await asyncio.sleep(2.5)

    # Now message should be available
    msg = await consume_one(sqs_broker, test_queue_url)
    assert msg is not None
    assert msg.body.method == "delayed.task"
    await msg.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_expire_at_header(sqs_broker, test_queue_url):
    """TTL header is preserved."""
    expire_at = time.time() + 3600
    request = JsonRpcRequest(method="ttl.task", id="ttl-123")
    await sqs_broker.publish(test_queue_url, request, expire_at=expire_at)

    msg = await consume_one(sqs_broker, test_queue_url)
    assert msg is not None
    assert Header.EXPIRE_AT in msg.headers
    received = float(msg.headers[Header.EXPIRE_AT])
    assert abs(received - expire_at) < 1.0
    await msg.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reply_to_header(sqs_broker, test_queue_url, results_queue_url):
    """Reply-to queue is preserved."""
    request = JsonRpcRequest(method="echo", params=["hello"], id="echo-123")
    await sqs_broker.publish(
        test_queue_url, request, reply_to=results_queue_url
    )

    msg = await consume_one(sqs_broker, test_queue_url)
    assert msg is not None
    assert msg.reply_to == results_queue_url
    await msg.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ack_removes_message(sqs_broker, test_queue_url):
    """Message deleted after ack()."""
    request = JsonRpcRequest(method="ack.test", id="ack-123")
    await sqs_broker.publish(test_queue_url, request)

    msg = await consume_one(sqs_broker, test_queue_url)
    assert msg is not None
    await msg.ack()

    # Verify message was deleted
    msg = await consume_one(sqs_broker, test_queue_url, timeout=0.5)
    assert msg is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_nack_redelivers_message(sqs_broker, test_queue_url):
    """Message redelivered after nack()."""
    request = JsonRpcRequest(method="nack.test", id="nack-123")
    await sqs_broker.publish(test_queue_url, request)

    msg = await consume_one(sqs_broker, test_queue_url)
    assert msg is not None
    orig_count = msg.delivery_count
    await msg.nack(delay=1.0)

    await asyncio.sleep(1.5)

    msg = await consume_one(sqs_broker, test_queue_url)
    assert msg is not None
    assert msg.body.method == "nack.test"
    assert msg.delivery_count == orig_count + 1
    await msg.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_request_response_pattern(
    sqs_broker, test_queue_url, results_queue_url
):
    """Full request/response cycle using two queues."""
    request = JsonRpcRequest(
        method="compute.sum",
        params={"a": 10, "b": 32},
        id="req-456",
    )
    await sqs_broker.publish(
        test_queue_url, request, reply_to=results_queue_url
    )

    # Consumer receives request
    req_msg = await consume_one(sqs_broker, test_queue_url)
    assert req_msg is not None
    assert req_msg.body.method == "compute.sum"
    assert req_msg.reply_to == results_queue_url

    result = req_msg.body.params["a"] + req_msg.body.params["b"]

    # Send response
    response = JsonRpcResponse(id=req_msg.correlation_id, result=result)
    await sqs_broker.publish(req_msg.reply_to, response)
    await req_msg.ack()

    # Producer receives response (using raw SQS client since it's a
    # response, not request)
    resp = await sqs_broker._client.receive_message(
        QueueUrl=results_queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=0,
        AttributeNames=["All"],
        MessageAttributeNames=["All"],
    )
    assert "Messages" in resp
    assert len(resp["Messages"]) == 1

    raw_msg = resp["Messages"][0]
    response = JsonRpcResponse.model_validate_json(raw_msg["Body"])
    assert response.result == 42

    # Clean up
    await sqs_broker._client.delete_message(
        QueueUrl=results_queue_url,
        ReceiptHandle=raw_msg["ReceiptHandle"],
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delivery_count_increments(sqs_broker, test_queue_url):
    """ApproximateReceiveCount increases on redelivery."""
    request = JsonRpcRequest(method="delivery.count", id="count-123")
    await sqs_broker.publish(test_queue_url, request)

    msg = await consume_one(sqs_broker, test_queue_url)
    assert msg is not None
    assert msg.delivery_count == 1
    await msg.nack(delay=1.0)

    await asyncio.sleep(1.5)

    msg = await consume_one(sqs_broker, test_queue_url)
    assert msg is not None
    assert msg.delivery_count == 2
    await msg.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_broker_lifecycle(sqs_endpoint):
    """Broker connects and disconnects cleanly."""
    broker = SqsBroker(dsn=sqs_endpoint)
    assert broker._client is None

    await broker.connect()
    assert broker._client is not None

    await broker.disconnect()
    assert broker._client is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_queue_name_extraction(sqs_broker, test_queue_url):
    """Queue name correctly extracted from URL."""
    request = JsonRpcRequest(method="name.test", id="name-123")
    await sqs_broker.publish(test_queue_url, request)

    msg = await consume_one(sqs_broker, test_queue_url)
    assert msg is not None
    assert msg.queue_name == "test-queue"
    await msg.ack()


# === Unit Tests for Queue Name Extraction ===


class TestSqsBrokerQueueNameExtraction:
    """Unit tests for queue name extraction from various identifiers."""

    def test_get_queue_name_from_arn(self, sqs_broker):
        """Extract queue name from SQS ARN."""
        arn = "arn:aws:sqs:us-east-1:123456789:my-queue"
        assert sqs_broker.get_queue_name(arn) == "my-queue"

    def test_get_queue_name_from_arn_case_insensitive(self, sqs_broker):
        """ARN with uppercase prefix works."""
        arn = "ARN:aws:sqs:eu-west-1:999:test-queue"
        assert sqs_broker.get_queue_name(arn) == "test-queue"

    def test_get_queue_name_from_url(self, sqs_broker):
        """Extract queue name from SQS URL."""
        url = "http://localhost:4566/000000000000/test-queue"
        assert sqs_broker.get_queue_name(url) == "test-queue"

    def test_get_queue_name_from_https_url(self, sqs_broker):
        """Extract queue name from HTTPS SQS URL."""
        url = "https://sqs.us-east-1.amazonaws.com/123456789/my-queue"
        assert sqs_broker.get_queue_name(url) == "my-queue"

    def test_get_queue_name_from_simple_name(self, sqs_broker):
        """Simple queue name returns as-is."""
        name = "queue-name"
        assert sqs_broker.get_queue_name(name) == "queue-name"

    def test_get_queue_name_strips_whitespace(self, sqs_broker):
        """Whitespace is stripped from queue name."""
        name = "  queue  "
        assert sqs_broker.get_queue_name(name) == "queue"

    def test_get_queue_name_empty_string(self, sqs_broker):
        """Empty string returns empty string."""
        assert sqs_broker.get_queue_name("") == ""

    def test_get_queue_name_whitespace_only(self, sqs_broker):
        """Whitespace-only string returns empty string."""
        assert sqs_broker.get_queue_name("   ") == ""

    def test_get_queue_name_arn_with_colons_in_name(self, sqs_broker):
        """ARN with colons in queue name extracts last segment."""
        arn = "arn:aws:sqs:region:account:multi:colon:name"
        assert sqs_broker.get_queue_name(arn) == "name"

    @pytest.mark.parametrize(
        "identifier,expected",
        [
            ("arn:aws:sqs:us-east-1:123:queue", "queue"),
            ("ARN:aws:sqs:eu-west-1:999:test", "test"),
            (
                "arn:aws:sqs:region:account:multi:colon:name",
                "name",
            ),
            (
                "http://localhost:4566/000000000000/test-queue",
                "test-queue",
            ),
            (
                "https://sqs.us-east-1.amazonaws.com/123/my-q",
                "my-q",
            ),
            ("simple-queue-name", "simple-queue-name"),
            ("queue", "queue"),
            ("  trimmed  ", "trimmed"),
            ("   ", ""),
            ("", ""),
        ],
    )
    def test_get_queue_name_various_formats(
        self, sqs_broker, identifier, expected
    ):
        """get_queue_name handles various identifier formats."""
        assert sqs_broker.get_queue_name(identifier) == expected


# === Integration Tests for Edge Cases ===


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reply_to_none_when_attribute_missing(sqs_broker, test_queue_url):
    """reply_to returns None when ReplyTo attribute is missing."""
    request = JsonRpcRequest(method="test.no.reply", id="123")
    await sqs_broker.publish(test_queue_url, request)  # No reply_to argument

    msg = await consume_one(sqs_broker, test_queue_url)
    assert msg is not None
    assert msg.reply_to is None
    await msg.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_disconnect_when_not_connected(sqs_endpoint):
    """disconnect() is safe when not connected."""
    broker = SqsBroker(dsn=sqs_endpoint)
    assert broker._client is None
    await broker.disconnect()  # Should not raise
    assert broker._client is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_double_disconnect_is_safe(sqs_endpoint):
    """disconnect() can be called multiple times."""
    broker = SqsBroker(dsn=sqs_endpoint)
    await broker.connect()
    await broker.disconnect()
    await broker.disconnect()  # Second call should not raise
    assert broker._client is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_poison_message_is_deleted(sqs_broker, test_queue_url):
    """Invalid JSON message is deleted from queue."""
    # Send invalid JSON directly via SQS client
    await sqs_broker._client.send_message(
        QueueUrl=test_queue_url,
        MessageBody="not valid json {{{",
    )

    # Try to consume - poison message should be deleted, we get nothing
    messages = await consume_all(sqs_broker, test_queue_url, timeout=1.0)
    assert len(messages) == 0  # Poison message was removed


@pytest.mark.integration
@pytest.mark.asyncio
async def test_poison_message_calls_hook(sqs_broker, test_queue_url):
    """on_poison_message hook is called for invalid JSON."""
    from unittest.mock import AsyncMock

    # Mock the hook
    sqs_broker.on_poison_message = AsyncMock()

    # Send invalid JSON
    resp = await sqs_broker._client.send_message(
        QueueUrl=test_queue_url,
        MessageBody="not json",
    )
    message_id = resp["MessageId"]

    # Consume - should trigger hook
    messages = await consume_all(sqs_broker, test_queue_url, timeout=1.0)

    # Verify hook was called
    sqs_broker.on_poison_message.assert_called_once()
    call_args = sqs_broker.on_poison_message.call_args
    assert call_args[0][0] == message_id  # First arg is message_id
    assert call_args[0][1] == "not json"  # Second arg is body
    assert len(messages) == 0  # Poison message deleted, not yielded


@pytest.mark.integration
@pytest.mark.asyncio
async def test_poison_message_handler_exception_logged(
    sqs_broker, test_queue_url, caplog
):
    """Exception in on_poison_message is logged with context."""
    import logging
    from unittest.mock import AsyncMock

    # Make hook raise exception
    sqs_broker.on_poison_message = AsyncMock(
        side_effect=RuntimeError("Hook failed")
    )

    # Send invalid JSON
    resp = await sqs_broker._client.send_message(
        QueueUrl=test_queue_url,
        MessageBody="invalid",
    )
    message_id = resp["MessageId"]

    with caplog.at_level(logging.ERROR):
        messages = await consume_all(sqs_broker, test_queue_url, timeout=1.0)

    # Verify error was logged
    assert "Failed to handle poison message" in caplog.text
    assert message_id in caplog.text  # message_id in log context
    assert len(messages) == 0  # Poison message still deleted
