"""Integration tests for Redis provider with real Redis."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from planq.enums import Header
from planq.message import BrokerMessage
from planq.models import JsonRpcRequest, JsonRpcResponse
from planq.providers.redis import (
    _DELAYED_QUEUES_KEY,
    RedisBroker,
    RedisConsumerConfig,
)


async def consume_one(
    broker: RedisBroker, queue: str, timeout: float = 1.0
) -> BrokerMessage | None:
    """Consume a single message with timeout, return None if no message."""
    try:
        async with asyncio.timeout(timeout):
            async for msg in broker.consume(queue, block_ms=0):
                return msg
    except TimeoutError:
        return None
    return None


async def consume_all(
    broker: RedisBroker, queue: str, timeout: float = 1.0
) -> list[BrokerMessage]:
    """Consume all available messages with timeout."""
    messages = []
    try:
        async with asyncio.timeout(timeout):
            async for msg in broker.consume(queue, block_ms=0):
                messages.append(msg)
    except TimeoutError:
        pass
    return messages


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
            group_name="test-group",
            consumer_name="test-consumer",
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
        "maxlen-stream",
        "maxlen-stream:delayed",
        "sched-q1",
        "sched-q1:delayed",
        "sched-q2",
        "sched-q2:delayed",
        "sched-q3",
        "sched-q3:delayed",
        "claim-stream",
        "claim-stream:delayed",
        "recovery-stream",
        "recovery-stream:delayed",
        _DELAYED_QUEUES_KEY,
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_publish_and_consume(redis_broker):
    """Full round-trip: publish -> consume -> ack."""
    request = JsonRpcRequest(
        method="test.method", params={"key": "value"}, id="123"
    )
    message_id = await redis_broker.publish("test-stream", request)
    assert message_id is not None

    msg = await consume_one(redis_broker, "test-stream")
    assert msg is not None
    assert msg.body.method == "test.method"
    assert msg.body.params == {"key": "value"}
    assert msg.message_id == message_id
    await msg.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_message_with_delay(redis_broker):
    """Delayed message not immediately visible, appears after scheduler."""
    request = JsonRpcRequest(method="delayed.task", id="delay-123")
    await redis_broker.publish("test-stream", request, delay=2)

    # Message should not be visible immediately (it's in the ZSET)
    msg = await consume_one(redis_broker, "test-stream", timeout=0.5)
    assert msg is None

    # Wait for scheduler to migrate it
    await asyncio.sleep(2.5)

    msg = await consume_one(redis_broker, "test-stream")
    assert msg is not None
    assert msg.body.method == "delayed.task"
    await msg.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_expire_at_header(redis_broker):
    """TTL header preserved through publish/consume."""
    expire_at = time.time() + 3600
    request = JsonRpcRequest(method="ttl.task", id="ttl-123")
    await redis_broker.publish("test-stream", request, expire_at=expire_at)

    msg = await consume_one(redis_broker, "test-stream")
    assert msg is not None
    assert Header.EXPIRE_AT in msg.headers
    received = float(msg.headers[Header.EXPIRE_AT])
    assert abs(received - expire_at) < 1.0
    await msg.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reply_to_header(redis_broker):
    """Reply-to queue preserved."""
    request = JsonRpcRequest(method="echo", params=["hello"], id="echo-123")
    await redis_broker.publish(
        "test-stream", request, reply_to="results-stream"
    )

    msg = await consume_one(redis_broker, "test-stream")
    assert msg is not None
    assert msg.reply_to == "results-stream"
    await msg.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ack_removes_message(redis_broker):
    """Message gone after ack."""
    request = JsonRpcRequest(method="ack.test", id="ack-123")
    await redis_broker.publish("test-stream", request)

    msg = await consume_one(redis_broker, "test-stream")
    assert msg is not None
    await msg.ack()

    # Verify message was deleted
    msg = await consume_one(redis_broker, "test-stream", timeout=0.5)
    assert msg is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_nack_redelivers_message(redis_broker):
    """nack -> delayed ZSET -> scheduler -> stream -> consume."""
    request = JsonRpcRequest(method="nack.test", id="nack-123")
    await redis_broker.publish("test-stream", request)

    msg = await consume_one(redis_broker, "test-stream")
    assert msg is not None
    orig_count = msg.delivery_count
    await msg.nack(delay=1.0)

    # Wait for scheduler to migrate delayed message
    await asyncio.sleep(2.0)

    msg = await consume_one(redis_broker, "test-stream")
    assert msg is not None
    assert msg.body.method == "nack.test"
    assert msg.delivery_count == orig_count + 1
    await msg.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_request_response_pattern(redis_broker):
    """Full request/response cycle with two queues."""
    request = JsonRpcRequest(
        method="compute.sum",
        params={"a": 10, "b": 32},
        id="req-456",
    )
    await redis_broker.publish(
        "test-stream", request, reply_to="results-stream"
    )

    # Consumer receives request
    req_msg = await consume_one(redis_broker, "test-stream")
    assert req_msg is not None
    assert req_msg.body.method == "compute.sum"
    assert req_msg.reply_to == "results-stream"

    result = req_msg.body.params["a"] + req_msg.body.params["b"]

    # Send response
    response = JsonRpcResponse(id=req_msg.correlation_id, result=result)
    await redis_broker.publish(req_msg.reply_to, response)
    await req_msg.ack()

    # Read response from results stream
    resp_entries = await redis_broker._client.xread(
        {"results-stream": "0-0"}, count=1, block=1000
    )
    assert resp_entries
    _stream, messages = resp_entries[0]
    assert len(messages) == 1
    _entry_id, fields = messages[0]
    resp = JsonRpcResponse.model_validate_json(fields["body"])
    assert resp.result == 42

    # Clean up
    await redis_broker._client.delete("results-stream")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delivery_count_increments(redis_broker):
    """Verify 1 on first delivery, 2 after nack."""
    request = JsonRpcRequest(method="delivery.count", id="count-123")
    await redis_broker.publish("test-stream", request)

    msg = await consume_one(redis_broker, "test-stream")
    assert msg is not None
    assert msg.delivery_count == 1
    await msg.nack(delay=1.0)

    await asyncio.sleep(2.0)

    msg = await consume_one(redis_broker, "test-stream")
    assert msg is not None
    assert msg.delivery_count == 2
    await msg.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_broker_lifecycle(redis_endpoint):
    """Connect/disconnect state transitions."""
    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="lifecycle-group",
            consumer_name="lifecycle-consumer",
        ),
    )
    assert broker._client is None

    await broker.connect()
    assert broker._client is not None

    await broker.disconnect()
    assert broker._client is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_disconnect_when_not_connected(redis_endpoint):
    """Idempotent disconnect."""
    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="disc-group",
            consumer_name="disc-consumer",
        ),
    )
    assert broker._client is None
    await broker.disconnect()  # Should not raise
    assert broker._client is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_double_disconnect_is_safe(redis_endpoint):
    """Repeated disconnect calls."""
    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="double-group",
            consumer_name="double-consumer",
        ),
    )
    await broker.connect()
    await broker.disconnect()
    await broker.disconnect()  # Second call should not raise
    assert broker._client is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_poison_message_is_deleted(redis_broker):
    """Invalid JSON message handled."""
    # Send invalid data directly to the stream
    await redis_broker._client.xadd(
        "test-stream",
        {"body": "not valid json {{{", "delivery_count": "1"},
    )

    # Try to consume - poison message should be deleted
    messages = await consume_all(redis_broker, "test-stream", timeout=1.0)
    assert len(messages) == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_poison_message_calls_hook(redis_broker):
    """on_poison_message hook invoked."""
    redis_broker.on_poison_message = AsyncMock()

    entry_id = await redis_broker._client.xadd(
        "test-stream",
        {"body": "not json", "delivery_count": "1"},
    )

    messages = await consume_all(redis_broker, "test-stream", timeout=1.0)

    redis_broker.on_poison_message.assert_called_once()
    call_args = redis_broker.on_poison_message.call_args
    assert call_args[0][0] == entry_id
    assert call_args[0][1] == "not json"
    assert len(messages) == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_group_created_automatically(redis_broker):
    """MKSTREAM + group creation."""
    # Consume from a new stream - group should be created
    fresh_stream = "fresh-test-stream"
    try:
        msg = await consume_one(redis_broker, fresh_stream, timeout=0.5)
        assert msg is None  # No messages, but no error

        # Verify group exists
        groups = await redis_broker._client.xinfo_groups(fresh_stream)
        assert len(groups) == 1
        assert groups[0]["name"] == "test-group"
    finally:
        await redis_broker._client.delete(fresh_stream)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_scheduler_migrates_delayed_messages(redis_broker):
    """Direct test of Lua script migration."""
    import json
    from uuid import uuid4

    # Manually add a message to the delayed ZSET with score in the past
    delayed_key = "test-stream:delayed"
    rpc = JsonRpcRequest(method="migrated.task", id="mig-123")
    payload = json.dumps(
        {
            "v": "1",
            "body": rpc.model_dump_json(),
            "reply_to": "",
            "expire_at": "",
            "delivery_count": "1",
            "headers": "{}",
            "delayed_id": str(uuid4()),
        }
    )
    # Score in the past so it's immediately eligible
    await redis_broker._client.zadd(delayed_key, {payload: time.time() - 10})
    await redis_broker._client.zadd(
        _DELAYED_QUEUES_KEY, {"test-stream": time.time()}
    )

    # Wait for scheduler to pick it up
    await asyncio.sleep(1.5)

    msg = await consume_one(redis_broker, "test-stream")
    assert msg is not None
    assert msg.body.method == "migrated.task"
    await msg.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reply_to_none_when_missing(redis_broker):
    """reply_to returns None when empty."""
    request = JsonRpcRequest(method="test.no.reply", id="123")
    await redis_broker.publish("test-stream", request)

    msg = await consume_one(redis_broker, "test-stream")
    assert msg is not None
    assert msg.reply_to is None
    await msg.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_user_headers_preserved(redis_broker):
    """User-defined headers round-trip through publish/consume."""
    request = JsonRpcRequest(method="headers.test", id="hdr-123")
    custom_headers = {"x-custom": "value", "x-trace": "abc123"}
    await redis_broker.publish("test-stream", request, headers=custom_headers)

    msg = await consume_one(redis_broker, "test-stream")
    assert msg is not None
    assert msg.headers["x-custom"] == "value"
    assert msg.headers["x-trace"] == "abc123"
    await msg.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_enqueued_at_from_entry_id(redis_broker):
    """enqueued_at derived from stream entry ID timestamp."""
    request = JsonRpcRequest(method="test.enqueued", id="enq-123")
    now = time.time()
    await redis_broker.publish("test-stream", request)

    msg = await consume_one(redis_broker, "test-stream")
    assert msg is not None
    # Redis server clock may differ slightly from local clock
    assert abs(msg.enqueued_at - now) < 1.0
    await msg.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reject_removes_message(redis_broker):
    """reject() deletes message from stream."""
    request = JsonRpcRequest(method="test.reject", id="rej-123")
    await redis_broker.publish("test-stream", request)

    msg = await consume_one(redis_broker, "test-stream")
    assert msg is not None
    await msg.reject()

    msg = await consume_one(redis_broker, "test-stream", timeout=0.5)
    assert msg is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_poison_message_hook_failure_logged(redis_broker):
    """When on_poison_message hook raises, error is logged and XACK is
    skipped so the entry stays in PEL for the next recovery pass."""
    original_hook = redis_broker.on_poison_message
    redis_broker.on_poison_message = AsyncMock(
        side_effect=RuntimeError("Hook failed")
    )

    try:
        await redis_broker._client.xadd(
            "test-stream",
            {"body": "invalid json", "delivery_count": "1"},
        )

        messages = await consume_all(redis_broker, "test-stream", timeout=1.0)
        assert len(messages) == 0
        redis_broker.on_poison_message.assert_called_once()

        # Hook failed → entry must remain in the consumer group PEL so
        # a future recovery pass can re-process it.
        pending = await redis_broker._client.xpending(
            "test-stream", "test-group"
        )
        assert pending["pending"] >= 1
    finally:
        redis_broker.on_poison_message = original_hook


@pytest.mark.integration
@pytest.mark.asyncio
async def test_malformed_headers_json_ignored(redis_broker):
    """Malformed headers JSON is silently ignored."""
    rpc = JsonRpcRequest(method="test.bad_headers", id="bh-123")
    await redis_broker._client.xadd(
        "test-stream",
        {
            "body": rpc.model_dump_json(),
            "delivery_count": "1",
            "reply_to": "",
            "expire_at": "",
            "headers": "not valid json {{{",
        },
    )

    msg = await consume_one(redis_broker, "test-stream")
    assert msg is not None
    assert msg.body.method == "test.bad_headers"
    assert "x-custom" not in msg.headers
    await msg.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_xgroup_create_non_busygroup_error(redis_endpoint):
    """Non-BUSYGROUP ResponseError from XGROUP CREATE is re-raised."""
    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="err-group",
            consumer_name="err-consumer",
        ),
    )
    await broker.connect()
    try:
        broker._client.xgroup_create = AsyncMock(
            side_effect=ResponseError("WRONGTYPE Operation against a key")
        )
        with pytest.raises(ResponseError, match="WRONGTYPE"):
            async for _ in broker.consume("test-stream", block_ms=0):
                break
    finally:
        await broker.disconnect()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_scheduler_logs_warning_on_migration_failure(
    redis_endpoint,
):
    """Scheduler logs warning when migration script fails."""
    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="sched-fail-group",
            consumer_name="sched-fail-consumer",
            scheduler_interval=0.3,
        ),
    )
    await broker.connect()
    try:
        await broker._client.zadd(
            _DELAYED_QUEUES_KEY, {"test-stream": time.time()}
        )
        broker._migrate_script = AsyncMock(
            side_effect=RuntimeError("Script error")
        )

        # Wait for scheduler to attempt migration
        await asyncio.sleep(0.8)

        broker._migrate_script.assert_called()
    finally:
        await broker._client.zrem(_DELAYED_QUEUES_KEY, "test-stream")
        await broker.disconnect()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consume_empty_read_continues(redis_broker):
    """Consumer continues polling when xreadgroup returns empty."""
    # Use block_ms=100 so xreadgroup returns empty after 100ms
    # instead of blocking forever (block_ms=0).
    # This exercises the `if not entries: continue` branch.
    try:
        async with asyncio.timeout(0.5):
            async for _ in redis_broker.consume("test-stream", block_ms=100):
                break
    except TimeoutError:
        pass


# -- XAUTOCLAIM tests --


@pytest.mark.integration
@pytest.mark.asyncio
async def test_xautoclaim_recovers_stuck_message(redis_endpoint):
    """XAUTOCLAIM reclaims messages stuck with a dead consumer."""
    queue = "claim-stream"

    # Consumer A reads a message but does NOT ack it
    broker_a = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="claim-group",
            consumer_name="consumer-a",
            claim_idle_ms=0,
        ),
    )
    await broker_a.connect()
    try:
        request = JsonRpcRequest(method="stuck.task", id="stuck-1")
        await broker_a.publish(queue, request)

        msg_a = await consume_one(broker_a, queue)
        assert msg_a is not None
        assert msg_a.body.method == "stuck.task"
        # Intentionally do NOT ack
    finally:
        await broker_a.disconnect()

    # Consumer B claims the stuck message (min_idle_time=1ms)
    broker_b = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="claim-group",
            consumer_name="consumer-b",
            claim_idle_ms=1,
            claim_interval=0,
        ),
    )
    await broker_b.connect()
    try:
        msg_b = await consume_one(broker_b, queue, timeout=2.0)
        assert msg_b is not None
        assert msg_b.body.method == "stuck.task"
        await msg_b.ack()
    finally:
        await broker_b.disconnect()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_xautoclaim_disabled_when_idle_ms_zero(
    redis_endpoint,
):
    """XAUTOCLAIM is skipped when claim_idle_ms=0."""
    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="no-claim-group",
            consumer_name="no-claim-consumer",
            claim_idle_ms=0,
        ),
    )
    await broker.connect()
    try:
        with patch.object(
            broker._client, "xautoclaim", new_callable=AsyncMock
        ) as mock_claim:
            try:
                async with asyncio.timeout(0.5):
                    async for _ in broker.consume("test-stream", block_ms=100):
                        break
            except TimeoutError:
                pass
            mock_claim.assert_not_called()
    finally:
        await broker.disconnect()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_xautoclaim_exception_logged_and_skipped(redis_endpoint):
    """XAUTOCLAIM failure is logged; consume falls through to XREADGROUP."""
    queue = "claim-stream"
    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="claim-exc-group",
            consumer_name="claim-exc-consumer",
            claim_idle_ms=1,
            claim_interval=0,
        ),
    )
    await broker.connect()
    try:
        request = JsonRpcRequest(method="after.exc", id="exc-1")
        await broker.publish(queue, request)

        broker._client.xautoclaim = AsyncMock(
            side_effect=RuntimeError("Connection lost")
        )

        msg = await consume_one(broker, queue, timeout=2.0)
        assert msg is not None
        assert msg.body.method == "after.exc"
        await msg.ack()
    finally:
        await broker.disconnect()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_xautoclaim_poison_message_skipped(redis_endpoint):
    """Poison message from XAUTOCLAIM is skipped; valid message delivered."""
    queue = "claim-stream"
    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="claim-poison-group",
            consumer_name="claim-poison-consumer",
            claim_idle_ms=1,
            claim_interval=0,
        ),
    )
    await broker.connect()
    try:
        request = JsonRpcRequest(method="valid.task", id="v-1")
        await broker.publish(queue, request)

        poison_entry = (
            "999-0",
            {"body": "not valid json", "delivery_count": "1"},
        )
        call_count = 0

        async def mock_xautoclaim(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return ("0-0", [poison_entry], [])
            return ("0-0", [], [])

        broker._client.xautoclaim = mock_xautoclaim

        msg = await consume_one(broker, queue, timeout=2.0)
        assert msg is not None
        assert msg.body.method == "valid.task"
        await msg.ack()
    finally:
        await broker.disconnect()


# -- MAXLEN tests --


@pytest.mark.integration
@pytest.mark.asyncio
async def test_publish_with_maxlen_caps_stream(redis_endpoint):
    """MAXLEN ~ caps stream length approximately."""
    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="maxlen-group",
            consumer_name="maxlen-consumer",
        ),
        max_stream_len=100,
    )
    await broker.connect()
    try:
        queue = "maxlen-stream"
        for i in range(200):
            request = JsonRpcRequest(
                method="maxlen.task", params={"i": i}, id=str(i)
            )
            await broker.publish(queue, request)

        stream_len = await broker._client.xlen(queue)
        # Approximate trimming: should be around 100, not 200
        assert stream_len <= 150
    finally:
        await broker.disconnect()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_publish_without_maxlen(redis_endpoint):
    """max_stream_len=None disables MAXLEN cap."""
    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="no-maxlen-group",
            consumer_name="no-maxlen-consumer",
        ),
        max_stream_len=None,
    )
    await broker.connect()
    try:
        queue = "maxlen-stream"
        for i in range(200):
            request = JsonRpcRequest(
                method="no-maxlen.task", params={"i": i}, id=str(i)
            )
            await broker.publish(queue, request)

        stream_len = await broker._client.xlen(queue)
        assert stream_len == 200
    finally:
        await broker.disconnect()


# -- Connection reliability tests --


@pytest.mark.integration
@pytest.mark.asyncio
async def test_connection_params_forwarded(redis_endpoint):
    """socket_timeout, health_check_interval, retry_on_timeout forwarded."""
    with patch(
        "planq.providers.redis.Redis.from_url", wraps=Redis.from_url
    ) as mock_from_url:
        broker = RedisBroker(
            dsn=redis_endpoint,
            consumer=RedisConsumerConfig(
                group_name="conn-group",
                consumer_name="conn-consumer",
            ),
            socket_timeout=3.0,
            health_check_interval=15,
            retry_on_timeout=False,
        )
        await broker.connect()
        try:
            mock_from_url.assert_called_once_with(
                redis_endpoint,
                decode_responses=True,
                socket_timeout=3.0,
                health_check_interval=15,
                retry_on_timeout=False,
                max_connections=100,
            )
        finally:
            await broker.disconnect()


# -- Scheduler optimization tests --


@pytest.mark.integration
@pytest.mark.asyncio
async def test_scheduler_migrates_multiple_queues_concurrently(
    redis_endpoint,
):
    """Scheduler migrates delayed messages from multiple queues."""
    import json
    from uuid import uuid4

    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="sched-multi-group",
            consumer_name="sched-multi-consumer",
            scheduler_interval=0.5,
        ),
    )
    await broker.connect()
    try:
        queues = ["sched-q1", "sched-q2", "sched-q3"]

        for q in queues:
            delayed_key = f"{q}:delayed"
            rpc = JsonRpcRequest(method=f"task.{q}", id=f"id-{q}")
            payload = json.dumps(
                {
                    "v": "1",
                    "body": rpc.model_dump_json(),
                    "reply_to": "",
                    "expire_at": "",
                    "delivery_count": "1",
                    "headers": "{}",
                    "delayed_id": str(uuid4()),
                }
            )
            await broker._client.zadd(delayed_key, {payload: time.time() - 10})
            await broker._client.zadd(_DELAYED_QUEUES_KEY, {q: time.time()})

        # Wait for scheduler to migrate all
        await asyncio.sleep(1.5)

        for q in queues:
            stream_len = await broker._client.xlen(q)
            assert stream_len >= 1, f"Queue {q} not migrated"
    finally:
        for q in ["sched-q1", "sched-q2", "sched-q3"]:
            await broker._client.delete(q)
            await broker._client.delete(f"{q}:delayed")
            await broker._client.zrem(_DELAYED_QUEUES_KEY, q)
        await broker.disconnect()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_migrate_loops_on_full_batch(redis_endpoint):
    """Migration loops when script returns a full batch."""
    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="migrate-loop-group",
            consumer_name="migrate-loop-consumer",
            scheduler_interval=0.3,
        ),
    )
    await broker.connect()
    try:
        await broker._client.zadd(
            _DELAYED_QUEUES_KEY, {"test-stream": time.time()}
        )
        broker._migrate_script = AsyncMock(
            side_effect=[100, 0, 0, 0, 0],
        )

        await asyncio.sleep(0.8)

        assert broker._migrate_script.call_count >= 2
    finally:
        await broker._client.zrem(_DELAYED_QUEUES_KEY, "test-stream")
        await broker.disconnect()


# -- PEL recovery tests --


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pel_recovery_on_startup(redis_endpoint):
    """Pending message re-delivered immediately on reconnect."""
    queue = "recovery-stream"

    # Consumer A reads a message but does NOT ack it
    broker_a = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="recovery-group",
            consumer_name="recovery-consumer",
            claim_idle_ms=0,
        ),
    )
    await broker_a.connect()
    try:
        request = JsonRpcRequest(method="recovery.task", id="rec-1")
        await broker_a.publish(queue, request)

        msg_a = await consume_one(broker_a, queue)
        assert msg_a is not None
        assert msg_a.body.method == "recovery.task"
        # Intentionally do NOT ack
    finally:
        await broker_a.disconnect()

    # Reconnect with the SAME consumer_name — PEL recovery
    # should re-deliver the pending message immediately
    broker_b = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="recovery-group",
            consumer_name="recovery-consumer",
            claim_idle_ms=0,
        ),
    )
    await broker_b.connect()
    try:
        msg_b = await consume_one(broker_b, queue, timeout=2.0)
        assert msg_b is not None
        assert msg_b.body.method == "recovery.task"
        await msg_b.ack()
    finally:
        await broker_b.disconnect()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pel_recovery_increments_delivery_count(redis_endpoint):
    """PEL-redelivered message reports delivery_count >= 2.

    The stream-entry delivery_count field is frozen at 1 (unchanged
    between the original XREADGROUP and the second one), but Redis's
    PEL times_delivered counter is now 2. The RedisMessage must sum
    them via the stream_field + (NEL - 1) formula so that
    PlanqConsumer's max-retries gate can see crash-driven
    redeliveries.
    """
    queue = "pel-delivery-count-stream"
    broker_a = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="pel-dc-group",
            consumer_name="pel-dc-consumer",
            claim_idle_ms=0,
        ),
    )
    await broker_a.connect()
    try:
        request = JsonRpcRequest(method="pel.dc.task", id="pdc-1")
        await broker_a.publish(queue, request)

        msg_a = await consume_one(broker_a, queue)
        assert msg_a is not None
        # First delivery: stream_field=1, NEL=1 → delivery_count=1
        assert msg_a.delivery_count == 1
        # Intentionally do NOT ack — leave in PEL
    finally:
        await broker_a.disconnect()

    broker_b = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="pel-dc-group",
            consumer_name="pel-dc-consumer",
            claim_idle_ms=0,
        ),
    )
    await broker_b.connect()
    try:
        msg_b = await consume_one(broker_b, queue, timeout=2.0)
        assert msg_b is not None
        assert msg_b.body.method == "pel.dc.task"
        # Second delivery via PEL recovery: stream_field=1, NEL=2
        # → delivery_count = 1 + (2 - 1) = 2
        assert msg_b.delivery_count == 2
        await msg_b.ack()
    finally:
        await broker_b.disconnect()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pel_recovery_failure_logged(redis_endpoint):
    """Recovery failure is logged; consume still delivers new messages."""
    queue = "recovery-stream"
    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="recovery-fail-group",
            consumer_name="recovery-fail-consumer",
            claim_idle_ms=0,
        ),
    )
    await broker.connect()
    try:
        request = JsonRpcRequest(method="after.recovery.fail", id="rf-1")
        await broker.publish(queue, request)

        original_xreadgroup = broker._client.xreadgroup
        call_count = 0

        async def flaky_xreadgroup(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            # First call is recovery (ID="0") — make it fail
            if call_count == 1:
                raise RuntimeError("Connection lost")
            return await original_xreadgroup(*args, **kwargs)

        broker._client.xreadgroup = flaky_xreadgroup

        msg = await consume_one(broker, queue, timeout=2.0)
        assert msg is not None
        assert msg.body.method == "after.recovery.fail"
        await msg.ack()
    finally:
        await broker.disconnect()


# -- Lua pcall test --


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lua_pcall_skips_corrupt_entry(redis_endpoint):
    """Corrupt ZSET entry is removed; valid entry is migrated."""
    import json
    from uuid import uuid4

    queue = "recovery-stream"
    delayed_key = f"{queue}:delayed"

    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="pcall-group",
            consumer_name="pcall-consumer",
            scheduler_interval=0.5,
        ),
    )
    await broker.connect()
    try:
        # Add a valid delayed message
        rpc = JsonRpcRequest(method="valid.lua", id="lua-1")
        valid_payload = json.dumps(
            {
                "v": "1",
                "body": rpc.model_dump_json(),
                "reply_to": "",
                "expire_at": "",
                "delivery_count": "1",
                "headers": "{}",
                "delayed_id": str(uuid4()),
            }
        )
        now = time.time()
        await broker._client.zadd(delayed_key, {valid_payload: now - 10})

        # Add a corrupt (non-JSON) entry
        await broker._client.zadd(delayed_key, {"not valid json {{{": now - 10})

        await broker._client.zadd(_DELAYED_QUEUES_KEY, {queue: time.time()})

        # Wait for scheduler to migrate
        await asyncio.sleep(1.5)

        # Corrupt entry should be removed from ZSET
        remaining = await broker._client.zcard(delayed_key)
        assert remaining == 0

        # Valid message should be in the stream
        msg = await consume_one(broker, queue, timeout=2.0)
        assert msg is not None
        assert msg.body.method == "valid.lua"
        await msg.ack()
    finally:
        await broker.disconnect()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delayed_queue_registered_cross_process(redis_endpoint):
    """Delayed messages published by a producer-only broker get migrated
    by a consumer running in a separate process that never directly
    touched the target queue — this is the ``_delayed_queues`` registry
    ZSET fix for the orphan-delayed-messages bug."""
    producer_queue = "cross-process-queue"
    consumer_queue = "unrelated-queue"

    producer_broker = RedisBroker(dsn=redis_endpoint)
    await producer_broker.connect()
    try:
        # Producer publishes with a short delay so the scheduler
        # tick will migrate it quickly.
        rpc = JsonRpcRequest(method="cross.process", id="cp-1")
        delayed_id = await producer_broker.publish(
            producer_queue, rpc, delay=0.1
        )
        assert delayed_id is not None
    finally:
        await producer_broker.disconnect()

    consumer_broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="cross-process-group",
            consumer_name="cross-process-consumer",
            scheduler_interval=0.2,
        ),
    )
    await consumer_broker.connect()
    try:
        # Consumer never directly consumes from producer_queue; the
        # scheduler must discover it via the registry ZSET.
        msg = await consume_one(consumer_broker, consumer_queue, timeout=0.3)
        assert msg is None

        # Wait for scheduler migration to complete.
        await asyncio.sleep(1.0)

        stream_len = await consumer_broker._client.xlen(producer_queue)
        assert stream_len >= 1, (
            "Delayed message not migrated — registry ZSET is not"
            " being consulted by the scheduler"
        )
    finally:
        await consumer_broker._client.delete(producer_queue)
        await consumer_broker._client.delete(f"{producer_queue}:delayed")
        await consumer_broker._client.zrem(_DELAYED_QUEUES_KEY, producer_queue)
        await consumer_broker.disconnect()


# -- ConnectionError propagation test --


@pytest.mark.integration
@pytest.mark.asyncio
async def test_xgroup_create_connection_error_propagates(
    redis_endpoint,
):
    """ConnectionError from xgroup_create propagates (not caught)."""
    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="conn-err-group",
            consumer_name="conn-err-consumer",
        ),
    )
    await broker.connect()
    try:
        broker._client.xgroup_create = AsyncMock(
            side_effect=ConnectionError("Connection refused")
        )
        with pytest.raises(ConnectionError, match="Connection refused"):
            async for _ in broker.consume("test-stream", block_ms=0):
                break
    finally:
        await broker.disconnect()


# -- Branch coverage: PEL recovery edge cases --


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pel_recovery_no_pending_messages(redis_endpoint):
    """Recovery xreadgroup returns empty when no PEL entries."""
    queue = "recovery-stream"
    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="recovery-empty-group",
            consumer_name="recovery-empty-consumer",
            claim_idle_ms=0,
        ),
    )
    await broker.connect()
    try:
        # Publish after connect so recovery has nothing pending
        request = JsonRpcRequest(method="after.recovery", id="ar-1")
        await broker.publish(queue, request)

        original_xreadgroup = broker._client.xreadgroup
        call_count = 0

        async def tracking_xreadgroup(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Recovery call — return truthy but empty messages
                return [(queue, [])]
            return await original_xreadgroup(*args, **kwargs)

        broker._client.xreadgroup = tracking_xreadgroup

        msg = await consume_one(broker, queue, timeout=2.0)
        assert msg is not None
        assert msg.body.method == "after.recovery"
        await msg.ack()
    finally:
        await broker.disconnect()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pel_recovery_poison_message_skipped(redis_endpoint):
    """Poison message in PEL during recovery is skipped."""
    queue = "recovery-stream"
    group = "recovery-poison-group"
    consumer = "recovery-poison-consumer"

    # Phase 1: create group and put entries into PEL using
    # raw xreadgroup (bypass consume's poison handler)
    client = Redis.from_url(redis_endpoint, decode_responses=True)
    try:
        await client.xgroup_create(queue, group, "0", mkstream=True)

        # Add a poison message and a valid message
        await client.xadd(
            queue,
            {"body": "not valid json", "delivery_count": "1"},
        )
        rpc = JsonRpcRequest(method="valid.recovery", id="vr-1")
        await client.xadd(
            queue,
            {
                "body": rpc.model_dump_json(),
                "delivery_count": "1",
                "reply_to": "",
                "expire_at": "",
                "headers": "{}",
            },
        )

        # Raw xreadgroup puts both into PEL without processing
        await client.xreadgroup(group, consumer, {queue: ">"}, count=10)
    finally:
        await client.aclose()

    # Phase 2: reconnect via RedisBroker — PEL recovery should
    # skip poison and yield valid message
    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name=group,
            consumer_name=consumer,
            claim_idle_ms=0,
        ),
    )
    await broker.connect()
    try:
        msg = await consume_one(broker, queue, timeout=2.0)
        assert msg is not None
        assert msg.body.method == "valid.recovery"
        await msg.ack()
    finally:
        await broker.disconnect()


# -- Branch coverage: XAUTOCLAIM empty result --


@pytest.mark.integration
@pytest.mark.asyncio
async def test_xautoclaim_returns_empty(redis_endpoint):
    """XAUTOCLAIM runs but finds no stuck messages."""
    queue = "claim-stream"
    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="claim-empty-group",
            consumer_name="claim-empty-consumer",
            claim_idle_ms=1,
            claim_interval=0,
        ),
    )
    await broker.connect()
    try:
        # Publish a message — it goes to stream directly
        request = JsonRpcRequest(method="after.empty.claim", id="aec-1")
        await broker.publish(queue, request)

        # Consume — XAUTOCLAIM runs first (claim_interval=0)
        # but finds nothing stuck, then XREADGROUP picks up
        # the new message
        msg = await consume_one(broker, queue, timeout=2.0)
        assert msg is not None
        assert msg.body.method == "after.empty.claim"
        await msg.ack()
    finally:
        await broker.disconnect()


# -- PEL pagination tests --


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pel_recovery_drains_full_pel(redis_endpoint):
    """PEL recovery reads all pending messages, not just first batch."""
    queue = "recovery-stream"
    group = "recovery-drain-group"
    consumer = "recovery-drain-consumer"
    total = 15

    # Phase 1: create group and put entries into PEL via raw
    # xreadgroup
    client = Redis.from_url(redis_endpoint, decode_responses=True)
    try:
        await client.xgroup_create(queue, group, "0", mkstream=True)

        for i in range(total):
            rpc = JsonRpcRequest(
                method="drain.task", params={"i": i}, id=f"d-{i}"
            )
            await client.xadd(
                queue,
                {
                    "body": rpc.model_dump_json(),
                    "delivery_count": "1",
                    "reply_to": "",
                    "expire_at": "",
                    "headers": "{}",
                },
            )

        # Raw xreadgroup puts all into PEL
        await client.xreadgroup(group, consumer, {queue: ">"}, count=total)
    finally:
        await client.aclose()

    # Phase 2: reconnect with small prefetch — all 15 must be
    # recovered
    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name=group,
            consumer_name=consumer,
            claim_idle_ms=0,
        ),
    )
    await broker.connect()
    try:
        messages = []
        async with asyncio.timeout(3.0):
            async for msg in broker.consume(queue, prefetch=5, block_ms=0):
                messages.append(msg)
        # Without pagination, only first 5 would be returned
        assert len(messages) == total
        for msg in messages:
            await msg.ack()
    except TimeoutError:
        # Without pagination, only first 5 would be returned
        assert len(messages) == total
        for msg in messages:
            await msg.ack()
    finally:
        await broker.disconnect()


# -- XAUTOCLAIM drain tests --


@pytest.mark.integration
@pytest.mark.asyncio
async def test_xautoclaim_drains_when_cursor_not_exhausted(
    redis_endpoint,
):
    """XAUTOCLAIM re-enters immediately when cursor not exhausted."""
    queue = "claim-stream"
    broker = RedisBroker(
        dsn=redis_endpoint,
        consumer=RedisConsumerConfig(
            group_name="claim-drain-group",
            consumer_name="claim-drain-consumer",
            claim_idle_ms=1,
            claim_interval=0,
        ),
    )
    await broker.connect()
    try:
        # Publish a normal message so consume doesn't block forever
        request = JsonRpcRequest(method="normal.task", id="nt-1")
        await broker.publish(queue, request)

        rpc_a = JsonRpcRequest(method="claimed.a", id="ca-1")
        rpc_b = JsonRpcRequest(method="claimed.b", id="cb-1")
        call_count = 0

        start_ids_received: list[str] = []

        async def mock_xautoclaim(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            sid = kwargs.get("start_id", "?")
            start_ids_received.append(sid)
            if call_count == 1:
                # Cursor not exhausted → drain should re-enter
                return (
                    "99-0",
                    [
                        (
                            "1-0",
                            {
                                "body": rpc_a.model_dump_json(),
                                "delivery_count": "1",
                                "reply_to": "",
                                "expire_at": "",
                                "headers": "{}",
                            },
                        )
                    ],
                    [],
                )
            if call_count == 2:
                # Cursor exhausted
                return (
                    "0-0",
                    [
                        (
                            "2-0",
                            {
                                "body": rpc_b.model_dump_json(),
                                "delivery_count": "1",
                                "reply_to": "",
                                "expire_at": "",
                                "headers": "{}",
                            },
                        )
                    ],
                    [],
                )
            return ("0-0", [], [])

        original_xreadgroup = broker._client.xreadgroup
        xreadgroup_blocks: list[int | None] = []

        async def tracking_xreadgroup(*args, **kwargs):
            xreadgroup_blocks.append(kwargs.get("block"))
            return await original_xreadgroup(*args, **kwargs)

        broker._client.xautoclaim = mock_xautoclaim
        broker._client.xreadgroup = tracking_xreadgroup

        messages = await consume_all(broker, queue, timeout=3.0)
        methods = {m.body.method for m in messages}

        # Both claimed batches must be delivered
        assert "claimed.a" in methods
        assert "claimed.b" in methods
        assert call_count >= 2

        # Cursor must advance: first call "0-0", second "99-0"
        assert start_ids_received[0] == "0-0"
        assert start_ids_received[1] == "99-0"

        # XREADGROUP after first XAUTOCLAIM (cursor not
        # exhausted) must be non-blocking
        assert xreadgroup_blocks[0] is None
        for msg in messages:
            await msg.ack()
    finally:
        await broker.disconnect()
