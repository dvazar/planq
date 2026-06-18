"""Tests for QueueStats value object and broker get_queue_depth."""

from __future__ import annotations

import dataclasses
import os

import pytest
import pytest_asyncio

from planq.models import JsonRpcRequest
from planq.providers.memory import InMemoryBroker
from planq.stats import QueueStats

# --- Task 1: QueueStats value object ---


def test_total_sums_all_buckets():
    stats = QueueStats(queue="default", pending=3, scheduled=2, in_flight=1)
    assert stats.total == 6


def test_total_is_zero_for_empty_queue():
    stats = QueueStats(queue="default", pending=0, scheduled=0, in_flight=0)
    assert stats.total == 0


def test_queue_stats_is_frozen():
    stats = QueueStats(queue="default", pending=1, scheduled=0, in_flight=0)
    with pytest.raises(dataclasses.FrozenInstanceError):
        stats.pending = 5  # type: ignore[misc]


# --- Task 2: InMemoryBroker.get_queue_depth ---


def _noop_request() -> JsonRpcRequest:
    return JsonRpcRequest(
        jsonrpc="2.0",
        method="noop",
        params={},
        id=None,
    )


@pytest.mark.asyncio
async def test_memory_depth_empty_queue_is_zero():
    broker = InMemoryBroker("memory://")
    await broker.connect()
    stats = await broker.get_queue_depth("default")
    assert (stats.pending, stats.scheduled, stats.in_flight) == (0, 0, 0)
    await broker.disconnect()


@pytest.mark.asyncio
async def test_memory_depth_counts_pending():
    broker = InMemoryBroker("memory://")
    await broker.connect()
    await broker.publish("default", _noop_request())
    await broker.publish("default", _noop_request())
    stats = await broker.get_queue_depth("default")
    assert stats.pending == 2
    assert stats.total == 2
    await broker.disconnect()


@pytest.mark.asyncio
async def test_memory_depth_unknown_queue_does_not_raise():
    broker = InMemoryBroker("memory://")
    await broker.connect()
    stats = await broker.get_queue_depth("never-used")
    assert stats.total == 0
    await broker.disconnect()


# --- Task 3: RedisBroker.get_queue_depth ---

_REDIS_ENDPOINT = "redis://localhost:16379"
_REDIS_STREAM = "depth-test-stream"
_TEST_GROUP = "test-group"
_TEST_CONSUMER = "test-consumer"


@pytest_asyncio.fixture
async def redis_broker_depth():
    """Connected RedisBroker with no consumer config (producer-only)."""
    from planq.providers.redis import RedisBroker

    broker = RedisBroker(dsn=_REDIS_ENDPOINT)
    await broker.connect()
    # Purge test keys before the test runs.
    try:
        await broker._client.delete(_REDIS_STREAM)
        await broker._client.delete(f"{_REDIS_STREAM}:delayed")
    except Exception:
        pass
    yield broker
    # Purge test keys after the test as well.
    try:
        await broker._client.delete(_REDIS_STREAM)
        await broker._client.delete(f"{_REDIS_STREAM}:delayed")
    except Exception:
        pass
    await broker.disconnect()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_depth_missing_stream_all_zero(redis_broker_depth):
    """Fresh/unused queue with no stream key -> all zero."""
    stats = await redis_broker_depth.get_queue_depth(_REDIS_STREAM)
    assert stats.total == 0
    assert stats.pending == 0
    assert stats.scheduled == 0
    assert stats.in_flight == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_depth_no_group_cold_start(redis_broker_depth):
    """Publish 2 with no consumer group -> XLEN fallback: pending=2."""
    req = JsonRpcRequest(method="noop", params={}, id=None)
    await redis_broker_depth.publish(_REDIS_STREAM, req)
    await redis_broker_depth.publish(_REDIS_STREAM, req)
    stats = await redis_broker_depth.get_queue_depth(_REDIS_STREAM)
    assert stats.pending == 2
    assert stats.in_flight == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_depth_backlog_via_lag(redis_broker_depth):
    """Publish 2, create group at id=0, no read -> lag=2, in_flight=0."""
    req = JsonRpcRequest(method="noop", params={}, id=None)
    await redis_broker_depth.publish(_REDIS_STREAM, req)
    await redis_broker_depth.publish(_REDIS_STREAM, req)
    # Create the consumer group at the beginning of the stream so lag = 2.
    await redis_broker_depth._client.xgroup_create(
        _REDIS_STREAM, _TEST_GROUP, "0"
    )
    stats = await redis_broker_depth.get_queue_depth(_REDIS_STREAM)
    assert stats.pending == 2
    assert stats.in_flight == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_depth_in_flight_via_pel(redis_broker_depth):
    """Publish 2, create group, xreadgroup both (no ack) -> lag=0, PEL=2."""
    req = JsonRpcRequest(method="noop", params={}, id=None)
    await redis_broker_depth.publish(_REDIS_STREAM, req)
    await redis_broker_depth.publish(_REDIS_STREAM, req)
    await redis_broker_depth._client.xgroup_create(
        _REDIS_STREAM, _TEST_GROUP, "0"
    )
    await redis_broker_depth._client.xreadgroup(
        _TEST_GROUP, _TEST_CONSUMER, {_REDIS_STREAM: ">"}, count=10
    )
    stats = await redis_broker_depth.get_queue_depth(_REDIS_STREAM)
    assert stats.pending == 0
    assert stats.in_flight == 2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_depth_partial_in_flight(redis_broker_depth):
    """Publish 2, create group, read 1 -> pending=1, in_flight=1."""
    req = JsonRpcRequest(method="noop", params={}, id=None)
    await redis_broker_depth.publish(_REDIS_STREAM, req)
    await redis_broker_depth.publish(_REDIS_STREAM, req)
    await redis_broker_depth._client.xgroup_create(
        _REDIS_STREAM, _TEST_GROUP, "0"
    )
    await redis_broker_depth._client.xreadgroup(
        _TEST_GROUP, _TEST_CONSUMER, {_REDIS_STREAM: ">"}, count=1
    )
    stats = await redis_broker_depth.get_queue_depth(_REDIS_STREAM)
    assert stats.pending == 1
    assert stats.in_flight == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_depth_drain_to_zero(redis_broker_depth):
    """Publish 2, read both, xack both -> all zero (scale-to-zero case)."""
    req = JsonRpcRequest(method="noop", params={}, id=None)
    await redis_broker_depth.publish(_REDIS_STREAM, req)
    await redis_broker_depth.publish(_REDIS_STREAM, req)
    await redis_broker_depth._client.xgroup_create(
        _REDIS_STREAM, _TEST_GROUP, "0"
    )
    entries = await redis_broker_depth._client.xreadgroup(
        _TEST_GROUP, _TEST_CONSUMER, {_REDIS_STREAM: ">"}, count=10
    )
    # Ack all delivered entries.
    for _stream, messages in entries:
        entry_ids = [eid for eid, _fields in messages]
        await redis_broker_depth._client.xack(
            _REDIS_STREAM, _TEST_GROUP, *entry_ids
        )
    stats = await redis_broker_depth.get_queue_depth(_REDIS_STREAM)
    assert stats.pending == 0
    assert stats.in_flight == 0
    assert stats.total == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_depth_delayed_counted_separately(redis_broker_depth):
    """Publish with delay -> scheduled=1, pending=0."""
    req = JsonRpcRequest(method="noop", params={}, id=None)
    await redis_broker_depth.publish(_REDIS_STREAM, req, delay=3600)
    stats = await redis_broker_depth.get_queue_depth(_REDIS_STREAM)
    assert stats.scheduled == 1
    assert stats.pending == 0


# --- Task 4: SqsBroker.get_queue_depth ---

_SQS_ENDPOINT = "http://localhost:19324"
# Use the queue predefined in tests/elasticmq.conf (sqs-limits = strict).
_SQS_QUEUE_URL = f"{_SQS_ENDPOINT}/000000000000/test-queue"


@pytest.fixture(scope="module", autouse=False)
def aws_credentials_depth():
    """Minimal AWS credentials so aiobotocore can reach ElasticMQ."""
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    yield


@pytest_asyncio.fixture
async def sqs_broker_depth(aws_credentials_depth):
    """Connected SqsBroker instance pointing at local ElasticMQ."""
    from planq.providers.sqs import SqsBroker

    broker = SqsBroker(dsn=_SQS_ENDPOINT)
    await broker.connect()
    # Purge the test queue before each test.
    try:
        await broker._client.purge_queue(QueueUrl=_SQS_QUEUE_URL)
    except Exception:
        pass
    yield broker
    # Purge after test as well.
    try:
        await broker._client.purge_queue(QueueUrl=_SQS_QUEUE_URL)
    except Exception:
        pass
    await broker.disconnect()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sqs_depth_empty_queue_is_zero(sqs_broker_depth):
    stats = await sqs_broker_depth.get_queue_depth(_SQS_QUEUE_URL)
    assert stats.total == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sqs_depth_counts_pending(sqs_broker_depth):
    req = JsonRpcRequest(method="noop", params={}, id=None)
    await sqs_broker_depth.publish(_SQS_QUEUE_URL, req)
    stats = await sqs_broker_depth.get_queue_depth(_SQS_QUEUE_URL)
    assert stats.pending >= 1  # SQS counts are approximate


# --- Edge-case / branch coverage for RedisBroker.get_queue_depth ---


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_depth_autoconnects_when_client_none():
    """get_queue_depth connects a fresh (unconnected) broker on demand."""
    from planq.providers.redis import RedisBroker

    broker = RedisBroker(dsn=_REDIS_ENDPOINT)
    assert broker._client is None  # not connected yet
    try:
        stats = await broker.get_queue_depth("depth-autoconnect-stream")
        assert stats.total == 0  # missing stream -> all zero
    finally:
        await broker.disconnect()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_depth_null_lag_falls_back_to_xlen(
    redis_broker_depth, monkeypatch
):
    """lag=None from XINFO GROUPS -> pending falls back to XLEN."""
    req = JsonRpcRequest(method="noop", params={}, id=None)
    await redis_broker_depth.publish(_REDIS_STREAM, req)
    await redis_broker_depth.publish(_REDIS_STREAM, req)

    async def fake_xinfo_groups(key):
        return [{"name": "g", "lag": None, "pending": 0}]

    monkeypatch.setattr(
        redis_broker_depth._client, "xinfo_groups", fake_xinfo_groups
    )
    stats = await redis_broker_depth.get_queue_depth(_REDIS_STREAM)
    assert stats.pending == 2  # XLEN fallback
    assert stats.in_flight == 0


def test_redis_select_group_prefers_configured_name():
    """_select_group returns the configured group, else the first."""
    from planq.providers.redis import RedisBroker, RedisConsumerConfig

    broker = RedisBroker(dsn=_REDIS_ENDPOINT)
    broker._consumer = RedisConsumerConfig(
        group_name="planq-workers", consumer_name="c1"
    )
    groups = [{"name": "other"}, {"name": "planq-workers"}]
    assert broker._select_group(groups)["name"] == "planq-workers"
    # Configured group absent -> fall back to the first group.
    assert broker._select_group([{"name": "other"}])["name"] == "other"
