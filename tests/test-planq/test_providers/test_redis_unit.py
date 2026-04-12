"""Unit tests for Redis provider (no real Redis required)."""

from __future__ import annotations

import asyncio
import json
import math
from contextlib import suppress
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError
from redis.exceptions import ConnectionError

from planq.models import JsonRpcRequest
from planq.providers.redis import (
    _DELAYED_QUEUES_KEY,
    _MAX_RECOVERY_ATTEMPTS,
    _STREAM_SCHEMA_VERSION,
    RedisBroker,
    RedisConsumerConfig,
    RedisMessage,
)

_BODY = '{"jsonrpc":"2.0","method":"ping","params":null,"id":null}'


@pytest.fixture
def broker() -> RedisBroker:
    """RedisBroker with a mocked client."""
    b = RedisBroker(
        dsn="redis://localhost",
        consumer=RedisConsumerConfig(
            group_name="g",
            consumer_name="c",
            claim_idle_ms=0,
        ),
    )
    b._client = AsyncMock()
    return b


class TestConsumeTransientError:
    """Cover the transient-error retry path in consume()."""

    @pytest.mark.asyncio
    async def test_transient_error_retries_then_yields(
        self, broker: RedisBroker
    ) -> None:
        """ConnectionError triggers backoff retry; next read succeeds."""
        call_count = 0

        async def xreadgroup_side(*a, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Recovery pass — no pending messages
                return []
            if call_count == 2:
                # First main-loop read — transient error
                raise ConnectionError("gone")
            if call_count == 3:
                # Retry succeeds
                return [("q", [("1000-0", {"body": _BODY})])]
            # Stop the loop
            raise asyncio.CancelledError

        broker._client.xreadgroup = AsyncMock(side_effect=xreadgroup_side)

        messages = []
        with patch("planq.providers.redis.full_jitter", return_value=0.0):
            with pytest.raises(asyncio.CancelledError):
                async for msg in broker.consume("q", block_ms=0):
                    messages.append(msg)

        assert len(messages) == 1
        assert messages[0].body.method == "ping"


class TestXAutoClaimBackoff:
    """Cover the XAUTOCLAIM error backoff path in consume()."""

    @pytest.mark.asyncio
    async def test_xautoclaim_error_triggers_full_jitter_backoff(
        self,
    ) -> None:
        """Repeated XAUTOCLAIM errors increment a consecutive counter
        and call full_jitter with growing attempt numbers."""
        b = RedisBroker(
            dsn="redis://localhost",
            consumer=RedisConsumerConfig(
                group_name="g",
                consumer_name="c",
                claim_idle_ms=1,
                claim_interval=0,
            ),
        )
        b._client = AsyncMock()
        b._client.xgroup_create = AsyncMock()
        b._client.xautoclaim = AsyncMock(side_effect=ConnectionError("boom"))

        xreadgroup_calls = 0

        async def xreadgroup_side(*a, **kw):
            nonlocal xreadgroup_calls
            xreadgroup_calls += 1
            if xreadgroup_calls == 1:
                # Recovery pass — empty
                return []
            # After several main-loop iterations, stop the loop
            if xreadgroup_calls >= 4:
                raise asyncio.CancelledError
            return []

        b._client.xreadgroup = AsyncMock(side_effect=xreadgroup_side)

        with patch(
            "planq.providers.redis.full_jitter", return_value=0.0
        ) as jitter:
            with pytest.raises(asyncio.CancelledError):
                async for _ in b.consume("q", block_ms=0):
                    pass

        # XAUTOCLAIM errors in at least two main-loop iterations
        # (the first claim_backoff_until is 0 so first attempt runs
        # immediately; subsequent iterations run only after the
        # backoff window closes — with return_value=0.0 for jitter,
        # the window is open every tick).
        assert jitter.call_count >= 2
        # full_jitter(attempts, base, max) — attempts must grow
        attempts = [call.args[0] for call in jitter.call_args_list]
        assert attempts == sorted(attempts)
        assert attempts[-1] > attempts[0]


class TestProducerOnlyBroker:
    """Cover the producer-only path (no RedisConsumerConfig)."""

    def test_construct_without_consumer(self) -> None:
        """Producer-only construction: consumer=None is allowed."""
        b = RedisBroker(dsn="redis://localhost")
        assert b._consumer is None
        assert b._scheduler_task is None

    @pytest.mark.asyncio
    async def test_connect_skips_scheduler_when_no_consumer(self) -> None:
        """connect() does not start the scheduler for producer-only.

        Also verifies that MIGRATE_LUA is not registered: producer-only
        brokers never execute it, so the registration is dead work.
        """
        b = RedisBroker(dsn="redis://localhost")

        fake_client = MagicMock()
        fake_client.register_script = MagicMock(return_value=MagicMock())
        with patch(
            "planq.providers.redis.Redis.from_url",
            return_value=fake_client,
        ):
            await b.connect()
        try:
            assert b._client is fake_client
            assert b._scheduler_task is None
            assert b._migrate_script is None
            fake_client.register_script.assert_not_called()
        finally:
            b._client = None

    @pytest.mark.asyncio
    async def test_connect_registers_script_when_consumer_present(
        self,
    ) -> None:
        """connect() registers MIGRATE_LUA only when consumer is set."""
        b = RedisBroker(
            dsn="redis://localhost",
            consumer=RedisConsumerConfig(group_name="g", consumer_name="c"),
        )

        fake_client = MagicMock()
        fake_script = MagicMock()
        fake_client.register_script = MagicMock(return_value=fake_script)
        with patch(
            "planq.providers.redis.Redis.from_url",
            return_value=fake_client,
        ):
            await b.connect()
        try:
            fake_client.register_script.assert_called_once()
            assert b._migrate_script is fake_script
            assert b._scheduler_task is not None
        finally:
            if b._scheduler_task is not None:
                b._scheduler_task.cancel()
                with suppress(asyncio.CancelledError):
                    await b._scheduler_task
            b._client = None
            b._migrate_script = None
            b._scheduler_task = None

    @pytest.mark.asyncio
    async def test_consume_without_consumer_raises(self) -> None:
        """consume() raises RuntimeError when no RedisConsumerConfig."""
        b = RedisBroker(dsn="redis://localhost")
        b._client = AsyncMock()

        with pytest.raises(RuntimeError, match="RedisConsumerConfig"):
            async for _ in b.consume("q"):
                pass


class TestConnectIdempotent:
    """Cover the idempotent early-return in connect()."""

    @pytest.mark.asyncio
    async def test_connect_twice_reuses_client(self) -> None:
        """Second connect() on producer-only broker is a no-op."""
        b = RedisBroker(dsn="redis://localhost")
        fake_client = MagicMock()
        fake_client.register_script = MagicMock(return_value=MagicMock())

        with patch(
            "planq.providers.redis.Redis.from_url",
            return_value=fake_client,
        ) as from_url:
            await b.connect()
            await b.connect()
        try:
            assert from_url.call_count == 1
            assert b._client is fake_client
        finally:
            b._client = None

    @pytest.mark.asyncio
    async def test_connect_twice_does_not_duplicate_scheduler(self) -> None:
        """Second connect() with consumer does not start a second scheduler."""
        b = RedisBroker(
            dsn="redis://localhost",
            consumer=RedisConsumerConfig(group_name="g", consumer_name="c"),
        )
        fake_client = MagicMock()
        fake_client.register_script = MagicMock(return_value=MagicMock())

        with patch(
            "planq.providers.redis.Redis.from_url",
            return_value=fake_client,
        ) as from_url:
            await b.connect()
            first_task = b._scheduler_task
            await b.connect()
            second_task = b._scheduler_task
        try:
            assert from_url.call_count == 1
            assert first_task is second_task
            fake_client.register_script.assert_called_once()
        finally:
            if b._scheduler_task is not None:
                b._scheduler_task.cancel()
                with suppress(asyncio.CancelledError):
                    await b._scheduler_task
            b._client = None
            b._migrate_script = None
            b._scheduler_task = None


class TestConnectReconnects:
    """Cover the reconnect-after-disconnect path of connect()."""

    @pytest.mark.asyncio
    async def test_connect_after_disconnect_rebuilds_client(self) -> None:
        """connect() after disconnect() builds a fresh client.

        This is the recovery path that lets a producer publish
        again after the broker has been torn down (e.g. by a
        sibling consumer's ``async with broker:`` block exiting).
        """
        b = RedisBroker(dsn="redis://localhost")
        client_v1 = MagicMock()
        client_v1.aclose = AsyncMock()
        client_v2 = MagicMock()

        clients = iter([client_v1, client_v2])

        with patch(
            "planq.providers.redis.Redis.from_url",
            side_effect=lambda *a, **kw: next(clients),
        ) as from_url:
            await b.connect()
            assert b._client is client_v1
            await b.disconnect()
            assert b._client is None
            await b.connect()
            assert b._client is client_v2
            assert from_url.call_count == 2
        b._client = None

    @pytest.mark.asyncio
    async def test_concurrent_connect_after_disconnect_creates_one_client(
        self,
    ) -> None:
        """Two concurrent connects after disconnect only create one client.

        Pre-acquires the broker's connect lock externally so both
        coroutines queue up on it; the first proceeds past the
        inner double-check and creates the client, the second
        wakes inside the lock and hits the inner ``return``.
        """
        b = RedisBroker(dsn="redis://localhost")
        b._connect_lock = asyncio.Lock()
        await b._connect_lock.acquire()

        fake_client = MagicMock()

        with patch(
            "planq.providers.redis.Redis.from_url",
            return_value=fake_client,
        ) as from_url:
            task_a = asyncio.create_task(b.connect())
            task_b = asyncio.create_task(b.connect())

            # Let both tasks reach `async with self._connect_lock`
            # and block on acquire.
            await asyncio.sleep(0)
            await asyncio.sleep(0)

            b._connect_lock.release()
            await asyncio.gather(task_a, task_b)

        assert from_url.call_count == 1
        assert b._client is fake_client
        b._client = None


class TestNotConnected:
    """Cover user-facing RuntimeError when broker is not connected."""

    @pytest.mark.asyncio
    async def test_publish_raises_when_not_connected(self) -> None:
        """publish() on an unconnected broker raises RuntimeError."""
        b = RedisBroker(dsn="redis://localhost")
        rpc = JsonRpcRequest(method="ping", id=None)
        with pytest.raises(RuntimeError, match="not connected"):
            await b.publish("q", rpc)

    @pytest.mark.asyncio
    async def test_consume_raises_when_not_connected(self) -> None:
        """consume() on an unconnected broker raises RuntimeError."""
        b = RedisBroker(
            dsn="redis://localhost",
            consumer=RedisConsumerConfig(group_name="g", consumer_name="c"),
        )
        with pytest.raises(RuntimeError, match="not connected"):
            async for _ in b.consume("q", block_ms=1):
                pass


class TestPoisonMessageXackFailure:
    """Cover the poison-message XACK failure warning path."""

    @pytest.mark.asyncio
    async def test_xack_failure_on_poison_is_logged(
        self, broker: RedisBroker
    ) -> None:
        """When XACK fails during poison handling, a warning is logged.

        The poison message stays in PEL and will be re-delivered on
        next recovery; surfacing the failure (instead of suppressing
        it) is what prevents silent infinite poison loops.
        """
        broker._client.xack = AsyncMock(
            side_effect=ConnectionError("xack boom")
        )

        with patch("planq.providers.redis.logger") as mock_logger:
            result = await broker._parse_entry(
                entry_id="1-0",
                fields={"body": "not valid json", "delivery_count": "1"},
                queue="q",
                queue_name="q",
                received_at=0.0,
            )

        assert result is None
        broker._client.xack.assert_awaited_once_with("q", "g", "1-0")
        mock_logger.warning.assert_called_once()
        call_args = mock_logger.warning.call_args
        assert "Failed to XACK poison message" in call_args.args[0]
        assert call_args.kwargs.get("exc_info") is True


class TestRedisConsumerConfigValidation:
    """Cover @field_validator error paths in RedisConsumerConfig."""

    @pytest.mark.parametrize("field", ["group_name", "consumer_name"])
    @pytest.mark.parametrize("value", ["", "   ", "\t\n"])
    def test_empty_or_whitespace_rejected(self, field: str, value: str) -> None:
        """Empty or whitespace-only identifiers are rejected."""
        kwargs = {"group_name": "g", "consumer_name": "c", field: value}
        with pytest.raises(ValidationError, match="must not be empty"):
            RedisConsumerConfig(**kwargs)

    def test_negative_claim_idle_ms_rejected(self) -> None:
        """Negative claim_idle_ms is rejected (0 is allowed)."""
        with pytest.raises(ValidationError, match="non-negative"):
            RedisConsumerConfig(
                group_name="g", consumer_name="c", claim_idle_ms=-1
            )

    def test_zero_claim_idle_ms_allowed(self) -> None:
        """claim_idle_ms=0 disables XAUTOCLAIM and is explicitly allowed."""
        cfg = RedisConsumerConfig(
            group_name="g", consumer_name="c", claim_idle_ms=0
        )
        assert cfg.claim_idle_ms == 0

    @pytest.mark.parametrize(
        "value, msg",
        [
            (-1.0, "non-negative"),
            (float("nan"), "NaN"),
            (math.inf, "infinite"),
            (-math.inf, "infinite"),
        ],
    )
    def test_claim_interval_rejected(self, value: float, msg: str) -> None:
        """Negative/NaN/Inf claim_interval rejected."""
        with pytest.raises(ValidationError, match=msg):
            RedisConsumerConfig(
                group_name="g", consumer_name="c", claim_interval=value
            )

    def test_zero_claim_interval_allowed(self) -> None:
        """claim_interval=0 means 'no throttling' and is allowed."""
        cfg = RedisConsumerConfig(
            group_name="g", consumer_name="c", claim_interval=0
        )
        assert cfg.claim_interval == 0

    @pytest.mark.parametrize(
        "value, msg",
        [
            (0.0, "positive"),
            (-1.0, "positive"),
            (float("nan"), "NaN"),
            (math.inf, "infinite"),
            (-math.inf, "infinite"),
        ],
    )
    def test_scheduler_interval_rejected(self, value: float, msg: str) -> None:
        """Zero/negative/NaN/Inf scheduler_interval rejected."""
        with pytest.raises(ValidationError, match=msg):
            RedisConsumerConfig(
                group_name="g",
                consumer_name="c",
                scheduler_interval=value,
            )


class TestRedisMessageDeliveryCount:
    """Cover the PEL-aware delivery_count property."""

    def _make_message(
        self, stream_count: str, pel_delivery_count: int
    ) -> RedisMessage:
        broker = MagicMock()
        broker._client = AsyncMock()
        return RedisMessage(
            raw={"body": "{}", "delivery_count": stream_count},
            body=JsonRpcRequest(method="ping", id=None),
            headers={},
            received_at=0.0,
            queue_name="q",
            broker=broker,
            stream_key="q",
            group_name="g",
            entry_id="1-0",
            pel_delivery_count=pel_delivery_count,
        )

    @pytest.mark.parametrize(
        "stream_count, pel_count, expected",
        [
            ("1", 1, 1),  # fresh delivery
            ("1", 2, 2),  # 1 crash, 0 nacks
            ("2", 1, 2),  # 1 nack, 0 crashes
            ("2", 2, 3),  # 1 nack + 1 crash
            ("5", 3, 7),  # 4 nacks + 2 crashes
        ],
    )
    def test_delivery_count_formula(
        self, stream_count: str, pel_count: int, expected: int
    ) -> None:
        """delivery_count = stream_field + (pel_delivery_count - 1)."""
        msg = self._make_message(stream_count, pel_count)
        assert msg.delivery_count == expected

    def test_default_pel_delivery_count_is_one(self) -> None:
        """Omitted pel_delivery_count defaults to 1 (first delivery)."""
        broker = MagicMock()
        broker._client = AsyncMock()
        msg = RedisMessage(
            raw={"body": "{}", "delivery_count": "3"},
            body=JsonRpcRequest(method="ping", id=None),
            headers={},
            received_at=0.0,
            queue_name="q",
            broker=broker,
            stream_key="q",
            group_name="g",
            entry_id="1-0",
        )
        assert msg.delivery_count == 3


class TestGetPelCounts:
    """Cover the _get_pel_counts batch helper."""

    @pytest.mark.asyncio
    async def test_empty_list_short_circuits(self, broker: RedisBroker) -> None:
        """Empty entry_ids returns empty dict without touching Redis."""
        broker._client.pipeline = MagicMock(
            side_effect=AssertionError("pipeline should not be called")
        )
        result = await broker._get_pel_counts("q", [])
        assert result == {}

    @pytest.mark.asyncio
    async def test_pel_counts_merged_from_pipeline(
        self, broker: RedisBroker
    ) -> None:
        """xpending_range results are collected into an entry_id dict."""
        pipe = AsyncMock()
        pipe.xpending_range = MagicMock()
        pipe.execute = AsyncMock(
            return_value=[
                [{"message_id": "1-0", "times_delivered": 2}],
                [],  # second entry no longer in PEL
                [{"message_id": "3-0", "times_delivered": 5}],
            ]
        )
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=None)
        broker._client.pipeline = MagicMock(return_value=pipe)

        result = await broker._get_pel_counts("q", ["1-0", "2-0", "3-0"])
        assert result == {"1-0": 2, "3-0": 5}


class TestSchemaVersion:
    """Cover the schema-version handling in publish and parse paths."""

    @pytest.mark.asyncio
    async def test_publish_immediate_includes_v_field(
        self, broker: RedisBroker
    ) -> None:
        """Immediate publish writes the ``v`` field to the stream entry."""
        broker._client.xadd = AsyncMock(return_value="1-0")
        rpc = JsonRpcRequest(method="ping", id=None)

        await broker.publish("q", rpc)

        broker._client.xadd.assert_awaited_once()
        _, kwargs = broker._client.xadd.call_args
        fields = broker._client.xadd.call_args.args[1]
        assert fields["v"] == _STREAM_SCHEMA_VERSION

    @pytest.mark.asyncio
    async def test_publish_delayed_includes_v_in_payload(
        self, broker: RedisBroker
    ) -> None:
        """Delayed publish serializes ``v`` into the ZSET payload."""
        pipe = AsyncMock()
        pipe.zadd = MagicMock()
        pipe.execute = AsyncMock(return_value=[1, 1])
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=None)
        broker._client.pipeline = MagicMock(return_value=pipe)

        rpc = JsonRpcRequest(method="ping", id=None)
        await broker.publish("q", rpc, delay=10)

        # First pipe.zadd call: delayed payload ZADD
        first_call = pipe.zadd.call_args_list[0]
        delayed_key, mapping = first_call.args
        assert delayed_key == "q:delayed"
        payload_json = next(iter(mapping))
        payload = json.loads(payload_json)
        assert payload["v"] == _STREAM_SCHEMA_VERSION

    @pytest.mark.asyncio
    async def test_parse_entry_warns_on_unknown_version(
        self, broker: RedisBroker
    ) -> None:
        """Unknown schema version triggers a warning but still parses."""
        broker._client.xack = AsyncMock()

        with patch("planq.providers.redis.logger") as mock_logger:
            msg = await broker._parse_entry(
                entry_id="1-0",
                fields={
                    "v": "99",
                    "body": JsonRpcRequest(
                        method="ping", id=None
                    ).model_dump_json(),
                    "delivery_count": "1",
                },
                queue="q",
                queue_name="q",
                received_at=0.0,
            )

        assert msg is not None
        assert msg.body.method == "ping"
        assert any(
            "Unknown stream schema version" in call.args[0]
            for call in mock_logger.warning.call_args_list
        )

    @pytest.mark.asyncio
    async def test_parse_entry_no_warning_for_legacy_entry(
        self, broker: RedisBroker
    ) -> None:
        """Legacy entries without the ``v`` field default to v1, no warning."""
        broker._client.xack = AsyncMock()

        with patch("planq.providers.redis.logger") as mock_logger:
            msg = await broker._parse_entry(
                entry_id="1-0",
                fields={
                    "body": JsonRpcRequest(
                        method="ping", id=None
                    ).model_dump_json(),
                    "delivery_count": "1",
                },
                queue="q",
                queue_name="q",
                received_at=0.0,
            )

        assert msg is not None
        assert not any(
            "Unknown stream schema version" in call.args[0]
            for call in mock_logger.warning.call_args_list
        )


class TestDelayedQueueRegistry:
    """Cover the persistent delayed-queue ZSET registry."""

    @pytest.mark.asyncio
    async def test_list_delayed_queues_reads_zset(
        self, broker: RedisBroker
    ) -> None:
        """``_list_delayed_queues`` returns members of the registry ZSET."""
        broker._client.zrange = AsyncMock(return_value=["q1", "q2"])

        result = await broker._list_delayed_queues()

        broker._client.zrange.assert_awaited_once_with(
            _DELAYED_QUEUES_KEY, 0, -1
        )
        assert result == ["q1", "q2"]

    @pytest.mark.asyncio
    async def test_publish_delayed_registers_queue_in_registry(
        self, broker: RedisBroker
    ) -> None:
        """Delayed publish issues both payload and registry ZADDs atomically."""
        pipe = AsyncMock()
        pipe.zadd = MagicMock()
        pipe.execute = AsyncMock(return_value=[1, 1])
        pipe.__aenter__ = AsyncMock(return_value=pipe)
        pipe.__aexit__ = AsyncMock(return_value=None)
        broker._client.pipeline = MagicMock(return_value=pipe)

        rpc = JsonRpcRequest(method="ping", id=None)
        delayed_id = await broker.publish("q", rpc, delay=5)

        broker._client.pipeline.assert_called_once_with(transaction=True)
        assert pipe.zadd.call_count == 2
        # Second call registers the queue in the registry ZSET.
        registry_call = pipe.zadd.call_args_list[1]
        assert registry_call.args[0] == _DELAYED_QUEUES_KEY
        assert "q" in registry_call.args[1]
        assert registry_call.kwargs.get("nx") is True
        # Returned ID is a UUID string, not a stream entry ID.
        assert len(delayed_id) == 36

    @pytest.mark.asyncio
    async def test_publish_immediate_does_not_touch_registry(
        self, broker: RedisBroker
    ) -> None:
        """Immediate publish bypasses the registry (no delayed payload)."""
        broker._client.xadd = AsyncMock(return_value="1-0")
        broker._client.pipeline = MagicMock(
            side_effect=AssertionError("pipeline should not be called")
        )

        rpc = JsonRpcRequest(method="ping", id=None)
        await broker.publish("q", rpc)

        broker._client.xadd.assert_awaited_once()


class TestSchedulerAutoRestart:
    """Cover the outer try/except in ``_run_scheduler``."""

    @pytest.mark.asyncio
    async def test_scheduler_catches_exception_and_retries(self) -> None:
        """Scheduler logs warning and loops again after an unexpected error."""
        b = RedisBroker(
            dsn="redis://localhost",
            consumer=RedisConsumerConfig(
                group_name="g",
                consumer_name="c",
                scheduler_interval=0.01,
            ),
        )
        b._client = AsyncMock()

        call_count = 0

        async def failing_list() -> list[str]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("boom")
            raise asyncio.CancelledError

        b._list_delayed_queues = failing_list  # type: ignore[method-assign]

        with patch("planq.providers.redis.full_jitter", return_value=0.0):
            with patch("planq.providers.redis.logger") as mock_logger:
                with pytest.raises(asyncio.CancelledError):
                    await b._run_scheduler()

        assert call_count >= 2
        assert any(
            "Scheduler loop error" in call.args[0]
            for call in mock_logger.warning.call_args_list
        )

    @pytest.mark.asyncio
    async def test_scheduler_reraises_cancelled_error(self) -> None:
        """CancelledError from the inner body propagates out of the loop."""
        b = RedisBroker(
            dsn="redis://localhost",
            consumer=RedisConsumerConfig(
                group_name="g",
                consumer_name="c",
                scheduler_interval=0.01,
            ),
        )
        b._client = AsyncMock()

        async def cancelled_list() -> list[str]:
            raise asyncio.CancelledError

        b._list_delayed_queues = cancelled_list  # type: ignore[method-assign]

        with pytest.raises(asyncio.CancelledError):
            await b._run_scheduler()


class TestRecoveryRetry:
    """Cover the recovery-pass retry loop in ``consume()``."""

    @pytest.mark.asyncio
    async def test_recovery_retries_on_connection_error(
        self, broker: RedisBroker
    ) -> None:
        """Transient error during recovery triggers retry, then success."""
        call_count = 0
        good_body = JsonRpcRequest(method="recovery", id=None).model_dump_json()

        async def xreadgroup_side(*a, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("flaky")
            if call_count == 2:
                return [("q", [("1-0", {"body": good_body})])]
            if call_count == 3:
                # Drain recovery pass.
                return []
            raise asyncio.CancelledError

        broker._client.xreadgroup = AsyncMock(side_effect=xreadgroup_side)
        broker._get_pel_counts = AsyncMock(return_value={})  # type: ignore[method-assign]

        messages = []
        with patch("planq.providers.redis.full_jitter", return_value=0.0):
            with pytest.raises(asyncio.CancelledError):
                async for msg in broker.consume("q", block_ms=0):
                    messages.append(msg)

        assert len(messages) == 1
        assert messages[0].body.method == "recovery"

    @pytest.mark.asyncio
    async def test_recovery_gives_up_after_max_attempts(
        self, broker: RedisBroker
    ) -> None:
        """After ``_MAX_RECOVERY_ATTEMPTS`` errors, recovery falls through."""
        call_count = 0

        async def always_fails(*a, **kw):
            nonlocal call_count
            call_count += 1
            if call_count <= _MAX_RECOVERY_ATTEMPTS + 1:
                raise ConnectionError("still flaky")
            raise asyncio.CancelledError

        broker._client.xreadgroup = AsyncMock(side_effect=always_fails)

        with patch("planq.providers.redis.full_jitter", return_value=0.0):
            with patch("planq.providers.redis.logger") as mock_logger:
                with pytest.raises(asyncio.CancelledError):
                    async for _ in broker.consume("q", block_ms=0):
                        pass

        assert any(
            "PEL recovery failed after" in call.args[0]
            for call in mock_logger.error.call_args_list
        )

    @pytest.mark.asyncio
    async def test_recovery_non_transient_error_falls_through(
        self, broker: RedisBroker
    ) -> None:
        """Non-transient exceptions break recovery without retry."""
        call_count = 0

        async def xreadgroup_side(*a, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("oops")
            raise asyncio.CancelledError

        broker._client.xreadgroup = AsyncMock(side_effect=xreadgroup_side)

        with patch("planq.providers.redis.logger") as mock_logger:
            with pytest.raises(asyncio.CancelledError):
                async for _ in broker.consume("q", block_ms=0):
                    pass

        assert any(
            "Failed to read pending messages on startup" in call.args[0]
            for call in mock_logger.warning.call_args_list
        )


class TestPoisonHookFailureNoXack:
    """Cover the poison hook-failure path where XACK is skipped."""

    @pytest.mark.asyncio
    async def test_hook_failure_skips_xack(self, broker: RedisBroker) -> None:
        """When on_poison_message raises, XACK must NOT be called.

        The entry stays in PEL so a next recovery pass has a chance
        to re-process it with a (hopefully fixed) hook — this is what
        prevents silent data loss from a misconfigured hook.
        """
        broker._client.xack = AsyncMock()
        broker.on_poison_message = AsyncMock(
            side_effect=RuntimeError("hook crashed")
        )

        with patch("planq.providers.redis.logger") as mock_logger:
            result = await broker._parse_entry(
                entry_id="1-0",
                fields={"body": "not valid json", "delivery_count": "1"},
                queue="q",
                queue_name="q",
                received_at=0.0,
            )

        assert result is None
        broker._client.xack.assert_not_called()
        assert any(
            "Failed to handle poison message" in call.args[0]
            for call in mock_logger.error.call_args_list
        )
