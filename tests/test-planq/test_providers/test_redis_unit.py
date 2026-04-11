"""Unit tests for Redis provider (no real Redis required)."""

from __future__ import annotations

import asyncio
import math
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError
from redis.exceptions import ConnectionError

from planq.providers.redis import RedisBroker, RedisConsumerConfig

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


class TestProducerOnlyBroker:
    """Cover the producer-only path (no RedisConsumerConfig)."""

    def test_construct_without_consumer(self) -> None:
        """Producer-only construction: consumer=None is allowed."""
        b = RedisBroker(dsn="redis://localhost")
        assert b._consumer is None
        assert b._scheduler_task is None

    @pytest.mark.asyncio
    async def test_connect_skips_scheduler_when_no_consumer(self) -> None:
        """connect() does not start the scheduler for producer-only."""
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
        finally:
            b._client = None

    @pytest.mark.asyncio
    async def test_consume_without_consumer_raises(self) -> None:
        """consume() raises RuntimeError when no RedisConsumerConfig."""
        b = RedisBroker(dsn="redis://localhost")
        b._client = AsyncMock()

        with pytest.raises(RuntimeError, match="RedisConsumerConfig"):
            async for _ in b.consume("q"):
                pass


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
