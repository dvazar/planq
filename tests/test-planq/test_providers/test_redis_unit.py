"""Unit tests for Redis provider (no real Redis required)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from redis.exceptions import ConnectionError

from planq.providers.redis import RedisBroker

_BODY = '{"jsonrpc":"2.0","method":"ping","params":null,"id":null}'


@pytest.fixture
def broker() -> RedisBroker:
    """RedisBroker with a mocked client."""
    b = RedisBroker(
        dsn="redis://localhost",
        group_name="g",
        consumer_name="c",
        claim_idle_ms=0,
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
