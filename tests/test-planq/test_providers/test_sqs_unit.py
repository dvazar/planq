"""Unit tests for SQS provider (no real AWS / ElasticMQ required)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from planq.providers.sqs import SqsBroker


def _make_session_factory() -> tuple[MagicMock, MagicMock, MagicMock]:
    """Build a mocked AioSession + client_ctx + client triple.

    Returns the session class mock, the client_ctx mock, and the
    underlying mocked client. The session class returns a session
    whose ``create_client(...)`` returns the client_ctx, whose
    ``__aenter__`` returns the client.
    """
    client = MagicMock(name="sqs_client")
    client_ctx = MagicMock(name="client_ctx")
    client_ctx.__aenter__ = AsyncMock(return_value=client)
    client_ctx.__aexit__ = AsyncMock(return_value=None)

    session = MagicMock(name="session")
    session.create_client = MagicMock(return_value=client_ctx)

    session_cls = MagicMock(name="AioSession", return_value=session)
    return session_cls, client_ctx, client


class TestSqsConnectIdempotent:
    """Cover the idempotent fast path and reconnect path."""

    @pytest.mark.asyncio
    async def test_connect_twice_reuses_client(self) -> None:
        """Second connect() is a no-op fast path."""
        session_cls, _client_ctx, client = _make_session_factory()

        with patch(
            "planq.providers.sqs.AioSession", session_cls
        ):
            broker = SqsBroker(dsn="http://localhost:9324")
            await broker.connect()
            first_client = broker._client
            await broker.connect()
            assert broker._client is first_client
            assert session_cls.call_count == 1

        broker._client = None
        broker._client_ctx = None

    @pytest.mark.asyncio
    async def test_connect_after_disconnect_rebuilds_client(self) -> None:
        """connect() after disconnect() builds a fresh client."""
        session_cls_v1, _ctx_v1, client_v1 = _make_session_factory()
        session_cls_v2, _ctx_v2, client_v2 = _make_session_factory()

        sessions = iter([session_cls_v1.return_value, session_cls_v2.return_value])

        def fake_session() -> MagicMock:
            return next(sessions)

        with patch(
            "planq.providers.sqs.AioSession", side_effect=fake_session
        ):
            broker = SqsBroker(dsn="http://localhost:9324")
            await broker.connect()
            assert broker._client is client_v1
            await broker.disconnect()
            assert broker._client is None
            await broker.connect()
            assert broker._client is client_v2

        broker._client = None
        broker._client_ctx = None

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
        session_cls, _client_ctx, _client = _make_session_factory()

        with patch(
            "planq.providers.sqs.AioSession", session_cls
        ):
            broker = SqsBroker(dsn="http://localhost:9324")
            broker._connect_lock = asyncio.Lock()
            await broker._connect_lock.acquire()

            task_a = asyncio.create_task(broker.connect())
            task_b = asyncio.create_task(broker.connect())

            await asyncio.sleep(0)
            await asyncio.sleep(0)

            broker._connect_lock.release()
            await asyncio.gather(task_a, task_b)

            assert session_cls.call_count == 1

        broker._client = None
        broker._client_ctx = None
