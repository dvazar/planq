"""Shared fixtures for parameter introspection tests."""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from planq.message import BrokerMessage
from planq.models import JsonRpcRequest


@pytest.fixture
def mock_msg():
    """Factory for creating mock BrokerMessage instances."""

    def _create(
        method: str = "test.method",
        params=None,
        msg_id: str | None = "test-123",
        delivery_count: int = 1,
        reply_to: str | None = "reply-queue",
    ):
        msg = MagicMock(spec=BrokerMessage)
        msg.body = JsonRpcRequest(method=method, params=params, id=msg_id)
        msg.correlation_id = msg_id
        msg.headers = {}
        msg.delivery_count = delivery_count
        msg.reply_to = reply_to
        msg.enqueued_at = time.time() - 0.1
        msg.received_at = time.time()
        msg.ack = AsyncMock()
        msg.nack = AsyncMock()
        msg.reject = AsyncMock()
        return msg

    return _create
