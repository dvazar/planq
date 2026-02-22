"""Shared fixtures and hypothesis strategies for middleware tests."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from hypothesis import strategies as st

from qanat import types as qanat_types
from qanat.message import BrokerMessage
from qanat.middleware import Middleware
from qanat.models import JsonRpcRequest, JsonRpcResponse

# Rebuild models with proper type namespace
JsonRpcRequest.model_rebuild(_types_namespace=qanat_types.__dict__)
JsonRpcResponse.model_rebuild(_types_namespace=qanat_types.__dict__)


# === Mock Fixtures ===


@pytest.fixture
def mock_broker_message():
    """Mock BrokerMessage with controllable properties."""
    msg = MagicMock(spec=BrokerMessage)
    msg.headers = {}
    msg.body = JsonRpcRequest(
        method="test.method",
        params={"key": "value"},
        id="test-123",
    )
    msg.correlation_id = "test-123"
    msg.delivery_count = 1
    msg.reply_to = None
    msg.raw_message = {"native": "data"}
    msg.ack = AsyncMock()
    msg.nack = AsyncMock()
    msg.reject = AsyncMock()
    return msg


@pytest.fixture
def mock_broker_message_with_ttl(mock_broker_message):
    """Factory for creating message with specific TTL header."""

    def _create(expire_at: str | float | int | None) -> BrokerMessage:
        """Create message with x-expire-at header."""
        msg = mock_broker_message
        if expire_at is not None:
            msg.headers = {"x-expire-at": str(expire_at)}
        else:
            msg.headers = {}
        msg.reject.reset_mock()
        return msg

    return _create


@pytest.fixture
def mock_call_next():
    """AsyncMock for call_next returning None."""
    return AsyncMock(return_value=None)


@pytest.fixture
def tracking_middleware():
    """Middleware that wraps call_next and records calls."""

    class TrackingMiddleware(Middleware):
        """Test implementation that tracks __call__ invocations."""

        def __init__(self):
            self.call_count = 0
            self.last_msg = None
            self.last_response = None

        async def __call__(self, msg, call_next):
            self.call_count += 1
            self.last_msg = msg
            self.last_response = await call_next(msg)
            return self.last_response

    return TrackingMiddleware()


@pytest.fixture
def mutating_middleware():
    """Middleware that mutates params and headers before call_next."""

    class MutatingMiddleware(Middleware):
        """Test implementation that mutates in-place."""

        async def __call__(self, msg, call_next):
            if msg.body.params is not None:
                msg.body.params["injected"] = "value"
            msg.headers["x-custom"] = "middleware"
            return await call_next(msg)

    return MutatingMiddleware()


# === Hypothesis Strategies ===


@st.composite
def valid_expire_at_values(draw):
    """Generate valid expire-at timestamp values (past/future)."""
    # Generate timestamps around current time
    # Use a base timestamp (2025-01-15 00:00:00 UTC = 1736899200)
    base = 1736899200.0
    offset = draw(st.floats(min_value=-86400 * 365, max_value=86400 * 365))
    return base + offset


@st.composite
def valid_headers_with_ttl(draw):
    """Generate headers with x-expire-at."""
    expire_at = draw(valid_expire_at_values())
    return {"x-expire-at": str(expire_at)}


@st.composite
def valid_params(draw):
    """Generate valid JSON-RPC params."""
    return draw(
        st.one_of(
            st.none(),
            st.dictionaries(
                keys=st.text(min_size=1, max_size=20),
                values=st.one_of(
                    st.text(),
                    st.integers(),
                    st.floats(allow_nan=False, allow_infinity=False),
                ),
                max_size=5,
            ),
            st.lists(
                st.one_of(st.text(), st.integers()),
                max_size=5,
            ),
        )
    )
