"""Shared fixtures and hypothesis strategies for middleware tests."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from hypothesis import strategies as st

from qanat import types as qanat_types
from qanat.consumer import QanatConsumer
from qanat.message import BrokerMessage
from qanat.middleware import Middleware
from qanat.models import JsonRpcRequest, JsonRpcResponse

# Rebuild models with proper type namespace
JsonRpcRequest.model_rebuild(_types_namespace=qanat_types.__dict__)
JsonRpcResponse.model_rebuild(_types_namespace=qanat_types.__dict__)


# === Mock Fixtures ===


@pytest.fixture
def mock_consumer():
    """Mock QanatConsumer instance."""
    return MagicMock(spec=QanatConsumer)


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
def concrete_middleware():
    """Concrete Middleware subclass for testing hooks."""

    class TestMiddleware(Middleware):
        """Test implementation that tracks hook calls."""

        def __init__(self):
            """Initialize tracking flags."""
            self.before_process_called = False
            self.after_process_called = False
            self.after_skip_called = False
            self.before_publish_called = False
            self.last_consumer = None
            self.last_msg = None
            self.last_result = None
            self.last_exception = None
            self.last_response = None
            self.last_headers = None

        async def before_process_message(self, consumer, msg):
            """Track before_process_message calls."""
            self.before_process_called = True
            self.last_consumer = consumer
            self.last_msg = msg

        async def after_process_message(
            self, consumer, msg, *, result=None, exception=None
        ):
            """Track after_process_message calls."""
            self.after_process_called = True
            self.last_consumer = consumer
            self.last_msg = msg
            self.last_result = result
            self.last_exception = exception

        async def after_skip_message(self, consumer, msg):
            """Track after_skip_message calls."""
            self.after_skip_called = True
            self.last_consumer = consumer
            self.last_msg = msg

        async def before_publish_response(
            self, consumer, msg, response, headers
        ):
            """Track before_publish_response calls."""
            self.before_publish_called = True
            self.last_consumer = consumer
            self.last_msg = msg
            self.last_response = response
            self.last_headers = headers

    return TestMiddleware()


@pytest.fixture
def mutating_middleware():
    """Middleware that mutates params and headers."""

    class MutatingMiddleware(Middleware):
        """Test implementation that mutates in-place."""

        async def before_process_message(self, consumer, msg):
            """Mutate params and headers in-place."""
            if msg.body.params is not None:
                msg.body.params["injected"] = "value"
            msg.headers["x-custom"] = "middleware"

        async def before_publish_response(
            self, consumer, msg, response, headers
        ):
            """Mutate headers in-place."""
            headers["x-response-header"] = "custom-value"

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
