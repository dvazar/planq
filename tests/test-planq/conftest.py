"""Shared fixtures and hypothesis strategies for planq tests."""

from __future__ import annotations

import pytest
from hypothesis import settings
from hypothesis import strategies as st

from planq.models import JsonRpcRequest

# Configure hypothesis for consistent test behavior
settings.register_profile("default", max_examples=100, deadline=1000)
settings.load_profile("default")


# === JsonRpcRequest Fixtures ===


@pytest.fixture
def json_rpc_notification():
    """JsonRpcRequest with id=None (notification)."""
    return JsonRpcRequest(
        method="test.notification",
        params={"data": "value"},
        id=None,
    )


@pytest.fixture
def json_rpc_request_string_id():
    """JsonRpcRequest with string ID."""
    return JsonRpcRequest(
        method="test.request",
        params={"data": "value"},
        id="request-123",
    )


@pytest.fixture
def json_rpc_request_int_id():
    """JsonRpcRequest with integer ID."""
    return JsonRpcRequest(
        method="test.request",
        params={"data": "value"},
        id=42,
    )


# === Raw Message Fixtures ===


@pytest.fixture
def raw_message_dict():
    """Simple dict as raw message."""
    return {"native_field": "native_value", "count": 1}


@pytest.fixture
def raw_message_object():
    """Custom object as raw message."""

    class NativeMessage:
        def __init__(self):
            self.id = "msg-001"
            self.data = b"binary data"

    return NativeMessage()


# === Headers Fixtures ===


@pytest.fixture
def empty_headers():
    """Empty headers dict."""
    return {}


@pytest.fixture
def headers_with_values():
    """Headers dict with sample values."""
    return {
        "x-expire-at": "1234567890.5",
        "x-max-retries": "3",
        "x-correlation-id": "abc-123",
    }


@pytest.fixture
def current_timestamp():
    """Fixed timestamp for reproducible tests."""
    return 1234567890.0


@pytest.fixture
def queue_name_default():
    """Default queue name for test messages."""
    return "test-queue"


@pytest.fixture
def broker_message_factory(current_timestamp, queue_name_default):
    """Factory for creating BrokerMessage instances with defaults.

    Usage:
        msg = broker_message_factory(raw=raw, body=body, headers=headers)
        msg = broker_message_factory(raw=raw, body=body, headers={},
                                     received_at=custom_time)
    """

    def _create(raw, body, headers, received_at=None, queue_name=None):
        from planq.message import BrokerMessage

        return BrokerMessage(
            raw=raw,
            body=body,
            headers=headers,
            received_at=received_at or current_timestamp,
            queue_name=queue_name or queue_name_default,
        )

    return _create


# === Hypothesis Strategies ===


@st.composite
def valid_jsonrpc_ids(draw):
    """Generate valid JSON-RPC ID values."""
    return draw(
        st.one_of(
            st.none(),
            st.text(min_size=1, max_size=50),
            st.integers(),
        )
    )


@st.composite
def valid_headers(draw):
    """Generate valid headers dictionaries."""
    return draw(
        st.dictionaries(
            keys=st.text(min_size=1, max_size=20),
            values=st.text(min_size=0, max_size=50),
            max_size=10,
        )
    )


@st.composite
def raw_messages(draw):
    """Generate various raw message types."""
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
            st.text(),
            st.integers(),
        )
    )


# === Execution Mode Test Fixtures ===


@pytest.fixture
def thread_consumer():
    """Consumer without process workers (THREAD/ASYNC only).

    Process mode disabled by setting process_workers=None.
    """
    from unittest.mock import MagicMock

    from planq.app import Planq
    from planq.consumer import PlanqConsumer

    broker = MagicMock()
    app = Planq(broker=broker)
    return PlanqConsumer(app, process_workers=None, middlewares=[])


@pytest.fixture
def process_consumer():
    """Consumer with process workers (all modes supported).

    Yields consumer with 2 worker processes, ensures cleanup.
    """
    from unittest.mock import MagicMock

    from planq.app import Planq
    from planq.consumer import PlanqConsumer

    broker = MagicMock()
    app = Planq(broker=broker)
    consumer = PlanqConsumer(app, process_workers=2, middlewares=[])
    yield consumer
    # Critical: cleanup process pool to prevent resource leaks
    if consumer._pool:
        consumer._pool.shutdown(wait=True)


@pytest.fixture
def sync_handler():
    """Synchronous handler for THREAD mode tests.

    Returns a simple result without blocking.
    """

    def handler(*args, **kwargs):
        return "sync result"

    return handler


@pytest.fixture
def slow_sync_handler():
    """Synchronous handler that sleeps for timeout tests.

    Args:
        duration: Sleep time in seconds (default 0.5s).
    """
    import time

    def handler(duration=0.5):
        time.sleep(duration)
        return "slow result"

    return handler


@pytest.fixture
def cancellation_aware_handler():
    """Handler that checks ctx.is_cancelled periodically.

    Returns "cancelled" if context is cancelled, "completed" otherwise.
    """
    import time

    from planq.context import get_planq_context

    def handler():
        ctx = get_planq_context()
        for _ in range(10):
            if ctx.is_cancelled:
                return "cancelled"
            time.sleep(0.1)
        return "completed"

    return handler
