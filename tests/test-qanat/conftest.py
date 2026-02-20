"""Shared fixtures and hypothesis strategies for qanat tests."""

from __future__ import annotations

import pytest
from hypothesis import settings
from hypothesis import strategies as st

from qanat import types as qanat_types
from qanat.models import JsonRpcRequest

# Configure hypothesis for consistent test behavior
settings.register_profile("default", max_examples=100, deadline=1000)
settings.load_profile("default")

# Rebuild JsonRpcRequest with proper type namespace
JsonRpcRequest.model_rebuild(_types_namespace=qanat_types.__dict__)


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
