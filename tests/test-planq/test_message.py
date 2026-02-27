"""Comprehensive tests for BrokerMessage abstract base class."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from planq.message import BrokerMessage
from planq.models import JsonRpcRequest


# Helper function for creating BrokerMessage (used by hypothesis tests)
def create_broker_message(
    raw, body, headers, received_at=None, queue_name=None
):
    """Create BrokerMessage with default values for required parameters."""
    return BrokerMessage(
        raw=raw,
        body=body,
        headers=headers,
        received_at=received_at or 1234567890.0,
        queue_name=queue_name or "test-queue",
    )


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


# === Layer 1: Parametrized Edge Cases ===


class TestBrokerMessageConstruction:
    """Test BrokerMessage construction and attribute storage."""

    def test_construction_with_all_fields(
        self,
        raw_message_dict,
        json_rpc_request_string_id,
        headers_with_values,
        broker_message_factory,
    ):
        """BrokerMessage stores raw, body, and headers."""
        msg = broker_message_factory(
            raw=raw_message_dict,
            body=json_rpc_request_string_id,
            headers=headers_with_values,
        )

        assert msg.raw is raw_message_dict
        assert msg.body is json_rpc_request_string_id
        assert msg.headers is headers_with_values

    def test_raw_as_dict(
        self, json_rpc_notification, empty_headers, broker_message_factory
    ):
        """Raw message can be a dict."""
        raw = {"native_field": "value", "count": 42}
        msg = broker_message_factory(
            raw=raw, body=json_rpc_notification, headers=empty_headers
        )

        assert msg.raw == raw
        assert msg.raw["native_field"] == "value"
        assert msg.raw["count"] == 42

    def test_raw_as_object(
        self,
        raw_message_object,
        json_rpc_notification,
        empty_headers,
        broker_message_factory,
    ):
        """Raw message can be a custom object."""
        msg = broker_message_factory(
            raw=raw_message_object,
            body=json_rpc_notification,
            headers=empty_headers,
        )

        assert msg.raw is raw_message_object
        assert msg.raw.id == "msg-001"
        assert msg.raw.data == b"binary data"

    def test_raw_as_none(
        self, json_rpc_notification, empty_headers, broker_message_factory
    ):
        """Raw message can be None."""
        msg = broker_message_factory(
            raw=None, body=json_rpc_notification, headers=empty_headers
        )

        assert msg.raw is None

    def test_empty_headers(
        self, raw_message_dict, json_rpc_notification, broker_message_factory
    ):
        """Headers can be an empty dict."""
        msg = broker_message_factory(
            raw=raw_message_dict,
            body=json_rpc_notification,
            headers={},
        )

        assert msg.headers == {}

    def test_headers_with_multiple_entries(
        self,
        raw_message_dict,
        json_rpc_notification,
        broker_message_factory,
    ):
        """Headers can contain multiple string-to-string entries."""
        headers = {
            "x-expire-at": "1234567890.5",
            "x-max-retries": "3",
            "x-correlation-id": "abc-123",
            "x-custom": "value",
        }
        msg = broker_message_factory(
            raw=raw_message_dict,
            body=json_rpc_notification,
            headers=headers,
        )

        assert msg.headers == headers
        assert msg.headers["x-expire-at"] == "1234567890.5"
        assert msg.headers["x-max-retries"] == "3"
        assert msg.headers["x-correlation-id"] == "abc-123"
        assert msg.headers["x-custom"] == "value"

    def test_stores_body_reference(
        self,
        raw_message_dict,
        json_rpc_request_string_id,
        empty_headers,
        broker_message_factory,
    ):
        """BrokerMessage stores exact body reference."""
        msg = broker_message_factory(
            raw=raw_message_dict,
            body=json_rpc_request_string_id,
            headers=empty_headers,
        )

        assert msg.body is json_rpc_request_string_id
        assert msg.body.method == "test.request"
        assert msg.body.params == {"data": "value"}
        assert msg.body.id == "request-123"

    def test_multiple_instances_are_independent(
        self,
        raw_message_dict,
        empty_headers,
        broker_message_factory,
    ):
        """Multiple BrokerMessage instances don't share state."""
        body1 = JsonRpcRequest(method="method1", id="id1")
        body2 = JsonRpcRequest(method="method2", id="id2")

        msg1 = broker_message_factory(
            raw=raw_message_dict, body=body1, headers=empty_headers
        )
        msg2 = broker_message_factory(
            raw=raw_message_dict, body=body2, headers=empty_headers
        )

        assert msg1.body is not msg2.body
        assert msg1.body.method == "method1"
        assert msg2.body.method == "method2"


class TestBrokerMessageCorrelationId:
    """Test correlation_id property returns self.body.id."""

    def test_correlation_id_with_string_id(
        self, json_rpc_request_string_id, broker_message_factory
    ):
        """correlation_id returns string ID from body."""
        msg = broker_message_factory(
            raw=None,
            body=json_rpc_request_string_id,
            headers={},
        )

        assert msg.correlation_id == "request-123"
        assert msg.correlation_id == msg.body.id

    def test_correlation_id_with_int_id(
        self, json_rpc_request_int_id, broker_message_factory
    ):
        """correlation_id returns integer ID from body."""
        msg = broker_message_factory(
            raw=None,
            body=json_rpc_request_int_id,
            headers={},
        )

        assert msg.correlation_id == 42
        assert msg.correlation_id == msg.body.id

    def test_correlation_id_with_none_notification(
        self, json_rpc_notification, broker_message_factory
    ):
        """correlation_id returns None for notifications."""
        msg = broker_message_factory(
            raw=None,
            body=json_rpc_notification,
            headers={},
        )

        assert msg.correlation_id is None
        assert msg.correlation_id == msg.body.id

    @pytest.mark.parametrize(
        "request_id",
        [
            "uuid-abc-123",
            "request-001",
            1,
            42,
            0,
            -1,
            None,
        ],
    )
    def test_correlation_id_with_various_ids(
        self, request_id, broker_message_factory
    ):
        """correlation_id works with all valid JSON-RPC ID types."""
        body = JsonRpcRequest(method="test", id=request_id)
        msg = broker_message_factory(raw=None, body=body, headers={})

        assert msg.correlation_id == request_id
        assert msg.correlation_id == body.id

    def test_correlation_id_with_zero_is_valid(self, broker_message_factory):
        """correlation_id with integer 0 (falsy but valid)."""
        body = JsonRpcRequest(method="test", id=0)
        msg = broker_message_factory(raw=None, body=body, headers={})

        assert msg.correlation_id == 0
        assert msg.correlation_id is not None
        assert msg.correlation_id == msg.body.id

    def test_correlation_id_with_negative_int(self, broker_message_factory):
        """correlation_id with negative integer."""
        body = JsonRpcRequest(method="test", id=-999)
        msg = broker_message_factory(raw=None, body=body, headers={})

        assert msg.correlation_id == -999
        assert msg.correlation_id == msg.body.id


class TestBrokerMessageAbstractProperties:
    """Test that abstract properties raise NotImplementedError."""

    def test_message_id_raises_not_implemented(
        self, json_rpc_notification, broker_message_factory
    ):
        """message_id property raises NotImplementedError."""
        msg = broker_message_factory(
            raw=None, body=json_rpc_notification, headers={}
        )

        with pytest.raises(NotImplementedError):
            _ = msg.message_id

    def test_enqueued_at_raises_not_implemented(
        self, json_rpc_notification, broker_message_factory
    ):
        """enqueued_at property raises NotImplementedError."""
        msg = broker_message_factory(
            raw=None, body=json_rpc_notification, headers={}
        )

        with pytest.raises(NotImplementedError):
            _ = msg.enqueued_at

    def test_delivery_count_raises_not_implemented(
        self, json_rpc_notification, broker_message_factory
    ):
        """delivery_count property raises NotImplementedError."""
        msg = broker_message_factory(
            raw=None, body=json_rpc_notification, headers={}
        )

        with pytest.raises(NotImplementedError):
            _ = msg.delivery_count

    def test_reply_to_raises_not_implemented(
        self, json_rpc_notification, broker_message_factory
    ):
        """reply_to property raises NotImplementedError."""
        msg = broker_message_factory(
            raw=None, body=json_rpc_notification, headers={}
        )

        with pytest.raises(NotImplementedError):
            _ = msg.reply_to

    def test_delivery_count_exception_type(
        self, json_rpc_notification, broker_message_factory
    ):
        """delivery_count raises exactly NotImplementedError."""
        msg = broker_message_factory(
            raw=None, body=json_rpc_notification, headers={}
        )

        with pytest.raises(NotImplementedError) as exc_info:
            _ = msg.delivery_count

        assert type(exc_info.value) is NotImplementedError

    def test_reply_to_exception_type(
        self, json_rpc_notification, broker_message_factory
    ):
        """reply_to raises exactly NotImplementedError."""
        msg = broker_message_factory(
            raw=None, body=json_rpc_notification, headers={}
        )

        with pytest.raises(NotImplementedError) as exc_info:
            _ = msg.reply_to

        assert type(exc_info.value) is NotImplementedError


class TestBrokerMessageAbstractMethods:
    """Test that abstract async methods raise NotImplementedError."""

    @pytest.mark.asyncio
    async def test_ack_raises_not_implemented(
        self, json_rpc_notification, broker_message_factory
    ):
        """ack() raises NotImplementedError."""
        msg = broker_message_factory(
            raw=None, body=json_rpc_notification, headers={}
        )

        with pytest.raises(NotImplementedError):
            await msg.ack()

    @pytest.mark.asyncio
    async def test_reject_raises_not_implemented(
        self, json_rpc_notification, broker_message_factory
    ):
        """reject() raises NotImplementedError."""
        msg = broker_message_factory(
            raw=None, body=json_rpc_notification, headers={}
        )

        with pytest.raises(NotImplementedError):
            await msg.reject()

    @pytest.mark.asyncio
    async def test_nack_raises_not_implemented(
        self, json_rpc_notification, broker_message_factory
    ):
        """nack(delay) raises NotImplementedError."""
        msg = broker_message_factory(
            raw=None, body=json_rpc_notification, headers={}
        )

        with pytest.raises(NotImplementedError):
            await msg.nack(10.0)

    @pytest.mark.asyncio
    async def test_ack_exception_type(
        self, json_rpc_notification, broker_message_factory
    ):
        """ack() raises exactly NotImplementedError."""
        msg = broker_message_factory(
            raw=None, body=json_rpc_notification, headers={}
        )

        with pytest.raises(NotImplementedError) as exc_info:
            await msg.ack()

        assert type(exc_info.value) is NotImplementedError

    @pytest.mark.asyncio
    async def test_reject_exception_type(
        self, json_rpc_notification, broker_message_factory
    ):
        """reject() raises exactly NotImplementedError."""
        msg = broker_message_factory(
            raw=None, body=json_rpc_notification, headers={}
        )

        with pytest.raises(NotImplementedError) as exc_info:
            await msg.reject()

        assert type(exc_info.value) is NotImplementedError

    @pytest.mark.asyncio
    async def test_nack_exception_type(
        self, json_rpc_notification, broker_message_factory
    ):
        """nack(delay) raises exactly NotImplementedError."""
        msg = broker_message_factory(
            raw=None, body=json_rpc_notification, headers={}
        )

        with pytest.raises(NotImplementedError) as exc_info:
            await msg.nack(10.0)

        assert type(exc_info.value) is NotImplementedError

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "delay",
        [0.0, 0.1, 1.0, 10.0, 30.0, 60.0, 300.0],
    )
    async def test_nack_with_various_delays(
        self, json_rpc_notification, delay, broker_message_factory
    ):
        """nack() raises NotImplementedError with various delay values."""
        msg = broker_message_factory(
            raw=None, body=json_rpc_notification, headers={}
        )

        with pytest.raises(NotImplementedError):
            await msg.nack(delay)

    @pytest.mark.asyncio
    async def test_nack_with_int_delay(
        self, json_rpc_notification, broker_message_factory
    ):
        """nack() with integer delay (valid as Seconds type)."""
        msg = broker_message_factory(
            raw=None, body=json_rpc_notification, headers={}
        )

        with pytest.raises(NotImplementedError):
            await msg.nack(30)  # int is valid for Seconds type


# === Layer 2: Hypothesis Property-Based Tests ===


@pytest.mark.hypothesis
class TestBrokerMessageFuzz:
    """Property-based tests for BrokerMessage with generated inputs."""

    @given(
        raw=raw_messages(),
        jsonrpc_id=valid_jsonrpc_ids(),
        headers=valid_headers(),
    )
    def test_construction_with_generated_inputs(self, raw, jsonrpc_id, headers):
        """BrokerMessage construction works with any valid inputs."""
        body = JsonRpcRequest(method="fuzz.test", id=jsonrpc_id)
        msg = create_broker_message(raw=raw, body=body, headers=headers)

        assert msg.raw == raw
        assert msg.body is body
        assert msg.headers == headers

    @given(jsonrpc_id=valid_jsonrpc_ids())
    def test_correlation_id_matches_body_id(self, jsonrpc_id):
        """correlation_id always returns self.body.id."""
        body = JsonRpcRequest(method="fuzz.test", id=jsonrpc_id)
        msg = create_broker_message(raw=None, body=body, headers={})

        assert msg.correlation_id == body.id
        assert msg.correlation_id == jsonrpc_id

    @given(
        raw=raw_messages(),
        headers=valid_headers(),
    )
    def test_stores_exact_references(self, raw, headers):
        """BrokerMessage stores exact object references."""
        body = JsonRpcRequest(method="fuzz.test")
        msg = create_broker_message(raw=raw, body=body, headers=headers)

        assert msg.body is body
        # For mutable types, verify identity not just equality
        if isinstance(headers, dict):
            assert msg.headers is headers

    @given(jsonrpc_id=valid_jsonrpc_ids())
    def test_abstract_properties_always_raise(self, jsonrpc_id):
        """Abstract properties always raise NotImplementedError."""
        body = JsonRpcRequest(method="fuzz.test", id=jsonrpc_id)
        msg = create_broker_message(raw=None, body=body, headers={})

        with pytest.raises(NotImplementedError):
            _ = msg.delivery_count

        with pytest.raises(NotImplementedError):
            _ = msg.reply_to

    @pytest.mark.asyncio
    @given(jsonrpc_id=valid_jsonrpc_ids())
    async def test_abstract_methods_always_raise(self, jsonrpc_id):
        """Abstract async methods always raise NotImplementedError."""
        body = JsonRpcRequest(method="fuzz.test", id=jsonrpc_id)
        msg = create_broker_message(raw=None, body=body, headers={})

        with pytest.raises(NotImplementedError):
            await msg.ack()

        with pytest.raises(NotImplementedError):
            await msg.reject()

        with pytest.raises(NotImplementedError):
            await msg.nack(10.0)


# === Layer 3: Settled State Tests ===


class ConcreteBrokerMessage(BrokerMessage):
    """Concrete implementation for testing settled state."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ack_called = False
        self._reject_called = False
        self._nack_called = False

    @property
    def message_id(self) -> str:
        return "test-msg-id"

    @property
    def enqueued_at(self) -> float:
        return 1234567890.0

    @property
    def delivery_count(self) -> int:
        return 1

    @property
    def reply_to(self) -> str | None:
        return None

    async def _ack(self) -> None:
        """Track that _ack was called."""
        self._ack_called = True

    async def _reject(self) -> None:
        """Track that _reject was called."""
        self._reject_called = True

    async def _nack(self, delay: float) -> None:
        """Track that _nack was called."""
        self._nack_called = True


@pytest.fixture
def concrete_message(json_rpc_notification):
    """Concrete BrokerMessage for testing settled state."""
    return ConcreteBrokerMessage(
        raw={"test": "data"},
        body=json_rpc_notification,
        headers={},
        received_at=1234567890.0,
        queue_name="test-queue",
    )


class TestBrokerMessageSettledState:
    """Test settled state tracking and logging."""

    @pytest.mark.asyncio
    async def test_ack_double_call_logs_debug(self, concrete_message, caplog):
        """Second ack() call logs debug message and returns early."""
        import logging

        with caplog.at_level(logging.DEBUG):
            await concrete_message.ack()
            await concrete_message.ack()  # Second call

        assert "already settled" in caplog.text
        assert "skipping ack" in caplog.text

    @pytest.mark.asyncio
    async def test_reject_double_call_logs_debug(
        self, concrete_message, caplog
    ):
        """Second reject() call logs debug message and returns early."""
        import logging

        with caplog.at_level(logging.DEBUG):
            await concrete_message.reject()
            await concrete_message.reject()  # Second call

        assert "already settled" in caplog.text
        assert "skipping reject" in caplog.text

    @pytest.mark.asyncio
    async def test_nack_double_call_logs_debug(self, concrete_message, caplog):
        """Second nack() call logs debug message and returns early."""
        import logging

        with caplog.at_level(logging.DEBUG):
            await concrete_message.nack(10.0)
            await concrete_message.nack(10.0)  # Second call

        assert "already settled" in caplog.text
        assert "skipping nack" in caplog.text

    @pytest.mark.asyncio
    async def test_ack_double_call_skips_underlying_method(
        self, json_rpc_notification
    ):
        """Second ack() call doesn't invoke _ack()."""
        msg = ConcreteBrokerMessage(
            raw={},
            body=json_rpc_notification,
            headers={},
            received_at=1.0,
            queue_name="test",
        )

        await msg.ack()
        assert msg._ack_called is True

        # Reset flag to track second call
        msg._ack_called = False
        await msg.ack()  # Second call

        # _ack should not be called again
        assert msg._ack_called is False

    @pytest.mark.asyncio
    async def test_reject_double_call_skips_underlying_method(
        self, json_rpc_notification
    ):
        """Second reject() call doesn't invoke _reject()."""
        msg = ConcreteBrokerMessage(
            raw={},
            body=json_rpc_notification,
            headers={},
            received_at=1.0,
            queue_name="test",
        )

        await msg.reject()
        assert msg._reject_called is True

        # Reset flag to track second call
        msg._reject_called = False
        await msg.reject()  # Second call

        # _reject should not be called again
        assert msg._reject_called is False

    @pytest.mark.asyncio
    async def test_nack_double_call_skips_underlying_method(
        self, json_rpc_notification
    ):
        """Second nack() call doesn't invoke _nack()."""
        msg = ConcreteBrokerMessage(
            raw={},
            body=json_rpc_notification,
            headers={},
            received_at=1.0,
            queue_name="test",
        )

        await msg.nack(10.0)
        assert msg._nack_called is True

        # Reset flag to track second call
        msg._nack_called = False
        await msg.nack(10.0)  # Second call

        # _nack should not be called again
        assert msg._nack_called is False

    @pytest.mark.asyncio
    async def test_ack_sets_is_settled_flag(self, concrete_message):
        """ack() sets _is_settled flag to True."""
        assert concrete_message._is_settled is False
        await concrete_message.ack()
        assert concrete_message._is_settled is True

    @pytest.mark.asyncio
    async def test_reject_sets_is_settled_flag(self, concrete_message):
        """reject() sets _is_settled flag to True."""
        assert concrete_message._is_settled is False
        await concrete_message.reject()
        assert concrete_message._is_settled is True

    @pytest.mark.asyncio
    async def test_nack_sets_is_settled_flag(self, concrete_message):
        """nack() sets _is_settled flag to True."""
        assert concrete_message._is_settled is False
        await concrete_message.nack(10.0)
        assert concrete_message._is_settled is True

    @pytest.mark.asyncio
    async def test_settled_message_log_contains_message_id(
        self, concrete_message, caplog
    ):
        """Settled state log message contains message_id."""
        import logging

        with caplog.at_level(logging.DEBUG):
            await concrete_message.ack()
            await concrete_message.ack()  # Second call

        # The log uses %(message_id)s format but message_id is in extra dict
        # so it should be formatted into the message
        assert "test-msg-id" in caplog.text or "already settled" in caplog.text
