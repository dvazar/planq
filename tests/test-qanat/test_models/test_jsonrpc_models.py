"""Comprehensive tests for JSON-RPC 2.0 models."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from qanat import types as qanat_types
from qanat.models import (
    JsonRpcErrorDetail,
    JsonRpcRequest,
    JsonRpcResponse,
)

# Rebuild the models with proper type namespace
JsonRpcRequest.model_rebuild(_types_namespace=qanat_types.__dict__)
JsonRpcResponse.model_rebuild(_types_namespace=qanat_types.__dict__)


# === Layer 1: Parametrized Edge Cases ===


class TestJsonRpcRequestValidation:
    """Tests for JsonRpcRequest model."""

    def test_default_construction(self):
        """Minimal valid request with just method name."""
        request = JsonRpcRequest(method="test.method")
        assert request.jsonrpc == "2.0"
        assert request.method == "test.method"
        assert request.params is None
        assert request.id is None

    def test_notification_with_no_id(self):
        """Request with id=None is a notification."""
        request = JsonRpcRequest(method="notify", id=None)
        assert request.id is None

    @pytest.mark.parametrize(
        "request_id",
        ["request-123", "uuid-abc", 1, 42, 0, -1, None],
    )
    def test_valid_id_types(self, request_id):
        """id can be string, int, or None."""
        request = JsonRpcRequest(method="test", id=request_id)
        assert request.id == request_id

    @pytest.mark.parametrize(
        "params",
        [
            {"key": "value"},
            {"a": 1, "b": 2},
            {},
            [1, 2, 3],
            ["a", "b", "c"],
            [],
            None,
        ],
    )
    def test_valid_params_types(self, params):
        """params can be dict, list, or None."""
        request = JsonRpcRequest(method="test", params=params)
        assert request.params == params

    def test_custom_jsonrpc_version(self):
        """Can explicitly set jsonrpc version to 2.0."""
        request = JsonRpcRequest(method="test", jsonrpc="2.0")
        assert request.jsonrpc == "2.0"

    def test_full_request_with_all_fields(self):
        """Request with all fields populated."""
        request = JsonRpcRequest(
            jsonrpc="2.0",
            method="user.create",
            params={"name": "Alice", "age": 30},
            id="req-001",
        )
        assert request.jsonrpc == "2.0"
        assert request.method == "user.create"
        assert request.params == {"name": "Alice", "age": 30}
        assert request.id == "req-001"


class TestJsonRpcRequestStrictMode:
    """Pydantic strict mode enforcement for JsonRpcRequest."""

    def test_method_must_be_string(self):
        """Strict mode: method must be string, not int."""
        with pytest.raises(ValidationError) as exc_info:
            JsonRpcRequest(method=123)

        errors = exc_info.value.errors()
        assert any(error["loc"] == ("method",) for error in errors)

    def test_jsonrpc_rejects_non_2_0(self):
        """Strict mode: jsonrpc must be exactly '2.0'."""
        with pytest.raises(ValidationError) as exc_info:
            JsonRpcRequest(method="test", jsonrpc="1.0")

        errors = exc_info.value.errors()
        assert any(error["loc"] == ("jsonrpc",) for error in errors)


class TestJsonRpcErrorDetailValidation:
    """Tests for JsonRpcErrorDetail model."""

    def test_minimal_construction(self):
        """Error with just code and message."""
        error = JsonRpcErrorDetail(code=-32700, message="Parse error")
        assert error.code == -32700
        assert error.message == "Parse error"
        assert error.data is None

    def test_with_data_field(self):
        """Error with optional data field."""
        error = JsonRpcErrorDetail(
            code=-32600,
            message="Invalid request",
            data={"details": "Missing method field"},
        )
        assert error.code == -32600
        assert error.message == "Invalid request"
        assert error.data == {"details": "Missing method field"}

    @pytest.mark.parametrize(
        "code",
        [-32700, -32600, -32601, -32602, -32603, 0, 1, -1, 100],
    )
    def test_various_error_codes(self, code):
        """Error code can be any integer."""
        error = JsonRpcErrorDetail(code=code, message="Error")
        assert error.code == code

    @pytest.mark.parametrize(
        "data",
        [
            None,
            {"key": "value"},
            ["item1", "item2"],
            "string data",
            42,
            True,
            {"nested": {"data": "value"}},
        ],
    )
    def test_data_accepts_any_json_value(self, data):
        """data field accepts any JSON-serializable value."""
        error = JsonRpcErrorDetail(
            code=-32000, message="Server error", data=data
        )
        assert error.data == data


class TestJsonRpcResponseValidation:
    """Tests for JsonRpcResponse model."""

    def test_success_response_with_result(self):
        """Response with result field (success case)."""
        response = JsonRpcResponse(result={"status": "ok"}, id="req-1")
        assert response.jsonrpc == "2.0"
        assert response.result == {"status": "ok"}
        assert response.error is None
        assert response.id == "req-1"

    def test_error_response_with_error(self):
        """Response with error field (error case)."""
        error = JsonRpcErrorDetail(code=-32601, message="Method not found")
        response = JsonRpcResponse(error=error, id="req-2")
        assert response.jsonrpc == "2.0"
        assert response.result is None
        assert response.error == error
        assert response.id == "req-2"

    @pytest.mark.parametrize(
        "response_id",
        ["response-123", 1, 42, 0, -1, None],
    )
    def test_valid_response_id_types(self, response_id):
        """Response id can be string, int, or None."""
        response = JsonRpcResponse(result="ok", id=response_id)
        assert response.id == response_id

    @pytest.mark.parametrize(
        "result",
        [
            {"key": "value"},
            [1, 2, 3],
            "string result",
            42,
            True,
            False,
            None,
        ],
    )
    def test_result_accepts_any_json_value(self, result):
        """result field accepts any JSON-serializable value."""
        response = JsonRpcResponse(result=result, id=1)
        assert response.result == result

    def test_both_result_and_error_none_is_valid(self):
        """Both result and error being None is structurally valid."""
        # Note: Spec says one must be set, but Pydantic model allows both None
        # Real validation happens at runtime/consumer level
        response = JsonRpcResponse(id=1)
        assert response.result is None
        assert response.error is None


class TestJsonRpcResponseStrictMode:
    """Pydantic strict mode enforcement for JsonRpcResponse."""

    def test_jsonrpc_rejects_non_2_0(self):
        """Strict mode: jsonrpc must be exactly '2.0'."""
        with pytest.raises(ValidationError) as exc_info:
            JsonRpcResponse(result="ok", id=1, jsonrpc="1.0")

        errors = exc_info.value.errors()
        assert any(error["loc"] == ("jsonrpc",) for error in errors)


# === Layer 2: Hypothesis Fuzzing ===


class TestJsonRpcRequestFuzz:
    """Property-based testing for JsonRpcRequest."""

    @pytest.mark.hypothesis
    @given(
        st.text(min_size=1, max_size=100),
        st.one_of(
            st.none(),
            st.text(min_size=1, max_size=50),
            st.integers(),
        ),
    )
    def test_valid_method_and_id_always_succeed(self, method, request_id):
        """Any non-empty method and valid id type succeeds."""
        request = JsonRpcRequest(method=method, id=request_id)
        assert request.method == method
        assert request.id == request_id

    @pytest.mark.hypothesis
    @given(
        st.one_of(
            st.none(),
            st.dictionaries(
                st.text(min_size=1, max_size=20),
                st.one_of(st.text(), st.integers(), st.booleans(), st.none()),
                max_size=10,
            ),
            st.lists(
                st.one_of(st.text(), st.integers(), st.booleans(), st.none()),
                max_size=10,
            ),
        )
    )
    def test_valid_params_always_succeed(self, params):
        """Any valid params type (dict, list, None) succeeds."""
        request = JsonRpcRequest(method="test", params=params)
        assert request.params == params


class TestJsonRpcErrorDetailFuzz:
    """Property-based testing for JsonRpcErrorDetail."""

    @pytest.mark.hypothesis
    @given(
        st.integers(min_value=-32768, max_value=32767),
        st.text(min_size=1, max_size=200),
    )
    def test_valid_code_and_message_always_succeed(self, code, message):
        """Any integer code and non-empty message succeeds."""
        error = JsonRpcErrorDetail(code=code, message=message)
        assert error.code == code
        assert error.message == message

    @pytest.mark.hypothesis
    @given(
        st.one_of(
            st.none(),
            st.text(),
            st.integers(),
            st.booleans(),
            st.dictionaries(
                st.text(min_size=1, max_size=10),
                st.text(),
                max_size=5,
            ),
            st.lists(st.text(), max_size=5),
        )
    )
    def test_data_accepts_various_types(self, data):
        """data field accepts None or any JSON-serializable value."""
        error = JsonRpcErrorDetail(code=-32000, message="Error", data=data)
        assert error.data == data


class TestJsonRpcResponseFuzz:
    """Property-based testing for JsonRpcResponse."""

    @pytest.mark.hypothesis
    @given(
        st.one_of(
            st.none(),
            st.text(),
            st.integers(),
            st.booleans(),
            st.dictionaries(st.text(), st.text(), max_size=5),
            st.lists(st.text(), max_size=5),
        ),
        st.one_of(
            st.none(),
            st.text(min_size=1, max_size=50),
            st.integers(),
        ),
    )
    def test_valid_result_and_id_always_succeed(self, result, response_id):
        """Any valid result type and id type succeeds."""
        response = JsonRpcResponse(result=result, id=response_id)
        assert response.result == result
        assert response.id == response_id


# === Layer 3: Integration Tests ===


class TestJsonRpcIntegration:
    """Cross-model interactions and protocol compliance."""

    def test_request_response_id_echo(self):
        """Response id should echo request id."""
        request = JsonRpcRequest(method="echo", id="req-123")
        response = JsonRpcResponse(result="echoed", id=request.id)
        assert response.id == request.id

    def test_notification_has_no_response(self):
        """Notification (id=None) should not expect response."""
        notification = JsonRpcRequest(method="log.info", id=None)
        assert notification.id is None
        # In practice, consumer would not send response for notifications

    def test_error_response_includes_error_detail(self):
        """Error response includes structured error detail."""
        error_detail = JsonRpcErrorDetail(
            code=-32601,
            message="Method not found",
            data={"method": "unknown.method"},
        )
        response = JsonRpcResponse(error=error_detail, id="req-456")
        assert response.error is not None
        assert response.error.code == -32601
        assert response.error.message == "Method not found"
        assert response.error.data == {"method": "unknown.method"}

    def test_success_response_has_no_error(self):
        """Success response has error=None."""
        response = JsonRpcResponse(result={"status": "ok"}, id=1)
        assert response.result is not None
        assert response.error is None

    def test_error_response_has_no_result(self):
        """Error response has result=None."""
        error = JsonRpcErrorDetail(code=-32700, message="Parse error")
        response = JsonRpcResponse(error=error, id=1)
        assert response.result is None
        assert response.error is not None

    def test_request_with_positional_params(self):
        """Request can have positional params (list)."""
        request = JsonRpcRequest(method="subtract", params=[42, 23], id=1)
        assert request.params == [42, 23]

    def test_request_with_named_params(self):
        """Request can have named params (dict)."""
        request = JsonRpcRequest(
            method="subtract",
            params={"subtrahend": 23, "minuend": 42},
            id=2,
        )
        assert request.params == {"subtrahend": 23, "minuend": 42}

    def test_round_trip_serialization(self):
        """Models can be serialized and deserialized."""
        original_request = JsonRpcRequest(
            method="test",
            params={"key": "value"},
            id="test-id",
        )

        # Simulate JSON round-trip
        request_dict = original_request.model_dump()
        reconstructed = JsonRpcRequest(**request_dict)

        assert reconstructed.jsonrpc == original_request.jsonrpc
        assert reconstructed.method == original_request.method
        assert reconstructed.params == original_request.params
        assert reconstructed.id == original_request.id
