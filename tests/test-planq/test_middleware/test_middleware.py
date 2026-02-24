"""Comprehensive tests for Middleware base class."""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, MagicMock

import pytest
from hypothesis import given

from planq.exceptions import RejectMessage, RetryMessage
from planq.middleware import Middleware
from planq.models import JsonRpcResponse

from .conftest import valid_headers_with_ttl, valid_params

# === Test Implementation Classes ===


class MinimalMiddleware(Middleware):
    """Minimal implementation without overriding __call__."""

    pass


class CallTrackingMiddleware(Middleware):
    """Middleware that tracks __call__ invocations."""

    def __init__(self):
        self.call_count = 0

    async def __call__(self, msg, call_next):
        self.call_count += 1
        return await call_next(msg)


# === Layer 1: Middleware Instantiation ===


class TestMiddlewareInit:
    """Test Middleware instantiation and base class behavior."""

    def test_base_middleware_can_be_instantiated_directly(self):
        """Middleware base class can be instantiated."""
        middleware = Middleware()
        assert isinstance(middleware, Middleware)

    def test_base_middleware_is_not_abstract(self):
        """Middleware doesn't use ABC pattern."""
        middleware = Middleware()
        assert middleware is not None

    def test_minimal_subclass_can_be_instantiated(self):
        """Middleware subclass without overrides can be instantiated."""
        middleware = MinimalMiddleware()
        assert isinstance(middleware, Middleware)
        assert isinstance(middleware, MinimalMiddleware)

    def test_multiple_instances_are_independent(self):
        """Multiple Middleware instances are independent."""
        m1 = CallTrackingMiddleware()
        m2 = CallTrackingMiddleware()

        assert m1 is not m2
        assert m1.call_count == 0
        assert m2.call_count == 0


# === Layer 2: Default __call__ Behavior ===


class TestMiddlewareDefaultCall:
    """Test that default __call__ delegates to call_next."""

    @pytest.mark.asyncio
    async def test_default_call_delegates_to_call_next(
        self, mock_broker_message, mock_call_next
    ):
        """Default __call__ calls call_next and returns its result."""
        middleware = Middleware()

        result = await middleware(mock_broker_message, mock_call_next)

        mock_call_next.assert_called_once_with(mock_broker_message)
        assert result is None

    @pytest.mark.asyncio
    async def test_default_call_returns_call_next_response(
        self, mock_broker_message
    ):
        """Default __call__ returns whatever call_next returns."""
        middleware = Middleware()
        response = JsonRpcResponse(jsonrpc="2.0", id="123", result={"ok": True})
        call_next = AsyncMock(return_value=response)

        result = await middleware(mock_broker_message, call_next)

        assert result is response

    @pytest.mark.asyncio
    async def test_default_call_has_no_side_effects(
        self, mock_broker_message, mock_call_next
    ):
        """Default __call__ doesn't mutate message."""
        middleware = Middleware()
        original_params = mock_broker_message.body.params.copy()
        original_headers = mock_broker_message.headers.copy()

        await middleware(mock_broker_message, mock_call_next)

        assert mock_broker_message.body.params == original_params
        assert mock_broker_message.headers == original_headers

    @pytest.mark.asyncio
    async def test_default_call_does_not_touch_broker_ops(
        self, mock_broker_message, mock_call_next
    ):
        """Default __call__ doesn't call ack/nack/reject."""
        middleware = Middleware()

        await middleware(mock_broker_message, mock_call_next)

        mock_broker_message.ack.assert_not_called()
        mock_broker_message.nack.assert_not_called()
        mock_broker_message.reject.assert_not_called()


# === Layer 3: __call__ Signature ===


class TestMiddlewareCallSignature:
    """Test __call__ method signature."""

    def test_call_is_async(self):
        """__call__ is a coroutine function."""
        middleware = Middleware()
        assert inspect.iscoroutinefunction(middleware.__call__)

    def test_call_parameter_names(self):
        """__call__ has correct parameter names."""
        sig = inspect.signature(Middleware.__call__)
        params = list(sig.parameters.keys())
        assert params == ["self", "msg", "call_next"]

    @pytest.mark.asyncio
    async def test_call_accepts_positional_args(
        self, mock_broker_message, mock_call_next
    ):
        """__call__ accepts positional args."""
        middleware = Middleware()
        await middleware(mock_broker_message, mock_call_next)

    @pytest.mark.asyncio
    async def test_call_accepts_keyword_args(
        self, mock_broker_message, mock_call_next
    ):
        """__call__ accepts keyword args."""
        middleware = Middleware()
        await middleware(msg=mock_broker_message, call_next=mock_call_next)


# === Layer 4: Subclassing Patterns ===


class TestMiddlewareSubclassing:
    """Test Middleware subclassing and override patterns."""

    def test_can_subclass_base_middleware(self):
        """Middleware can be subclassed."""

        class CustomMiddleware(Middleware):
            pass

        middleware = CustomMiddleware()
        assert isinstance(middleware, Middleware)
        assert isinstance(middleware, CustomMiddleware)

    @pytest.mark.asyncio
    async def test_subclass_can_pre_process(
        self, mock_broker_message, mock_call_next
    ):
        """Subclass can pre-process before call_next."""

        class PreProcessMiddleware(Middleware):
            async def __call__(self, msg, call_next):
                msg.headers["x-pre"] = "processed"
                return await call_next(msg)

        middleware = PreProcessMiddleware()
        await middleware(mock_broker_message, mock_call_next)

        assert mock_broker_message.headers["x-pre"] == "processed"

    @pytest.mark.asyncio
    async def test_subclass_can_post_process(self, mock_broker_message):
        """Subclass can post-process after call_next."""
        response = JsonRpcResponse(jsonrpc="2.0", id="123", result={"ok": True})
        call_next = AsyncMock(return_value=response)

        class PostProcessMiddleware(Middleware):
            async def __call__(self, msg, call_next):
                resp = await call_next(msg)
                if resp is not None:
                    resp.headers["x-post"] = "processed"
                return resp

        middleware = PostProcessMiddleware()
        result = await middleware(mock_broker_message, call_next)

        assert result is not None
        assert result.headers["x-post"] == "processed"

    @pytest.mark.asyncio
    async def test_subclass_can_short_circuit(
        self, mock_broker_message, mock_call_next
    ):
        """Subclass can short-circuit without calling call_next."""

        class ShortCircuitMiddleware(Middleware):
            async def __call__(self, msg, call_next):
                return None  # Skip processing

        middleware = ShortCircuitMiddleware()
        result = await middleware(mock_broker_message, mock_call_next)

        assert result is None
        mock_call_next.assert_not_called()

    @pytest.mark.asyncio
    async def test_subclass_can_raise_retry_message(
        self, mock_broker_message, mock_call_next
    ):
        """Subclass can raise RetryMessage."""

        class RetryMiddleware(Middleware):
            async def __call__(self, msg, call_next):
                raise RetryMessage(delay=5.0)

        middleware = RetryMiddleware()
        with pytest.raises(RetryMessage) as exc_info:
            await middleware(mock_broker_message, mock_call_next)

        assert exc_info.value.delay == 5.0

    @pytest.mark.asyncio
    async def test_subclass_can_raise_reject_message(
        self, mock_broker_message, mock_call_next
    ):
        """Subclass can raise RejectMessage."""

        class RejectMiddleware(Middleware):
            async def __call__(self, msg, call_next):
                raise RejectMessage

        middleware = RejectMiddleware()
        with pytest.raises(RejectMessage):
            await middleware(mock_broker_message, mock_call_next)

    @pytest.mark.asyncio
    async def test_subclass_can_mutate_params(
        self, mock_broker_message, mock_call_next
    ):
        """Subclass can mutate msg.body.params in-place."""

        class MutatingMiddleware(Middleware):
            async def __call__(self, msg, call_next):
                msg.body.params["injected"] = True
                return await call_next(msg)

        middleware = MutatingMiddleware()
        await middleware(mock_broker_message, mock_call_next)

        assert mock_broker_message.body.params["injected"] is True

    @pytest.mark.asyncio
    async def test_subclass_can_mutate_headers(
        self, mock_broker_message, mock_call_next
    ):
        """Subclass can mutate msg.headers in-place."""

        class MutatingMiddleware(Middleware):
            async def __call__(self, msg, call_next):
                msg.headers["x-middleware"] = "processed"
                return await call_next(msg)

        middleware = MutatingMiddleware()
        await middleware(mock_broker_message, mock_call_next)

        assert mock_broker_message.headers["x-middleware"] == "processed"

    def test_subclass_can_have_custom_init(self):
        """Middleware subclass can have custom __init__."""

        class ConfigurableMiddleware(Middleware):
            def __init__(self, prefix: str):
                self.prefix = prefix

        middleware = ConfigurableMiddleware("test-")
        assert middleware.prefix == "test-"


# === Layer 5: Multiple Calls ===


class TestMiddlewareMultipleCalls:
    """Test behavior when __call__ is invoked multiple times."""

    @pytest.mark.asyncio
    async def test_can_be_called_multiple_times(
        self, mock_broker_message, mock_call_next
    ):
        """__call__ can be called multiple times."""
        middleware = CallTrackingMiddleware()

        await middleware(mock_broker_message, mock_call_next)
        await middleware(mock_broker_message, mock_call_next)

        assert middleware.call_count == 2

    @pytest.mark.asyncio
    async def test_returns_coroutine(self, mock_broker_message, mock_call_next):
        """__call__ returns awaitable coroutine."""
        middleware = Middleware()
        coro = middleware(mock_broker_message, mock_call_next)

        assert inspect.iscoroutine(coro)
        await coro


# === Layer 6: Integration Patterns ===


class TestMiddlewareIntegrationPatterns:
    """Test realistic middleware usage patterns."""

    @pytest.mark.asyncio
    async def test_middleware_can_track_state_across_calls(
        self, mock_broker_message, mock_call_next
    ):
        """Middleware instance can maintain state across calls."""

        class StatefulMiddleware(Middleware):
            def __init__(self):
                self.messages_seen = 0

            async def __call__(self, msg, call_next):
                self.messages_seen += 1
                return await call_next(msg)

        middleware = StatefulMiddleware()

        await middleware(mock_broker_message, mock_call_next)
        await middleware(mock_broker_message, mock_call_next)

        assert middleware.messages_seen == 2

    @pytest.mark.asyncio
    async def test_middleware_can_read_message_metadata(
        self, mock_broker_message, mock_call_next
    ):
        """Middleware can inspect message metadata."""

        class MetadataMiddleware(Middleware):
            def __init__(self):
                self.correlation_ids = []

            async def __call__(self, msg, call_next):
                self.correlation_ids.append(msg.correlation_id)
                return await call_next(msg)

        middleware = MetadataMiddleware()
        await middleware(mock_broker_message, mock_call_next)

        assert middleware.correlation_ids == ["test-123"]

    @pytest.mark.asyncio
    async def test_middleware_can_enrich_response_headers(
        self, mock_broker_message
    ):
        """Middleware can enrich response headers post-call_next."""
        response = JsonRpcResponse(jsonrpc="2.0", id="123", result={"ok": True})
        call_next = AsyncMock(return_value=response)

        class HeaderMiddleware(Middleware):
            async def __call__(self, msg, call_next):
                resp = await call_next(msg)
                if resp is not None:
                    resp.headers["x-trace-id"] = "abc-123"
                return resp

        middleware = HeaderMiddleware()
        result = await middleware(mock_broker_message, call_next)

        assert result.headers["x-trace-id"] == "abc-123"


# === Layer 7: Docstring Verification ===


class TestMiddlewareDocumentation:
    """Test that Middleware has proper documentation."""

    def test_base_middleware_has_docstring(self):
        """Middleware class has docstring."""
        assert Middleware.__doc__ is not None
        assert len(Middleware.__doc__) > 0

    def test_call_has_docstring(self):
        """__call__ has docstring."""
        assert Middleware.__call__.__doc__ is not None

    def test_docstring_mentions_call_next(self):
        """Middleware docstring mentions call_next."""
        docstring = Middleware.__doc__
        assert "call_next" in docstring

    def test_docstring_mentions_mutate(self):
        """Middleware docstring mentions mutation capability."""
        docstring = Middleware.__doc__
        assert "mutate" in docstring or "in-place" in docstring


# === Layer 8: Property-Based Testing ===


class TestMiddlewarePropertyBased:
    """Property-based tests with hypothesis."""

    @pytest.mark.asyncio
    @given(headers=valid_headers_with_ttl())
    async def test_call_with_random_headers(self, headers):
        """__call__ handles random headers."""
        from planq.message import BrokerMessage
        from planq.models import JsonRpcRequest

        mock_msg = MagicMock(spec=BrokerMessage)
        mock_msg.headers = headers
        mock_msg.body = JsonRpcRequest(
            method="test.method", params={}, id="test-123"
        )

        call_next = AsyncMock(return_value=None)
        middleware = Middleware()

        result = await middleware(mock_msg, call_next)

        assert result is None
        assert mock_msg.headers == headers

    @pytest.mark.asyncio
    @given(params=valid_params())
    async def test_call_with_random_params(self, params):
        """__call__ handles random params."""
        from planq.message import BrokerMessage
        from planq.models import JsonRpcRequest

        mock_msg = MagicMock(spec=BrokerMessage)
        mock_msg.headers = {}
        mock_msg.body = JsonRpcRequest(
            method="test.method",
            params=params,
            id="test-123",
        )

        call_next = AsyncMock(return_value=None)
        middleware = Middleware()

        result = await middleware(mock_msg, call_next)

        assert result is None

    @pytest.mark.asyncio
    @given(params=valid_params())
    async def test_mutating_middleware_with_random_params(self, params):
        """Mutating middleware can modify random params."""
        from planq.message import BrokerMessage
        from planq.models import JsonRpcRequest

        mock_msg = MagicMock(spec=BrokerMessage)
        mock_msg.headers = {}
        mock_msg.body = JsonRpcRequest(
            method="test.method",
            params=params,
            id="test-123",
        )

        call_next = AsyncMock(return_value=None)

        class MutatingMiddleware(Middleware):
            async def __call__(self, msg, call_next):
                if msg.body.params is not None and isinstance(
                    msg.body.params, dict
                ):
                    msg.body.params["injected"] = "value"
                msg.headers["x-custom"] = "middleware"
                return await call_next(msg)

        middleware = MutatingMiddleware()

        await middleware(mock_msg, call_next)

        if params is not None and isinstance(params, dict):
            assert mock_msg.body.params["injected"] == "value"
        assert mock_msg.headers["x-custom"] == "middleware"
