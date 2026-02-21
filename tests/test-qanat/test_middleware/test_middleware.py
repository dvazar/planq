"""Comprehensive tests for Middleware base class."""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock

import pytest
from hypothesis import given

from qanat.middleware import Middleware
from qanat.models import JsonRpcResponse

from .conftest import valid_headers_with_ttl, valid_params

# === Test Implementation Classes ===


class MinimalMiddleware(Middleware):
    """Minimal implementation without overriding any hooks."""

    pass


class HookTrackingMiddleware(Middleware):
    """Middleware that tracks which hooks were called."""

    def __init__(self):
        """Initialize tracking flags."""
        self.before_process_count = 0
        self.after_process_count = 0
        self.after_skip_count = 0
        self.before_publish_count = 0

    async def before_process_message(self, consumer, msg):
        """Track before_process_message calls."""
        self.before_process_count += 1

    async def after_process_message(
        self, consumer, msg, *, result=None, exception=None
    ):
        """Track after_process_message calls."""
        self.after_process_count += 1

    async def after_skip_message(self, consumer, msg):
        """Track after_skip_message calls."""
        self.after_skip_count += 1

    async def before_publish_response(self, consumer, msg, response, headers):
        """Track before_publish_response calls."""
        self.before_publish_count += 1


# === Layer 1: Middleware Instantiation ===


class TestMiddlewareInit:
    """Test Middleware instantiation and base class behavior."""

    def test_middleware_can_be_instantiated_directly(self):
        """Middleware base class can be instantiated."""
        middleware = Middleware()
        assert isinstance(middleware, Middleware)

    def test_middleware_is_not_abstract(self):
        """Middleware doesn't use ABC pattern."""
        # If it were abstract, instantiation would raise TypeError
        middleware = Middleware()
        assert middleware is not None

    def test_minimal_subclass_can_be_instantiated(self):
        """Middleware subclass without overrides can be instantiated."""
        middleware = MinimalMiddleware()
        assert isinstance(middleware, Middleware)
        assert isinstance(middleware, MinimalMiddleware)

    def test_middleware_has_no_init_parameters(self):
        """Middleware __init__ takes no parameters."""
        # Should succeed with no args
        middleware = Middleware()
        assert middleware is not None

    def test_multiple_instances_are_independent(self):
        """Multiple Middleware instances are independent."""
        m1 = HookTrackingMiddleware()
        m2 = HookTrackingMiddleware()

        assert m1 is not m2
        assert m1.before_process_count == 0
        assert m2.before_process_count == 0


# === Layer 2: Hook Default Behavior ===


class TestMiddlewareHooksDefaultBehavior:
    """Test that all hooks are no-ops by default."""

    @pytest.mark.asyncio
    async def test_before_process_message_is_noop(
        self, mock_consumer, mock_broker_message
    ):
        """before_process_message returns None by default."""
        middleware = Middleware()

        result = await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_after_process_message_is_noop(
        self, mock_consumer, mock_broker_message
    ):
        """after_process_message returns None by default."""
        middleware = Middleware()

        result = await middleware.after_process_message(
            mock_consumer,
            mock_broker_message,
            result={"status": "ok"},
            exception=None,
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_after_skip_message_is_noop(
        self, mock_consumer, mock_broker_message
    ):
        """after_skip_message returns None by default."""
        middleware = Middleware()

        result = await middleware.after_skip_message(
            mock_consumer, mock_broker_message
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_before_publish_response_is_noop(
        self, mock_consumer, mock_broker_message
    ):
        """before_publish_response returns None by default."""
        middleware = Middleware()
        response = JsonRpcResponse(jsonrpc="2.0", id="123", result={"ok": True})
        headers = {}

        result = await middleware.before_publish_response(
            mock_consumer, mock_broker_message, response, headers
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_before_process_has_no_side_effects(
        self, mock_consumer, mock_broker_message
    ):
        """before_process_message doesn't mutate message."""
        middleware = Middleware()
        original_params = mock_broker_message.body.params.copy()
        original_headers = mock_broker_message.headers.copy()

        await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert mock_broker_message.body.params == original_params
        assert mock_broker_message.headers == original_headers

    @pytest.mark.asyncio
    async def test_after_process_has_no_side_effects(
        self, mock_consumer, mock_broker_message
    ):
        """after_process_message doesn't call ack/nack/reject."""
        middleware = Middleware()

        await middleware.after_process_message(
            mock_consumer,
            mock_broker_message,
            result=None,
            exception=None,
        )

        mock_broker_message.ack.assert_not_called()
        mock_broker_message.nack.assert_not_called()
        mock_broker_message.reject.assert_not_called()


# === Layer 3: Hook Signatures ===


class TestMiddlewareHooksSignatures:
    """Test hook method signatures and parameters."""

    @pytest.mark.asyncio
    async def test_before_process_message_signature(
        self, mock_consumer, mock_broker_message
    ):
        """before_process_message accepts (consumer, msg)."""
        middleware = Middleware()

        # Should accept positional args
        await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        # Should accept keyword args
        await middleware.before_process_message(
            consumer=mock_consumer, msg=mock_broker_message
        )

    @pytest.mark.asyncio
    async def test_after_process_message_signature(
        self, mock_consumer, mock_broker_message
    ):
        """after_process_message accepts (consumer, msg, result, exception)."""
        middleware = Middleware()

        # result and exception are keyword-only
        await middleware.after_process_message(
            mock_consumer,
            mock_broker_message,
            result={"data": "value"},
            exception=None,
        )

        await middleware.after_process_message(
            mock_consumer,
            mock_broker_message,
            result=None,
            exception=ValueError("test"),
        )

    @pytest.mark.asyncio
    async def test_after_skip_message_signature(
        self, mock_consumer, mock_broker_message
    ):
        """after_skip_message accepts (consumer, msg)."""
        middleware = Middleware()

        # Should accept positional args
        await middleware.after_skip_message(mock_consumer, mock_broker_message)

        # Should accept keyword args
        await middleware.after_skip_message(
            consumer=mock_consumer, msg=mock_broker_message
        )

    @pytest.mark.asyncio
    async def test_before_publish_response_signature(
        self, mock_consumer, mock_broker_message
    ):
        """before_publish_response accepts all parameters."""
        middleware = Middleware()
        response = JsonRpcResponse(jsonrpc="2.0", id="123", result={})
        headers = {}

        # Should accept positional args
        await middleware.before_publish_response(
            mock_consumer, mock_broker_message, response, headers
        )

        # Should accept keyword args
        await middleware.before_publish_response(
            consumer=mock_consumer,
            msg=mock_broker_message,
            response=response,
            headers=headers,
        )

    def test_before_process_message_parameter_names(self):
        """before_process_message has correct parameter names."""
        sig = inspect.signature(Middleware.before_process_message)
        params = list(sig.parameters.keys())
        assert params == ["self", "consumer", "msg"]

    def test_after_process_message_parameter_names(self):
        """after_process_message has correct parameter names."""
        sig = inspect.signature(Middleware.after_process_message)
        params = list(sig.parameters.keys())
        assert params == ["self", "consumer", "msg", "result", "exception"]

    def test_after_skip_message_parameter_names(self):
        """after_skip_message has correct parameter names."""
        sig = inspect.signature(Middleware.after_skip_message)
        params = list(sig.parameters.keys())
        assert params == ["self", "consumer", "msg"]

    def test_before_publish_response_parameter_names(self):
        """before_publish_response has correct parameter names."""
        sig = inspect.signature(Middleware.before_publish_response)
        params = list(sig.parameters.keys())
        assert params == ["self", "consumer", "msg", "response", "headers"]


# === Layer 4: Subclassing Patterns ===


class TestMiddlewareSubclassing:
    """Test Middleware subclassing and override patterns."""

    def test_can_subclass_middleware(self):
        """Middleware can be subclassed."""

        class CustomMiddleware(Middleware):
            pass

        middleware = CustomMiddleware()
        assert isinstance(middleware, Middleware)
        assert isinstance(middleware, CustomMiddleware)

    @pytest.mark.asyncio
    async def test_subclass_can_override_before_process(
        self, mock_consumer, mock_broker_message
    ):
        """Subclass can override before_process_message."""

        class CustomMiddleware(Middleware):
            async def before_process_message(self, consumer, msg):
                msg.headers["custom"] = "value"

        middleware = CustomMiddleware()
        await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert mock_broker_message.headers["custom"] == "value"

    @pytest.mark.asyncio
    async def test_subclass_can_override_after_process(
        self, mock_consumer, mock_broker_message
    ):
        """Subclass can override after_process_message."""
        calls = []

        class CustomMiddleware(Middleware):
            async def after_process_message(
                self, consumer, msg, *, result=None, exception=None
            ):
                calls.append((result, exception))

        middleware = CustomMiddleware()
        await middleware.after_process_message(
            mock_consumer,
            mock_broker_message,
            result="success",
            exception=None,
        )

        assert calls == [("success", None)]

    @pytest.mark.asyncio
    async def test_subclass_can_override_after_skip(
        self, mock_consumer, mock_broker_message
    ):
        """Subclass can override after_skip_message."""
        calls = []

        class CustomMiddleware(Middleware):
            async def after_skip_message(self, consumer, msg):
                calls.append(msg.correlation_id)

        middleware = CustomMiddleware()
        await middleware.after_skip_message(mock_consumer, mock_broker_message)

        assert calls == ["test-123"]

    @pytest.mark.asyncio
    async def test_subclass_can_override_before_publish(
        self, mock_consumer, mock_broker_message
    ):
        """Subclass can override before_publish_response."""

        class CustomMiddleware(Middleware):
            async def before_publish_response(
                self, consumer, msg, response, headers
            ):
                headers["x-custom"] = "injected"

        middleware = CustomMiddleware()
        response = JsonRpcResponse(jsonrpc="2.0", id="123", result={})
        headers = {}

        await middleware.before_publish_response(
            mock_consumer, mock_broker_message, response, headers
        )

        assert headers["x-custom"] == "injected"

    @pytest.mark.asyncio
    async def test_subclass_can_mutate_params(
        self, mock_consumer, mock_broker_message
    ):
        """Subclass can mutate msg.body.params in-place."""

        class MutatingMiddleware(Middleware):
            async def before_process_message(self, consumer, msg):
                msg.body.params["injected"] = True

        middleware = MutatingMiddleware()
        await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert mock_broker_message.body.params["injected"] is True

    @pytest.mark.asyncio
    async def test_subclass_can_mutate_headers(
        self, mock_consumer, mock_broker_message
    ):
        """Subclass can mutate msg.headers in-place."""

        class MutatingMiddleware(Middleware):
            async def before_process_message(self, consumer, msg):
                msg.headers["x-middleware"] = "processed"

        middleware = MutatingMiddleware()
        mock_broker_message.headers = {}

        await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert mock_broker_message.headers["x-middleware"] == "processed"

    def test_subclass_can_have_custom_init(self):
        """Middleware subclass can have custom __init__."""

        class ConfigurableMiddleware(Middleware):
            def __init__(self, prefix: str):
                self.prefix = prefix

        middleware = ConfigurableMiddleware("test-")
        assert middleware.prefix == "test-"


# === Layer 5: Async Behavior ===


class TestMiddlewareHooksAsyncBehavior:
    """Test that all hooks are async and return coroutines."""

    def test_before_process_message_is_async(self):
        """before_process_message is a coroutine function."""
        middleware = Middleware()
        assert inspect.iscoroutinefunction(middleware.before_process_message)

    def test_after_process_message_is_async(self):
        """after_process_message is a coroutine function."""
        middleware = Middleware()
        assert inspect.iscoroutinefunction(middleware.after_process_message)

    def test_after_skip_message_is_async(self):
        """after_skip_message is a coroutine function."""
        middleware = Middleware()
        assert inspect.iscoroutinefunction(middleware.after_skip_message)

    def test_before_publish_response_is_async(self):
        """before_publish_response is a coroutine function."""
        middleware = Middleware()
        assert inspect.iscoroutinefunction(middleware.before_publish_response)

    @pytest.mark.asyncio
    async def test_before_process_returns_coroutine(
        self, mock_consumer, mock_broker_message
    ):
        """before_process_message returns awaitable coroutine."""
        middleware = Middleware()
        coro = middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert inspect.iscoroutine(coro)
        await coro

    @pytest.mark.asyncio
    async def test_hooks_can_be_awaited_multiple_times(
        self, mock_consumer, mock_broker_message
    ):
        """Hooks can be called and awaited multiple times."""
        middleware = HookTrackingMiddleware()

        for _ in range(3):
            await middleware.before_process_message(
                mock_consumer, mock_broker_message
            )

        assert middleware.before_process_count == 3


# === Layer 6: Multiple Hook Calls ===


class TestMiddlewareMultipleHookCalls:
    """Test behavior when hooks are called multiple times."""

    @pytest.mark.asyncio
    async def test_before_process_can_be_called_multiple_times(
        self, mock_consumer, mock_broker_message
    ):
        """before_process_message can be called multiple times."""
        middleware = HookTrackingMiddleware()

        await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )
        await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert middleware.before_process_count == 2

    @pytest.mark.asyncio
    async def test_after_process_can_be_called_multiple_times(
        self, mock_consumer, mock_broker_message
    ):
        """after_process_message can be called multiple times."""
        middleware = HookTrackingMiddleware()

        await middleware.after_process_message(
            mock_consumer, mock_broker_message, result=None, exception=None
        )
        await middleware.after_process_message(
            mock_consumer, mock_broker_message, result=None, exception=None
        )

        assert middleware.after_process_count == 2

    @pytest.mark.asyncio
    async def test_different_hooks_are_independent(
        self, mock_consumer, mock_broker_message
    ):
        """Calling one hook doesn't affect others."""
        middleware = HookTrackingMiddleware()

        await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert middleware.before_process_count == 1
        assert middleware.after_process_count == 0
        assert middleware.after_skip_count == 0
        assert middleware.before_publish_count == 0


# === Layer 7: Parameter Validation ===


class TestMiddlewareParameterHandling:
    """Test middleware behavior with various parameter values."""

    @pytest.mark.asyncio
    async def test_after_process_with_result_only(
        self, mock_consumer, mock_broker_message
    ):
        """after_process_message with result, no exception."""
        middleware = Middleware()

        result = await middleware.after_process_message(
            mock_consumer,
            mock_broker_message,
            result={"status": "ok"},
            exception=None,
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_after_process_with_exception_only(
        self, mock_consumer, mock_broker_message
    ):
        """after_process_message with exception, no result."""
        middleware = Middleware()

        result = await middleware.after_process_message(
            mock_consumer,
            mock_broker_message,
            result=None,
            exception=ValueError("error"),
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_after_process_with_both_none(
        self, mock_consumer, mock_broker_message
    ):
        """after_process_message with both result and exception None."""
        middleware = Middleware()

        result = await middleware.after_process_message(
            mock_consumer,
            mock_broker_message,
            result=None,
            exception=None,
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_before_publish_with_empty_headers(
        self, mock_consumer, mock_broker_message
    ):
        """before_publish_response with empty headers dict."""
        middleware = Middleware()
        response = JsonRpcResponse(jsonrpc="2.0", id="123", result={})
        headers = {}

        await middleware.before_publish_response(
            mock_consumer, mock_broker_message, response, headers
        )

        assert headers == {}

    @pytest.mark.asyncio
    async def test_before_publish_with_existing_headers(
        self, mock_consumer, mock_broker_message
    ):
        """before_publish_response preserves existing headers."""
        middleware = Middleware()
        response = JsonRpcResponse(jsonrpc="2.0", id="123", result={})
        headers = {"x-existing": "value"}

        await middleware.before_publish_response(
            mock_consumer, mock_broker_message, response, headers
        )

        assert headers == {"x-existing": "value"}


# === Layer 8: Docstring Verification ===


class TestMiddlewareDocumentation:
    """Test that Middleware has proper documentation."""

    def test_middleware_has_docstring(self):
        """Middleware class has docstring."""
        assert Middleware.__doc__ is not None
        assert len(Middleware.__doc__) > 0

    def test_before_process_message_has_docstring(self):
        """before_process_message has docstring."""
        assert Middleware.before_process_message.__doc__ is not None

    def test_after_process_message_has_docstring(self):
        """after_process_message has docstring."""
        assert Middleware.after_process_message.__doc__ is not None

    def test_after_skip_message_has_docstring(self):
        """after_skip_message has docstring."""
        assert Middleware.after_skip_message.__doc__ is not None

    def test_before_publish_response_has_docstring(self):
        """before_publish_response has docstring."""
        assert Middleware.before_publish_response.__doc__ is not None

    def test_docstring_mentions_no_op_default(self):
        """Middleware docstring mentions no-op default behavior."""
        docstring = Middleware.__doc__.lower()
        assert "no-op" in docstring or "noop" in docstring

    def test_docstring_mentions_mutation_capability(self):
        """Middleware docstring mentions in-place mutation."""
        docstring = Middleware.__doc__
        assert "mutate" in docstring or "in-place" in docstring


# === Layer 9: Property-Based Testing ===


class TestMiddlewarePropertyBased:
    """Property-based tests with hypothesis."""

    @pytest.mark.asyncio
    @given(headers=valid_headers_with_ttl())
    async def test_before_process_with_random_headers(self, headers):
        """before_process_message handles random headers."""
        from qanat.consumer import QanatConsumer
        from qanat.message import BrokerMessage
        from qanat.models import JsonRpcRequest

        # Create fresh mocks for each hypothesis example
        mock_consumer = MagicMock(spec=QanatConsumer)
        mock_msg = MagicMock(spec=BrokerMessage)
        mock_msg.headers = headers
        mock_msg.body = JsonRpcRequest(
            method="test.method", params={}, id="test-123"
        )

        middleware = Middleware()

        result = await middleware.before_process_message(
            mock_consumer, mock_msg
        )

        assert result is None
        # Headers should be unchanged by base middleware
        assert mock_msg.headers == headers

    @pytest.mark.asyncio
    @given(params=valid_params())
    async def test_before_process_with_random_params(self, params):
        """before_process_message handles random params."""
        from qanat.consumer import QanatConsumer
        from qanat.message import BrokerMessage
        from qanat.models import JsonRpcRequest

        # Create fresh mocks for each hypothesis example
        mock_consumer = MagicMock(spec=QanatConsumer)
        mock_msg = MagicMock(spec=BrokerMessage)
        mock_msg.headers = {}
        mock_msg.body = JsonRpcRequest(
            method="test.method",
            params=params,
            id="test-123",
        )

        middleware = Middleware()

        result = await middleware.before_process_message(
            mock_consumer, mock_msg
        )

        assert result is None

    @pytest.mark.asyncio
    @given(params=valid_params())
    async def test_mutating_middleware_with_random_params(self, params):
        """Mutating middleware can modify random params."""
        from qanat.consumer import QanatConsumer
        from qanat.message import BrokerMessage
        from qanat.models import JsonRpcRequest

        # Create fresh mocks and middleware for each hypothesis example
        mock_consumer = MagicMock(spec=QanatConsumer)
        mock_msg = MagicMock(spec=BrokerMessage)
        mock_msg.headers = {}
        mock_msg.body = JsonRpcRequest(
            method="test.method",
            params=params,
            id="test-123",
        )

        class MutatingMiddleware(Middleware):
            async def before_process_message(self, consumer, msg):
                if msg.body.params is not None and isinstance(
                    msg.body.params, dict
                ):
                    msg.body.params["injected"] = "value"
                msg.headers["x-custom"] = "middleware"

        middleware = MutatingMiddleware()

        await middleware.before_process_message(mock_consumer, mock_msg)

        # Should have injected the custom value
        if params is not None and isinstance(params, dict):
            assert mock_msg.body.params["injected"] == "value"
        # Headers should always be mutated
        assert mock_msg.headers["x-custom"] == "middleware"


# === Layer 10: Integration Patterns ===


class TestMiddlewareIntegrationPatterns:
    """Test realistic middleware usage patterns."""

    @pytest.mark.asyncio
    async def test_middleware_can_track_state_across_hooks(
        self, mock_consumer, mock_broker_message
    ):
        """Middleware instance can maintain state across hook calls."""

        class StatefulMiddleware(Middleware):
            def __init__(self):
                self.messages_processed = 0
                self.errors_seen = 0

            async def before_process_message(self, consumer, msg):
                self.messages_processed += 1

            async def after_process_message(
                self, consumer, msg, *, result=None, exception=None
            ):
                if exception is not None:
                    self.errors_seen += 1

        middleware = StatefulMiddleware()

        await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )
        await middleware.after_process_message(
            mock_consumer,
            mock_broker_message,
            result=None,
            exception=ValueError(),
        )

        assert middleware.messages_processed == 1
        assert middleware.errors_seen == 1

    @pytest.mark.asyncio
    async def test_middleware_can_access_consumer_state(
        self, mock_consumer, mock_broker_message
    ):
        """Middleware can read consumer state."""
        mock_consumer.queue_name = "test-queue"

        class ConsumerAwareMiddleware(Middleware):
            def __init__(self):
                self.queue_names = []

            async def before_process_message(self, consumer, msg):
                self.queue_names.append(consumer.queue_name)

        middleware = ConsumerAwareMiddleware()
        await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert middleware.queue_names == ["test-queue"]

    @pytest.mark.asyncio
    async def test_middleware_can_read_message_metadata(
        self, mock_consumer, mock_broker_message
    ):
        """Middleware can inspect message metadata."""

        class MetadataMiddleware(Middleware):
            def __init__(self):
                self.correlation_ids = []

            async def before_process_message(self, consumer, msg):
                self.correlation_ids.append(msg.correlation_id)

        middleware = MetadataMiddleware()
        await middleware.before_process_message(
            mock_consumer, mock_broker_message
        )

        assert middleware.correlation_ids == ["test-123"]
