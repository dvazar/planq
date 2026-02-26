"""Comprehensive tests for planq exception hierarchy."""

from __future__ import annotations

import pytest

from planq.exceptions import (
    FeatureNotSupportedError,
    HandlerTimeout,
    MaxRetriesExceeded,
    MethodNotFound,
    PlanqError,
    ProcessShutdown,
    RejectMessage,
)

# === Layer 1: Exception Hierarchy ===


class TestExceptionInheritance:
    """Verify exception hierarchy and inheritance relationships."""

    def test_planq_error_inherits_from_exception(self):
        """PlanqError is a subclass of Exception."""
        assert issubclass(PlanqError, Exception)

    def test_method_not_found_inherits_from_planq_error(self):
        """MethodNotFound inherits from PlanqError."""
        assert issubclass(MethodNotFound, PlanqError)

    def test_handler_timeout_inherits_from_planq_error(self):
        """HandlerTimeout inherits from PlanqError."""
        assert issubclass(HandlerTimeout, PlanqError)

    def test_process_shutdown_inherits_from_planq_error(self):
        """ProcessShutdown inherits from PlanqError."""
        assert issubclass(ProcessShutdown, PlanqError)

    def test_feature_not_supported_inherits_from_planq_error(self):
        """FeatureNotSupportedError inherits from PlanqError."""
        assert issubclass(FeatureNotSupportedError, PlanqError)


# === Layer 2: Base Exception ===


class TestPlanqError:
    """Test base PlanqError exception."""

    def test_can_raise_planq_error(self):
        """PlanqError can be raised."""
        with pytest.raises(PlanqError):
            raise PlanqError("test error")

    def test_can_catch_planq_error(self):
        """PlanqError can be caught."""
        try:
            raise PlanqError("test error")
        except PlanqError as exc:
            assert str(exc) == "test error"

    def test_can_catch_as_exception(self):
        """PlanqError can be caught as Exception."""
        try:
            raise PlanqError("test error")
        except Exception as exc:
            assert isinstance(exc, PlanqError)
            assert str(exc) == "test error"

    def test_planq_error_with_custom_message(self):
        """PlanqError stores custom message."""
        exc = PlanqError("custom message")
        assert str(exc) == "custom message"

    def test_planq_error_with_empty_message(self):
        """PlanqError can be created with empty message."""
        exc = PlanqError()
        assert str(exc) == ""


class TestRejectMessage:
    """Test RejectMessage exception."""

    def test_reject_message_inherits_from_planq_error(self):
        """RejectMessage is a subclass of PlanqError."""
        assert issubclass(RejectMessage, PlanqError)

    def test_can_raise_reject_message(self):
        """RejectMessage can be raised."""
        with pytest.raises(RejectMessage):
            raise RejectMessage("test rejection")

    def test_can_catch_reject_message(self):
        """RejectMessage can be caught."""
        try:
            raise RejectMessage("test rejection")
        except RejectMessage as exc:
            assert str(exc) == "test rejection"

    def test_can_catch_as_planq_error(self):
        """RejectMessage can be caught as PlanqError."""
        try:
            raise RejectMessage("test rejection")
        except PlanqError as exc:
            assert isinstance(exc, RejectMessage)
            assert str(exc) == "test rejection"

    def test_can_catch_as_exception(self):
        """RejectMessage can be caught as Exception."""
        try:
            raise RejectMessage("test rejection")
        except Exception as exc:
            assert isinstance(exc, RejectMessage)
            assert str(exc) == "test rejection"


# === Layer 3: Simple Exceptions ===


class TestMethodNotFound:
    """Test MethodNotFound exception."""

    def test_can_raise_method_not_found(self):
        """MethodNotFound can be raised."""
        with pytest.raises(MethodNotFound):
            raise MethodNotFound("order.payment.process")

    def test_can_catch_method_not_found(self):
        """MethodNotFound can be caught."""
        try:
            raise MethodNotFound("order.payment.process")
        except MethodNotFound as exc:
            assert (
                str(exc)
                == "No registered handler for method 'order.payment.process'"
            )

    def test_method_not_found_inherits_from_reject_message(self):
        """MethodNotFound is a subclass of RejectMessage."""
        assert issubclass(MethodNotFound, RejectMessage)

    def test_can_catch_as_planq_error(self):
        """MethodNotFound can be caught as PlanqError."""
        try:
            raise MethodNotFound("order.payment.process")
        except PlanqError as exc:
            assert isinstance(exc, MethodNotFound)

    def test_can_catch_as_exception(self):
        """MethodNotFound can be caught as Exception."""
        try:
            raise MethodNotFound("order.payment.process")
        except Exception as exc:
            assert isinstance(exc, MethodNotFound)


# === Layer 4: HandlerTimeout Exception ===


class TestHandlerTimeout:
    """Test HandlerTimeout exception with custom __init__."""

    def test_can_raise_handler_timeout(self):
        """HandlerTimeout can be raised."""
        with pytest.raises(HandlerTimeout):
            raise HandlerTimeout(30.0)

    def test_can_catch_handler_timeout(self):
        """HandlerTimeout can be caught."""
        try:
            raise HandlerTimeout(30.0)
        except HandlerTimeout as exc:
            assert exc.time_limit == 30.0

    def test_can_catch_as_planq_error(self):
        """HandlerTimeout can be caught as PlanqError."""
        try:
            raise HandlerTimeout(30.0)
        except PlanqError as exc:
            assert isinstance(exc, HandlerTimeout)

    def test_can_catch_as_exception(self):
        """HandlerTimeout can be caught as Exception."""
        try:
            raise HandlerTimeout(30.0)
        except Exception as exc:
            assert isinstance(exc, HandlerTimeout)

    @pytest.mark.parametrize(
        "time_limit",
        [0.1, 1.0, 30.0, 3600.0],
    )
    def test_message_with_time_limit(self, time_limit):
        """Message formatting includes time_limit value."""
        exc = HandlerTimeout(time_limit)
        expected = f"Handler exceeded time limit of {time_limit}s."
        assert str(exc) == expected
        assert exc.time_limit == time_limit

    def test_message_without_time_limit(self):
        """Message formatting without time_limit uses generic message."""
        exc = HandlerTimeout(None)
        assert str(exc) == "Handler exceeded its time limit."
        assert exc.time_limit is None

    def test_default_time_limit_is_none(self):
        """HandlerTimeout() with no args defaults time_limit to None."""
        exc = HandlerTimeout()
        assert exc.time_limit is None
        assert str(exc) == "Handler exceeded its time limit."

    def test_stores_time_limit_attribute(self):
        """HandlerTimeout stores time_limit as attribute."""
        exc = HandlerTimeout(60.0)
        assert hasattr(exc, "time_limit")
        assert exc.time_limit == 60.0

    def test_time_limit_zero_is_valid(self):
        """time_limit=0.0 is a valid value."""
        exc = HandlerTimeout(0.0)
        assert exc.time_limit == 0.0
        assert str(exc) == "Handler exceeded time limit of 0.0s."


class TestMaxRetriesExceeded:
    """Test MaxRetriesExceeded exception."""

    def test_max_retries_exceeded_inherits_from_reject_message(self):
        """MaxRetriesExceeded is a subclass of RejectMessage."""
        assert issubclass(MaxRetriesExceeded, RejectMessage)

    def test_max_retries_exceeded_inherits_from_planq_error(self):
        """MaxRetriesExceeded is a subclass of PlanqError (via RejectMessage)."""
        assert issubclass(MaxRetriesExceeded, PlanqError)

    def test_can_raise_max_retries_exceeded(self):
        """MaxRetriesExceeded can be raised."""
        with pytest.raises(MaxRetriesExceeded):
            raise MaxRetriesExceeded(3, "process_payment")

    def test_can_catch_max_retries_exceeded(self):
        """MaxRetriesExceeded can be caught."""
        try:
            raise MaxRetriesExceeded(3, "process_payment")
        except MaxRetriesExceeded as exc:
            assert exc.max_attempts == 3
            assert exc.method == "process_payment"

    def test_can_catch_as_reject_message(self):
        """MaxRetriesExceeded can be caught as RejectMessage."""
        try:
            raise MaxRetriesExceeded(3, "process_payment")
        except RejectMessage as exc:
            assert isinstance(exc, MaxRetriesExceeded)

    def test_can_catch_as_planq_error(self):
        """MaxRetriesExceeded can be caught as PlanqError."""
        try:
            raise MaxRetriesExceeded(3, "process_payment")
        except PlanqError as exc:
            assert isinstance(exc, MaxRetriesExceeded)

    def test_can_catch_as_exception(self):
        """MaxRetriesExceeded can be caught as Exception."""
        try:
            raise MaxRetriesExceeded(3, "process_payment")
        except Exception as exc:
            assert isinstance(exc, MaxRetriesExceeded)

    @pytest.mark.parametrize(
        "max_attempts,method",
        [
            (1, "send_email"),
            (3, "process_payment"),
            (5, "retry.task"),
            (10, "order.payment.process"),
        ],
    )
    def test_message_formatting(self, max_attempts, method):
        """Message formatting includes max_attempts and method."""
        exc = MaxRetriesExceeded(max_attempts, method)
        expected = (
            f"Max retries ({max_attempts}) exceeded for method '{method}'"
        )
        assert str(exc) == expected

    def test_stores_max_attempts_attribute(self):
        """MaxRetriesExceeded stores max_attempts attribute."""
        exc = MaxRetriesExceeded(5, "test_method")
        assert hasattr(exc, "max_attempts")
        assert exc.max_attempts == 5

    def test_stores_method_attribute(self):
        """MaxRetriesExceeded stores method attribute."""
        exc = MaxRetriesExceeded(3, "process_order")
        assert hasattr(exc, "method")
        assert exc.method == "process_order"

    def test_stores_both_attributes(self):
        """MaxRetriesExceeded stores both max_attempts and method."""
        exc = MaxRetriesExceeded(7, "retry.payment")
        assert exc.max_attempts == 7
        assert exc.method == "retry.payment"
        expected = "Max retries (7) exceeded for method 'retry.payment'"
        assert str(exc) == expected

    def test_zero_max_attempts_is_valid(self):
        """max_attempts=0 is a valid value (no retries allowed)."""
        exc = MaxRetriesExceeded(0, "no_retry_task")
        assert exc.max_attempts == 0
        assert str(exc) == "Max retries (0) exceeded for method 'no_retry_task'"

    def test_empty_method_name_is_valid(self):
        """Empty method name is technically valid."""
        exc = MaxRetriesExceeded(3, "")
        assert exc.method == ""
        assert str(exc) == "Max retries (3) exceeded for method ''"


# === Layer 5: ProcessShutdown Exception ===


class TestProcessShutdown:
    """Test ProcessShutdown exception with fixed message."""

    def test_can_raise_process_shutdown(self):
        """ProcessShutdown can be raised."""
        with pytest.raises(ProcessShutdown):
            raise ProcessShutdown()

    def test_can_catch_process_shutdown(self):
        """ProcessShutdown can be caught."""
        try:
            raise ProcessShutdown()
        except ProcessShutdown as exc:
            assert str(exc) == "Worker process is shutting down"

    def test_can_catch_as_planq_error(self):
        """ProcessShutdown can be caught as PlanqError."""
        try:
            raise ProcessShutdown()
        except PlanqError as exc:
            assert isinstance(exc, ProcessShutdown)

    def test_can_catch_as_exception(self):
        """ProcessShutdown can be caught as Exception."""
        try:
            raise ProcessShutdown()
        except Exception as exc:
            assert isinstance(exc, ProcessShutdown)

    def test_fixed_message(self):
        """ProcessShutdown always has the same message."""
        exc = ProcessShutdown()
        assert str(exc) == "Worker process is shutting down"

    def test_takes_no_parameters(self):
        """ProcessShutdown __init__ requires no parameters."""
        # Should succeed with no args
        exc = ProcessShutdown()
        assert str(exc) == "Worker process is shutting down"

    def test_multiple_instances_have_same_message(self):
        """All ProcessShutdown instances have identical messages."""
        exc1 = ProcessShutdown()
        exc2 = ProcessShutdown()
        assert str(exc1) == str(exc2)
        assert str(exc1) == "Worker process is shutting down"


# === Layer 6: FeatureNotSupportedError Exception ===


class TestFeatureNotSupportedError:
    """Test FeatureNotSupportedError with parameterized message."""

    def test_can_raise_feature_not_supported(self):
        """FeatureNotSupportedError can be raised."""
        with pytest.raises(FeatureNotSupportedError):
            raise FeatureNotSupportedError("delay", "RabbitMQ")

    def test_can_catch_feature_not_supported(self):
        """FeatureNotSupportedError can be caught."""
        try:
            raise FeatureNotSupportedError("delay", "RabbitMQ")
        except FeatureNotSupportedError as exc:
            assert exc.feature == "delay"
            assert exc.provider == "RabbitMQ"

    def test_can_catch_as_planq_error(self):
        """FeatureNotSupportedError can be caught as PlanqError."""
        try:
            raise FeatureNotSupportedError("delay", "RabbitMQ")
        except PlanqError as exc:
            assert isinstance(exc, FeatureNotSupportedError)

    def test_can_catch_as_exception(self):
        """FeatureNotSupportedError can be caught as Exception."""
        try:
            raise FeatureNotSupportedError("delay", "RabbitMQ")
        except Exception as exc:
            assert isinstance(exc, FeatureNotSupportedError)

    @pytest.mark.parametrize(
        "feature,provider",
        [
            ("delay", "RabbitMQ"),
            ("ttl", "Redis"),
            ("priority", "SQS"),
            ("transactions", "Kafka"),
            ("dead-letter-queue", "GCP Pub/Sub"),
        ],
    )
    def test_message_formatting(self, feature, provider):
        """Message formatting includes feature and provider."""
        exc = FeatureNotSupportedError(feature, provider)
        expected = f"{provider} does not support the '{feature}' feature."
        assert str(exc) == expected

    def test_stores_feature_attribute(self):
        """FeatureNotSupportedError stores feature attribute."""
        exc = FeatureNotSupportedError("delay", "RabbitMQ")
        assert hasattr(exc, "feature")
        assert exc.feature == "delay"

    def test_stores_provider_attribute(self):
        """FeatureNotSupportedError stores provider attribute."""
        exc = FeatureNotSupportedError("delay", "RabbitMQ")
        assert hasattr(exc, "provider")
        assert exc.provider == "RabbitMQ"

    def test_stores_both_attributes(self):
        """FeatureNotSupportedError stores both feature and provider."""
        exc = FeatureNotSupportedError("ttl", "Redis")
        assert exc.feature == "ttl"
        assert exc.provider == "Redis"
        expected = "Redis does not support the 'ttl' feature."
        assert str(exc) == expected

    def test_empty_strings_are_valid(self):
        """Empty strings for feature/provider are technically valid."""
        exc = FeatureNotSupportedError("", "")
        assert exc.feature == ""
        assert exc.provider == ""
        assert str(exc) == " does not support the '' feature."
