"""Comprehensive tests for qanat exception hierarchy."""

from __future__ import annotations

import pytest

from qanat.exceptions import (
    FeatureNotSupportedError,
    HandlerTimeout,
    MethodNotFound,
    ProcessShutdown,
    QanatError,
)

# === Layer 1: Exception Hierarchy ===


class TestExceptionInheritance:
    """Verify exception hierarchy and inheritance relationships."""

    def test_qanat_error_inherits_from_exception(self):
        """QanatError is a subclass of Exception."""
        assert issubclass(QanatError, Exception)

    def test_method_not_found_inherits_from_qanat_error(self):
        """MethodNotFound inherits from QanatError."""
        assert issubclass(MethodNotFound, QanatError)

    def test_handler_timeout_inherits_from_qanat_error(self):
        """HandlerTimeout inherits from QanatError."""
        assert issubclass(HandlerTimeout, QanatError)

    def test_process_shutdown_inherits_from_qanat_error(self):
        """ProcessShutdown inherits from QanatError."""
        assert issubclass(ProcessShutdown, QanatError)

    def test_feature_not_supported_inherits_from_qanat_error(self):
        """FeatureNotSupportedError inherits from QanatError."""
        assert issubclass(FeatureNotSupportedError, QanatError)


# === Layer 2: Base Exception ===


class TestQanatError:
    """Test base QanatError exception."""

    def test_can_raise_qanat_error(self):
        """QanatError can be raised."""
        with pytest.raises(QanatError):
            raise QanatError("test error")

    def test_can_catch_qanat_error(self):
        """QanatError can be caught."""
        try:
            raise QanatError("test error")
        except QanatError as exc:
            assert str(exc) == "test error"

    def test_can_catch_as_exception(self):
        """QanatError can be caught as Exception."""
        try:
            raise QanatError("test error")
        except Exception as exc:
            assert isinstance(exc, QanatError)
            assert str(exc) == "test error"

    def test_qanat_error_with_custom_message(self):
        """QanatError stores custom message."""
        exc = QanatError("custom message")
        assert str(exc) == "custom message"

    def test_qanat_error_with_empty_message(self):
        """QanatError can be created with empty message."""
        exc = QanatError()
        assert str(exc) == ""


# === Layer 3: Simple Exceptions ===


class TestMethodNotFound:
    """Test MethodNotFound exception."""

    def test_can_raise_method_not_found(self):
        """MethodNotFound can be raised."""
        with pytest.raises(MethodNotFound):
            raise MethodNotFound("handler not registered")

    def test_can_catch_method_not_found(self):
        """MethodNotFound can be caught."""
        try:
            raise MethodNotFound("handler not registered")
        except MethodNotFound as exc:
            assert str(exc) == "handler not registered"

    def test_can_catch_as_qanat_error(self):
        """MethodNotFound can be caught as QanatError."""
        try:
            raise MethodNotFound("handler not registered")
        except QanatError as exc:
            assert isinstance(exc, MethodNotFound)

    def test_can_catch_as_exception(self):
        """MethodNotFound can be caught as Exception."""
        try:
            raise MethodNotFound("handler not registered")
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

    def test_can_catch_as_qanat_error(self):
        """HandlerTimeout can be caught as QanatError."""
        try:
            raise HandlerTimeout(30.0)
        except QanatError as exc:
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

    def test_can_catch_as_qanat_error(self):
        """ProcessShutdown can be caught as QanatError."""
        try:
            raise ProcessShutdown()
        except QanatError as exc:
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

    def test_can_catch_as_qanat_error(self):
        """FeatureNotSupportedError can be caught as QanatError."""
        try:
            raise FeatureNotSupportedError("delay", "RabbitMQ")
        except QanatError as exc:
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
