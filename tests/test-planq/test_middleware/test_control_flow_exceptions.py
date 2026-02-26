"""Comprehensive tests for RetryMessage and RejectMessage exceptions."""

from __future__ import annotations

import pytest

from planq.exceptions import PlanqError, RejectMessage, RetryMessage

# === Layer 1: RetryMessage Exception ===


class TestRetryMessageInheritance:
    """Verify RetryMessage inheritance and hierarchy."""

    def test_retry_message_inherits_from_planq_error(self):
        """RetryMessage is a subclass of PlanqError."""
        assert issubclass(RetryMessage, PlanqError)

    def test_retry_message_inherits_from_exception(self):
        """RetryMessage is a subclass of Exception."""
        assert issubclass(RetryMessage, Exception)

    def test_retry_message_can_be_raised(self):
        """RetryMessage can be raised."""
        with pytest.raises(RetryMessage):
            raise RetryMessage(delay=5.0)

    def test_retry_message_can_be_caught(self):
        """RetryMessage can be caught."""
        try:
            raise RetryMessage(delay=3.0)
        except RetryMessage as exc:
            assert exc.delay == 3.0

    def test_retry_message_can_be_caught_as_planq_error(self):
        """RetryMessage can be caught as PlanqError."""
        try:
            raise RetryMessage(delay=1.0)
        except PlanqError as exc:
            assert isinstance(exc, RetryMessage)

    def test_retry_message_can_be_caught_as_exception(self):
        """RetryMessage can be caught as Exception."""
        try:
            raise RetryMessage(delay=2.0)
        except Exception as exc:
            assert isinstance(exc, RetryMessage)


class TestRetryMessageConstruction:
    """Test RetryMessage instantiation and attributes."""

    def test_retry_message_stores_delay(self):
        """RetryMessage stores delay attribute."""
        exc = RetryMessage(delay=5.0)
        assert exc.delay == 5.0

    def test_retry_message_stores_integer_delay(self):
        """RetryMessage stores integer delay."""
        exc = RetryMessage(delay=10)
        assert exc.delay == 10

    def test_retry_message_stores_fractional_delay(self):
        """RetryMessage stores fractional delay."""
        exc = RetryMessage(delay=0.5)
        assert exc.delay == 0.5

    def test_multiple_instances_are_independent(self):
        """Multiple RetryMessage instances have independent delays."""
        exc1 = RetryMessage(delay=1.0)
        exc2 = RetryMessage(delay=2.0)
        assert exc1.delay == 1.0
        assert exc2.delay == 2.0
        assert exc1.delay != exc2.delay


# === Layer 2: RejectMessage Exception ===


class TestRejectMessageInheritance:
    """Verify RejectMessage inheritance and hierarchy."""

    def test_reject_message_inherits_from_planq_error(self):
        """RejectMessage is a subclass of PlanqError."""
        assert issubclass(RejectMessage, PlanqError)

    def test_reject_message_inherits_from_exception(self):
        """RejectMessage is a subclass of Exception."""
        assert issubclass(RejectMessage, Exception)

    def test_reject_message_can_be_raised(self):
        """RejectMessage can be raised."""
        with pytest.raises(RejectMessage):
            raise RejectMessage

    def test_reject_message_can_be_caught(self):
        """RejectMessage can be caught."""
        try:
            raise RejectMessage
        except RejectMessage:
            pass  # Successfully caught

    def test_reject_message_can_be_caught_as_planq_error(self):
        """RejectMessage can be caught as PlanqError."""
        try:
            raise RejectMessage
        except PlanqError as exc:
            assert isinstance(exc, RejectMessage)

    def test_reject_message_can_be_caught_as_exception(self):
        """RejectMessage can be caught as Exception."""
        try:
            raise RejectMessage
        except Exception as exc:
            assert isinstance(exc, RejectMessage)


class TestRejectMessageConstruction:
    """Test RejectMessage instantiation."""

    def test_reject_message_without_arguments(self):
        """RejectMessage can be created without arguments."""
        exc = RejectMessage()
        assert isinstance(exc, RejectMessage)

    def test_reject_message_with_custom_message(self):
        """RejectMessage can be created with custom message."""
        exc = RejectMessage("custom reason")
        assert str(exc) == "custom reason"

    def test_reject_message_has_docstring(self):
        """RejectMessage has docstring."""
        assert RejectMessage.__doc__ is not None
        docstring = RejectMessage.__doc__.lower()
        assert "reject" in docstring


# === Layer 3: Exception Behavior ===


class TestControlFlowExceptionBehavior:
    """Test control flow exceptions in exception handling contexts."""

    def test_retry_message_can_be_raised_multiple_times(self):
        """RetryMessage can be raised and caught multiple times."""
        for i in range(3):
            with pytest.raises(RetryMessage):
                raise RetryMessage(delay=float(i + 1))

    def test_reject_message_can_be_raised_multiple_times(self):
        """RejectMessage can be raised and caught multiple times."""
        for _ in range(3):
            with pytest.raises(RejectMessage):
                raise RejectMessage

    def test_retry_preserves_traceback(self):
        """RetryMessage preserves traceback when caught."""
        try:
            raise RetryMessage(delay=1.0)
        except RetryMessage as exc:
            import sys

            tb = sys.exc_info()[2]
            assert tb is not None
            assert exc.delay == 1.0

    def test_reject_preserves_traceback(self):
        """RejectMessage preserves traceback when caught."""
        try:
            raise RejectMessage
        except RejectMessage:
            import sys

            tb = sys.exc_info()[2]
            assert tb is not None

    def test_retry_and_reject_are_distinct(self):
        """RetryMessage and RejectMessage are distinct types."""
        assert RetryMessage is not RejectMessage

        with pytest.raises(RetryMessage):
            raise RetryMessage(delay=1.0)

        with pytest.raises(RejectMessage):
            raise RejectMessage

    def test_retry_not_caught_by_reject_handler(self):
        """RetryMessage is not caught by RejectMessage handler."""
        with pytest.raises(RetryMessage):
            try:
                raise RetryMessage(delay=1.0)
            except RejectMessage:
                pytest.fail("Should not be caught")

    def test_reject_not_caught_by_retry_handler(self):
        """RejectMessage is not caught by RetryMessage handler."""
        with pytest.raises(RejectMessage):
            try:
                raise RejectMessage
            except RetryMessage:
                pytest.fail("Should not be caught")
