"""Comprehensive tests for SkipMessage exception."""

from __future__ import annotations

import pytest

from qanat.middleware import SkipMessage

# === Layer 1: Exception Hierarchy ===


class TestSkipMessageInheritance:
    """Verify SkipMessage inheritance and hierarchy."""

    def test_skip_message_inherits_from_exception(self):
        """SkipMessage is a subclass of Exception."""
        assert issubclass(SkipMessage, Exception)

    def test_skip_message_can_be_raised(self):
        """SkipMessage can be raised."""
        with pytest.raises(SkipMessage):
            raise SkipMessage("test")

    def test_skip_message_can_be_caught(self):
        """SkipMessage can be caught."""
        try:
            raise SkipMessage("test message")
        except SkipMessage as exc:
            assert str(exc) == "test message"

    def test_skip_message_can_be_caught_as_exception(self):
        """SkipMessage can be caught as Exception."""
        try:
            raise SkipMessage("test message")
        except Exception as exc:
            assert isinstance(exc, SkipMessage)
            assert str(exc) == "test message"


# === Layer 2: Message Construction ===


class TestSkipMessageConstruction:
    """Test SkipMessage instantiation and message handling."""

    def test_skip_message_with_custom_message(self):
        """SkipMessage stores custom message."""
        exc = SkipMessage("TTL expired")
        assert str(exc) == "TTL expired"

    def test_skip_message_with_empty_message(self):
        """SkipMessage can be created with empty message."""
        exc = SkipMessage("")
        assert str(exc) == ""

    def test_skip_message_without_arguments(self):
        """SkipMessage can be created without arguments."""
        exc = SkipMessage()
        assert str(exc) == ""

    def test_skip_message_with_multiline_message(self):
        """SkipMessage can store multiline messages."""
        message = "Line 1\nLine 2\nLine 3"
        exc = SkipMessage(message)
        assert str(exc) == message

    def test_skip_message_with_unicode_message(self):
        """SkipMessage can store unicode messages."""
        message = "сообщение об ошибке"
        exc = SkipMessage(message)
        assert str(exc) == message
        assert isinstance(str(exc), str)


# === Layer 3: Usage Contract ===


class TestSkipMessageUsageContract:
    """Test SkipMessage usage patterns and documentation."""

    def test_docstring_warns_about_reject_requirement(self):
        """Docstring explains msg.reject() requirement."""
        assert SkipMessage.__doc__ is not None
        docstring = SkipMessage.__doc__
        assert "reject()" in docstring or "nack()" in docstring
        assert "MUST" in docstring or "must" in docstring

    def test_skip_message_str_representation(self):
        """SkipMessage string representation equals message."""
        exc = SkipMessage("test error")
        assert str(exc) == "test error"

    def test_skip_message_repr_contains_message(self):
        """SkipMessage repr contains the message."""
        exc = SkipMessage("test error")
        repr_str = repr(exc)
        assert "test error" in repr_str

    def test_multiple_instances_are_independent(self):
        """Multiple SkipMessage instances have independent messages."""
        exc1 = SkipMessage("message one")
        exc2 = SkipMessage("message two")
        assert str(exc1) == "message one"
        assert str(exc2) == "message two"
        assert str(exc1) != str(exc2)


# === Layer 4: Exception Behavior ===


class TestSkipMessageExceptionBehavior:
    """Test SkipMessage behavior in exception handling contexts."""

    def test_can_be_raised_and_caught_multiple_times(self):
        """SkipMessage can be raised and caught multiple times."""
        for i in range(3):
            with pytest.raises(SkipMessage):
                raise SkipMessage(f"iteration {i}")

    def test_exception_args_attribute(self):
        """SkipMessage args attribute contains the message."""
        exc = SkipMessage("test message")
        assert exc.args == ("test message",)

    def test_exception_with_multiple_args(self):
        """SkipMessage with multiple args (edge case)."""
        # Exception base class allows multiple args
        exc = SkipMessage("arg1", "arg2")
        # First arg becomes the message
        assert exc.args == ("arg1", "arg2")

    def test_exception_traceback_preserved(self):
        """SkipMessage preserves traceback when caught."""
        try:
            raise SkipMessage("with traceback")
        except SkipMessage as exc:
            import sys

            tb = sys.exc_info()[2]
            assert tb is not None
            assert exc.args == ("with traceback",)


# === Layer 5: Integration Patterns ===


class TestSkipMessageIntegrationPatterns:
    """Test SkipMessage in realistic usage scenarios."""

    @pytest.mark.asyncio
    async def test_skip_message_after_reject(self):
        """Pattern: reject() then raise SkipMessage."""
        from unittest.mock import AsyncMock

        mock_msg = AsyncMock()
        mock_msg.reject = AsyncMock()

        # Simulate middleware pattern
        await mock_msg.reject()

        with pytest.raises(SkipMessage):
            raise SkipMessage()

        mock_msg.reject.assert_called_once()

    @pytest.mark.asyncio
    async def test_skip_message_after_nack(self):
        """Pattern: nack(delay) then raise SkipMessage."""
        from unittest.mock import AsyncMock

        mock_msg = AsyncMock()
        mock_msg.nack = AsyncMock()

        # Simulate middleware pattern
        await mock_msg.nack(30)

        with pytest.raises(SkipMessage):
            raise SkipMessage("delayed retry")

        mock_msg.nack.assert_called_once_with(30)

    def test_skip_message_in_middleware_context(self):
        """SkipMessage is intended for middleware abort."""
        # Docstring should mention middleware context
        assert SkipMessage.__doc__ is not None
        docstring = SkipMessage.__doc__.lower()
        assert "middleware" in docstring or "abort" in docstring
