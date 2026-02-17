"""Custom exceptions for the agnosticq package."""


class AgnosticQError(Exception):
    """Base exception for all agnosticq errors."""


class MessageExpired(AgnosticQError):
    """Raised when a message's TTL has been exceeded.

    The consumer rejects the message without retrying when
    ``time.time() > x-expire-at``.
    """


class MaxRetriesExceeded(AgnosticQError):
    """Raised when a message has exhausted its retry budget.

    The consumer rejects the message permanently when
    ``delivery_count > x-max-retries``.
    """


class MethodNotFound(AgnosticQError):
    """Raised when no registered handler exists for a given method name."""
