class AgnosticQError(Exception):
    """Base exception for agnosticq."""


class MessageExpired(AgnosticQError):
    """TTL exceeded."""


class MaxRetriesExceeded(AgnosticQError):
    """Retries exhausted."""


class MethodNotFound(AgnosticQError):
    """No route for method."""
