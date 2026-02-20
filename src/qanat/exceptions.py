"""Custom exceptions for the qanat package."""


class QanatError(Exception):
    """Base exception for all qanat errors."""


class MessageExpired(QanatError):
    """Raised when a message's TTL has been exceeded.

    The consumer rejects the message without retrying when
    ``time.time() > x-expire-at``.
    """


class MethodNotFound(QanatError):
    """Raised when no registered handler exists for a given method name."""


class HandlerTimeout(QanatError):
    """Raised when a handler exceeds its configured time_limit.

    Example: a handler registered with ``time_limit=30`` that runs for
    more than 30 seconds.
    """

    def __init__(self, time_limit: float | None = None) -> None:
        """Initialize with the exceeded time limit.

        Args:
            time_limit: The time limit in seconds that was exceeded.
                If ``None``, a generic message is used.
        """
        if time_limit is not None:
            msg = f"Handler exceeded time limit of {time_limit}s."
        else:
            msg = "Handler exceeded its time limit."
        super().__init__(msg)
        self.time_limit = time_limit


class ProcessShutdown(QanatError):
    """Raised inside a PROCESS-mode worker on SIGTERM during shutdown."""

    def __init__(self) -> None:
        super().__init__("Worker process is shutting down")


class FeatureNotSupportedError(QanatError):
    """Raised when a feature is not supported by the broker provider.

    Example: passing ``delay`` to a provider with no native
    scheduled-delivery capability.
    """

    def __init__(self, feature: str, provider: str) -> None:
        """Initialize with the unsupported feature and provider name.

        Args:
            feature: Name of the unsupported feature (e.g. ``"delay"``).
            provider: Human-readable broker provider name (e.g. ``"RabbitMQ"``).
        """
        super().__init__(
            f"{provider} does not support the '{feature}' feature."
        )
        self.feature = feature
        self.provider = provider
