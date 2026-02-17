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


class FeatureNotSupportedError(AgnosticQError):
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
