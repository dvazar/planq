"""Custom exceptions for the planq package."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from planq.types import Seconds


class PlanqError(Exception):
    """Base exception for all planq errors."""


class HandlerCancelled(PlanqError):
    """Base for any cooperative cancellation of a running handler.

    Raised from :meth:`planq.context.PlanqContext.check_cancellation`
    when the handler is asked to stop. The concrete cause — a deadline
    (:class:`HandlerTimeout`) or worker shutdown (:class:`Shutdown`) —
    is carried by the subclass, so handlers can catch the base to run
    cleanup regardless of why they were canceled.
    """


class HandlerTimeout(HandlerCancelled):
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


class Shutdown(HandlerCancelled):
    """Raised inside an ASYNC/THREAD handler when the consumer stops.

    Signals that the worker is shutting down (e.g. on SIGTERM from an
    orchestrator). Handlers that cooperatively call
    :meth:`planq.context.PlanqContext.check_cancellation` see this
    exception and may persist progress before unwinding. Unlike a
    handler error, a ``Shutdown`` always requeues the message rather
    than rejecting it.
    """

    def __init__(self, message: str = "Consumer is shutting down") -> None:
        """Initialize with an optional human-readable reason.

        Args:
            message: Description of why the handler is being stopped.
        """
        super().__init__(message)


class ProcessShutdown(Shutdown):
    """Raised inside a PROCESS-mode worker on SIGTERM during shutdown."""

    def __init__(self) -> None:
        super().__init__("Worker process is shutting down")


class FeatureNotSupportedError(PlanqError):
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


class RetryMessage(PlanqError):
    """Signal the transport layer to nack with a delay.

    Raised when the message should be requeued for later processing.
    """

    def __init__(self, delay: Seconds | None = None) -> None:
        """Initialize with the retry delay.

        Args:
            delay: Backoff delay in seconds before the message
                becomes visible again.
        """
        if delay is not None and delay <= 0:
            raise ValueError("delay must be positive")

        self.delay = delay


#: Alias for RetryMessage for more concise usage in handlers.
Retry = RetryMessage


class RejectMessage(PlanqError):
    """Signal the transport layer to permanently reject the message."""


class MethodNotFound(RejectMessage):
    """Raised when no registered handler exists for a given method name."""

    def __init__(self, method: str) -> None:
        """Initialize with the unresolvable method name.

        Args:
            method: JSON-RPC method name that has no registered handler.
        """
        super().__init__(f"No registered handler for method '{method}'")
        self.method = method


class MaxRetriesExceeded(RejectMessage):
    """Raised when a message has reached its maximum number of retries."""

    def __init__(self, max_attempts: int, method: str) -> None:
        """Initialize with the exhausted retry count and method name.

        Args:
            max_attempts: Total delivery attempts that were made.
            method: JSON-RPC method name whose retries were exhausted.
        """
        super().__init__(
            f"Max retries ({max_attempts}) exceeded for method '{method}'"
        )
        self.max_attempts = max_attempts
        self.method = method


class InvalidParamsError(RejectMessage):
    """JSON-RPC params validation failure (-32602).

    Extends ``RejectMessage`` because invalid params are permanent
    failures (retrying won't fix bad params). Caught explicitly in
    ``_router_endpoint`` to return a proper ``-32602`` JSON-RPC error.
    """

    def __init__(
        self,
        errors: list[dict[str, Any]],
        method: str,
    ) -> None:
        """Initialize with validation errors and method name.

        Args:
            errors: List of error dicts with ``loc``, ``msg``,
                and ``type`` keys describing each validation failure.
            method: JSON-RPC method name for error context.
        """
        self.errors = errors
        self.method = method
        summary = "; ".join(e.get("msg", str(e)) for e in errors[:3])
        super().__init__(f"Invalid params for '{method}': {summary}")
