"""Abstract base class for broker-specific message wrappers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agnosticq.models import JsonRpcRequest
    from agnosticq.types import Headers, JsonRpcId, Seconds


class BrokerMessage:
    """Provider-agnostic wrapper around a single in-flight broker message.

    Each provider subclass stores the native message object in ``raw``
    and exposes a uniform interface for acknowledgement, rejection, and
    negative acknowledgement with backoff.

    Attributes:
        raw: The native message object returned by the broker SDK.
        body: The parsed and validated JSON-RPC request.
        headers: Normalised message headers (``x-max-retries``,
            ``x-expire-at``, etc.).
    """

    def __init__(
        self,
        raw: Any,
        body: JsonRpcRequest,
        headers: Headers,
    ) -> None:
        """Store the raw message, parsed body, and normalised headers.

        Args:
            raw: Native message object from the broker SDK.
            body: Validated :class:`~agnosticq.models.JsonRpcRequest`.
            headers: Flat string-to-string header mapping extracted
                from broker-specific metadata.
        """
        self.raw = raw
        self.body = body
        self.headers = headers

    @property
    def correlation_id(self) -> JsonRpcId:
        """JSON-RPC message identifier from the request body.

        ``None`` for notifications (fire-and-forget messages).
        """
        return self.body.id

    @property
    def delivery_count(self) -> int:
        """Number of times this message has been delivered by the broker.

        Raises:
            NotImplementedError: Must be overridden by subclasses.
        """
        raise NotImplementedError

    @property
    def reply_to(self) -> str | None:
        """Queue name to publish the response to, or ``None`` for notifications.

        Raises:
            NotImplementedError: Must be overridden by subclasses.
        """
        raise NotImplementedError

    async def ack(self) -> None:
        """Acknowledge successful processing and remove the message.

        Raises:
            NotImplementedError: Must be overridden by subclasses.
        """
        raise NotImplementedError

    async def reject(self) -> None:
        """Permanently discard the message without retrying.

        Use when TTL is exceeded, retries are exhausted, or no handler
        is registered for the method.

        Raises:
            NotImplementedError: Must be overridden by subclasses.
        """
        raise NotImplementedError

    async def nack(self, delay: Seconds) -> None:
        """Return the message to the queue after a backoff delay.

        The message will become visible again after ``delay`` seconds,
        allowing another consumer to re-process it.

        Args:
            delay: Visibility delay in seconds before the message is
                requeued for redelivery.

        Raises:
            NotImplementedError: Must be overridden by subclasses.
        """
        raise NotImplementedError
