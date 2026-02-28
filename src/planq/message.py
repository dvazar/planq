"""Abstract base class for broker-specific message wrappers."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from planq.models import JsonRpcRequest
    from planq.types import Headers, JsonRpcId, Seconds

logger = logging.getLogger(__name__)


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
        received_at: Seconds,
        queue_name: str,
    ) -> None:
        """Store the raw message, parsed body, and normalised headers.

        Args:
            raw: Native message object from the broker SDK.
            body: Validated :class:`~planq.models.JsonRpcRequest`.
            headers: Flat string-to-string header mapping extracted
                from broker-specific metadata.
            received_at: Unix timestamp when the message was received.
            queue_name: Name of the queue this message was received from.
        """
        self.raw = raw
        self.body = body
        self.headers = headers
        self.received_at = received_at
        self.queue_name = queue_name

        self._is_settled: bool = False

    @property
    def message_id(self) -> str:
        """Unique identifier for the message from the broker's perspective.

        May be used for logging, tracing, and debugging. Not used by
        the library for routing or deduplication.

        Raises:
            NotImplementedError: Must be overridden by subclasses.
        """
        raise NotImplementedError

    @property
    def enqueued_at(self) -> float:
        """Unix timestamp when the message was enqueued.

        Raises:
            NotImplementedError: Must be overridden by subclasses.
        """
        raise NotImplementedError

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

    @property
    def correlation_id(self) -> JsonRpcId:
        """JSON-RPC message identifier from the request body.

        ``None`` for notifications (fire-and-forget messages).
        """
        return self.body.id

    async def ack(self) -> None:
        """Acknowledge successful processing and remove the message.

        Raises:
            NotImplementedError: ``_ack`` must be overridden by subclasses.
        """
        if self._is_settled:
            logger.debug(
                "Message %(message_id)s is already settled, skipping ack",
            )
            return

        await self._ack()
        self._is_settled = True

    async def _ack(self) -> None:
        """Provider-specific implementation of message acknowledgement.

        Raises:
            NotImplementedError: Must be overridden by subclasses.
        """
        raise NotImplementedError

    async def reject(self) -> None:
        """Permanently discard the message without retrying.

        Use when TTL is exceeded, retries are exhausted, or no handler
        is registered for the method.

        Raises:
            NotImplementedError: ``_reject`` must be overridden by subclasses.
        """
        if self._is_settled:
            logger.debug(
                "Message %(message_id)s is already settled, skipping reject"
            )
            return

        await self._reject()
        self._is_settled = True

    async def _reject(self) -> None:
        """Provider-specific implementation of message rejection.

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
            NotImplementedError: ``_nack`` must be overridden by subclasses.
        """
        if self._is_settled:
            logger.debug(
                "Message %(message_id)s is already settled, skipping nack"
            )
            return

        await self._nack(delay)
        self._is_settled = True

    async def _nack(self, delay: Seconds) -> None:
        """Provider-specific implementation of negative acknowledgement
        with backoff.

        Args:
            delay: Visibility delay in seconds before the message is
                requeued for redelivery.

        Raises:
            NotImplementedError: Must be overridden by subclasses.
        """
        raise NotImplementedError
