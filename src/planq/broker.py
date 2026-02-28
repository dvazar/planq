"""Abstract base class for message broker implementations."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from types import TracebackType
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from planq.message import BrokerMessage
    from planq.models import JsonRpcRequest, JsonRpcResponse
    from planq.types import Headers, Seconds

logger = logging.getLogger(__name__)

MAX_LOG_PAYLOAD_SIZE: Final[int] = 1000


class BaseBroker:
    """Provider-agnostic interface for a message broker.

    Subclass this and override all methods to add a new transport
    (SQS, Azure Service Bus, Google PUB/SUB, etc.). Methods raise
    ``NotImplementedError`` by default so missing overrides fail fast.

    Attributes:
        dsn: Connection string or endpoint URL for the broker.
    """

    def __init__(self, dsn: str) -> None:
        """Initialize the broker with a connection string.

        Args:
            dsn: Transport-specific connection string or endpoint URL
        """
        self.dsn = dsn

    def get_queue_name(self, identifier: str) -> str:
        """Derive a queue name from an identifier.

        By default, returns the identifier unchanged. Override to apply
        provider-specific naming rules or transformations.

        Args:
            identifier: URL, ARN, logical name or identifier for the queue.

        Returns:
            The derived queue name to use for publishing or consuming.
        """
        return identifier.strip()

    async def connect(self) -> None:
        """Open the underlying transport connection.

        Called automatically by :meth:`__aenter__`.

        Raises:
            NotImplementedError: Must be overridden by subclasses.
        """
        raise NotImplementedError

    async def disconnect(self) -> None:
        """Close the underlying transport connection and release resources.

        Called automatically by :meth:`__aexit__`.

        Raises:
            NotImplementedError: Must be overridden by subclasses.
        """
        raise NotImplementedError

    async def publish(
        self,
        queue: str,
        rpc: JsonRpcRequest | JsonRpcResponse,
        *,
        delay: Seconds | None = None,
        expire_at: float | None = None,
        reply_to: str | None = None,
        headers: Headers | None = None,
    ) -> str:
        """Serialize and publish a JSON-RPC message to a queue.

        Args:
            queue: Destination queue name or URL.
            rpc: The JSON-RPC request or response to publish.
            delay: Seconds to defer delivery before the message becomes visible
                to consumers. Behavior is provider-specific; see the concrete
                broker's docstring for the supported range. ``None`` means
                immediate delivery. Providers without native scheduled-delivery
                support raise
                :exc:`~planq.exceptions.FeatureNotSupportedError`.
            expire_at: Unix timestamp after which the message should be
                discarded. Stored as a broker header.
            reply_to: Optional queue name where the consumer should send
                its response (request/response pattern).
            headers: Optional mapping of additional user-defined headers to
                attach to the message. Values are always stored as strings.
                Do not shadow reserved keys used by the framework
                (``ReplyTo``, ``ExpireAt``).

        Returns:
            The provider-assigned message identifier string.

        Raises:
            NotImplementedError: Must be overridden by subclasses.
        """
        raise NotImplementedError

    async def consume(
        self,
        queue: str,
        *,
        prefetch: int = 10,
    ) -> AsyncIterator[BrokerMessage]:
        """Yield messages from a queue as an async generator.

        Implementations should use long-polling (or an equivalent
        backpressure mechanism) rather than a tight polling loop.

        Args:
            queue: Source queue name or URL.
            prefetch: Maximum number of messages to fetch per poll.

        Yields:
            :class:`~planq.message.BrokerMessage` instances ready
            for processing.

        Raises:
            NotImplementedError: Must be overridden by subclasses.
        """
        raise NotImplementedError
        yield  # noqa: RET503 — make this a generator

    async def on_poison_message(
        self,
        message_id: str,
        raw_body: str | bytes,
        queue: str,
        error: Exception,
    ) -> None:
        """Handle a message that failed JSON-RPC parsing.

        The default implementation logs the error and truncates the raw
        body to avoid flooding logs. Override to send to a dead-letter
        queue or alert system.

        Args:
            message_id: Message identifier.
            raw_body: The raw string body of the unprocessable message.
            queue: The queue from which the message was consumed.
            error: The exception raised during parsing.
        """
        body_size = len(raw_body)
        is_truncated = body_size > MAX_LOG_PAYLOAD_SIZE

        safe_body = raw_body[:MAX_LOG_PAYLOAD_SIZE]
        if isinstance(safe_body, bytes):
            safe_body = safe_body.decode("utf-8", errors="replace")
        if is_truncated:
            safe_body += f"... [truncated, total size: {body_size} bytes]"

        ctx = {
            "message_id": message_id,
            "queue_name": self.get_queue_name(queue),
            "body_size": body_size,
            "is_truncated": is_truncated,
            "raw_body_snippet": safe_body,
        }
        logger.error(
            "Poison message detected in queue '%(queue_name)s': "
            "failed to parse body. Message ID: %(message_id)s.",
            ctx,
            extra=ctx,
            exc_info=error,
        )

    async def __aenter__(self) -> BaseBroker:
        """Connect and return self for use as an async context manager.

        Returns:
            This broker instance after connecting.
        """
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Disconnect when exiting the async context manager.

        Args:
            exc_type: Exception type, if any.
            exc_val: Exception value, if any.
            exc_tb: Exception traceback, if any.
        """
        await self.disconnect()
