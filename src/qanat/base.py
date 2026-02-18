"""Abstract base class for message broker implementations."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qanat.message import BrokerMessage
    from qanat.models import JsonRpcRequest, JsonRpcResponse
    from qanat.types import Headers, Seconds

logger = logging.getLogger(__name__)

_MAX_POISON_BODY_LOG = 500


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
        max_retries: int | None = None,
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
                :exc:`~qanat.exceptions.FeatureNotSupportedError`.
            max_retries: Maximum delivery attempts before the message is
                permanently rejected. Stored as a broker header.
            expire_at: Unix timestamp after which the message should be
                discarded. Stored as a broker header.
            reply_to: Optional queue name where the consumer should send
                its response (request/response pattern).
            headers: Optional mapping of additional user-defined headers to
                attach to the message. Values are always stored as strings.
                Do not shadow reserved keys used by the framework
                (``ReplyTo``, ``MaxRetries``, ``ExpireAt``).

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
            :class:`~qanat.message.BrokerMessage` instances ready
            for processing.

        Raises:
            NotImplementedError: Must be overridden by subclasses.
        """
        raise NotImplementedError
        yield  # noqa: RET503 — make this a generator

    async def on_poison_message(
        self,
        raw_body: str,
        error: Exception,
    ) -> None:
        """Handle a message that failed JSON-RPC parsing.

        The default implementation logs the error and truncates the raw
        body to avoid flooding logs. Override to send to a dead-letter
        queue or alert system.

        Args:
            raw_body: The raw string body of the unprocessable message.
            error: The exception raised during parsing.
        """
        truncated = (
            raw_body[:_MAX_POISON_BODY_LOG] + "..."
            if len(raw_body) > _MAX_POISON_BODY_LOG
            else raw_body
        )
        logger.error(
            "Poison message discarded, body: %s",
            truncated,
            exc_info=error,
        )

    async def __aenter__(self) -> BaseBroker:
        """Connect and return self for use as an async context manager.

        Returns:
            This broker instance after connecting.
        """
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore[type-arg]
        """Disconnect when exiting the async context manager.

        Args:
            exc_type: Exception type, if any.
            exc_val: Exception value, if any.
            exc_tb: Exception traceback, if any.
        """
        await self.disconnect()
