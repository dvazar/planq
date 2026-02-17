"""AWS SQS broker implementation using ``aiobotocore``."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, override

from aiobotocore.session import AioSession

from agnosticq.base import BaseBroker
from agnosticq.enums import Header
from agnosticq.message import BrokerMessage
from agnosticq.models import JsonRpcRequest, JsonRpcResponse
from agnosticq.types import Headers

if TYPE_CHECKING:
    from agnosticq.types import Seconds

logger = logging.getLogger(__name__)


class SqsBrokerMessage(BrokerMessage):
    """SQS-specific message wrapper backed by a ``ReceiptHandle``.

    Acknowledgement and rejection both delete the message via ``DeleteMessage``.
    Negative acknowledgement uses ``ChangeMessageVisibility`` to implement
    backoff.

    Attributes:
        raw: Raw SQS message dict as returned by ``receive_message``.
        body: Parsed JSON-RPC request.
        headers: Normalised agnosticq headers extracted from
            ``MessageAttributes``.
    """

    def __init__(
        self,
        raw: dict[str, Any],
        body: JsonRpcRequest,
        headers: Headers,
        sqs_client: Any,
        queue_url: str,
    ) -> None:
        """Store SQS-specific fields alongside the common message data.

        Args:
            raw: Raw SQS message dict (includes ``ReceiptHandle``,
                ``Attributes``, ``MessageAttributes``, etc.).
            body: Validated JSON-RPC request parsed from the message body.
            headers: Normalised agnosticq headers.
            sqs_client: Active ``aiobotocore`` SQS client.
            queue_url: Full SQS queue URL used for all API calls.
        """
        super().__init__(raw, body, headers)
        self._sqs_client = sqs_client
        self._queue_url = queue_url
        self._receipt_handle: str = raw["ReceiptHandle"]

    @property
    @override
    def delivery_count(self) -> int:
        """Number of times SQS has delivered this message.

        Derived from the ``ApproximateReceiveCount`` SQS attribute.
        """
        return int(self.raw["Attributes"]["ApproximateReceiveCount"])

    @override
    async def ack(self) -> None:
        """Delete the message from SQS to signal successful processing."""
        await self._sqs_client.delete_message(
            QueueUrl=self._queue_url,
            ReceiptHandle=self._receipt_handle,
        )

    @override
    async def reject(self) -> None:
        """Delete the message from SQS without retrying."""
        await self._sqs_client.delete_message(
            QueueUrl=self._queue_url,
            ReceiptHandle=self._receipt_handle,
        )

    @property
    @override
    def reply_to(self) -> str | None:
        """Queue URL from the ``ReplyTo`` message attribute, or ``None``."""
        if attr := self.raw.get("MessageAttributes", {}).get("ReplyTo"):
            return attr["StringValue"] or None
        return None

    @override
    async def nack(self, delay: Seconds) -> None:
        """Extend the visibility timeout to defer redelivery.

        Args:
            delay: Seconds before the message becomes visible again.
                Passed as ``VisibilityTimeout`` to SQS (truncated to int).
        """
        await self._sqs_client.change_message_visibility(
            QueueUrl=self._queue_url,
            ReceiptHandle=self._receipt_handle,
            VisibilityTimeout=int(delay),
        )


class SqsBroker(BaseBroker):
    """AWS SQS broker using long-polling and ``aiobotocore``.

    Connects lazily on :meth:`connect` and tears down on :meth:`disconnect`.
    Headers are mapped to SQS ``MessageAttributes`` with ``DataType=Number``.

    Attributes:
        dsn: SQS endpoint URL (e.g. ``http://localhost:4566`` for LocalStack,
            or the full AWS regional endpoint).
    """

    def __init__(self, dsn: str) -> None:
        """Initialise the SQS broker.

        Args:
            dsn: SQS endpoint URL passed to ``aiobotocore`` as ``endpoint_url``.
        """
        super().__init__(dsn)
        self._session: AioSession | None = None
        self._client: Any = None
        self._client_ctx: Any = None

    @override
    async def connect(self) -> None:
        """Create an ``aiobotocore`` SQS client and enter its context."""
        self._session = AioSession()
        self._client_ctx = self._session.create_client(
            "sqs",
            endpoint_url=self.dsn,
        )
        self._client = await self._client_ctx.__aenter__()

    @override
    async def disconnect(self) -> None:
        """Exit the ``aiobotocore`` client context and release resources."""
        if self._client_ctx is not None:
            await self._client_ctx.__aexit__(None, None, None)
            self._client_ctx = None
            self._client = None

    @override
    async def publish(
        self,
        queue: str,
        rpc: JsonRpcRequest | JsonRpcResponse,
        *,
        reply_to: str | None = None,
        max_retries: int | None = None,
        expire_at: float | None = None,
    ) -> str:
        """Serialize and send a JSON-RPC message to an SQS queue.

        Optional metadata is encoded as ``MessageAttributes`` with
        ``DataType=Number`` (or ``String`` for ``ReplyTo``).

        Args:
            queue: Destination SQS queue URL.
            rpc: JSON-RPC request or response to send.
            reply_to: Optional queue URL for the consumer's response.
            max_retries: Maximum delivery attempts stored as
                ``MaxRetries`` attribute.
            expire_at: Unix timestamp stored as ``ExpireAt`` attribute.

        Returns:
            The SQS ``MessageId`` of the sent message.
        """
        message_body = rpc.model_dump_json()

        attrs: dict[str, dict[str, str]] = {}
        if reply_to:
            attrs["ReplyTo"] = {
                "DataType": "String",
                "StringValue": reply_to,
            }
        if max_retries is not None:
            attrs["MaxRetries"] = {
                "DataType": "Number",
                "StringValue": str(max_retries),
            }
        if expire_at is not None:
            attrs["ExpireAt"] = {
                "DataType": "Number",
                "StringValue": str(expire_at),
            }

        kwargs: dict[str, Any] = {
            "QueueUrl": queue,
            "MessageBody": message_body,
        }
        if attrs:
            kwargs["MessageAttributes"] = attrs

        resp = await self._client.send_message(**kwargs)
        return resp["MessageId"]

    @override
    async def consume(
        self,
        queue: str,
        *,
        prefetch: int = 10,
    ) -> AsyncIterator[SqsBrokerMessage]:
        """Long-poll an SQS queue and yield parsed messages indefinitely.

        Uses ``WaitTimeSeconds=20`` and batch size ``min(prefetch, 10)``
        (SQS maximum). Poison messages (unparseable bodies) are logged
        via :meth:`~agnosticq.base.BaseBroker.on_poison_message` and
        then deleted.

        Args:
            queue: Source SQS queue URL.
            prefetch: Desired batch size (capped at 10 by SQS).

        Yields:
            :class:`SqsBrokerMessage` instances ready for processing.
        """
        while True:
            resp = await self._client.receive_message(
                QueueUrl=queue,
                MaxNumberOfMessages=min(prefetch, 10),
                WaitTimeSeconds=20,
                AttributeNames=["All"],
                MessageAttributeNames=["All"],
            )

            for raw_msg in resp.get("Messages", ()):
                try:
                    body = JsonRpcRequest.model_validate_json(raw_msg["Body"])
                except Exception as exc:
                    try:
                        await self.on_poison_message(raw_msg["Body"], exc)
                        await self._client.delete_message(
                            QueueUrl=queue,
                            ReceiptHandle=raw_msg["ReceiptHandle"],
                        )
                    except Exception as e:
                        logger.exception(
                            "Failed to handle poison message %s",
                            raw_msg.get("MessageId", "unknown"),
                            exc_info=e,
                        )
                    continue

                headers: Headers = {}
                msg_attrs = raw_msg.get("MessageAttributes", {})

                max_retries_attr = msg_attrs.get("MaxRetries")
                if max_retries_attr is not None:
                    headers[Header.MAX_RETRIES] = max_retries_attr[
                        "StringValue"
                    ]

                expire_at_attr = msg_attrs.get("ExpireAt")
                if expire_at_attr is not None:
                    headers[Header.EXPIRE_AT] = expire_at_attr["StringValue"]

                yield SqsBrokerMessage(
                    raw=raw_msg,
                    body=body,
                    headers=headers,
                    sqs_client=self._client,
                    queue_url=queue,
                )
