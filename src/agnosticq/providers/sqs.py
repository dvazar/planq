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
    def __init__(
        self,
        raw: dict[str, Any],
        body: JsonRpcRequest,
        headers: Headers,
        sqs_client: Any,
        queue_url: str,
    ) -> None:
        super().__init__(raw, body, headers)
        self._sqs_client = sqs_client
        self._queue_url = queue_url
        self._receipt_handle: str = raw["ReceiptHandle"]

    @property
    @override
    def delivery_count(self) -> int:
        return int(self.raw["Attributes"]["ApproximateReceiveCount"])

    @override
    async def ack(self) -> None:
        await self._sqs_client.delete_message(
            QueueUrl=self._queue_url,
            ReceiptHandle=self._receipt_handle,
        )

    @override
    async def reject(self) -> None:
        await self._sqs_client.delete_message(
            QueueUrl=self._queue_url,
            ReceiptHandle=self._receipt_handle,
        )

    @property
    @override
    def reply_to(self) -> str | None:
        if attr := self.raw.get("MessageAttributes", {}).get("ReplyTo"):
            return attr["StringValue"] or None
        return None

    @override
    async def nack(self, delay: Seconds) -> None:
        await self._sqs_client.change_message_visibility(
            QueueUrl=self._queue_url,
            ReceiptHandle=self._receipt_handle,
            VisibilityTimeout=int(delay),
        )


class SqsBroker(BaseBroker):
    def __init__(self, dsn: str) -> None:
        super().__init__(dsn)
        self._session: AioSession | None = None
        self._client: Any = None
        self._client_ctx: Any = None

    @override
    async def connect(self) -> None:
        self._session = AioSession()
        self._client_ctx = self._session.create_client(
            "sqs",
            endpoint_url=self.dsn,
        )
        self._client = await self._client_ctx.__aenter__()

    @override
    async def disconnect(self) -> None:
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
