from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agnosticq.models import JsonRpcRequest
    from agnosticq.types import Headers, JsonRpcId, Seconds


class BrokerMessage:
    def __init__(
        self,
        raw: Any,
        body: JsonRpcRequest,
        headers: Headers,
    ) -> None:
        self.raw = raw
        self.body = body
        self.headers = headers

    @property
    def correlation_id(self) -> JsonRpcId:
        return self.body.id

    @property
    def delivery_count(self) -> int:
        raise NotImplementedError

    @property
    def reply_to(self) -> str | None:
        raise NotImplementedError

    async def ack(self) -> None:
        raise NotImplementedError

    async def reject(self) -> None:
        raise NotImplementedError

    async def nack(self, delay: Seconds) -> None:
        raise NotImplementedError
