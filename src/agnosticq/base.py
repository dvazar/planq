from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agnosticq.message import BrokerMessage
    from agnosticq.models import JsonRpcRequest, JsonRpcResponse

logger = logging.getLogger(__name__)

_MAX_POISON_BODY_LOG = 500


class BaseBroker:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    async def connect(self) -> None:
        raise NotImplementedError

    async def disconnect(self) -> None:
        raise NotImplementedError

    async def publish(
        self,
        queue: str,
        rpc: JsonRpcRequest | JsonRpcResponse,
        *,
        reply_to: str | None = None,
        max_retries: int | None = None,
        expire_at: float | None = None,
    ) -> str:
        raise NotImplementedError

    async def consume(
        self,
        queue: str,
        *,
        prefetch: int = 10,
    ) -> AsyncIterator[BrokerMessage]:
        raise NotImplementedError
        yield  # noqa: RET503 — make this a generator

    async def on_poison_message(
        self,
        raw_body: str,
        error: Exception,
    ) -> None:
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
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore[type-arg]
        await self.disconnect()
