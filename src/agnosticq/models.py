from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from agnosticq.types import JsonRpcId, JsonRpcParams, JsonRpcVersion


@dataclass(frozen=True, slots=True)
class ConsumerSettings:
    concurrency: int = 10
    retry_base_delay: float = 1.0
    retry_max_delay: float = 300.0
    retry_jitter: float = 1.0  # uniform(0, jitter)


class JsonRpcRequest(BaseModel):
    model_config = ConfigDict(strict=True)

    jsonrpc: JsonRpcVersion = "2.0"
    method: str
    params: JsonRpcParams = None
    id: JsonRpcId = None


class JsonRpcErrorDetail(BaseModel):
    code: int
    message: str
    data: Any | None = None


class JsonRpcResponse(BaseModel):
    model_config = ConfigDict(strict=True)

    jsonrpc: JsonRpcVersion = "2.0"
    result: Any | None = None
    error: JsonRpcErrorDetail | None = None
    id: JsonRpcId
