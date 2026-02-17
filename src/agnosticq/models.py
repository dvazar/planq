"""Pydantic data models for JSON-RPC 2.0 messages and consumer configuration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from agnosticq.types import JsonRpcId, JsonRpcParams, JsonRpcVersion


@dataclass(frozen=True, slots=True)
class ConsumerSettings:
    """Immutable runtime settings for ``AgnosticConsumer``."""

    # Maximum number of messages processed concurrently.
    concurrency: int = 10

    # Initial backoff delay in seconds; doubles with each retry attempt.
    retry_base_delay: float = 1.0

    # Maximum backoff delay in seconds; caps exponential growth.
    retry_max_delay: float = 300.0

    # Upper bound for uniform jitter added to the backoff value.
    retry_jitter: float = 1.0  # uniform(0, jitter)

    # Max requeue attempts when no route matches the method name.
    unroutable_max_retries: int = 10


class JsonRpcRequest(BaseModel):
    """Incoming JSON-RPC 2.0 request or notification.

    A message with ``id=None`` is a *notification* (fire-and-forget).
    A message with a non-``None`` ``id`` expects a response published
    to the ``reply_to`` queue.
    """

    model_config = ConfigDict(strict=True)

    # Protocol version; must always be "2.0".
    jsonrpc: JsonRpcVersion = "2.0"
    # Name of the remote procedure to invoke.
    method: str
    # Optional positional (list) or named (dict) parameters.
    params: JsonRpcParams = None
    # Request identifier; ``None`` for notifications.
    id: JsonRpcId = None


class JsonRpcErrorDetail(BaseModel):
    """Structured error payload embedded in a :class:`JsonRpcResponse`.

    Follows the JSON-RPC 2.0 error object specification.
    """

    # Numeric error code (use :class:`~agnosticq.enums.JsonRpcError` constants).
    code: int
    # Human-readable error description.
    message: str
    # Optional additional error context; may be any JSON-serialisable value.
    data: Any | None = None


class JsonRpcResponse(BaseModel):
    """JSON-RPC 2.0 response published to the ``reply_to`` queue.

    Exactly one of ``result`` or ``error`` must be set; both being
    non-``None`` simultaneously is semantically invalid per the spec.
    """

    model_config = ConfigDict(strict=True)

    # Protocol version; must always be "2.0".
    jsonrpc: JsonRpcVersion = "2.0"
    # Successful return value; mutually exclusive with ``error``.
    result: Any | None = None
    # Error detail when the handler raised; mutually exclusive with ``result``.
    error: JsonRpcErrorDetail | None = None
    # Echo of the original request's ``id``.
    id: JsonRpcId
