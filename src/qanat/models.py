"""Pydantic data models for JSON-RPC 2.0 messages and consumer configuration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from qanat.enums import ExecutionMode
    from qanat.types import JsonRpcId, JsonRpcParams, JsonRpcVersion


@dataclass(frozen=True, slots=True)
class ConsumerSettings:
    """Immutable runtime settings for ``QanatConsumer``."""

    # Maximum number of messages processed concurrently.
    concurrency: int = 10

    # Initial backoff delay in seconds; doubles with each retry attempt.
    retry_base_delay: float = 1.0

    # Maximum backoff delay in seconds; caps exponential growth.
    retry_max_delay: float = 300.0

    # Max requeue attempts when no route matches the method name.
    unroutable_max_retries: int = 10

    # Grace period (seconds) between SIGALRM and SIGKILL for timed-out workers.
    process_timeout_grace_period: float = 5.0


@dataclass(frozen=True, slots=True)
class TaskRoute:
    """Immutable route descriptor for a registered task handler.

    Attributes:
        handler: The callable registered for this method name.
        mode: Execution strategy.
        time_limit: Max wall-clock seconds the handler may run.
            None means unlimited.
        grace_period: Seconds after SIGALRM before SIGKILL in PROCESS mode.
            None defers to ConsumerSettings.process_timeout_grace_period.
    """

    # The callable to invoke for this method name.
    handler: Callable[..., Any]
    # Execution strategy.
    mode: ExecutionMode
    # Maximum allowed execution time in seconds; None means unlimited.
    time_limit: float | None = None
    # Grace period override for PROCESS mode; None uses global setting.
    grace_period: float | None = None


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

    # Numeric error code (use :class:`~qanat.enums.JsonRpcError` constants).
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
