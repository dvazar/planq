"""Pydantic data models for JSON-RPC 2.0 messages and consumer configuration."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any, Callable

from pydantic import BaseModel, ConfigDict, Field, field_validator

if TYPE_CHECKING:
    from qanat.enums import ExecutionMode
    from qanat.types import JsonRpcId, JsonRpcParams, JsonRpcVersion


class ConsumerSettings(BaseModel):
    """Immutable runtime settings for ``QanatConsumer``."""

    model_config = ConfigDict(frozen=True, strict=True)

    # Maximum number of messages processed concurrently.
    # Must be > 0.
    concurrency: int = 10

    # Maximum retries for routes without explicit max_retries.
    # None defers to DEFAULT_MAX_RETRIES.
    # Zero means one attempt (initial delivery only, no retries).
    max_retries: int | None = None

    # Initial backoff delay in seconds; doubles with each retry attempt.
    # Must be > 0.
    retry_base_delay: float = 1.0

    # Maximum backoff delay in seconds; caps exponential growth.
    # Must be > 0.
    retry_max_delay: float = 300.0

    # Grace period (seconds) between SIGALRM and SIGKILL for timed-out workers.
    # Must be > 0.
    process_timeout_grace_period: float = 5.0

    @field_validator("concurrency")
    @classmethod
    def validate_concurrency(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("concurrency must be positive")
        return v

    @field_validator("max_retries")
    @classmethod
    def validate_max_retries(cls, v: int | None) -> int | None:
        if v is not None and v < 0:
            raise ValueError(
                "max_retries must be non-negative "
                "(0 = one attempt with no retries, "
                "None = use DEFAULT_MAX_RETRIES)"
            )
        return v

    @field_validator(
        "retry_base_delay",
        "retry_max_delay",
        "process_timeout_grace_period",
    )
    @classmethod
    def validate_positive_float(cls, v: float, info) -> float:
        if math.isnan(v):
            raise ValueError(f"{info.field_name} cannot be NaN")
        if math.isinf(v):
            raise ValueError(f"{info.field_name} cannot be infinite")
        if v <= 0:
            raise ValueError(f"{info.field_name} must be positive")
        return v


class TaskRoute(BaseModel):
    """Immutable route descriptor for a registered task handler.

    Attributes:
        handler: The callable registered for this method name.
        mode: Execution strategy.
        time_limit: Max wall-clock seconds the handler may run.
            None means unlimited.
        grace_period: Seconds after SIGALRM before SIGKILL in PROCESS mode.
            None defers to ConsumerSettings.process_timeout_grace_period.
    """

    model_config = ConfigDict(
        frozen=True, arbitrary_types_allowed=True, strict=True
    )

    # The callable to invoke for this method name.
    handler: Callable[..., Any]

    # Execution strategy.
    mode: ExecutionMode

    # Maximum delivery attempts for this handler.
    # None defers to ConsumerSettings.max_retries or DEFAULT_MAX_RETRIES.
    # Zero means one attempt (initial delivery only, no retries).
    max_retries: int | None = None

    # Maximum allowed execution time in seconds.
    # None means unlimited.
    time_limit: float | None = None

    # Grace period override for PROCESS mode timeout handling.
    # None uses ConsumerSettings.process_timeout_grace_period.
    grace_period: float | None = None

    @field_validator("max_retries")
    @classmethod
    def validate_max_retries(cls, v: int | None) -> int | None:
        if v is not None and v < 0:
            raise ValueError(
                "max_retries must be non-negative "
                "(0 = one attempt with no retries, None = use consumer default)"
            )
        return v

    @field_validator("time_limit", "grace_period")
    @classmethod
    def validate_positive_optional_float(
        cls, v: float | None, info
    ) -> float | None:
        if v is not None:
            if math.isnan(v):
                raise ValueError(f"{info.field_name} cannot be NaN")
            if math.isinf(v):
                raise ValueError(
                    f"{info.field_name} cannot be infinite "
                    "(use None for unlimited)"
                )
            if v <= 0:
                raise ValueError(
                    f"{info.field_name} must be positive when specified"
                )
        return v


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

    @field_validator("id")
    @classmethod
    def validate_empty_string(cls, v: JsonRpcId) -> JsonRpcId:
        if v == "":
            return None
        return v


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

    # Transport-only headers enriched by middleware and handlers.
    # Excluded from JSON serialization (model_dump / model_dump_json).
    headers: dict[str, str] = Field(default_factory=dict, exclude=True)


class TaskResult:
    """Optional wrapper for handler return values with headers.

    Handlers return ``TaskResult`` instead of a plain value when
    they need to attach custom transport headers to the outgoing
    response (e.g. ``x-rate-limit``, ``x-trace-id``).

    Example::

        @consumer.task("my.method")
        async def handle(name: str) -> TaskResult:
            return TaskResult(
                result={"greeting": f"Hi {name}"},
                headers={"x-rate-limit": "100"},
            )
    """

    __slots__ = ("result", "headers")

    def __init__(
        self,
        result: Any,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Initialize with a result value and optional headers.

        Args:
            result: The handler's return value.
            headers: Optional transport headers to attach to the
                outgoing response message.
        """
        self.result = result
        self.headers = headers or {}
