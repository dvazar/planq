"""planq — transport-agnostic async task queue for Python 3.12+.

Public API
----------

Core classes:

- :class:`~planq.consumer.PlanqConsumer` — main consumer; register
  handlers with :meth:`~planq.consumer.PlanqConsumer.task` and start
  with :meth:`~planq.consumer.PlanqConsumer.run`.
- :class:`~planq.base.BaseBroker` — base class for broker providers.
- :class:`~planq.message.BrokerMessage` — base class for message wrappers.

Configuration:

- :class:`~planq.models.ConsumerSettings` — concurrency and retry tuning.

JSON-RPC models:

- :class:`~planq.models.JsonRpcRequest`
- :class:`~planq.models.JsonRpcResponse`
- :class:`~planq.models.JsonRpcErrorDetail`
- :class:`~planq.models.TaskResult` — optional handler return wrapper
  with transport headers.

Enumerations:

- :class:`~planq.enums.ExecutionMode` — ASYNC / THREAD / PROCESS.
- :class:`~planq.enums.Header` — standard broker header names.
- :class:`~planq.enums.JsonRpcError` — standard JSON-RPC error codes.

Middleware:

- :class:`~planq.middleware.Middleware` — base class for onion-style
  middleware with a single ``__call__(msg, call_next)`` entry point.
- :class:`~planq.middleware.DeadlineMiddleware` — built-in deadline
  enforcement with clock drift tolerance.

Control flow exceptions:

- :class:`~planq.exceptions.RetryMessage` — signal transport to nack.
- :class:`~planq.exceptions.RejectMessage` — signal transport to reject.
"""

from planq.broker import BaseBroker
from planq.consumer import PlanqConsumer
from planq.context import PlanqContext, get_planq_context
from planq.enums import ExecutionMode, Header, JsonRpcError, LogEvent
from planq.exceptions import (
    FeatureNotSupportedError,
    HandlerTimeout,
    InvalidParamsError,
    ProcessShutdown,
    RejectMessage,
    Retry,
    RetryMessage,
)
from planq.log import get_planq_logger, instrument_logging
from planq.message import BrokerMessage
from planq.middleware import (
    DeadlineMiddleware,
    Middleware,
)
from planq.models import (
    ConsumerSettings,
    JsonRpcErrorDetail,
    JsonRpcRequest,
    JsonRpcResponse,
    TaskResult,
    TaskRoute,
)

__all__ = [
    "Middleware",
    "BaseBroker",
    "BrokerMessage",
    "ConsumerSettings",
    "DeadlineMiddleware",
    "ExecutionMode",
    "FeatureNotSupportedError",
    "HandlerTimeout",
    "Header",
    "InvalidParamsError",
    "JsonRpcError",
    "LogEvent",
    "JsonRpcErrorDetail",
    "JsonRpcRequest",
    "JsonRpcResponse",
    "ProcessShutdown",
    "PlanqConsumer",
    "RejectMessage",
    "Retry",
    "RetryMessage",
    "PlanqContext",
    "TaskResult",
    "TaskRoute",
    "get_planq_context",
    "get_planq_logger",
    "instrument_logging",
]
