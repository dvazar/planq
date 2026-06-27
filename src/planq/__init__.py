"""planq — transport-agnostic async task queue for Python 3.12+.

Public API
----------

Core classes:

- :class:`~planq.app.Planq` — primary entry point; binds a broker to
  a task registry. Register handlers with :meth:`~planq.app.Planq.task`
  and publish via :meth:`~planq.app.PlanqTask.send`.
- :class:`~planq.app.PlanqTask` — callable wrapper returned by
  ``@app.task()``; adds ``.send()`` for publishing and
  ``.options()`` for transport configuration.
- :class:`~planq.app.TaskSender` — builder returned by
  ``PlanqTask.options()``; holds transport options with a typed
  ``.send()``.
- :class:`~planq.consumer.PlanqConsumer` — runs the consumer loop;
  accepts a :class:`Planq` app and calls
  :meth:`~planq.consumer.PlanqConsumer.run` to start processing.
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

Tracing:

- :class:`~planq.tracing.TraceContext` — immutable W3C trace context.
- :func:`~planq.tracing.parse_traceparent_and_generate_span` — parse
  a ``traceparent`` header and generate a child span.

Control flow exceptions:

- :class:`~planq.exceptions.RetryMessage` — signal transport to nack.
- :class:`~planq.exceptions.RejectMessage` — signal transport to reject.
"""

from planq.app import Planq, PlanqTask, SyncPlanq, TaskSender
from planq.broker import BaseBroker
from planq.consumer import PlanqConsumer
from planq.context import PlanqContext, get_planq_context
from planq.enums import ExecutionMode, Header, JsonRpcError, LogEvent
from planq.exceptions import (
    FeatureNotSupportedError,
    HandlerCancelled,
    HandlerTimeout,
    InvalidParamsError,
    ProcessShutdown,
    RejectMessage,
    Retry,
    RetryMessage,
    Shutdown,
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
from planq.providers.memory import InMemoryBroker, InMemoryMessage
from planq.tracing import TraceContext, parse_traceparent_and_generate_span

__all__ = [
    "Planq",
    "PlanqTask",
    "SyncPlanq",
    "TaskSender",
    "Middleware",
    "BaseBroker",
    "BrokerMessage",
    "ConsumerSettings",
    "DeadlineMiddleware",
    "ExecutionMode",
    "FeatureNotSupportedError",
    "HandlerCancelled",
    "HandlerTimeout",
    "Header",
    "InMemoryBroker",
    "InMemoryMessage",
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
    "Shutdown",
    "PlanqContext",
    "TaskResult",
    "TaskRoute",
    "TraceContext",
    "get_planq_context",
    "get_planq_logger",
    "instrument_logging",
    "parse_traceparent_and_generate_span",
]
