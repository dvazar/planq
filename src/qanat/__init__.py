"""qanat — transport-agnostic async task queue for Python 3.12+.

Public API
----------

Core classes:

- :class:`~qanat.consumer.QanatConsumer` — main consumer; register
  handlers with :meth:`~qanat.consumer.QanatConsumer.task` and start
  with :meth:`~qanat.consumer.QanatConsumer.run`.
- :class:`~qanat.base.BaseBroker` — base class for broker providers.
- :class:`~qanat.message.BrokerMessage` — base class for message wrappers.

Configuration:

- :class:`~qanat.models.ConsumerSettings` — concurrency and retry tuning.

JSON-RPC models:

- :class:`~qanat.models.JsonRpcRequest`
- :class:`~qanat.models.JsonRpcResponse`
- :class:`~qanat.models.JsonRpcErrorDetail`
- :class:`~qanat.models.TaskResult` — optional handler return wrapper
  with transport headers.

Enumerations:

- :class:`~qanat.enums.ExecutionMode` — ASYNC / THREAD / PROCESS.
- :class:`~qanat.enums.Header` — standard broker header names.
- :class:`~qanat.enums.JsonRpcError` — standard JSON-RPC error codes.

Middleware:

- :class:`~qanat.middleware.Middleware` — base class for onion-style
  middleware with a single ``__call__(msg, call_next)`` entry point.
- :class:`~qanat.middleware.DeadlineMiddleware` — built-in deadline
  enforcement with clock drift tolerance.

Control flow exceptions:

- :class:`~qanat.exceptions.RetryMessage` — signal transport to nack.
- :class:`~qanat.exceptions.RejectMessage` — signal transport to reject.
"""

from qanat.broker import BaseBroker
from qanat.consumer import QanatConsumer
from qanat.context import QanatContext, get_qanat_context
from qanat.enums import ExecutionMode, Header, JsonRpcError
from qanat.exceptions import (
    FeatureNotSupportedError,
    HandlerTimeout,
    ProcessShutdown,
    RejectMessage,
    Retry,
    RetryMessage,
)
from qanat.log import instrument_logging
from qanat.message import BrokerMessage
from qanat.middleware import (
    DeadlineMiddleware,
    Middleware,
)
from qanat.models import (
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
    "JsonRpcError",
    "JsonRpcErrorDetail",
    "JsonRpcRequest",
    "JsonRpcResponse",
    "ProcessShutdown",
    "QanatConsumer",
    "RejectMessage",
    "Retry",
    "RetryMessage",
    "QanatContext",
    "TaskResult",
    "TaskRoute",
    "get_qanat_context",
    "instrument_logging",
]
