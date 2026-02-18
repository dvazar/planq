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

Enumerations:

- :class:`~qanat.enums.ExecutionMode` — ASYNC / THREAD / PROCESS.
- :class:`~qanat.enums.Header` — standard broker header names.
- :class:`~qanat.enums.JsonRpcError` — standard JSON-RPC error codes.

Middleware:

- :class:`~qanat.middleware.Middleware` — base class for lifecycle hooks.
- :class:`~qanat.middleware.SkipMessage` — abort message processing.
- :class:`~qanat.middleware.TtlMiddleware` — built-in TTL enforcement.
- :class:`~qanat.middleware.MaxRetriesMiddleware` — built-in retry cap.
"""

import logging

from qanat.base import BaseBroker
from qanat.consumer import QanatConsumer
from qanat.context import TaskContext, current_task_ctx, get_task_context
from qanat.enums import ExecutionMode, Header, JsonRpcError
from qanat.exceptions import (
    FeatureNotSupportedError,
    HandlerTimeout,
    ProcessShutdown,
)
from qanat.message import BrokerMessage
from qanat.middleware import (
    MaxRetriesMiddleware,
    Middleware,
    SkipMessage,
    TtlMiddleware,
)
from qanat.models import (
    ConsumerSettings,
    JsonRpcErrorDetail,
    JsonRpcRequest,
    JsonRpcResponse,
    TaskRoute,
)

# Prevent "No handlers found" warning if user doesn't configure logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    "QanatConsumer",
    "BaseBroker",
    "BrokerMessage",
    "ConsumerSettings",
    "ExecutionMode",
    "FeatureNotSupportedError",
    "HandlerTimeout",
    "Header",
    "JsonRpcError",
    "JsonRpcErrorDetail",
    "JsonRpcRequest",
    "JsonRpcResponse",
    "MaxRetriesMiddleware",
    "Middleware",
    "ProcessShutdown",
    "SkipMessage",
    "TaskContext",
    "TaskRoute",
    "TtlMiddleware",
    "current_task_ctx",
    "get_task_context",
]
