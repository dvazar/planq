"""agnosticq — transport-agnostic async task queue for Python 3.12+.

Public API
----------

Core classes:

- :class:`~agnosticq.consumer.AgnosticConsumer` — main consumer; register
  handlers with :meth:`~agnosticq.consumer.AgnosticConsumer.task` and start
  with :meth:`~agnosticq.consumer.AgnosticConsumer.run`.
- :class:`~agnosticq.base.BaseBroker` — base class for broker providers.
- :class:`~agnosticq.message.BrokerMessage` — base class for message wrappers.

Configuration:

- :class:`~agnosticq.models.ConsumerSettings` — concurrency and retry tuning.

JSON-RPC models:

- :class:`~agnosticq.models.JsonRpcRequest`
- :class:`~agnosticq.models.JsonRpcResponse`
- :class:`~agnosticq.models.JsonRpcErrorDetail`

Enumerations:

- :class:`~agnosticq.enums.ExecutionMode` — ASYNC / THREAD / PROCESS.
- :class:`~agnosticq.enums.Header` — standard broker header names.
- :class:`~agnosticq.enums.JsonRpcError` — standard JSON-RPC error codes.

Middleware:

- :class:`~agnosticq.middleware.Middleware` — base class for lifecycle hooks.
- :class:`~agnosticq.middleware.SkipMessage` — abort message processing.
- :class:`~agnosticq.middleware.TtlMiddleware` — built-in TTL enforcement.
- :class:`~agnosticq.middleware.MaxRetriesMiddleware` — built-in retry cap.
"""

import logging

from agnosticq.base import BaseBroker
from agnosticq.consumer import AgnosticConsumer
from agnosticq.enums import ExecutionMode, Header, JsonRpcError
from agnosticq.message import BrokerMessage
from agnosticq.middleware import (
    MaxRetriesMiddleware,
    Middleware,
    SkipMessage,
    TtlMiddleware,
)
from agnosticq.models import (
    ConsumerSettings,
    JsonRpcErrorDetail,
    JsonRpcRequest,
    JsonRpcResponse,
)

# Prevent "No handlers found" warning if user doesn't configure logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    "AgnosticConsumer",
    "BaseBroker",
    "BrokerMessage",
    "ConsumerSettings",
    "ExecutionMode",
    "Header",
    "JsonRpcError",
    "JsonRpcErrorDetail",
    "JsonRpcRequest",
    "JsonRpcResponse",
    "MaxRetriesMiddleware",
    "Middleware",
    "SkipMessage",
    "TtlMiddleware",
]
