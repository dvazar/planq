"""Per-invocation execution context and structured logging filter."""

from __future__ import annotations

import logging
import os
import threading
from collections import ChainMap
from contextvars import ContextVar
from typing import TYPE_CHECKING

from planq.exceptions import HandlerTimeout

if TYPE_CHECKING:
    from planq.message import BrokerMessage
    from planq.models import TaskRoute
    from planq.tracing import TraceContext
    from planq.types import Seconds


class PlanqContext:
    """Execution context for a single task handler invocation.

    Provides access to message metadata, route configuration, and
    cancellation primitives for handlers running in THREAD or PROCESS
    execution modes.

    Attributes:
        trace: W3C Trace Context for the current invocation.
        msg: The BrokerMessage being processed.
        route: TaskRoute configuration for the current handler.
        max_attempts: Effective retry limit (1 + max_retries).
        broker_latency: Time between enqueue and receive (seconds).
        internal_latency: Time between receive and handler invocation
            (seconds).
        rpc_duration: Total duration of any RPC calls made by the
            handler (seconds).
        rpc_cpu: Total CPU time consumed by any RPC calls (seconds).
        pipeline_duration: Total duration of any child pipelines spawned
            by the handler (seconds).
        pipeline_cpu: Total CPU time consumed by any child pipelines
            spawned by the handler (seconds).
    """

    def __init__(self) -> None:
        """Initialize an empty context with no message or route bound."""
        self.trace: TraceContext | None = None
        self.msg: BrokerMessage | None = None
        self.route: TaskRoute | None = None
        self.max_attempts: int | None = None
        self.broker_latency: Seconds | None = None
        self.internal_latency: Seconds | None = None
        self.rpc_duration: Seconds | None = None
        self.rpc_cpu: Seconds | None = None
        self.pipeline_duration: Seconds | None = None
        self.pipeline_cpu: Seconds | None = None

        self._stop_event = threading.Event()

    @property
    def is_cancelled(self) -> bool:
        """True if cancellation has been requested."""
        return self._stop_event.is_set()

    def cancel(self) -> None:
        """Signal cancellation to the thread. Called by the library."""
        self._stop_event.set()

    def check_cancellation(self) -> None:
        """Raise HandlerTimeout if cancellation has been requested.

        Raises:
            HandlerTimeout: If cancel() has been called.
        """
        if self.is_cancelled:
            raise HandlerTimeout()


#: Active PlanqContext for the current handler invocation
_planq_context: ContextVar[PlanqContext | None] = ContextVar(
    "_planq_context",
    default=None,
)


def get_planq_context() -> PlanqContext:
    """Return the active PlanqContext for the current handler invocation.

    Returns:
        The PlanqContext for the running handler invocation.
    """
    ctx = _planq_context.get()
    if ctx is None:
        ctx = PlanqContext()
        _planq_context.set(ctx)
    return ctx


class PlanqContextFilter(logging.Filter):
    """Logging filter that injects PlanqContext fields into log records.

    Automatically extracts message metadata, route configuration, and
    latency metrics from the active PlanqContext and attaches them to
    every log record as attributes.

    Attributes:
        default_value: Placeholder string used when context fields are
            not available (e.g., outside handler execution).
    """

    def __init__(self, default_value: str | None = None) -> None:
        """Initialize with a placeholder for missing context fields.

        Args:
            default_value: Value used when a string context field is
                not available (e.g. outside handler execution).
                Defaults to ``None``. Numeric fields always default
                to ``None`` regardless of this setting.
        """
        super().__init__()
        self.default_value = default_value

    def filter(self, record: logging.LogRecord) -> bool:
        """Inject PlanqContext fields into the log record.

        Args:
            record: The log record to enrich with context attributes.

        Returns:
            Always ``True`` (never suppresses records).
        """
        record.process_id = os.getpid()
        record.thread_id = threading.get_ident()

        ctx = get_planq_context()

        if (trace := ctx.trace) is not None:
            record.trace_id = trace.trace_id
            record.span_id = trace.span_id
            record.parent_span_id = trace.parent_span_id

        if (msg := ctx.msg) is not None:
            record.queue_name = msg.queue_name
            record.message_id = msg.message_id
            if (correlation_id := msg.correlation_id) is not None:
                record.correlation_id = correlation_id
            else:
                record.correlation_id = self.default_value
            record.method = msg.body.method
            record.current_attempt = msg.delivery_count
            record.reply_to = msg.reply_to or self.default_value
            record.headers = msg.headers

        if (route := ctx.route) is not None:
            record.handler = route.handler.__qualname__
            record.execution_mode = route.mode.value
            record.time_limit_seconds = route.time_limit

        if ctx.max_attempts is not None:
            record.max_attempts = ctx.max_attempts

        if ctx.broker_latency is not None:
            record.broker_latency_seconds = ctx.broker_latency
            record.internal_latency_seconds = ctx.internal_latency

        if ctx.rpc_duration is not None:
            record.rpc_duration_seconds = ctx.rpc_duration
            record.rpc_cpu_seconds = ctx.rpc_cpu

        if ctx.pipeline_duration is not None:
            record.pipeline_duration_seconds = ctx.pipeline_duration
            record.pipeline_cpu_seconds = ctx.pipeline_cpu

        if isinstance(record.args, dict) and not isinstance(
            record.args, ChainMap
        ):
            record.args = ChainMap(record.args, vars(record))

        return True
