from __future__ import annotations

import logging
import threading
from contextvars import ContextVar
from typing import TYPE_CHECKING

from qanat.exceptions import HandlerTimeout

if TYPE_CHECKING:
    from qanat.message import BrokerMessage
    from qanat.models import TaskRoute
    from qanat.types import Seconds


class QanatContext:
    """"""

    def __init__(self) -> None:
        self.broker_message_id: str | None = None
        self.msg: BrokerMessage | None = None
        self.route: TaskRoute | None = None
        self.max_attempts: int | None = None
        self.broker_latency: Seconds | None = None
        self.internal_latency: Seconds | None = None

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


#: Active QanatContext for the current handler invocation
_qanat_context: ContextVar[QanatContext | None] = ContextVar(
    "_qanat_context",
    default=None,
)


def get_qanat_context() -> QanatContext:
    """Return the active QanatContext for the current handler invocation.

    Returns:
        The QanatContext for the running handler invocation.
    """
    ctx = _qanat_context.get()
    if ctx is None:
        ctx = QanatContext()
        _qanat_context.set(ctx)
    return ctx


class QanatContextFilter(logging.Filter):
    """
    A filter that extracts data from contextvars
    and automatically attaches it to any log entry.
    """

    def __init__(self, default_value="-"):
        super().__init__()
        self.default_value = default_value

    def filter(self, record: logging.LogRecord) -> bool:
        ctx = get_qanat_context()

        if (msg := ctx.msg) is not None:
            record.queue_name = msg.queue_name
            record.broker_message_id = msg.broker_message_id
            if (correlation_id := msg.correlation_id) is not None:
                record.correlation_id = correlation_id
            else:
                record.correlation_id = self.default_value
            record.method = msg.body.method
            record.attempt = msg.delivery_count
            record.reply_to = msg.reply_to or self.default_value
            record.qanat_headers = msg.headers
        else:
            record.queue_name = self.default_value
            record.broker_message_id = self.default_value
            record.correlation_id = self.default_value
            record.method = self.default_value
            record.attempt = self.default_value
            record.reply_to = self.default_value
            record.qanat_headers = {}

        if (route := ctx.route) is not None:
            record.handler = route.handler.__qualname__
            record.execution_mode = route.mode.value
            record.time_limit = (
                route.time_limit
                if route.time_limit is not None
                else self.default_value
            )
        else:
            record.handler = self.default_value
            record.execution_mode = self.default_value
            record.time_limit = self.default_value

        record.max_attempts = (
            ctx.max_attempts
            if ctx.max_attempts is not None
            else self.default_value
        )
        record.broker_latency_sec = (
            ctx.broker_latency
            if ctx.broker_latency is not None
            else self.default_value
        )
        record.internal_latency_sec = (
            ctx.internal_latency
            if ctx.internal_latency is not None
            else self.default_value
        )

        return True
