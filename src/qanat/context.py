"""Task context API for cooperative cancellation in THREAD-mode handlers."""

from __future__ import annotations

import threading
from contextvars import ContextVar

from qanat.exceptions import HandlerTimeout


class TaskContext:
    """Cooperative cancellation token passed to THREAD-mode handlers.

    Attributes:
        _stop_event: Internal event set on cancellation.
    """

    def __init__(self) -> None:
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


#: Active task context for the current THREAD-mode execution.
#: None when not inside a THREAD-mode handler with a time_limit.
current_task_ctx: ContextVar[TaskContext | None] = ContextVar(
    "current_task_ctx",
    default=None,
)


def get_task_context() -> TaskContext:
    """Return the active TaskContext for the current thread.

    Returns:
        The TaskContext for the running handler invocation.
    """
    ctx = current_task_ctx.get()
    if ctx is None:
        ctx = TaskContext()
        current_task_ctx.set(ctx)
    return ctx
