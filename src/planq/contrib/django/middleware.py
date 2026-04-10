"""Django-specific middleware for PlanQ."""

from __future__ import annotations

from typing import TYPE_CHECKING

from django.db import close_old_connections

from planq.middleware import Middleware

if TYPE_CHECKING:
    from planq.message import BrokerMessage
    from planq.middleware import CallNext
    from planq.models import JsonRpcResponse


class DjangoDbMiddleware(Middleware):
    """Close stale Django DB connections before and after each task.

    Automatically prepended to the middleware list by
    :func:`~planq.contrib.django.setup.get_planq_middlewares`.
    """

    async def __call__(
        self,
        msg: BrokerMessage,
        call_next: CallNext,
    ) -> JsonRpcResponse | None:
        """Close stale connections around task execution.

        Args:
            msg: The incoming broker message.
            call_next: Awaitable that invokes the next middleware
                or the terminal router endpoint.

        Returns:
            The JSON-RPC response (for requests) or ``None``
            (for notifications or skipped messages).
        """
        close_old_connections()
        try:
            return await call_next(msg)
        finally:
            close_old_connections()
