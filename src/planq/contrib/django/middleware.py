"""Django-specific middleware for PlanQ."""

from __future__ import annotations

from typing import TYPE_CHECKING

from django.db import close_old_connections

from planq.middleware import Middleware

if TYPE_CHECKING:
    from planq.context import PlanqContext


class DjangoDbMiddleware(Middleware):
    """Close stale Django DB connections around each task handler.

    Uses :meth:`before_execute` / :meth:`after_execute` so that
    ``close_old_connections()`` runs in the handler's execution
    context — the same thread where Django ORM operations happen.

    Automatically prepended to the middleware list by
    :func:`~planq.contrib.django.setup.get_planq_middlewares`.
    """

    def before_execute(self, ctx: PlanqContext) -> None:
        """Close stale DB connections before handler execution.

        Args:
            ctx: The execution context for the current handler
                invocation.
        """
        close_old_connections()

    def after_execute(self, ctx: PlanqContext) -> None:
        """Close stale DB connections after handler execution.

        Args:
            ctx: The execution context for the current handler
                invocation.
        """
        close_old_connections()
