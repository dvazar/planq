"""Onion-style middleware system for PlanqConsumer.

Each middleware wraps the next stage of the pipeline via a single
``__call__(self, msg, call_next)`` entry point. Middleware can
pre-process (before ``call_next``), post-process (after), or
short-circuit (return early / raise control-flow exceptions).
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import TYPE_CHECKING, Callable

from planq.enums import Header, JsonRpcError, LogEvent
from planq.log import get_planq_logger
from planq.models import JsonRpcErrorDetail, JsonRpcResponse

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from planq.message import BrokerMessage
    from planq.types import Seconds

    type CallNext = Callable[[BrokerMessage], Awaitable[JsonRpcResponse | None]]

logger = get_planq_logger(__name__)


class Middleware:
    """Base class for onion-style middleware.

    The default ``__call__`` delegates to ``call_next`` unchanged.
    Subclass and override to add pre-processing, post-processing,
    or short-circuit logic. Middleware may mutate ``msg.headers``
    and ``msg.body.params`` in-place before calling ``call_next``,
    and may inspect or enrich the returned response afterwards.
    """

    async def __call__(
        self,
        msg: BrokerMessage,
        call_next: CallNext,
    ) -> JsonRpcResponse | None:
        """Process the message and delegate to the next pipeline stage.

        Args:
            msg: The incoming broker message.
            call_next: Awaitable that invokes the next middleware
                or the terminal router endpoint.

        Returns:
            The JSON-RPC response (for requests) or ``None``
            (for notifications or skipped messages).
        """
        return await call_next(msg)


class DeadlineMiddleware(Middleware):
    """Drops messages whose deadline has expired before processing.

    Reads the ``x-expire-at`` header (Unix timestamp). If the current
    time exceeds that value (plus optional clock drift tolerance), the
    message is short-circuited:

    - **Request** (has ``correlation_id``): returns a
      ``JsonRpcResponse`` with error code ``-32001``.
    - **Notification** (no ``correlation_id``): returns ``None``
      (silently dropped).

    Attributes:
        leeway: Clock drift tolerance in seconds. Messages are
            rejected only if ``time.time() > expire_at + leeway``.
            Defaults to 0.0 (strict deadline enforcement).
    """

    def __init__(self, leeway: Seconds = 0.0):
        """Initialize DeadlineMiddleware with clock drift tolerance.

        Args:
            leeway: Clock drift tolerance in seconds (non-negative).
                Allows messages slightly past their deadline to be
                processed if within tolerance. Defaults to 0.0
                (no tolerance).

        Raises:
            ValueError: If leeway is negative.
        """
        if leeway < 0:
            raise ValueError("leeway must be non-negative")

        if leeway > 60:
            logger.warning(
                "DeadlineMiddleware configured with leeway=%.1fs (>60s),"
                " may indicate misconfiguration",
                leeway,
                extra={
                    "event": LogEvent.DEADLINE_LEEWAY_WARNING,
                    "leeway_seconds": leeway,
                },
            )

        self.leeway = leeway

    async def __call__(
        self,
        msg: BrokerMessage,
        call_next: CallNext,
    ) -> JsonRpcResponse | None:
        """Drop the message if its deadline has expired.

        Args:
            msg: The incoming message to inspect.
            call_next: The next pipeline stage.

        Returns:
            ``JsonRpcResponse`` with error for expired requests,
            ``None`` for expired notifications, or the result of
            ``call_next`` for non-expired messages.
        """
        expire_at = msg.headers.get(Header.EXPIRE_AT)
        if (
            expire_at is not None
            and time.time() > float(expire_at) + self.leeway
        ):
            expire_at_iso = datetime.fromtimestamp(float(expire_at)).isoformat()
            log_ctx = {
                "event": LogEvent.MESSAGE_DEADLINE_EXCEEDED,
                "expire_at_iso": expire_at_iso,
                "expire_at_seconds": expire_at,
                "leeway_seconds": self.leeway,
            }
            logger.warning(
                "Message deadline exceeded before processing could begin."
                " Message ID: %(message_id)s, Expire at: %(expire_at_iso)s,"
                " Leeway: %(leeway_seconds).1fs",
                log_ctx,
                extra=log_ctx,
            )
            if msg.correlation_id is not None:
                return JsonRpcResponse(
                    id=msg.correlation_id,
                    error=JsonRpcErrorDetail(
                        code=JsonRpcError.DEADLINE_EXCEEDED,
                        message="Message deadline exceeded",
                    ),
                )
            return None

        return await call_next(msg)
