"""Pluggable middleware system for QanatConsumer lifecycle hooks."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from qanat.enums import Header

if TYPE_CHECKING:
    from qanat.consumer import QanatConsumer
    from qanat.message import BrokerMessage
    from qanat.models import JsonRpcResponse
    from qanat.types import Headers, Seconds

logger = logging.getLogger(__name__)


class SkipMessage(Exception):
    """Raised by middleware to abort processing of the current message.

    Middleware MUST call ``msg.reject()`` or ``msg.nack(delay)`` before
    raising this exception. Failing to do so will cause the message to
    remain invisible until its SQS visibility timeout expires, after
    which it will be redelivered.

    Example::

        async def before_process_message(self, consumer, msg):
            if should_skip(msg):
                await msg.reject()
                raise SkipMessage()
    """


class Middleware:
    """Base class for QanatConsumer lifecycle hooks.

    All hook methods are no-ops by default. Subclass and override only
    the hooks you need. Hooks are called in registration order for
    ``before_*`` hooks; ``after_*`` hooks use the same order (not reversed).

    Hooks and their contracts:

    - :meth:`before_process_message`: Called before routing and execution.
      May mutate ``msg.body.params`` and ``msg.headers`` in-place. May raise
      :class:`SkipMessage` (after calling ``msg.reject()``/``msg.nack()``)
      to abort the pipeline.
    - :meth:`after_process_message`: Called after execution (success or
      failure), before ack/nack. Exceptions are caught and logged; processing
      continues.
    - :meth:`after_skip_message`: Called when a :class:`SkipMessage` was
      raised during ``before_process_message``. Exceptions are caught and
      logged.
    - :meth:`before_publish_response`: Called in the request/response flow
      just before publishing the reply. Mutate ``headers`` in-place to attach
      custom SQS ``MessageAttributes`` to the response. Exceptions are caught
      and logged; publishing continues without the failed middleware's headers.
    """

    async def before_process_message(
        self,
        consumer: QanatConsumer,
        msg: BrokerMessage,
    ) -> None:
        """Called before routing and handler execution.

        Args:
            consumer: The :class:`~qanat.consumer.QanatConsumer`
                instance processing the message.
            msg: The incoming message. Headers and ``body.params`` may be
                mutated in-place.
        """

    async def after_process_message(
        self,
        consumer: QanatConsumer,
        msg: BrokerMessage,
        *,
        result: Any = None,
        exception: Exception | None = None,
    ) -> None:
        """Called after handler execution, before ack/nack.

        Invoked regardless of whether the handler succeeded or failed.
        Exceptions raised here are caught and logged; processing continues.

        Args:
            consumer: The :class:`~qanat.consumer.QanatConsumer`
                instance processing the message.
            msg: The message that was processed.
            result: The return value of the handler, or ``None`` on failure.
            exception: The exception raised by the handler, or ``None`` on
                success.
        """

    async def after_skip_message(
        self,
        consumer: QanatConsumer,
        msg: BrokerMessage,
    ) -> None:
        """Called when a middleware raised :class:`SkipMessage`.

        Exceptions raised here are caught and logged.

        Args:
            consumer: The :class:`~qanat.consumer.QanatConsumer`
                instance processing the message.
            msg: The message that was skipped.
        """

    async def before_publish_response(
        self,
        consumer: QanatConsumer,
        msg: BrokerMessage,
        response: JsonRpcResponse,
        headers: Headers,
    ) -> None:
        """Called before publishing the JSON-RPC response.

        Only fired in the request/response flow — when the incoming message
        has both a ``correlation_id`` and a ``reply_to`` queue. Mutate
        ``headers`` in-place to attach custom ``MessageAttributes`` to the
        response message.

        Exceptions raised here are caught and logged; publishing continues.

        Args:
            consumer: The :class:`~qanat.consumer.QanatConsumer`
                instance processing the message.
            msg: The original request message.
            response: The :class:`~qanat.models.JsonRpcResponse` about
                to be published.
            headers: Mutable ``dict[str, str]`` to populate with custom
                SQS ``MessageAttributes`` (all values treated as ``String``
                type). Do not shadow reserved keys: ``ReplyTo``,
                ``MaxRetries``, ``ExpireAt``.
        """


class DeadlineMiddleware(Middleware):
    """Rejects messages whose deadline has expired before processing begins.

    Reads the ``x-expire-at`` header (Unix timestamp). If the current time
    exceeds that value (plus optional clock drift tolerance), the message is
    rejected and :class:`SkipMessage` is raised, preventing handler execution.

    The ``leeway`` parameter provides tolerance for clock drift between
    publisher and consumer nodes. For example, with ``leeway=2.0``, a message
    with ``expire_at=12:00:10`` would be accepted at consumer time ``12:00:11``
    (1 second past deadline, but within 2-second tolerance).

    Attributes:
        leeway: Clock drift tolerance in seconds. Messages are rejected only
            if ``time.time() > expire_at + leeway``. Defaults to 0.0 (strict
            deadline enforcement).
    """

    def __init__(self, leeway: Seconds = 0.0):
        """Initialize DeadlineMiddleware with clock drift tolerance.

        Args:
            leeway: Clock drift tolerance in seconds (non-negative). Allows
                messages slightly past their deadline to be processed if
                within tolerance. Defaults to 0.0 (no tolerance).

        Raises:
            ValueError: If leeway is negative.
        """
        if leeway < 0:
            raise ValueError("leeway must be non-negative")

        if leeway > 60:
            logger.warning(
                f"{type(self).__name__} configured with leeway=%.1fs (>60s). "
                "This is unusually large and may indicate misconfiguration.",
                leeway,
            )

        self.leeway = leeway

    async def before_process_message(
        self,
        consumer: QanatConsumer,
        msg: BrokerMessage,
    ) -> None:
        """Reject the message if its deadline has expired.

        Args:
            consumer: The consumer instance (unused).
            msg: The incoming message to inspect.

        Raises:
            SkipMessage: After calling ``msg.reject()`` when the message
                deadline has expired (accounting for configured leeway).
        """
        expire_at = msg.headers.get(Header.EXPIRE_AT)
        if (
            expire_at is not None
            and time.time() > float(expire_at) + self.leeway
        ):
            await msg.reject()
            raise SkipMessage()
