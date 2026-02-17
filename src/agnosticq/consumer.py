"""Core message consumer implementing the smart-consumer architecture."""

from __future__ import annotations

import asyncio
import logging
import signal
import time
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from random import uniform
from typing import TYPE_CHECKING, Any, Callable

from agnosticq.base import BaseBroker
from agnosticq.enums import ExecutionMode, Header, JsonRpcError
from agnosticq.models import (
    ConsumerSettings,
    JsonRpcErrorDetail,
    JsonRpcResponse,
)

if TYPE_CHECKING:
    from agnosticq.message import BrokerMessage
    from agnosticq.types import Seconds

logger = logging.getLogger(__name__)


class AgnosticConsumer:
    """Transport-agnostic async task queue consumer.

    Processes messages through a fixed middleware pipeline:
    TTL check → retry check → routing → execution → ack/nack.

    Register handlers with the :meth:`task` decorator, then call
    :meth:`run` to start consuming. Handles SIGINT/SIGTERM for
    graceful shutdown.

    Attributes:
        broker: The broker instance used to publish and consume messages.
        routes: Mapping of method name to ``(handler, ExecutionMode)`` pair.
    """

    def __init__(
        self,
        broker: BaseBroker,
        process_workers: int | None = None,
        settings: ConsumerSettings | None = None,
    ) -> None:
        """Initialize the consumer with a broker and optional settings.

        Args:
            broker: Connected (or lazy) broker instance.
            process_workers: Number of worker processes for
                ``ExecutionMode.PROCESS`` tasks. ``None`` disables
                process-pool execution.
            settings: Runtime tuning parameters. Defaults to
                :class:`~agnosticq.models.ConsumerSettings` with default
                values if not provided.
        """
        self.broker = broker
        self.routes: dict[str, tuple[Callable[..., Any], ExecutionMode]] = {}
        self._pool: ProcessPoolExecutor | None = (
            ProcessPoolExecutor(max_workers=process_workers)
            if process_workers
            else None
        )
        self._settings = (
            settings if settings is not None else ConsumerSettings()
        )

    def task(
        self,
        name: str,
        mode: ExecutionMode = ExecutionMode.ASYNC,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a callable as the handler for a named JSON-RPC method.

        Usage::

            @consumer.task("my.method", mode=ExecutionMode.THREAD)
            def handle(name: str, greeting: str = "Hi"):
                ...

        Args:
            name: JSON-RPC method name that routes to this handler.
            mode: Execution strategy for the handler. Defaults to
                ``ExecutionMode.ASYNC``.

        Returns:
            A decorator that registers the wrapped function and returns
            it unchanged.
        """

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.routes[name] = (func, mode)
            return func

        return decorator

    def _calculate_backoff(self, delivery_count: int) -> Seconds:
        s = self._settings
        return min(
            s.retry_base_delay * 2 ** (delivery_count - 1)
            + uniform(0, s.retry_jitter),
            s.retry_max_delay,
        )

    async def _execute(
        self,
        handler: Callable[..., Any],
        mode: ExecutionMode,
        params: Any,
    ) -> Any:
        args: tuple[Any, ...] = ()
        kwargs: dict[str, Any] = {}
        if isinstance(params, list):
            args = tuple(params)
        elif isinstance(params, dict):
            kwargs = params

        match mode:
            case ExecutionMode.ASYNC:
                return await handler(*args, **kwargs)
            case ExecutionMode.THREAD:
                return await asyncio.to_thread(handler, *args, **kwargs)
            case ExecutionMode.PROCESS:
                if self._pool is None:
                    raise RuntimeError(
                        "ProcessPoolExecutor not configured; "
                        "set process_workers in AgnosticConsumer"
                    )
                loop = asyncio.get_running_loop()
                fn = partial(handler, *args, **kwargs)
                return await loop.run_in_executor(self._pool, fn)

    async def _process_message(self, msg: BrokerMessage) -> None:
        # 1. Check TTL
        expire_at = msg.headers.get(Header.EXPIRE_AT)
        if expire_at is not None and time.time() > float(expire_at):
            logger.debug("Message expired, rejecting: %s", msg.body.method)
            await msg.reject()
            return

        # 2. Check retries
        max_retries = msg.headers.get(Header.MAX_RETRIES)
        if max_retries is not None and msg.delivery_count > int(max_retries):
            logger.debug("Max retries exceeded, rejecting: %s", msg.body.method)
            await msg.reject()
            return

        # 3. Route
        route = self.routes.get(msg.body.method)
        if route is None:
            logger.warning("No route for method: %s", msg.body.method)
            await msg.reject()
            return

        handler, mode = route

        # 4. Execute handler
        payload: Any = None
        handler_exc: Exception | None = None
        try:
            payload = await self._execute(handler, mode, msg.body.params)
        except Exception as exc:
            handler_exc = exc

        # 5. Notification flow (no id → fire-and-forget)
        if msg.correlation_id is None:
            if handler_exc is not None:
                backoff = self._calculate_backoff(msg.delivery_count)
                logger.error(
                    "Handler %s failed, nacking with %.1fs delay",
                    msg.body.method,
                    backoff,
                    exc_info=handler_exc,
                )
                await msg.nack(backoff)
                return
            await msg.ack()
            return

        # 6. Request flow (has id → send response to reply_to)
        if msg.reply_to is None:
            logger.warning(
                "Request %s has id but no reply_to, acking without reply",
                msg.body.method,
            )
            await msg.ack()
            return

        if handler_exc is not None:
            response = JsonRpcResponse(
                id=msg.correlation_id,
                error=JsonRpcErrorDetail(
                    code=JsonRpcError.INTERNAL_ERROR,
                    message=str(handler_exc),
                ),
            )
        else:
            response = JsonRpcResponse(id=msg.correlation_id, result=payload)

        try:
            await self.broker.publish(msg.reply_to, response)
        except Exception:
            backoff = self._calculate_backoff(msg.delivery_count)
            logger.exception(
                "Failed to publish response for %s, nacking with %.1fs delay",
                msg.body.method,
                backoff,
            )
            await msg.nack(backoff)
            return

        await msg.ack()

    async def _guarded_process(
        self,
        msg: BrokerMessage,
        sem: asyncio.Semaphore,
    ) -> None:
        try:
            await self._process_message(msg)
        finally:
            sem.release()

    async def run(self, queue: str) -> None:
        """Start consuming messages from the given queue until shutdown.

        Installs SIGINT/SIGTERM handlers that trigger a clean drain of
        in-flight messages before exiting. Shuts down the process pool
        (if any) after the event loop exits.

        Args:
            queue: Source queue name or URL to consume from.
        """
        shutdown_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, shutdown_event.set)

        try:
            async with self.broker:
                async with asyncio.TaskGroup() as tg:
                    sem = asyncio.Semaphore(self._settings.concurrency)
                    async for msg in self.broker.consume(
                        queue,
                        prefetch=self._settings.concurrency,
                    ):
                        if shutdown_event.is_set():
                            break
                        await sem.acquire()
                        tg.create_task(self._guarded_process(msg, sem))
        finally:
            if self._pool is not None:
                self._pool.shutdown(wait=True)
