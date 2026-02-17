"""Core message consumer implementing the smart-consumer architecture."""

from __future__ import annotations

import asyncio
import logging
import signal
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from random import uniform
from typing import TYPE_CHECKING, Any, Callable

from agnosticq.base import BaseBroker
from agnosticq.enums import ExecutionMode, JsonRpcError
from agnosticq.middleware import (
    MaxRetriesMiddleware,
    SkipMessage,
    TtlMiddleware,
)
from agnosticq.models import (
    ConsumerSettings,
    JsonRpcErrorDetail,
    JsonRpcResponse,
)

if TYPE_CHECKING:
    from agnosticq.message import BrokerMessage
    from agnosticq.middleware import Middleware
    from agnosticq.types import Seconds

logger = logging.getLogger(__name__)


class AgnosticConsumer:
    """Transport-agnostic async task queue consumer.

    Processes messages through a pluggable middleware pipeline followed by
    routing and execution. Built-in middlewares handle TTL and retry checks.

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
        middlewares: list[Middleware] | None = None,
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
            middlewares: Ordered list of
                :class:`~agnosticq.middleware.Middleware` instances. Defaults
                to ``[TtlMiddleware(), MaxRetriesMiddleware()]`` when ``None``.
                Pass an empty list to disable all middleware.
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
        self._middlewares: list[Middleware] = (
            middlewares
            if middlewares is not None
            else [TtlMiddleware(), MaxRetriesMiddleware()]
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
        # 1. Run before_process_message middleware hooks
        try:
            for mw in self._middlewares:
                await mw.before_process_message(self, msg)
        except SkipMessage:
            for mw in self._middlewares:
                try:
                    await mw.after_skip_message(self, msg)
                except Exception as exc:
                    logger.exception(
                        "Middleware %s.after_skip_message raised an error",
                        type(mw).__name__,
                        exc_info=exc,
                    )
            return

        # 2. Route
        route = self.routes.get(msg.body.method)
        if route is None:
            if msg.delivery_count > self._settings.unroutable_max_retries:
                logger.error(
                    "No route for method %s after %d attempts, rejecting",
                    msg.body.method,
                    msg.delivery_count,
                )
                await msg.reject()
                return
            backoff = self._calculate_backoff(msg.delivery_count)
            logger.warning(
                "No route for method: %s, nacking with %.1fs delay",
                msg.body.method,
                backoff,
            )
            await msg.nack(backoff)
            return

        handler, mode = route

        # 3. Execute handler
        payload: Any = None
        handler_exc: Exception | None = None
        try:
            payload = await self._execute(handler, mode, msg.body.params)
        except Exception as exc:
            handler_exc = exc

        # 4. Run after_process_message middleware hooks
        for mw in self._middlewares:
            try:
                await mw.after_process_message(
                    self,
                    msg,
                    result=payload,
                    exception=handler_exc,
                )
            except Exception as exc:
                logger.exception(
                    "Middleware %s.after_process_message raised an error",
                    type(mw).__name__,
                    exc_info=exc,
                )

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

        # 7. Run before_publish_response middleware hooks
        out_headers: dict[str, str] = {}
        for mw in self._middlewares:
            try:
                await mw.before_publish_response(
                    self,
                    msg,
                    response,
                    out_headers,
                )
            except Exception as exc:
                logger.exception(
                    "Middleware %s.before_publish_response raised an error",
                    type(mw).__name__,
                    exc_info=exc,
                )

        try:
            await self.broker.publish(
                msg.reply_to,
                response,
                headers=out_headers or None,
            )
        except Exception as exc:
            backoff = self._calculate_backoff(msg.delivery_count)
            logger.exception(
                "Failed to publish response for %s, nacking with %.1fs delay",
                msg.body.method,
                backoff,
                exc_info=exc,
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
