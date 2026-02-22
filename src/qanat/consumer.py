"""Core message consumer implementing the smart-consumer architecture."""

from __future__ import annotations

import asyncio
import logging
import multiprocessing
import os
import signal
import sys
import threading
import time
import uuid
from concurrent.futures import Future, ProcessPoolExecutor
from functools import partial
from random import uniform
from typing import TYPE_CHECKING, Any, Callable, Final

from qanat.context import get_qanat_context
from qanat.enums import ExecutionMode, JsonRpcError
from qanat.exceptions import (
    FeatureNotSupportedError,
    HandlerTimeout,
    ProcessShutdown,
    RejectMessage,
    RetryMessage,
)
from qanat.middleware import (
    DeadlineMiddleware,
    Middleware,
)
from qanat.models import (
    ConsumerSettings,
    JsonRpcErrorDetail,
    JsonRpcResponse,
    TaskResult,
    TaskRoute,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from qanat.broker import BaseBroker
    from qanat.message import BrokerMessage
    from qanat.types import Seconds

    type CallNext = Callable[[BrokerMessage], Awaitable[JsonRpcResponse | None]]

logger = logging.getLogger(__name__)

DEFAULT_MAX_RETRIES: Final[int] = 3


def _sigalrm_handler(signum: int, frame: object) -> None:
    raise HandlerTimeout()


def _sigterm_handler(signum: int, frame: object) -> None:
    raise ProcessShutdown()


def _worker_main(
    task_id: str,
    monitoring_queue: Any,
    fn: Callable[..., Any],
    /,
    *args: Any,
    **kwargs: Any,
) -> Any:
    signal.signal(signal.SIGALRM, _sigalrm_handler)
    signal.signal(signal.SIGTERM, _sigterm_handler)
    monitoring_queue.put((task_id, os.getpid()))
    try:
        return fn(*args, **kwargs)
    finally:
        signal.signal(signal.SIGALRM, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)


class _ProcessPool:
    """Wraps ProcessPoolExecutor with PID tracking and signal-based kill.

    Uses a single Manager().Queue() shared across all workers to collect
    (task_id, pid) handshakes. A background thread maintains the
    active_pids map and handles the KOS (Kill-On-Sight) race condition:
    if kill_task() is called before the PID is registered, the task_id
    is added to kos; when the PID arrives, the monitor kills it immediately.

    Attributes:
        _executor: Underlying ProcessPoolExecutor.
        _active_pids: Mapping of live task_id → worker PID.
        _kos: Set of task_ids to be killed immediately on PID registration.
    """

    def __init__(self, max_workers: int) -> None:
        self._manager = multiprocessing.Manager()
        self._monitoring_queue = self._manager.Queue()
        self._executor = ProcessPoolExecutor(max_workers=max_workers)
        self._active_pids: dict[str, int] = {}
        self._kos: set[str] = set()
        self._lock = threading.Lock()
        self._monitor = threading.Thread(target=self._monitor_pids, daemon=True)
        self._monitor.start()

    def _monitor_pids(self) -> None:
        while True:
            try:
                task_id, pid = self._monitoring_queue.get()
                if task_id is None:  # poison pill → shutdown
                    break
                with self._lock:
                    if task_id in self._kos:
                        self._kos.discard(task_id)
                        try:
                            os.kill(pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                    else:
                        self._active_pids[task_id] = pid
            except Exception as exc:
                logger.exception(
                    f"{type(self).__name__} monitor error", exc_info=exc
                )

    def submit(
        self,
        fn: Callable[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[Future[Any], str]:
        """Submit a callable for execution and return a (future, task_id) pair.

        Args:
            fn: Callable to execute in the worker process.
            *args: Positional arguments forwarded to fn.
            **kwargs: Keyword arguments forwarded to fn.

        Returns:
            A tuple of (Future, task_id) where task_id can be passed to
            kill_task().
        """
        task_id = uuid.uuid4().hex
        wrapped = partial(
            _worker_main,
            task_id,
            self._monitoring_queue,
            fn,
            *args,
            **kwargs,
        )
        future: Future[Any] = self._executor.submit(wrapped)
        future.add_done_callback(lambda _: self._cleanup(task_id))
        return future, task_id

    def _cleanup(self, task_id: str) -> None:
        with self._lock:
            self._active_pids.pop(task_id, None)

    def kill_task(self, task_id: str, sig: int = signal.SIGALRM) -> None:
        """Send signal to worker running task_id, or add to KOS if not started.

        Args:
            task_id: ID returned by submit().
            sig: Signal to send. SIGALRM for soft kill, SIGKILL for hard kill.
        """
        with self._lock:
            pid = self._active_pids.get(task_id)
            if pid is None:
                self._kos.add(task_id)
                return
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            pass

    def shutdown(self, *, wait: bool = True) -> None:
        """Shut down the pool, monitor thread, and manager.

        Args:
            wait: If True, block until all workers finish.
        """
        self._monitoring_queue.put((None, None))  # poison pill
        self._executor.shutdown(wait=wait)
        self._manager.shutdown()


class QanatConsumer:
    """Transport-agnostic async task queue consumer.

    Processes messages through an onion-style middleware pipeline
    followed by routing and execution. Built-in middlewares handle
    deadline checks; retry and transport operations are consolidated
    in ``_process_message``.

    Register handlers with the :meth:`task` decorator, then call
    :meth:`run` to start consuming. Handles SIGINT/SIGTERM for
    graceful shutdown.

    Attributes:
        broker: The broker instance used to publish and consume messages.
        routes: Mapping of method name to :class:`~qanat.models.TaskRoute`.
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
                :class:`~qanat.models.ConsumerSettings` with default
                values if not provided.
            middlewares: Ordered list of
                :class:`~qanat.middleware.Middleware` instances.
                Defaults to ``[DeadlineMiddleware()]`` when ``None``.
                Pass an empty list to disable all middleware.
        """
        self.broker = broker
        self.routes: dict[str, TaskRoute] = {}

        self._pool: _ProcessPool | None = (
            _ProcessPool(max_workers=process_workers)
            if process_workers
            else None
        )
        self._settings = (
            settings if settings is not None else ConsumerSettings()
        )
        self._middlewares: list[Middleware] = (
            middlewares if middlewares is not None else [DeadlineMiddleware()]
        )
        self._build_pipeline()

    def task(
        self,
        name: str,
        mode: ExecutionMode = ExecutionMode.ASYNC,
        *,
        max_retries: int | None = None,
        time_limit: float | None = None,
        grace_period: float | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a callable as the handler for a named JSON-RPC method.

        Usage::

            @consumer.task("my.method", mode=ExecutionMode.THREAD,
                           time_limit=30.0)
            def handle(name: str, greeting: str = "Hi"):
                ...

        Args:
            name: JSON-RPC method name that routes to this handler.
            mode: Execution strategy for the handler. Defaults to
                ``ExecutionMode.ASYNC``.
            max_retries: Maximum number of retry attempts for this handler.
                Must be non-negative. Zero means no retries (a single attempt
                only), useful for idempotent operations that should fail fast.
                ``None`` defers to
                :attr:`~qanat.models.ConsumerSettings.max_retries`
                or ``DEFAULT_MAX_RETRIES`` (3).
            time_limit: Maximum wall-clock seconds the handler may run.
                ``None`` means no limit. On expiry, raises
                :exc:`~qanat.exceptions.HandlerTimeout`.
            grace_period: Seconds between SIGALRM and SIGKILL for
                ``ExecutionMode.PROCESS`` handlers. ``None`` defers to
                :attr:`~qanat.models.ConsumerSettings.process_timeout_grace_period`.

        Returns:
            A decorator that registers the wrapped function and returns
            it unchanged.

        Raises:
            pydantic.ValidationError: If max_retries is negative, or
                time_limit/grace_period are non-positive.
        """

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.routes[name] = TaskRoute(
                handler=func,
                mode=mode,
                max_retries=max_retries,
                time_limit=time_limit,
                grace_period=grace_period,
            )
            return func

        return decorator

    handler = task

    def _calculate_backoff(self, delivery_count: int) -> Seconds:
        # Full Jitter Backoff Strategy
        s = self._settings
        exponential_cap = min(
            s.retry_max_delay,
            s.retry_base_delay * (2 ** (delivery_count - 1)),
        )
        return uniform(0, exponential_cap)

    def _get_max_retries(self, route: TaskRoute) -> int:
        """Determine max retries using priority: route → settings → default.

        Args:
            route: The route being executed.

        Returns:
            The effective max retries value to use.
        """
        if route.max_retries is not None:
            return route.max_retries
        if self._settings.max_retries is not None:
            return self._settings.max_retries
        return DEFAULT_MAX_RETRIES

    async def _execute_async(
        self,
        handler: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        time_limit: float | None,
    ) -> Any:
        if time_limit is None:
            return await handler(*args, **kwargs)
        try:
            async with asyncio.timeout(time_limit):
                return await handler(*args, **kwargs)
        except TimeoutError as e:
            raise HandlerTimeout(time_limit) from e

    async def _execute_thread(
        self,
        handler: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        time_limit: float | None,
    ) -> Any:
        if time_limit is None:
            return await asyncio.to_thread(handler, *args, **kwargs)

        task_ctx = get_qanat_context()
        try:
            async with asyncio.timeout(time_limit):
                return await asyncio.to_thread(handler, *args, **kwargs)
        except TimeoutError as e:
            task_ctx.cancel()
            raise HandlerTimeout(time_limit) from e

    async def _execute_process(
        self,
        handler: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        time_limit: float | None,
        grace_period: float | None,
    ) -> Any:
        if self._pool is None:
            raise RuntimeError(
                f"ProcessPoolExecutor not configured; "
                f"set process_workers in {type(self).__name__}"
            )

        if sys.platform == "win32" and time_limit is not None:
            raise FeatureNotSupportedError("process_time_limit", "Windows")

        cf_future, task_id = self._pool.submit(handler, *args, **kwargs)
        future = asyncio.wrap_future(cf_future)

        if time_limit is None:
            return await future

        grace = (
            grace_period
            if grace_period is not None
            else self._settings.process_timeout_grace_period
        )
        try:
            async with asyncio.timeout(time_limit):
                return await future
        except TimeoutError as e:
            self._pool.kill_task(task_id, signal.SIGALRM)
            await asyncio.sleep(grace)
            self._pool.kill_task(task_id, signal.SIGKILL)
            raise HandlerTimeout(time_limit) from e

    async def _execute(self, route: TaskRoute, params: Any) -> Any:
        args: tuple[Any, ...] = ()
        kwargs: dict[str, Any] = {}
        if isinstance(params, list):
            args = tuple(params)
        elif isinstance(params, dict):
            kwargs = params

        match route.mode:
            case ExecutionMode.ASYNC:
                return await self._execute_async(
                    handler=route.handler,
                    args=args,
                    kwargs=kwargs,
                    time_limit=route.time_limit,
                )
            case ExecutionMode.THREAD:
                return await self._execute_thread(
                    handler=route.handler,
                    args=args,
                    kwargs=kwargs,
                    time_limit=route.time_limit,
                )
            case ExecutionMode.PROCESS:
                return await self._execute_process(
                    handler=route.handler,
                    args=args,
                    kwargs=kwargs,
                    time_limit=route.time_limit,
                    grace_period=route.grace_period,
                )

    def _build_pipeline(self) -> None:
        """Build the middleware chain ending at _router_endpoint."""
        pipeline: CallNext = self._router_endpoint
        for mw in reversed(self._middlewares):
            pipeline = self._wrap_middleware(mw, pipeline)
        self._pipeline = pipeline

    @staticmethod
    def _wrap_middleware(mw: Middleware, call_next: CallNext) -> CallNext:
        async def wrapped(msg: BrokerMessage) -> JsonRpcResponse | None:
            return await mw(msg, call_next)

        return wrapped

    async def _router_endpoint(
        self,
        msg: BrokerMessage,
    ) -> JsonRpcResponse | None:
        """Terminal pipeline stage: routing, execution, and retry logic.

        Args:
            msg: The incoming broker message.

        Returns:
            ``JsonRpcResponse`` for requests with reply_to,
            ``None`` otherwise.

        Raises:
            RetryMessage: When the message should be requeued.
            RejectMessage: When the message should be permanently discarded.
        """
        # 1. Route
        if (route := self.routes.get(msg.body.method)) is None:
            logger.error("No route for message, rejecting")
            raise RejectMessage

        # 2. Execute handler
        ctx = get_qanat_context()
        ctx.route = route
        ctx.max_attempts = self._get_max_retries(route) + 1

        handler_exc: Exception | None = None
        result: Any = None
        try:
            result = await self._execute(route, msg.body.params)
        except RetryMessage:
            raise  # propagate RetryMessage without modification
        except Exception as exc:
            handler_exc = exc

        # 3. Handle errors
        if handler_exc is not None:
            log_ctx = {
                "error_type": type(handler_exc).__name__,
                "error_msg": str(handler_exc),
            }

            if msg.delivery_count < ctx.max_attempts:
                logger.warning(
                    "Message processing failed. "
                    "Reason: %(error_type)s(%(error_msg)r)",
                    log_ctx,
                    extra=log_ctx,
                )
                raise RetryMessage

            logger.error(
                "Message processing permanently "
                "failed after %(max_attempts)d attempts",
                extra=log_ctx,
                exc_info=handler_exc,
            )

            if msg.correlation_id is None:
                raise RejectMessage

            if not msg.reply_to:
                logger.info(
                    "Message has correlation id but no reply_to, "
                    "rejecting without reply",
                )
                raise RejectMessage

            return JsonRpcResponse(
                id=msg.correlation_id,
                error=JsonRpcErrorDetail(
                    code=JsonRpcError.INTERNAL_ERROR,
                    message=str(handler_exc),
                ),
            )

        # 4. Unwrap TaskResult
        handler_headers: dict[str, str] | None = None
        if isinstance(result, TaskResult):
            handler_headers = result.headers
            result = result.result

        # 5. Build response for requests
        if msg.correlation_id is not None:
            if not msg.reply_to:
                logger.info(
                    "Message has correlation id but no reply_to, "
                    "acking without reply",
                )
                return None

            response = JsonRpcResponse(
                id=msg.correlation_id,
                result=result,
            )
            if handler_headers:
                response.headers.update(handler_headers)

            return response

        return None

    async def _process_message(self, msg: BrokerMessage) -> None:
        """Run the middleware pipeline and handle transport operations.

        This method is purely transport logic: it runs the pipeline,
        publishes responses, and translates control-flow exceptions
        into broker ack/nack/reject calls.

        Args:
            msg: The incoming broker message.
        """
        ctx = get_qanat_context()
        ctx.msg = msg
        ctx.broker_latency = round(msg.received_at - msg.enqueued_at, 3)
        ctx.internal_latency = round(time.time() - msg.received_at, 3)

        try:
            response = await self._pipeline(msg)

            if (
                response is not None
                and msg.correlation_id is not None
                and msg.reply_to
            ):
                outbound_headers = response.headers or None
                try:
                    await self.broker.publish(
                        msg.reply_to,
                        response,
                        headers=outbound_headers,
                    )
                except Exception as exc:
                    backoff = self._calculate_backoff(msg.delivery_count)
                    logger.exception(
                        "Failed to publish response, "
                        f"nacking with {backoff:.1f} delay",
                        extra={"delay_seconds": backoff},
                        exc_info=exc,
                    )
                    await msg.nack(backoff)
                    return

            await msg.ack()

        except RetryMessage as exc:
            if (backoff := exc.delay) is None:
                backoff = self._calculate_backoff(msg.delivery_count)

            log_ctx = {"delay_seconds": backoff}
            logger.info(str(exc), log_ctx, extra=log_ctx)
            await msg.nack(backoff)

        except RejectMessage:
            await msg.reject()

        except Exception as exc:
            logger.exception("Critical unhandled pipeline error", exc_info=exc)
            await msg.reject()

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
