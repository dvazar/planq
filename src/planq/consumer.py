"""Core message consumer implementing the smart-consumer architecture."""

from __future__ import annotations

import asyncio
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
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Final,
    Literal,
    ParamSpec,
    TypeVar,
    overload,
)

from planq.context import get_planq_context
from planq.enums import ExecutionMode, Header, JsonRpcError, LogEvent
from planq.exceptions import (
    FeatureNotSupportedError,
    HandlerTimeout,
    InvalidParamsError,
    MaxRetriesExceeded,
    MethodNotFound,
    ProcessShutdown,
    RejectMessage,
    RetryMessage,
)
from planq.log import get_planq_logger
from planq.middleware import (
    DeadlineMiddleware,
    Middleware,
)
from planq.models import (
    ConsumerSettings,
    JsonRpcErrorDetail,
    JsonRpcResponse,
    TaskResult,
    TaskRoute,
)
from planq.params import (
    ParamsConverter,
    analyze_signature,
)
from planq.tracing import parse_traceparent_and_generate_span

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from planq.broker import BaseBroker
    from planq.message import BrokerMessage
    from planq.types import RetryCondition, Seconds

    type CallNext = Callable[[BrokerMessage], Awaitable[JsonRpcResponse | None]]

P = ParamSpec("P")  # Captures function parameters (*args, **kwargs)
T = TypeVar("T")  # Captures return type

logger = get_planq_logger(__name__)

DEFAULT_MAX_RETRIES: Final[int] = 3


def _sigalrm_handler(signum: int, frame: object) -> None:
    raise HandlerTimeout()


def _sigterm_handler(signum: int, frame: object) -> None:
    raise ProcessShutdown()


def should_retry(
    exc: Exception,
    retry_on: (
        RetryCondition | list[RetryCondition] | tuple[RetryCondition, ...]
    ),
) -> bool:
    """Check if exception matches any retry condition.

    Args:
        exc: The exception that was raised.
        retry_on: Single condition or list of conditions to check.

    Returns:
        True if exception matches any condition, False otherwise.
    """
    if isinstance(retry_on, (list, tuple)):
        conditions = retry_on
    else:
        conditions = (retry_on,)

    for condition in conditions:
        if isinstance(condition, type) and issubclass(condition, Exception):
            if isinstance(exc, condition):
                return True

        elif callable(condition):
            try:
                if condition(exc):
                    return True
            except Exception as predicate_exc:
                logger.error(
                    "Error evaluating retry predicate: %s",
                    predicate_exc,
                    exc_info=predicate_exc,
                    extra={"event": LogEvent.RETRY_PREDICATE_ERROR},
                )
                return False

    return False


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
                logger.error(
                    "Process pool monitor error",
                    exc_info=exc,
                    extra={"event": LogEvent.PROCESS_MONITOR_ERROR},
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


class PlanqConsumer:
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
        routes: Mapping of method name to :class:`~planq.models.TaskRoute`.
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
                :class:`~planq.models.ConsumerSettings` with default
                values if not provided.
            middlewares: Ordered list of
                :class:`~planq.middleware.Middleware` instances.
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
        self._params_converter = ParamsConverter()
        self._build_pipeline()
        self._reject_callbacks = []

    @overload
    def task(
        self,
        name: str,
        mode: Literal[ExecutionMode.ASYNC] = ...,
        *,
        time_limit: Seconds | None = None,
        grace_period: Seconds | None = None,
        max_retries: int | None = None,
        retry_on: (
            RetryCondition
            | list[RetryCondition]
            | tuple[RetryCondition, ...]
            | None
        ) = None,
    ) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]: ...

    @overload
    def task(
        self,
        name: str,
        mode: Literal[ExecutionMode.THREAD, ExecutionMode.PROCESS],
        *,
        time_limit: Seconds | None = None,
        grace_period: Seconds | None = None,
        max_retries: int | None = None,
        retry_on: (
            RetryCondition
            | list[RetryCondition]
            | tuple[RetryCondition, ...]
            | None
        ) = None,
    ) -> Callable[[Callable[P, T]], Callable[P, T]]: ...

    @overload
    def task(
        self,
        name: str,
        mode: ExecutionMode = ...,
        *,
        time_limit: Seconds | None = None,
        grace_period: Seconds | None = None,
        max_retries: int | None = None,
        retry_on: (
            RetryCondition
            | list[RetryCondition]
            | tuple[RetryCondition, ...]
            | None
        ) = None,
    ) -> Callable[
        [Callable[P, Awaitable[T] | T]], Callable[P, Awaitable[T] | T]
    ]: ...

    def task(
        self,
        name: str,
        mode: ExecutionMode = ExecutionMode.ASYNC,
        *,
        time_limit: Seconds | None = None,
        grace_period: Seconds | None = None,
        max_retries: int | None = None,
        retry_on: (
            RetryCondition
            | list[RetryCondition]
            | tuple[RetryCondition, ...]
            | None
        ) = None,
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
            time_limit: Maximum wall-clock seconds the handler may run.
                ``None`` means no limit. On expiry, raises
                :exc:`~planq.exceptions.HandlerTimeout`.
            grace_period: Seconds between SIGALRM and SIGKILL for
                ``ExecutionMode.PROCESS`` handlers. ``None`` defers to
                :attr:`~planq.models.ConsumerSettings.process_timeout_grace_period`.
            max_retries: Maximum number of retry attempts for this handler.
                Must be non-negative. Zero means no retries (a single attempt
                only), useful for idempotent operations that should fail fast.
                ``None`` defers to
                :attr:`~planq.models.ConsumerSettings.max_retries`
                or ``DEFAULT_MAX_RETRIES`` (3).
            retry_on: Exception types or predicates that enable retries.
                - None (default): Do NOT retry any exceptions.
                - Single type: retry_on=ValueError
                - Multiple types: retry_on=[ValueError, KeyError]
                - Callable: retry_on=lambda exc: "temporary" in str(exc)
                - Mixed: retry_on=[ValueError, lambda exc: ...]
                If exception doesn't match any condition, message is rejected
                immediately without counting toward max_retries.

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
                time_limit=time_limit,
                grace_period=grace_period,
                max_retries=max_retries,
                retry_on=retry_on,
                param_meta=analyze_signature(func),
            )
            return func

        return decorator

    handler = task

    def on_reject(
        self,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a callback invoked when a message is rejected.

        The decorated function receives the :class:`BrokerMessage` and
        the :class:`RejectMessage` exception that caused rejection.

        Returns:
            A decorator that registers the callback and returns it
            unchanged.
        """

        def decorator(
            func: Callable[..., Any],
        ) -> Callable[..., Any]:
            self._reject_callbacks.append(func)
            return func

        return decorator

    async def _execute_reject_callbacks(
        self,
        msg: BrokerMessage,
        exc: Exception,
    ) -> None:
        if not self._reject_callbacks:
            return

        results = await asyncio.gather(
            *(callback(msg, exc) for callback in self._reject_callbacks),
            return_exceptions=True,
        )

        for callback, result in zip(self._reject_callbacks, results):
            if isinstance(result, Exception):
                log_ctx = {
                    "event": LogEvent.REJECT_CALLBACK_ERROR,
                    "callback": callback.__qualname__,
                }
                logger.error(
                    "Reject callback %(callback)r failed",
                    log_ctx,
                    exc_info=result,
                    extra=log_ctx,
                )

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

        task_ctx = get_planq_context()
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

    async def _execute(
        self,
        route: TaskRoute,
        params: Any,
        method: str,
    ) -> Any:
        args, kwargs = self._params_converter.convert(
            signature=route.param_meta,
            params=params,
            method=method,
            dataclass_parser=self._settings.dataclass_parser,
        )

        start_perf = time.perf_counter()
        start_process = time.process_time()

        try:
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
                case _:  # pragma: no cover
                    # Unreachable: all ExecutionMode values handled above
                    raise AssertionError(
                        f"Unknown execution mode: {route.mode}"
                    )
        finally:
            ctx = get_planq_context()
            ctx.rpc_duration = round(time.perf_counter() - start_perf, 4)
            ctx.rpc_cpu = round(time.process_time() - start_process, 4)

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
        method = msg.body.method

        # 1. Route
        if (route := self.routes.get(method)) is None:
            raise MethodNotFound(method)

        # 2. Execute handler
        ctx = get_planq_context()
        ctx.route = route
        ctx.max_attempts = max_attempts = self._get_max_retries(route) + 1

        handler_exc: Exception | None = None
        result: Any = None
        try:
            result = await self._execute(route, msg.body.params, method)
        except RetryMessage:
            raise  # propagate RetryMessage without modification
        except InvalidParamsError as exc:
            if msg.correlation_id is not None and msg.reply_to:
                return JsonRpcResponse(
                    id=msg.correlation_id,
                    error=JsonRpcErrorDetail(
                        code=JsonRpcError.INVALID_PARAMS,
                        message=str(exc),
                        data=exc.errors,
                    ),
                )
            raise  # falls to RejectMessage handling
        except Exception as exc:
            handler_exc = exc

        # 3. Handle errors
        if handler_exc is not None:
            if (
                route.retry_on is None
                or should_retry(handler_exc, route.retry_on) is False
            ):
                raise RejectMessage(
                    "Message processing failed with a non-retryable "
                    f"error for method '{method}'."
                ) from handler_exc

            if msg.delivery_count < max_attempts:
                log_ctx = {
                    "event": LogEvent.HANDLER_RETRYING,
                    "error_type": type(handler_exc).__name__,
                    "error_msg": str(handler_exc),
                }
                logger.warning(
                    "Message processing failed for method %(method)r."
                    " Retrying. Reason: %(error_type)s(%(error_msg)r)",
                    log_ctx,
                    extra=log_ctx,
                )
                raise RetryMessage

            if msg.correlation_id is None or not msg.reply_to:
                raise MaxRetriesExceeded(max_attempts, method) from handler_exc

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
        ctx = get_planq_context()
        ctx.msg = msg
        ctx.trace = parse_traceparent_and_generate_span(
            msg.headers.get(Header.TRACEPARENT)
        )
        ctx.broker_latency = round(msg.received_at - msg.enqueued_at, 3)
        ctx.internal_latency = round(time.time() - msg.received_at, 3)

        try:
            start_perf = time.perf_counter()
            start_process = time.process_time()

            response = await self._pipeline(msg)

            ctx.pipeline_duration = round(time.perf_counter() - start_perf, 4)
            ctx.pipeline_cpu = round(time.process_time() - start_process, 4)

            if (
                response is not None
                and msg.correlation_id is not None
                and msg.reply_to
            ):
                response.headers.setdefault(
                    Header.TRACEPARENT,
                    ctx.trace.to_traceparent(),
                )
                try:
                    await self.broker.publish(
                        msg.reply_to,
                        response,
                        headers=response.headers,
                    )
                except Exception as exc:
                    backoff = self._calculate_backoff(msg.delivery_count)
                    log_ctx = {
                        "event": LogEvent.PUBLISH_RESPONSE_FAILED,
                        "delay_seconds": backoff,
                    }
                    logger.error(
                        "Failed to publish response to %(reply_to)r"
                        " for method %(method)r. Message ID: %(message_id)s."
                        " Nacking with %(delay_seconds).1fs delay.",
                        log_ctx,
                        exc_info=exc,
                        extra=log_ctx,
                    )
                    await msg.nack(backoff)
                    return

            await msg.ack()

        except RetryMessage as exc:
            if (backoff := exc.delay) is None:
                backoff = self._calculate_backoff(msg.delivery_count)
            log_ctx = {
                "event": LogEvent.MESSAGE_REQUEUEING,
                "delay_seconds": backoff,
            }
            logger.info(
                "Message ID: %(message_id)s. Requeueing for retry."
                " Method: %(method)r,"
                " Attempt: %(current_attempt)d/%(max_attempts)d,"
                " Delay: %(delay_seconds).1fs.",
                log_ctx,
                extra=log_ctx,
            )
            await msg.nack(backoff)

        except RejectMessage as exc:
            log_ctx = {
                "event": LogEvent.MESSAGE_REJECTING,
                "reason": str(exc),
            }
            logger.error(
                "Message ID: %(message_id)s. Rejecting. %(reason)s",
                log_ctx,
                extra=log_ctx,
                exc_info=exc,
            )
            await self._execute_reject_callbacks(msg, exc)
            await msg.reject()

        except Exception as exc:
            backoff = self._calculate_backoff(msg.delivery_count)
            log_ctx = {
                "event": LogEvent.PIPELINE_ERROR,
                "delay_seconds": backoff,
            }
            logger.error(
                "Unhandled pipeline error during message processing."
                " Method: %(method)r, Message ID: %(message_id)s."
                " Nacking with %(delay_seconds).1fs delay.",
                log_ctx,
                exc_info=exc,
                extra=log_ctx,
            )
            await msg.nack(backoff)

    async def _guarded_process(
        self,
        msg: BrokerMessage,
        sem: asyncio.Semaphore,
    ) -> None:
        try:
            await self._process_message(msg)
        except Exception as exc:
            logger.error(
                "Broker operation failed for method %(method)r."
                " Message ID: %(message_id)s."
                " Relying on broker's visibility timeout for redelivery.",
                exc_info=exc,
                extra={"event": LogEvent.BROKER_OPERATION_FAILED},
            )
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
