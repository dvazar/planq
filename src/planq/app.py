"""Central application object and task wrapper for planq."""

from __future__ import annotations

import asyncio
import functools
import threading
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    ParamSpec,
    TypeVar,
)

from planq.enums import ExecutionMode, Header
from planq.models import JsonRpcId, JsonRpcRequest, TaskRoute
from planq.params import analyze_signature

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from planq.broker import BaseBroker
    from planq.types import RetryCondition, Seconds

P = ParamSpec("P")
T = TypeVar("T")


class PlanqTask(Generic[P, T]):
    """Callable wrapper that adds `.send()` to a task handler.

    Preserves the original function for direct calls (consumer
    side) and adds `.send()` for publishing messages (producer
    side). Use ``.options()`` to configure transport options.

    Attributes:
        name: The JSON-RPC method name for this task.
        queue_name: Default destination queue for `.send()`.
    """

    def __init__(
        self,
        func: Callable[P, T],
        app: Planq,
        name: str,
        queue_name: str,
    ) -> None:
        """Initialize the task wrapper.

        Args:
            func: The original handler function.
            app: Planq application instance.
            name: JSON-RPC method name.
            queue_name: Default destination queue.
        """
        self._func = func
        self._app = app
        self.name = name
        self.queue_name = queue_name
        functools.update_wrapper(self, func)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        """Execute the wrapped function directly."""
        return self._func(*args, **kwargs)

    def send(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> str | Coroutine[Any, Any, str]:
        """Publish this task to the broker without options.

        Shortcut for ``task.options().send(...)``. All arguments
        become JSON-RPC params.

        Returns:
            The broker-assigned message ID string, or a
            coroutine that resolves to it (async apps).
        """
        return TaskSender(
            task=self,
            transport={},
            correlation_id=None,
        ).send(*args, **kwargs)

    def options(
        self,
        *,
        correlation_id: JsonRpcId = None,
        delay: Seconds | None = None,
        expire_at: float | None = None,
        reply_to: str | None = None,
        traceparent: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> TaskSender[P]:
        """Configure transport options for publishing.

        Returns a :class:`TaskSender` whose ``.send()``
        preserves the original function signature.

        Args:
            correlation_id: JSON-RPC request id. Non-None
                values enable request/response pattern.
            delay: Delay in seconds before processing.
            expire_at: Unix timestamp for message TTL.
            reply_to: Queue name for response delivery.
            traceparent: W3C traceparent header value.
            headers: Custom transport headers.

        Returns:
            A TaskSender with configured transport options.
        """
        transport: dict[str, Any] = {}

        if delay is not None:
            transport["delay"] = delay

        if expire_at is not None:
            transport["expire_at"] = expire_at

        if reply_to is not None:
            transport["reply_to"] = reply_to

        if traceparent is not None:
            if headers is None:
                headers = {}
            headers[Header.TRACEPARENT.value] = traceparent

        if headers:
            transport["headers"] = headers

        return TaskSender(
            task=self,
            transport=transport,
            correlation_id=correlation_id,
        )

    async def _send(
        self,
        request: JsonRpcRequest,
        transport: dict[str, Any],
    ) -> str:
        """Publish the request to the broker.

        Args:
            request: The JSON-RPC request to publish.
            transport: Extracted transport options.

        Returns:
            The broker-assigned message ID.
        """
        return await self._app.broker.publish(
            self.queue_name,
            request,
            **transport,
        )


class TaskSender(Generic[P]):
    """Builder for publishing a task with transport options.

    Created by :meth:`PlanqTask.options`. Holds transport
    configuration and exposes a typed ``.send()`` method.
    """

    def __init__(
        self,
        task: PlanqTask[P, Any],
        correlation_id: JsonRpcId,
        transport: dict[str, Any],
    ) -> None:
        """Initialize the sender.

        Args:
            task: The PlanqTask to publish.
            correlation_id: JSON-RPC request id.
            transport: Transport options for broker.publish().
        """
        self._task = task
        self._correlation_id = correlation_id
        self._transport = transport

    def send(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> str | Coroutine[Any, Any, str]:
        """Publish this task to the broker.

        All arguments become JSON-RPC params.

        Returns:
            The broker-assigned message ID string, or a
            coroutine that resolves to it (async apps).
        """
        if kwargs:
            params = kwargs
        elif args:
            params = list(args)
        else:
            params = None

        request = JsonRpcRequest(
            method=self._task.name,
            params=params,
            id=self._correlation_id,
        )

        coro = self._task._send(request, self._transport)
        return self._task._app._dispatch(coro)


class Planq:
    """Central application object binding a broker to a task
    registry.

    Attributes:
        broker: The broker instance for publishing and consuming.
        routes: Mapping of method name to TaskRoute.
    """

    def __init__(self, broker: BaseBroker) -> None:
        """Initialize with a broker instance.

        Args:
            broker: Connected (or lazy) broker instance.
        """
        self.broker = broker
        self.routes: dict[str, TaskRoute] = {}

    def _dispatch(
        self,
        coro: Coroutine[Any, Any, str],
    ) -> Coroutine[Any, Any, str]:
        """Dispatch a coroutine for execution.

        Async app returns the coroutine as-is for the caller
        to ``await``.

        Args:
            coro: The coroutine to dispatch.

        Returns:
            The coroutine unchanged.
        """
        return coro

    def task(
        self,
        name: str | None = None,
        queue_name: str = "default",
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
    ) -> Callable[[Callable[P, T]], PlanqTask[P, T]]:
        """Register a callable as a task and wrap it in PlanqTask.

        Args:
            name: JSON-RPC method name. Auto-generated from
                module path if None.
            queue_name: Default destination queue for .send().
            mode: Execution strategy for the handler.
            time_limit: Max wall-clock seconds the handler may
                run.
            grace_period: PROCESS mode SIGALRM-to-SIGKILL delay.
            max_retries: Maximum retry attempts.
            retry_on: Exception types or predicates that enable
                retries.

        Returns:
            Decorator that wraps the function in PlanqTask.

        Raises:
            ValueError: If a task with the same name is already
                registered.
        """

        def decorator(func: Callable[P, T]) -> PlanqTask[P, T]:
            task_name = name if name is not None else _resolve_task_name(func)

            if task_name in self.routes:
                raise ValueError(f"Task {task_name!r} already registered")

            self.routes[task_name] = TaskRoute(
                handler=func,
                mode=mode,
                queue_name=queue_name,
                time_limit=time_limit,
                grace_period=grace_period,
                max_retries=max_retries,
                retry_on=retry_on,
                param_meta=analyze_signature(func),
            )

            return PlanqTask(
                func=func,
                app=self,
                name=task_name,
                queue_name=queue_name,
            )

        return decorator

    handler = task
    rpc = task


class SyncPlanq(Planq):
    """Synchronous variant of Planq using a daemon thread.

    Manages a background event loop in a daemon thread. The
    broker connects eagerly in ``__init__`` and all ``.send()``
    calls block until the broker confirms the message.

    Use as a context manager for clean shutdown::

        with SyncPlanq(broker=SqsBroker(...)) as app:
            resize_image.send(url="...", width=100)
            resize_image.options(delay=5).send(url="...")
    """

    def __init__(self, broker: BaseBroker) -> None:
        """Initialize and start the background event loop.

        Args:
            broker: Broker instance (connected eagerly).
        """
        super().__init__(broker)

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run_background_loop,
            daemon=True,
        )
        self._thread.start()
        self._run_sync(self.broker.connect())

    def _run_background_loop(self) -> None:
        """Run the event loop in the background thread."""
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _run_sync(self, coro: Coroutine[Any, Any, Any]) -> Any:
        """Submit a coroutine to the background loop and block.

        Args:
            coro: Coroutine to execute.

        Returns:
            The coroutine's return value.
        """
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()

    def _dispatch(
        self,
        coro: Coroutine[Any, Any, str],
    ) -> str:  # type: ignore[override]
        """Dispatch a coroutine synchronously.

        Submits the coroutine to the background event loop and
        blocks until it completes.

        Args:
            coro: The coroutine to dispatch.

        Returns:
            The broker-assigned message ID.
        """
        return self._run_sync(coro)

    def close(self) -> None:
        """Disconnect the broker and stop the background loop."""
        self._run_sync(self.broker.disconnect())
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()
        self._loop.close()

    def __enter__(self) -> SyncPlanq:
        """Enter context manager (already connected)."""
        return self

    def __exit__(self, *exc: object) -> None:
        """Exit context manager, disconnecting the broker."""
        self.close()


def _resolve_task_name(func: Callable[..., Any]) -> str:
    """Resolve task name from module path and qualified name.

    Args:
        func: The function to generate a name for.

    Returns:
        Dotted name like "app.tasks.images.resize_image".
        Strips ``__main__`` prefix.
    """
    module = func.__module__
    qualname = func.__qualname__
    if module == "__main__":
        return qualname
    return f"{module}.{qualname}"
