"""Comprehensive tests for Planq app and PlanqTask."""

from __future__ import annotations

import asyncio
import threading
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from planq.app import (
    Planq,
    PlanqTask,
    SyncPlanq,
    TaskSender,
    _resolve_task_name,
)
from planq.enums import ExecutionMode
from planq.models import JsonRpcRequest, TaskRoute

# === Helper Functions ===


def _make_func(
    name: str = "my_task",
    module: str = "app.tasks",
    qualname: str | None = None,
):
    """Create a stub function with controllable __module__ and
    __qualname__."""

    def func():
        pass

    func.__name__ = name
    func.__module__ = module
    func.__qualname__ = qualname if qualname is not None else name
    return func


def _make_broker() -> MagicMock:
    """Create a MagicMock broker with an AsyncMock publish."""
    broker = MagicMock()
    broker.publish = AsyncMock(return_value="msg-id-123")
    broker.connect = AsyncMock()
    broker.disconnect = AsyncMock()
    return broker


def _make_app() -> tuple[Planq, MagicMock]:
    """Create a Planq app backed by a mock broker."""
    broker = _make_broker()
    app = Planq(broker)
    return app, broker


# === TestGenerateTaskName ===


class TestGenerateTaskName:
    """Tests for _resolve_task_name helper."""

    def test_regular_module(self):
        """Combines module and qualname with a dot."""
        func = _make_func(
            module="app.tasks.images",
            qualname="resize_image",
        )

        result = _resolve_task_name(func)

        assert result == "app.tasks.images.resize_image"

    def test_main_module_stripped(self):
        """Strips __main__ prefix, returning bare qualname."""
        func = _make_func(
            module="__main__",
            qualname="my_task",
        )

        result = _resolve_task_name(func)

        assert result == "my_task"

    def test_nested_class_method(self):
        """Handles nested class qualname correctly."""
        func = _make_func(
            module="app.services",
            qualname="ImageService.resize",
        )

        result = _resolve_task_name(func)

        assert result == "app.services.ImageService.resize"


# === TestPlanqTaskCall ===


class TestPlanqTaskCall:
    """Tests for PlanqTask.__call__ (direct execution)."""

    def test_call_sync_function(self):
        """Direct call works for a sync function."""
        app, _ = _make_app()

        def add(a, b):
            return a + b

        task = PlanqTask(add, app, "test.add", "default")

        result = task(2, 3)

        assert result == 5

    @pytest.mark.asyncio
    async def test_call_async_function(self):
        """Direct call works for an async function."""
        app, _ = _make_app()

        async def greet(name):
            return f"hello {name}"

        task = PlanqTask(greet, app, "test.greet", "q")

        result = await task("world")

        assert result == "hello world"

    def test_preserves_function_name(self):
        """update_wrapper preserves __name__ and __wrapped__."""
        app, _ = _make_app()

        def my_handler():
            pass

        task = PlanqTask(my_handler, app, "test.handler", "q")

        assert task.__name__ == "my_handler"
        assert task.__wrapped__ is my_handler


# === TestPlanqTaskSendAsync ===


class TestPlanqTaskSendAsync:
    """Tests for PlanqTask.send() in async context."""

    @pytest.mark.asyncio
    async def test_send_basic(self):
        """Publishes JsonRpcRequest with correct queue, method,
        params, id=None."""
        app, broker = _make_app()
        task = PlanqTask(lambda x: x, app, "resize", "images")

        await task.send(x=42)

        broker.publish.assert_awaited_once()
        args, kwargs = broker.publish.call_args
        assert args[0] == "images"
        request = args[1]
        assert isinstance(request, JsonRpcRequest)
        assert request.method == "resize"
        assert request.params == {"x": 42}
        assert request.id is None

    @pytest.mark.asyncio
    async def test_send_positional_params(self):
        """Positional args become list params."""
        app, broker = _make_app()
        task = PlanqTask(lambda: None, app, "t", "q")

        await task.send(1, "two", 3.0)

        args, _ = broker.publish.call_args
        request = args[1]
        assert request.params == [1, "two", 3.0]

    @pytest.mark.asyncio
    async def test_send_no_params(self):
        """No args -> params=None."""
        app, broker = _make_app()
        task = PlanqTask(lambda: None, app, "t", "q")

        await task.send()

        args, _ = broker.publish.call_args
        request = args[1]
        assert request.params is None

    @pytest.mark.asyncio
    async def test_send_returns_message_id(self):
        """send() returns the broker-assigned message ID."""
        app, broker = _make_app()
        broker.publish = AsyncMock(return_value="abc-123")
        task = PlanqTask(lambda: None, app, "t", "q")

        result = await task.send()

        assert result == "abc-123"


# === TestTaskSender ===


class TestTaskSender:
    """Tests for TaskSender builder."""

    @pytest.mark.asyncio
    async def test_send_basic(self):
        """TaskSender.send() publishes correct request."""
        app, broker = _make_app()
        task = PlanqTask(lambda x: x, app, "resize", "images")
        sender = TaskSender(task=task, transport={}, correlation_id=None)

        await sender.send(x=42)

        broker.publish.assert_awaited_once()
        args, kwargs = broker.publish.call_args
        assert args[0] == "images"
        request = args[1]
        assert isinstance(request, JsonRpcRequest)
        assert request.method == "resize"
        assert request.params == {"x": 42}
        assert request.id is None

    @pytest.mark.asyncio
    async def test_send_with_transport(self):
        """Transport options forwarded to broker.publish."""
        app, broker = _make_app()
        task = PlanqTask(lambda: None, app, "t", "q")
        sender = TaskSender(
            task=task,
            transport={"delay": 30.0, "expire_at": 9999.0},
            correlation_id=None,
        )

        await sender.send()

        _, kwargs = broker.publish.call_args
        assert kwargs["delay"] == 30.0
        assert kwargs["expire_at"] == 9999.0

    @pytest.mark.asyncio
    async def test_send_with_correlation_id(self):
        """correlation_id sets JsonRpcRequest.id."""
        app, broker = _make_app()
        task = PlanqTask(lambda: None, app, "t", "q")
        sender = TaskSender(
            task=task,
            transport={},
            correlation_id="req-123",
        )

        await sender.send()

        args, _ = broker.publish.call_args
        request = args[1]
        assert request.id == "req-123"

    @pytest.mark.asyncio
    async def test_send_positional_params(self):
        """Positional args become list params."""
        app, broker = _make_app()
        task = PlanqTask(lambda: None, app, "t", "q")
        sender = TaskSender(task=task, transport={}, correlation_id=None)

        await sender.send(1, "two", 3.0)

        args, _ = broker.publish.call_args
        request = args[1]
        assert request.params == [1, "two", 3.0]

    @pytest.mark.asyncio
    async def test_send_no_params(self):
        """No args -> params=None."""
        app, broker = _make_app()
        task = PlanqTask(lambda: None, app, "t", "q")
        sender = TaskSender(task=task, transport={}, correlation_id=None)

        await sender.send()

        args, _ = broker.publish.call_args
        request = args[1]
        assert request.params is None

    @pytest.mark.asyncio
    async def test_send_returns_message_id(self):
        """send() returns broker-assigned message ID."""
        app, broker = _make_app()
        broker.publish = AsyncMock(return_value="abc-123")
        task = PlanqTask(lambda: None, app, "t", "q")
        sender = TaskSender(task=task, transport={}, correlation_id=None)

        result = await sender.send()

        assert result == "abc-123"


# === TestPlanqTaskOptions ===


class TestPlanqTaskOptions:
    """Tests for PlanqTask.options() builder."""

    def test_options_returns_task_sender(self):
        """options() returns a TaskSender instance."""
        app, _ = _make_app()
        task = PlanqTask(lambda: None, app, "t", "q")

        sender = task.options()

        assert isinstance(sender, TaskSender)

    @pytest.mark.asyncio
    async def test_options_delay(self):
        """options(delay=...) forwards delay to broker."""
        app, broker = _make_app()
        task = PlanqTask(lambda: None, app, "t", "q")

        await task.options(delay=30.0).send()

        _, kwargs = broker.publish.call_args
        assert kwargs["delay"] == 30.0

    @pytest.mark.asyncio
    async def test_options_expire_at(self):
        """options(expire_at=...) forwards to broker."""
        app, broker = _make_app()
        task = PlanqTask(lambda: None, app, "t", "q")

        await task.options(expire_at=9999.0).send()

        _, kwargs = broker.publish.call_args
        assert kwargs["expire_at"] == 9999.0

    @pytest.mark.asyncio
    async def test_options_reply_to(self):
        """options(reply_to=...) forwards to broker."""
        app, broker = _make_app()
        task = PlanqTask(lambda: None, app, "t", "q")

        await task.options(reply_to="rq").send()

        _, kwargs = broker.publish.call_args
        assert kwargs["reply_to"] == "rq"

    @pytest.mark.asyncio
    async def test_options_headers(self):
        """options(headers=...) forwards to broker."""
        app, broker = _make_app()
        task = PlanqTask(lambda: None, app, "t", "q")

        await task.options(
            headers={"x-custom": "val"},
        ).send()

        _, kwargs = broker.publish.call_args
        assert kwargs["headers"] == {"x-custom": "val"}

    @pytest.mark.asyncio
    async def test_options_traceparent(self):
        """options(traceparent=...) merged into headers."""
        app, broker = _make_app()
        task = PlanqTask(lambda: None, app, "t", "q")

        await task.options(
            traceparent="00-abc-def-01",
        ).send()

        _, kwargs = broker.publish.call_args
        assert kwargs["headers"] == {
            "traceparent": "00-abc-def-01",
        }

    @pytest.mark.asyncio
    async def test_options_traceparent_merges_headers(self):
        """Traceparent merges with existing headers."""
        app, broker = _make_app()
        task = PlanqTask(lambda: None, app, "t", "q")

        await task.options(
            headers={"x-custom": "val"},
            traceparent="00-abc-def-01",
        ).send()

        _, kwargs = broker.publish.call_args
        assert kwargs["headers"] == {
            "x-custom": "val",
            "traceparent": "00-abc-def-01",
        }

    @pytest.mark.asyncio
    async def test_options_correlation_id(self):
        """options(correlation_id=...) sets request id."""
        app, broker = _make_app()
        task = PlanqTask(lambda: None, app, "t", "q")

        await task.options(
            correlation_id="req-123",
        ).send()

        args, _ = broker.publish.call_args
        request = args[1]
        assert request.id == "req-123"

    @pytest.mark.asyncio
    async def test_options_all(self):
        """All options forwarded at once."""
        app, broker = _make_app()
        task = PlanqTask(lambda x: x, app, "t", "q")

        await task.options(
            delay=10.0,
            expire_at=9999.0,
            reply_to="rq",
            headers={"h": "v"},
            traceparent="tp",
            correlation_id="c-1",
        ).send(x=1)

        args, kwargs = broker.publish.call_args
        request = args[1]
        assert request.id == "c-1"
        assert request.params == {"x": 1}
        assert kwargs["delay"] == 10.0
        assert kwargs["expire_at"] == 9999.0
        assert kwargs["reply_to"] == "rq"
        assert kwargs["headers"]["h"] == "v"
        assert kwargs["headers"]["traceparent"] == "tp"

    @pytest.mark.asyncio
    async def test_options_none_values_excluded(self):
        """None values are not included in transport."""
        app, broker = _make_app()
        task = PlanqTask(lambda: None, app, "t", "q")

        await task.options(delay=None).send()

        _, kwargs = broker.publish.call_args
        assert "delay" not in kwargs


# === TestSyncPlanq ===


class TestSyncPlanq:
    """Tests for the SyncPlanq synchronous wrapper."""

    def test_init_does_not_start_thread_or_connect(self):
        """SyncPlanq construction is lazy: no thread, no connect."""
        broker = _make_broker()

        app = SyncPlanq(broker)

        assert app._thread is None
        assert app._loop is None
        broker.connect.assert_not_awaited()
        app.close()

    def test_first_send_starts_thread_and_connects(self):
        """First .send() lazily creates the loop and connects."""
        broker = _make_broker()

        app = SyncPlanq(broker)

        @app.task("t", queue_name="q")
        def my_task(x: int): ...

        my_task.send(x=1)

        assert app._thread is not None
        assert app._thread.is_alive()
        broker.connect.assert_awaited_once()
        app.close()

    def test_subsequent_sends_publish_again(self):
        """Two .send() calls publish two messages on the same loop."""
        broker = _make_broker()

        app = SyncPlanq(broker)

        @app.task("t", queue_name="q")
        def my_task(x: int): ...

        my_task.send(x=1)
        my_task.send(x=2)

        assert broker.publish.call_count == 2
        app.close()

    def test_concurrent_first_send_thread_safe(self):
        """Many threads racing on first .send() init the loop once."""
        broker = _make_broker()

        async def slow_connect():
            await asyncio.sleep(0.05)

        broker.connect = AsyncMock(side_effect=slow_connect)

        app = SyncPlanq(broker)

        @app.task("t", queue_name="q")
        def my_task(x: int): ...

        n_threads = 16
        barrier = threading.Barrier(n_threads)
        errors: list[BaseException] = []
        loops_seen: set[int] = set()
        lock = threading.Lock()

        def worker():
            try:
                barrier.wait()
                my_task.send(x=1)
                with lock:
                    if app._loop is not None:
                        loops_seen.add(id(app._loop))
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        assert len(loops_seen) == 1
        assert broker.publish.call_count == n_threads
        app.close()

    def test_ensure_loop_returns_if_loop_set_after_fast_path(self):
        """Double-checked-locking under-lock guard.

        If a competing thread sets the loop between the fast-path check and
        acquiring the init lock, ``_ensure_loop`` must return from inside the
        lock without starting a second background thread. The real concurrent
        test above cannot deterministically force that exact interleaving, so
        here the lock itself sets the loop on entry -- making the second
        (under-lock) check see it and take the early-return branch.
        """
        broker = _make_broker()
        app = SyncPlanq(broker)

        sentinel = object()
        real_lock = app._init_lock

        class _RacingLock:
            def __enter__(self):
                # The competitor "won": the loop is set the instant we enter
                # the critical section, so the under-lock check returns early.
                app._loop = sentinel
                return real_lock.__enter__()

            def __exit__(self, *exc):
                return real_lock.__exit__(*exc)

        app._init_lock = _RacingLock()

        assert app._loop is None  # fast path sees None -> proceeds to the lock
        app._ensure_loop()  # under-lock check is now True -> early return

        assert app._loop is sentinel
        assert app._thread is None  # no second loop/thread was started

        app._loop = None  # drop the sentinel before cleanup
        app.close()

    def test_close_without_send_is_safe(self):
        """close() on a never-used app does not touch the broker."""
        broker = _make_broker()

        app = SyncPlanq(broker)
        app.close()

        broker.connect.assert_not_awaited()
        broker.disconnect.assert_not_awaited()

    def test_send_returns_message_id(self):
        """Sync .send() returns a plain string message ID."""
        broker = _make_broker()
        broker.connect = AsyncMock()
        broker.disconnect = AsyncMock()

        app = SyncPlanq(broker)

        @app.task("t", queue_name="q")
        def my_task(x: int): ...

        result = my_task.send(x=1)

        assert result == "msg-id-123"
        app.close()

    def test_send_publishes_correct_request(self):
        """Sync .send() publishes the correct JsonRpcRequest."""
        broker = _make_broker()
        broker.connect = AsyncMock()
        broker.disconnect = AsyncMock()

        app = SyncPlanq(broker)

        @app.task("resize", queue_name="images")
        def resize(url: str): ...

        resize.send(url="http://img.png")

        args, _ = broker.publish.call_args
        assert args[0] == "images"
        request = args[1]
        assert isinstance(request, JsonRpcRequest)
        assert request.method == "resize"
        assert request.params == {"url": "http://img.png"}
        app.close()

    def test_close_disconnects_and_stops_thread(self):
        """close() disconnects broker and joins thread after send."""
        broker = _make_broker()
        broker.connect = AsyncMock()
        broker.disconnect = AsyncMock()

        app = SyncPlanq(broker)

        @app.task("t", queue_name="q")
        def my_task(x: int): ...

        my_task.send(x=1)
        thread = app._thread
        app.close()

        broker.disconnect.assert_awaited_once()
        assert thread is not None
        assert not thread.is_alive()

    def test_context_manager_no_send_is_noop(self):
        """`with SyncPlanq(...)` without sends performs no I/O."""
        broker = _make_broker()
        broker.connect = AsyncMock()
        broker.disconnect = AsyncMock()

        with SyncPlanq(broker) as app:
            assert app._thread is None
            assert app._loop is None

        broker.connect.assert_not_awaited()
        broker.disconnect.assert_not_awaited()

    def test_context_manager_with_send(self):
        """`with SyncPlanq(...)` connects on send and disconnects."""
        broker = _make_broker()
        broker.connect = AsyncMock()
        broker.disconnect = AsyncMock()

        with SyncPlanq(broker) as app:

            @app.task("t", queue_name="q")
            def my_task(x: int): ...

            my_task.send(x=1)
            assert app._thread is not None
            assert app._thread.is_alive()
            thread = app._thread

        broker.connect.assert_awaited_once()
        broker.disconnect.assert_awaited_once()
        assert not thread.is_alive()

    def test_task_decorator_works(self):
        """@app.task() registers routes and returns PlanqTask."""
        broker = _make_broker()
        broker.connect = AsyncMock()
        broker.disconnect = AsyncMock()

        app = SyncPlanq(broker)

        @app.task("greet", queue_name="tasks")
        def greet(name: str): ...

        assert "greet" in app.routes
        assert isinstance(greet, PlanqTask)
        app.close()

    def test_options_send_with_delay(self):
        """Sync .options().send() forwards transport options."""
        broker = _make_broker()
        broker.connect = AsyncMock()
        broker.disconnect = AsyncMock()

        app = SyncPlanq(broker)

        @app.task("t", queue_name="q")
        def my_task(x: int): ...

        my_task.options(delay=30.0).send(x=1)

        _, kwargs = broker.publish.call_args
        assert kwargs["delay"] == 30.0
        app.close()


# === TestSyncPlanqBindLoop ===


class _ExternalLoop:
    """Helper that runs an asyncio loop in a daemon thread."""

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        self._ready.wait()

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.call_soon(self._ready.set)
        self.loop.run_forever()

    def close(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join()
        self.loop.close()


class TestSyncPlanqBindLoop:
    """Tests for SyncPlanq.bind_loop() (embedded ASGI mode)."""

    def test_bind_loop_does_not_start_thread(self):
        """bind_loop() reuses the external loop, no bg thread."""
        broker = _make_broker()
        external = _ExternalLoop()
        try:
            app = SyncPlanq(broker)
            app.bind_loop(external.loop)

            assert app._loop is external.loop
            assert app._thread is None
        finally:
            external.close()

    def test_bind_loop_dispatches_to_external_loop(self):
        """Publish coro runs on the bound external loop."""
        broker = _make_broker()
        captured: dict[str, Any] = {}

        async def capture_loop(*args: Any, **kwargs: Any) -> str:
            captured["publish_loop"] = asyncio.get_running_loop()
            return "msg-id-123"

        broker.publish = AsyncMock(side_effect=capture_loop)

        external = _ExternalLoop()
        try:
            app = SyncPlanq(broker)
            app.bind_loop(external.loop)

            @app.task("t", queue_name="q")
            def my_task(x: int): ...

            result = my_task.send(x=1)

            assert result == "msg-id-123"
            assert captured["publish_loop"] is external.loop
        finally:
            external.close()

    def test_bind_loop_connect_runs_on_external_loop(self):
        """Lazy connect also runs on the bound external loop."""
        broker = _make_broker()
        captured: dict[str, Any] = {}

        async def capture_connect_loop() -> None:
            captured["connect_loop"] = asyncio.get_running_loop()

        broker.connect = AsyncMock(side_effect=capture_connect_loop)

        external = _ExternalLoop()
        try:
            app = SyncPlanq(broker)
            app.bind_loop(external.loop)

            @app.task("t", queue_name="q")
            def my_task(x: int): ...

            my_task.send(x=1)

            assert captured["connect_loop"] is external.loop
        finally:
            external.close()

    def test_bind_loop_close_does_not_stop_external_loop(self):
        """close() leaves the externally-owned loop running."""
        broker = _make_broker()
        external = _ExternalLoop()
        try:
            app = SyncPlanq(broker)
            app.bind_loop(external.loop)

            @app.task("t", queue_name="q")
            def my_task(x: int): ...

            my_task.send(x=1)
            app.close()

            assert external.loop.is_running()
            assert external.thread.is_alive()
            broker.disconnect.assert_not_awaited()
        finally:
            external.close()

    def test_bind_loop_after_send_raises(self):
        """Calling bind_loop after the bg loop was created raises."""
        broker = _make_broker()
        app = SyncPlanq(broker)

        @app.task("t", queue_name="q")
        def my_task(x: int): ...

        my_task.send(x=1)

        external = _ExternalLoop()
        try:
            with pytest.raises(RuntimeError, match="already"):
                app.bind_loop(external.loop)
        finally:
            external.close()
            app.close()

    def test_bind_loop_twice_raises(self):
        """Calling bind_loop twice raises."""
        broker = _make_broker()
        external1 = _ExternalLoop()
        external2 = _ExternalLoop()
        try:
            app = SyncPlanq(broker)
            app.bind_loop(external1.loop)

            with pytest.raises(RuntimeError, match="already"):
                app.bind_loop(external2.loop)
        finally:
            external1.close()
            external2.close()


# === TestPlanqApp ===


class TestPlanqApp:
    """Tests for the Planq central application object."""

    def test_init_stores_broker(self):
        """Planq stores the broker instance."""
        broker = _make_broker()

        app = Planq(broker)

        assert app.broker is broker

    def test_init_empty_routes(self):
        """Planq starts with an empty routes dict."""
        broker = _make_broker()

        app = Planq(broker)

        assert app.routes == {}

    def test_task_decorator_registers_route(self):
        """task() registers a TaskRoute in self.routes."""
        broker = _make_broker()
        app = Planq(broker)

        @app.task("images.resize")
        def resize(url):
            pass

        assert "images.resize" in app.routes
        route = app.routes["images.resize"]
        assert isinstance(route, TaskRoute)
        assert route.handler is resize.__wrapped__
        assert route.mode == ExecutionMode.ASYNC

    def test_task_decorator_returns_planq_task(self):
        """task() returns a PlanqTask instance."""
        broker = _make_broker()
        app = Planq(broker)

        @app.task("images.resize")
        def resize(url):
            pass

        assert isinstance(resize, PlanqTask)
        assert resize.name == "images.resize"

    def test_task_auto_name(self):
        """task() auto-generates name from module path."""
        broker = _make_broker()
        app = Planq(broker)

        @app.task()
        def resize_image():
            pass

        # Name should be module.qualname
        expected = (
            f"{resize_image.__wrapped__.__module__}"
            f".{resize_image.__wrapped__.__qualname__}"
        )
        assert expected in app.routes

    def test_task_duplicate_name_raises(self):
        """Registering duplicate task name raises ValueError."""
        broker = _make_broker()
        app = Planq(broker)

        @app.task("dup.task")
        def first():
            pass

        with pytest.raises(ValueError, match="already registered"):

            @app.task("dup.task")
            def second():
                pass

    def test_task_with_execution_options(self):
        """task() forwards execution options to TaskRoute."""
        broker = _make_broker()
        app = Planq(broker)

        @app.task(
            "cpu.work",
            mode=ExecutionMode.PROCESS,
            time_limit=30.0,
            grace_period=5.0,
            max_retries=5,
            retry_on=ValueError,
        )
        def cpu_work():
            pass

        route = app.routes["cpu.work"]
        assert route.mode == ExecutionMode.PROCESS
        assert route.time_limit == 30.0
        assert route.grace_period == 5.0
        assert route.max_retries == 5
        assert route.retry_on is ValueError

    def test_task_with_queue_name(self):
        """task() forwards queue_name to TaskRoute and
        PlanqTask."""
        broker = _make_broker()
        app = Planq(broker)

        @app.task("t", queue_name="high-priority")
        def handler():
            pass

        assert app.routes["t"].queue_name == "high-priority"
        assert handler.queue_name == "high-priority"

    def test_handler_alias(self):
        """handler is an alias for task at class level."""
        assert Planq.handler is Planq.task

    def test_rpc_alias(self):
        """rpc is an alias for task at class level."""
        assert Planq.rpc is Planq.task

    def test_task_stores_param_meta(self):
        """task() populates param_meta via analyze_signature."""
        broker = _make_broker()
        app = Planq(broker)

        @app.task("greet")
        def greet(name: str, count: int = 1):
            pass

        route = app.routes["greet"]
        assert route.param_meta is not None
        assert len(route.param_meta.params) == 2


# === TestEagerMode ===


class TestEagerMode:
    """Tests for Planq eager mode (no broker)."""

    def test_planq_eager_flag_default_false(self) -> None:
        broker = _make_broker()
        app = Planq(broker)
        assert app.eager is False

    def test_planq_eager_flag_true(self) -> None:
        broker = _make_broker()
        app = Planq(broker, eager=True)
        assert app.eager is True

    @pytest.mark.asyncio
    async def test_eager_send_calls_handler_directly(
        self,
    ) -> None:
        broker = _make_broker()
        app = Planq(broker, eager=True)

        called_with: dict[str, Any] = {}

        @app.task(name="test.echo")
        async def echo(msg: str) -> str:
            called_with["msg"] = msg
            return msg

        result = await echo.send(msg="hello")
        assert called_with["msg"] == "hello"
        assert result == "eager"
        broker.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_eager_send_via_options(self) -> None:
        broker = _make_broker()
        app = Planq(broker, eager=True)

        called = False

        @app.task(name="test.work")
        async def work(x: int) -> int:
            nonlocal called
            called = True
            return x * 2

        result = await work.options(delay=10).send(x=5)
        assert called
        assert result == "eager"
        broker.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_eager_sync_handler(self) -> None:
        broker = _make_broker()
        app = Planq(broker, eager=True)

        @app.task(name="test.sync")
        def sync_task(n: int) -> int:
            return n + 1

        result = await sync_task.send(n=5)
        assert result == "eager"

    @pytest.mark.asyncio
    async def test_eager_send_with_kwargs(self) -> None:
        broker = _make_broker()
        app = Planq(broker, eager=True)

        received_args: dict[str, Any] = {}

        @app.task(name="test.kwargs")
        async def kwargs_task(a: int, b: int) -> int:
            received_args.update({"a": a, "b": b})
            return a + b

        result = await kwargs_task.send(a=1, b=2)
        assert received_args == {"a": 1, "b": 2}
        assert result == "eager"

    @pytest.mark.asyncio
    async def test_eager_send_with_list_params(
        self,
    ) -> None:
        """Eager mode handles list params (positional)."""
        broker = _make_broker()
        app = Planq(broker, eager=True)

        received: list[Any] = []

        @app.task(name="test.list_params")
        async def list_task(a: int, b: int) -> None:
            received.extend([a, b])

        request = JsonRpcRequest(
            jsonrpc="2.0",
            method="test.list_params",
            params=[1, 2],
            id=None,
        )
        result = await list_task._send(request, {})
        assert received == [1, 2]
        assert result == "eager"

    @pytest.mark.asyncio
    async def test_eager_send_with_none_params(
        self,
    ) -> None:
        """Eager mode handles None params (no arguments)."""
        broker = _make_broker()
        app = Planq(broker, eager=True)

        called = False

        @app.task(name="test.no_params")
        async def no_params_task() -> None:
            nonlocal called
            called = True

        request = JsonRpcRequest(
            jsonrpc="2.0",
            method="test.no_params",
            params=None,
            id=None,
        )
        result = await no_params_task._send(request, {})
        assert called
        assert result == "eager"

    @pytest.mark.asyncio
    async def test_non_eager_publishes_normally(
        self,
    ) -> None:
        broker = _make_broker()
        app = Planq(broker, eager=False)

        @app.task(name="test.normal")
        async def normal(x: int) -> int:
            return x

        await normal.send(x=1)
        broker.publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_reconnects_after_broker_disconnect(
        self,
    ) -> None:
        """Producer auto-reconnects when broker becomes disconnected.

        Reproduces the embedded-mode bug: the consumer's
        ``async with self.broker:`` block exits (e.g. crash,
        lifespan shutdown) and clears the broker's client. The
        producer's next ``.send()`` must call ``connect()`` again
        instead of relying on a stale "already connected" cache.
        """
        state = {"connected": False}

        async def fake_connect() -> None:
            state["connected"] = True

        async def fake_disconnect() -> None:
            state["connected"] = False

        async def fake_publish(*_args: Any, **_kwargs: Any) -> str:
            if not state["connected"]:
                raise RuntimeError("not connected")
            return "msg-id"

        broker = MagicMock()
        broker.connect = AsyncMock(side_effect=fake_connect)
        broker.disconnect = AsyncMock(side_effect=fake_disconnect)
        broker.publish = AsyncMock(side_effect=fake_publish)

        app = Planq(broker, eager=False)

        @app.task("t", queue_name="q")
        async def my_task(x: int) -> int:
            return x

        result1 = await my_task.send(x=1)
        assert result1 == "msg-id"

        await broker.disconnect()
        assert state["connected"] is False

        result2 = await my_task.send(x=2)
        assert result2 == "msg-id"
        assert state["connected"] is True


# === TestSyncPlanqEager ===


class TestSyncPlanqEager:
    """Tests for SyncPlanq in eager mode."""

    def test_sync_eager_no_background_thread(self) -> None:
        broker = _make_broker()
        app = SyncPlanq(broker, eager=True)
        assert app._thread is None
        assert app._loop is None

    def test_sync_eager_send(self) -> None:
        broker = _make_broker()
        app = SyncPlanq(broker, eager=True)

        called = False

        @app.task(name="test.sync_eager")
        def my_task(x: int) -> int:
            nonlocal called
            called = True
            return x

        result = my_task.send(x=42)
        assert called
        assert result == "eager"

    def test_sync_eager_close_is_safe(self) -> None:
        broker = _make_broker()
        app = SyncPlanq(broker, eager=True)
        app.close()  # should not raise


# === TestGetQueueDepth ===


class TestGetQueueDepth:
    """Tests for Planq.get_queue_depth and SyncPlanq.get_queue_depth."""

    @pytest.mark.asyncio
    async def test_async_app_get_queue_depth_delegates_to_broker(self):
        """Async Planq.get_queue_depth returns QueueStats from the broker."""
        from planq.models import JsonRpcRequest
        from planq.providers.memory import InMemoryBroker
        from planq.stats import QueueStats

        app = Planq(broker=InMemoryBroker("memory://"))
        await app.broker.connect()
        await app.broker.publish(
            "default",
            JsonRpcRequest(
                jsonrpc="2.0", method="noop", params=None, id=None
            ),
        )
        stats = await app.get_queue_depth("default")
        assert isinstance(stats, QueueStats)
        assert stats.pending == 1

    def test_sync_app_get_queue_depth_returns_stats(self):
        """SyncPlanq.get_queue_depth bridges to the background loop.

        Uses a real enqueue (task.send) to put one message on the
        queue before reading depth.
        """
        from planq.providers.memory import InMemoryBroker
        from planq.stats import QueueStats

        app = SyncPlanq(broker=InMemoryBroker("memory://"))
        with app:

            @app.task("depth.noop", queue_name="default")
            def noop_task() -> None: ...

            noop_task.send()
            stats = app.get_queue_depth("default")
        assert isinstance(stats, QueueStats)
        assert stats.pending == 1

    def test_sync_eager_get_queue_depth_uses_asyncio_run(self):
        """In eager mode, get_queue_depth runs the coro via asyncio.run."""
        from planq.providers.memory import InMemoryBroker
        from planq.stats import QueueStats

        app = SyncPlanq(broker=InMemoryBroker("memory://"), eager=True)
        stats = app.get_queue_depth("default")
        assert isinstance(stats, QueueStats)
        assert stats.pending == 0
