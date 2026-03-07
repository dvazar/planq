"""Comprehensive tests for Planq app and PlanqTask."""

from __future__ import annotations

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

    def test_init_starts_thread_and_connects(self):
        """SyncPlanq starts a daemon thread and connects the
        broker."""
        broker = _make_broker()
        broker.connect = AsyncMock()
        broker.disconnect = AsyncMock()

        app = SyncPlanq(broker)

        assert app._thread.is_alive()
        broker.connect.assert_awaited_once()
        app.close()

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
        """close() disconnects broker and joins thread."""
        broker = _make_broker()
        broker.connect = AsyncMock()
        broker.disconnect = AsyncMock()

        app = SyncPlanq(broker)
        app.close()

        broker.disconnect.assert_awaited_once()
        assert not app._thread.is_alive()

    def test_context_manager(self):
        """with SyncPlanq(...) connects and disconnects."""
        broker = _make_broker()
        broker.connect = AsyncMock()
        broker.disconnect = AsyncMock()

        with SyncPlanq(broker) as app:
            assert app._thread.is_alive()

        broker.connect.assert_awaited_once()
        broker.disconnect.assert_awaited_once()
        assert not app._thread.is_alive()

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
