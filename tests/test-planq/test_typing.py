"""Type checking tests for @app.task() decorator.

These tests verify that @app.task() correctly wraps handlers
in PlanqTask objects and that the wrapped functions remain
callable at runtime.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from planq.app import Planq, PlanqTask
from planq.consumer import PlanqConsumer
from planq.enums import ExecutionMode


def test_async_mode_returns_planq_task() -> None:
    """ASYNC mode returns a PlanqTask wrapper."""
    broker = MagicMock()
    app = Planq(broker=broker)

    @app.task("test", mode=ExecutionMode.ASYNC)
    async def handler(x: int, y: str) -> bool:
        return True

    assert isinstance(handler, PlanqTask)
    assert handler._func is not None


def test_thread_mode_returns_planq_task() -> None:
    """THREAD mode returns a PlanqTask wrapper."""
    broker = MagicMock()
    app = Planq(broker=broker)

    @app.task("test", mode=ExecutionMode.THREAD)
    def handler(x: int) -> int:
        return x * 2

    assert isinstance(handler, PlanqTask)


def test_process_mode_returns_planq_task() -> None:
    """PROCESS mode returns a PlanqTask wrapper."""
    broker = MagicMock()
    app = Planq(broker=broker)

    @app.task("test", mode=ExecutionMode.PROCESS)
    def handler(data: list[int]) -> int:
        return sum(data)

    assert isinstance(handler, PlanqTask)


def test_variadic_args() -> None:
    """PlanqTask wraps handlers with *args and **kwargs."""
    broker = MagicMock()
    app = Planq(broker=broker)

    @app.task("test", mode=ExecutionMode.ASYNC)
    async def handler(*args: int, **kwargs: str) -> tuple[int, ...]:
        return args

    assert isinstance(handler, PlanqTask)


def test_optional_parameters() -> None:
    """PlanqTask wraps handlers with optional parameters."""
    broker = MagicMock()
    app = Planq(broker=broker)

    @app.task("test", mode=ExecutionMode.ASYNC)
    async def handler(x: int, y: str = "default") -> str:
        return f"{x}{y}"

    assert isinstance(handler, PlanqTask)


def test_no_return_type() -> None:
    """PlanqTask wraps handlers with no explicit return."""
    broker = MagicMock()
    app = Planq(broker=broker)

    @app.task("test", mode=ExecutionMode.ASYNC)
    async def handler(value: int) -> None:
        print(value)

    assert isinstance(handler, PlanqTask)


def test_complex_types() -> None:
    """PlanqTask wraps handlers with complex type annotations."""
    broker = MagicMock()
    app = Planq(broker=broker)

    @app.task("test", mode=ExecutionMode.THREAD)
    def handler(
        items: list[dict[str, int]], mapping: dict[str, list[str]]
    ) -> tuple[list[int], dict[str, str]]:
        result_list = [v for d in items for v in d.values()]
        result_dict = {k: ",".join(v) for k, v in mapping.items()}
        return result_list, result_dict

    assert isinstance(handler, PlanqTask)


def test_handler_alias() -> None:
    """app.handler() is an alias for app.task()."""
    broker = MagicMock()
    app = Planq(broker=broker)

    @app.handler("test", mode=ExecutionMode.ASYNC)
    async def handler(x: int) -> str:
        return str(x)

    assert isinstance(handler, PlanqTask)
    assert "test" in app.routes


def test_default_mode() -> None:
    """Default mode (ASYNC) works without explicit specification."""
    broker = MagicMock()
    app = Planq(broker=broker)

    @app.task("test")
    async def handler(value: str) -> int:
        return len(value)

    assert isinstance(handler, PlanqTask)


@pytest.mark.asyncio
async def test_typed_handler_runtime_behavior() -> None:
    """PlanqTask preserves runtime behavior of the handler."""
    broker = MagicMock()
    app = Planq(broker=broker)

    @app.task("typed.method", mode=ExecutionMode.ASYNC)
    async def add(x: int, y: int) -> int:
        return x + y

    consumer = PlanqConsumer(app, middlewares=[])

    # Verify registration works
    assert "typed.method" in consumer.routes
    route = consumer.routes["typed.method"]
    assert route.handler is add._func
    assert route.mode == ExecutionMode.ASYNC

    # Verify the handler still works at runtime (PlanqTask.__call__)
    result = await add(2, 3)
    assert result == 5
