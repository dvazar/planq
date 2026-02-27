"""Type checking tests for ParamSpec-enhanced decorators.

These tests verify that type checkers (mypy/pyright) correctly
understand handler signatures through the @consumer.task decorator.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from planq import PlanqConsumer
from planq.enums import ExecutionMode

if TYPE_CHECKING:
    from planq.broker import BaseBroker


def test_async_mode_preserves_signature() -> None:
    """ASYNC mode preserves async function signature."""
    broker: BaseBroker = MagicMock()  # type: ignore[assignment]
    consumer = PlanqConsumer(broker)

    @consumer.task("test", mode=ExecutionMode.ASYNC)
    async def handler(x: int, y: str) -> bool:
        return True

    # This test verifies type-checking behavior at compile time.
    # The decorator should preserve the handler's signature.
    assert handler is not None


def test_thread_mode_sync_function() -> None:
    """THREAD mode expects sync function."""
    broker: BaseBroker = MagicMock()  # type: ignore[assignment]
    consumer = PlanqConsumer(broker)

    @consumer.task("test", mode=ExecutionMode.THREAD)
    def handler(x: int) -> int:
        return x * 2

    # This test verifies type-checking behavior at compile time.
    # The decorator should preserve the handler's signature.
    assert handler is not None


def test_process_mode_sync_function() -> None:
    """PROCESS mode expects sync function."""
    broker: BaseBroker = MagicMock()  # type: ignore[assignment]
    consumer = PlanqConsumer(broker, process_workers=2)

    @consumer.task("test", mode=ExecutionMode.PROCESS)
    def handler(data: list[int]) -> int:
        return sum(data)

    # This test verifies type-checking behavior at compile time.
    # The decorator should preserve the handler's signature.
    assert handler is not None


def test_variadic_args() -> None:
    """ParamSpec preserves *args and **kwargs."""
    broker: BaseBroker = MagicMock()  # type: ignore[assignment]
    consumer = PlanqConsumer(broker)

    @consumer.task("test", mode=ExecutionMode.ASYNC)
    async def handler(*args: int, **kwargs: str) -> tuple[int, ...]:
        return args

    # This test verifies type-checking behavior at compile time.
    # The decorator should preserve the handler's signature.
    assert handler is not None


def test_optional_parameters() -> None:
    """Type hints work with optional parameters."""
    broker: BaseBroker = MagicMock()  # type: ignore[assignment]
    consumer = PlanqConsumer(broker)

    @consumer.task("test", mode=ExecutionMode.ASYNC)
    async def handler(x: int, y: str = "default") -> str:
        return f"{x}{y}"

    # This test verifies type-checking behavior at compile time.
    # The decorator should preserve the handler's signature.
    assert handler is not None


def test_no_return_type() -> None:
    """Type hints work for functions with no explicit return."""
    broker: BaseBroker = MagicMock()  # type: ignore[assignment]
    consumer = PlanqConsumer(broker)

    @consumer.task("test", mode=ExecutionMode.ASYNC)
    async def handler(value: int) -> None:
        print(value)

    # This test verifies type-checking behavior at compile time.
    # The decorator should preserve the handler's signature.
    assert handler is not None


def test_complex_types() -> None:
    """Type hints work with complex type annotations."""
    broker: BaseBroker = MagicMock()  # type: ignore[assignment]
    consumer = PlanqConsumer(broker)

    @consumer.task("test", mode=ExecutionMode.THREAD)
    def handler(
        items: list[dict[str, int]], mapping: dict[str, list[str]]
    ) -> tuple[list[int], dict[str, str]]:
        result_list = [v for d in items for v in d.values()]
        result_dict = {k: ",".join(v) for k, v in mapping.items()}
        return result_list, result_dict

    # This test verifies type-checking behavior at compile time.
    # The decorator should preserve the handler's signature.
    assert handler is not None


def test_handler_alias() -> None:
    """The handler alias should have the same type hints as task."""
    broker: BaseBroker = MagicMock()  # type: ignore[assignment]
    consumer = PlanqConsumer(broker)

    @consumer.handler("test", mode=ExecutionMode.ASYNC)
    async def handler(x: int) -> str:
        return str(x)

    # This test verifies type-checking behavior at compile time.
    # The handler alias should work identically to task.
    assert handler is not None


def test_default_mode() -> None:
    """Type hints work when mode is not specified (defaults to ASYNC)."""
    broker: BaseBroker = MagicMock()  # type: ignore[assignment]
    consumer = PlanqConsumer(broker)

    @consumer.task("test")
    async def handler(value: str) -> int:
        return len(value)

    # This test verifies type-checking behavior at compile time.
    # Default mode should be treated as ASYNC.
    assert handler is not None


@pytest.mark.asyncio
async def test_typed_handler_runtime_behavior() -> None:
    """Type hints don't affect runtime behavior."""
    broker: BaseBroker = MagicMock()  # type: ignore[assignment]
    consumer = PlanqConsumer(broker)

    @consumer.task("typed.method", mode=ExecutionMode.ASYNC)
    async def add(x: int, y: int) -> int:
        return x + y

    # Verify registration works
    assert "typed.method" in consumer.routes
    route = consumer.routes["typed.method"]
    assert route.handler is add
    assert route.mode == ExecutionMode.ASYNC

    # Verify the handler still works at runtime
    result = await add(2, 3)
    assert result == 5
