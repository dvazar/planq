"""Tests for Middleware execution hooks called by PlanqConsumer.

Verifies that before_execute / after_execute run in the correct
thread context for each ExecutionMode.
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

import pytest

from planq.app import Planq
from planq.consumer import PlanqConsumer
from planq.context import PlanqContext, get_planq_context
from planq.middleware import Middleware


class HookTrackingMiddleware(Middleware):
    """Records thread IDs and call order for execution hooks."""

    def __init__(self):
        self.before_thread_id: int | None = None
        self.after_thread_id: int | None = None
        self.before_ctx: PlanqContext | None = None
        self.after_ctx: PlanqContext | None = None
        self.call_order: list[str] = []

    def before_execute(self, ctx):
        self.before_thread_id = threading.get_ident()
        self.before_ctx = ctx
        self.call_order.append("before")

    def after_execute(self, ctx):
        self.after_thread_id = threading.get_ident()
        self.after_ctx = ctx
        self.call_order.append("after")


@pytest.fixture
def make_consumer():
    """Factory to create PlanqConsumer with given middlewares."""

    def _make(middlewares):
        broker = MagicMock()
        app = Planq(broker=broker)
        return PlanqConsumer(
            app,
            process_workers=None,
            middlewares=middlewares,
        )

    return _make


class TestExecutionHooksThreadMode:
    """Verify hooks run in the worker thread for THREAD mode."""

    @pytest.mark.asyncio
    async def test_before_execute_runs_in_worker_thread(self, make_consumer):
        """before_execute runs in worker thread, not event loop."""
        tracker = HookTrackingMiddleware()
        consumer = make_consumer([tracker])
        event_loop_thread = threading.get_ident()

        def handler():
            return "ok"

        get_planq_context()

        await consumer._execute_thread(
            handler=handler,
            args=(),
            kwargs={},
            time_limit=None,
        )

        assert tracker.before_thread_id is not None
        assert tracker.before_thread_id != event_loop_thread

    @pytest.mark.asyncio
    async def test_after_execute_runs_in_worker_thread(self, make_consumer):
        """after_execute runs in worker thread, not event loop."""
        tracker = HookTrackingMiddleware()
        consumer = make_consumer([tracker])
        event_loop_thread = threading.get_ident()

        def handler():
            return "ok"

        get_planq_context()

        await consumer._execute_thread(
            handler=handler,
            args=(),
            kwargs={},
            time_limit=None,
        )

        assert tracker.after_thread_id is not None
        assert tracker.after_thread_id != event_loop_thread

    @pytest.mark.asyncio
    async def test_hooks_run_in_same_thread_as_handler(self, make_consumer):
        """Both hooks and handler share the same worker thread."""
        tracker = HookTrackingMiddleware()
        consumer = make_consumer([tracker])
        handler_thread_id = None

        def handler():
            nonlocal handler_thread_id
            handler_thread_id = threading.get_ident()
            return "ok"

        get_planq_context()

        await consumer._execute_thread(
            handler=handler,
            args=(),
            kwargs={},
            time_limit=None,
        )

        assert handler_thread_id is not None
        assert tracker.before_thread_id == handler_thread_id
        assert tracker.after_thread_id == handler_thread_id

    @pytest.mark.asyncio
    async def test_hook_order_is_before_handler_after(self, make_consumer):
        """Hooks wrap handler: before -> handler -> after."""
        call_log: list[str] = []

        class LoggingMiddleware(Middleware):
            def before_execute(self, ctx):
                call_log.append("before")

            def after_execute(self, ctx):
                call_log.append("after")

        consumer = make_consumer([LoggingMiddleware()])

        def handler():
            call_log.append("handler")
            return "ok"

        get_planq_context()

        await consumer._execute_thread(
            handler=handler,
            args=(),
            kwargs={},
            time_limit=None,
        )

        assert call_log == ["before", "handler", "after"]

    @pytest.mark.asyncio
    async def test_after_execute_called_on_handler_exception(
        self, make_consumer
    ):
        """after_execute runs even when handler raises."""
        tracker = HookTrackingMiddleware()
        consumer = make_consumer([tracker])

        def failing_handler():
            raise ValueError("boom")

        get_planq_context()

        with pytest.raises(ValueError, match="boom"):
            await consumer._execute_thread(
                handler=failing_handler,
                args=(),
                kwargs={},
                time_limit=None,
            )

        assert tracker.call_order == ["before", "after"]

    @pytest.mark.asyncio
    async def test_hooks_receive_planq_context(self, make_consumer):
        """Hooks receive the current PlanqContext."""
        tracker = HookTrackingMiddleware()
        consumer = make_consumer([tracker])

        ctx = get_planq_context()

        def handler():
            return "ok"

        await consumer._execute_thread(
            handler=handler,
            args=(),
            kwargs={},
            time_limit=None,
        )

        assert tracker.before_ctx is ctx
        assert tracker.after_ctx is ctx

    @pytest.mark.asyncio
    async def test_hooks_with_time_limit(self, make_consumer):
        """Hooks run correctly when time_limit is set."""
        tracker = HookTrackingMiddleware()
        consumer = make_consumer([tracker])
        event_loop_thread = threading.get_ident()

        def handler():
            return "ok"

        get_planq_context()

        await consumer._execute_thread(
            handler=handler,
            args=(),
            kwargs={},
            time_limit=5.0,
        )

        assert tracker.before_thread_id != event_loop_thread
        assert tracker.after_thread_id != event_loop_thread
        assert tracker.call_order == ["before", "after"]


class TestExecutionHooksAsyncMode:
    """Verify hooks run in the event loop for ASYNC mode."""

    @pytest.mark.asyncio
    async def test_hooks_run_in_event_loop_thread(self, make_consumer):
        """For ASYNC mode, hooks run in event loop thread."""
        tracker = HookTrackingMiddleware()
        consumer = make_consumer([tracker])
        event_loop_thread = threading.get_ident()

        async def handler():
            return "ok"

        get_planq_context()

        await consumer._execute_async(
            handler=handler,
            args=(),
            kwargs={},
            time_limit=None,
        )

        assert tracker.before_thread_id == event_loop_thread
        assert tracker.after_thread_id == event_loop_thread

    @pytest.mark.asyncio
    async def test_hook_order_async(self, make_consumer):
        """Hooks wrap async handler: before -> handler -> after."""
        call_log: list[str] = []

        class LoggingMiddleware(Middleware):
            def before_execute(self, ctx):
                call_log.append("before")

            def after_execute(self, ctx):
                call_log.append("after")

        consumer = make_consumer([LoggingMiddleware()])

        async def handler():
            call_log.append("handler")
            return "ok"

        get_planq_context()

        await consumer._execute_async(
            handler=handler,
            args=(),
            kwargs={},
            time_limit=None,
        )

        assert call_log == ["before", "handler", "after"]

    @pytest.mark.asyncio
    async def test_after_execute_called_on_async_handler_exception(
        self, make_consumer
    ):
        """after_execute runs even when async handler raises."""
        tracker = HookTrackingMiddleware()
        consumer = make_consumer([tracker])

        async def failing_handler():
            raise ValueError("async boom")

        get_planq_context()

        with pytest.raises(ValueError, match="async boom"):
            await consumer._execute_async(
                handler=failing_handler,
                args=(),
                kwargs={},
                time_limit=None,
            )

        assert tracker.call_order == ["before", "after"]


class TestExecutionHooksMultipleMiddlewares:
    """Verify hook ordering with multiple middlewares."""

    @pytest.mark.asyncio
    async def test_before_execute_order_matches_middleware_list(
        self, make_consumer
    ):
        """before_execute called in middleware list order."""
        order: list[str] = []

        class MW_A(Middleware):
            def before_execute(self, ctx):
                order.append("A")

        class MW_B(Middleware):
            def before_execute(self, ctx):
                order.append("B")

        consumer = make_consumer([MW_A(), MW_B()])

        def handler():
            return "ok"

        get_planq_context()

        await consumer._execute_thread(
            handler=handler,
            args=(),
            kwargs={},
            time_limit=None,
        )

        assert order == ["A", "B"]

    @pytest.mark.asyncio
    async def test_after_execute_order_is_reversed(self, make_consumer):
        """after_execute called in reverse middleware list order."""
        order: list[str] = []

        class MW_A(Middleware):
            def after_execute(self, ctx):
                order.append("A")

        class MW_B(Middleware):
            def after_execute(self, ctx):
                order.append("B")

        consumer = make_consumer([MW_A(), MW_B()])

        def handler():
            return "ok"

        get_planq_context()

        await consumer._execute_thread(
            handler=handler,
            args=(),
            kwargs={},
            time_limit=None,
        )

        assert order == ["B", "A"]

    @pytest.mark.asyncio
    async def test_noop_middlewares_do_not_interfere(self, make_consumer):
        """Middlewares without hook overrides don't break the chain."""
        order: list[str] = []

        class HookMW(Middleware):
            def before_execute(self, ctx):
                order.append("before")

            def after_execute(self, ctx):
                order.append("after")

        consumer = make_consumer([Middleware(), HookMW(), Middleware()])

        def handler():
            order.append("handler")
            return "ok"

        get_planq_context()

        await consumer._execute_thread(
            handler=handler,
            args=(),
            kwargs={},
            time_limit=None,
        )

        assert order == ["before", "handler", "after"]


class TestExecutionHooksExceptionSafety:
    """Verify hooks handle exceptions correctly."""

    @pytest.mark.asyncio
    async def test_after_execute_runs_when_before_execute_raises(
        self, make_consumer
    ):
        """after_execute runs even if before_execute raises."""
        call_log: list[str] = []

        class FailingBeforeMiddleware(Middleware):
            def before_execute(self, ctx):
                call_log.append("before")
                raise RuntimeError("setup failed")

            def after_execute(self, ctx):
                call_log.append("after")

        consumer = make_consumer([FailingBeforeMiddleware()])

        def handler():
            call_log.append("handler")
            return "ok"

        get_planq_context()

        with pytest.raises(RuntimeError, match="setup failed"):
            await consumer._execute_thread(
                handler=handler,
                args=(),
                kwargs={},
                time_limit=None,
            )

        assert "before" in call_log
        assert "handler" not in call_log
        assert "after" in call_log

    @pytest.mark.asyncio
    async def test_all_after_execute_run_when_one_raises(self, make_consumer):
        """All after_execute hooks run even if one raises."""
        call_log: list[str] = []

        class MW_A(Middleware):
            def after_execute(self, ctx):
                call_log.append("A_after")

        class MW_Fail(Middleware):
            def after_execute(self, ctx):
                call_log.append("Fail_after")
                raise RuntimeError("teardown failed")

        class MW_B(Middleware):
            def after_execute(self, ctx):
                call_log.append("B_after")

        # Order: A, Fail, B
        # Reversed after_execute: B, Fail, A
        consumer = make_consumer([MW_A(), MW_Fail(), MW_B()])

        def handler():
            return "ok"

        get_planq_context()

        await consumer._execute_thread(
            handler=handler,
            args=(),
            kwargs={},
            time_limit=None,
        )

        assert "B_after" in call_log
        assert "Fail_after" in call_log
        assert "A_after" in call_log

    @pytest.mark.asyncio
    async def test_async_after_execute_runs_when_before_raises(
        self, make_consumer
    ):
        """ASYNC mode: after_execute runs if before_execute raises."""
        call_log: list[str] = []

        class FailingBeforeMiddleware(Middleware):
            def before_execute(self, ctx):
                call_log.append("before")
                raise RuntimeError("async setup failed")

            def after_execute(self, ctx):
                call_log.append("after")

        consumer = make_consumer([FailingBeforeMiddleware()])

        async def handler():
            call_log.append("handler")
            return "ok"

        get_planq_context()

        with pytest.raises(RuntimeError, match="async setup failed"):
            await consumer._execute_async(
                handler=handler,
                args=(),
                kwargs={},
                time_limit=None,
            )

        assert "before" in call_log
        assert "handler" not in call_log
        assert "after" in call_log


def _process_handler():
    """Module-level handler for process mode (must be picklable)."""
    return "process result"


class TestExecutionHooksProcessMode:
    """Verify hooks are NOT called for PROCESS mode."""

    @pytest.mark.asyncio
    async def test_hooks_not_called_for_process_mode(self):
        """PROCESS mode does not call execution hooks."""
        tracker = HookTrackingMiddleware()
        broker = MagicMock()
        app = Planq(broker=broker)
        consumer = PlanqConsumer(
            app,
            process_workers=2,
            middlewares=[tracker],
        )

        try:
            get_planq_context()

            result = await consumer._execute_process(
                handler=_process_handler,
                args=(),
                kwargs={},
                time_limit=None,
                grace_period=None,
            )

            assert result == "process result"
            assert tracker.before_thread_id is None
            assert tracker.after_thread_id is None
        finally:
            if consumer._pool:
                consumer._pool.shutdown(wait=True)
