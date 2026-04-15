"""Tests for planq.contrib.django.middleware."""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock, patch

import pytest

from planq.context import PlanqContext
from planq.contrib.django.middleware import DjangoDbMiddleware
from planq.middleware import Middleware


@pytest.fixture
def middleware() -> DjangoDbMiddleware:
    return DjangoDbMiddleware()


@pytest.fixture
def ctx() -> PlanqContext:
    return PlanqContext()


class TestDjangoDbMiddleware:
    def test_is_middleware_subclass(
        self, middleware: DjangoDbMiddleware
    ) -> None:
        assert isinstance(middleware, Middleware)

    @patch("planq.contrib.django.middleware.close_old_connections")
    def test_before_execute_calls_close(
        self,
        mock_close: MagicMock,
        middleware: DjangoDbMiddleware,
        ctx: PlanqContext,
    ) -> None:
        middleware.before_execute(ctx)
        mock_close.assert_called_once()

    @patch("planq.contrib.django.middleware.close_old_connections")
    def test_after_execute_calls_close(
        self,
        mock_close: MagicMock,
        middleware: DjangoDbMiddleware,
        ctx: PlanqContext,
    ) -> None:
        middleware.after_execute(ctx)
        mock_close.assert_called_once()

    @patch("planq.contrib.django.middleware.close_old_connections")
    def test_both_hooks_call_close_twice_total(
        self,
        mock_close: MagicMock,
        middleware: DjangoDbMiddleware,
        ctx: PlanqContext,
    ) -> None:
        middleware.before_execute(ctx)
        middleware.after_execute(ctx)
        assert mock_close.call_count == 2

    def test_does_not_override_call(
        self, middleware: DjangoDbMiddleware
    ) -> None:
        """DjangoDbMiddleware uses default __call__ (pass-through)."""
        assert type(middleware).__call__ is Middleware.__call__

    def test_hooks_are_sync(self, middleware: DjangoDbMiddleware) -> None:
        """Hooks are sync methods, not coroutines."""
        assert not inspect.iscoroutinefunction(middleware.before_execute)
        assert not inspect.iscoroutinefunction(middleware.after_execute)
