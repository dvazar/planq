"""Tests for planq.contrib.django.middleware."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from planq.contrib.django.middleware import (
    DjangoDbMiddleware,
)


@pytest.fixture
def middleware() -> DjangoDbMiddleware:
    return DjangoDbMiddleware()


@pytest.fixture
def mock_message() -> MagicMock:
    return MagicMock()


class TestDjangoDbMiddleware:
    @pytest.mark.asyncio
    @patch("planq.contrib.django.middleware.close_old_connections")
    async def test_calls_close_before_and_after(
        self,
        mock_close: MagicMock,
        middleware: DjangoDbMiddleware,
        mock_message: MagicMock,
    ) -> None:
        call_next = AsyncMock(return_value=None)
        await middleware(mock_message, call_next)

        assert mock_close.call_count == 2
        call_next.assert_called_once_with(mock_message)

    @pytest.mark.asyncio
    @patch("planq.contrib.django.middleware.close_old_connections")
    async def test_calls_close_on_exception(
        self,
        mock_close: MagicMock,
        middleware: DjangoDbMiddleware,
        mock_message: MagicMock,
    ) -> None:
        call_next = AsyncMock(side_effect=ValueError("boom"))
        with pytest.raises(ValueError, match="boom"):
            await middleware(mock_message, call_next)

        assert mock_close.call_count == 2

    @pytest.mark.asyncio
    @patch("planq.contrib.django.middleware.close_old_connections")
    async def test_returns_call_next_result(
        self,
        mock_close: MagicMock,
        middleware: DjangoDbMiddleware,
        mock_message: MagicMock,
    ) -> None:
        expected = MagicMock()
        call_next = AsyncMock(return_value=expected)
        result = await middleware(mock_message, call_next)
        assert result is expected
