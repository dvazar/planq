"""Tests for planq.contrib.django.setup."""

from __future__ import annotations

import pytest
from django.test import override_settings

import planq.contrib.django.setup as _setup_mod
from planq import Planq, SyncPlanq
from planq.contrib.django.setup import (
    configure_planq,
    get_planq_app,
    get_planq_middlewares,
)
from planq.providers.memory import InMemoryBroker


@pytest.fixture(autouse=True)
def _reset_singleton() -> None:
    """Reset the module-level singleton before each test."""
    _setup_mod._app = None


class TestGetPlanqApp:
    """Tests for get_planq_app()."""

    def test_raises_before_configure(self) -> None:
        with pytest.raises(RuntimeError, match="not configured"):
            get_planq_app()

    def test_returns_app_after_configure(self) -> None:
        configure_planq()
        app = get_planq_app()
        assert isinstance(app, Planq)

    def test_returns_same_instance(self) -> None:
        configure_planq()
        assert get_planq_app() is get_planq_app()


class TestConfigurePlanq:
    """Tests for configure_planq()."""

    def test_creates_correct_broker(self) -> None:
        app = configure_planq()
        assert isinstance(app.broker, InMemoryBroker)

    @override_settings(
        PLANQ={
            "BROKER_CLASS": ("planq.providers.memory.InMemoryBroker"),
            "BROKER_OPTIONS": {"dsn": "memory://test"},
        }
    )
    def test_passes_broker_options(self) -> None:
        app = configure_planq()
        assert app.broker.dsn == "memory://test"

    @override_settings(
        PLANQ={
            "BROKER_CLASS": ("planq.providers.memory.InMemoryBroker"),
            "EAGER": True,
        }
    )
    def test_eager_flag(self) -> None:
        app = configure_planq()
        assert app.eager is True

    def test_eager_default_false(self) -> None:
        app = configure_planq()
        assert app.eager is False

    def test_lazy_connection(self) -> None:
        """configure_planq() does not call broker.connect()."""
        from unittest.mock import AsyncMock, patch

        with patch.object(
            InMemoryBroker, "connect", new_callable=AsyncMock
        ) as mock_connect:
            configure_planq()
            mock_connect.assert_not_called()

    def test_re_entrant(self) -> None:
        """Calling configure_planq() again replaces singleton."""
        app1 = configure_planq()
        app2 = configure_planq()
        assert app1 is not app2
        assert get_planq_app() is app2


class TestAppClass:
    """Tests for the PLANQ['APP_CLASS'] setting."""

    def test_default_app_class_is_planq(self) -> None:
        """Without APP_CLASS, configure_planq() builds Planq."""
        app = configure_planq()
        assert type(app) is Planq

    @override_settings(
        PLANQ={
            "BROKER_CLASS": ("planq.providers.memory.InMemoryBroker"),
            "APP_CLASS": "planq.SyncPlanq",
        }
    )
    def test_app_class_sync_planq(self) -> None:
        """APP_CLASS dotted path resolves to SyncPlanq."""
        app = configure_planq()
        assert type(app) is SyncPlanq

    @override_settings(
        PLANQ={
            "BROKER_CLASS": ("planq.providers.memory.InMemoryBroker"),
            "APP_CLASS": "planq.SyncPlanq",
        }
    )
    def test_sync_app_class_does_not_connect(self) -> None:
        """SyncPlanq via APP_CLASS does not start loop on configure."""
        app = configure_planq()
        assert isinstance(app, SyncPlanq)
        assert app._loop is None
        assert app._thread is None

    @override_settings(
        PLANQ={
            "BROKER_CLASS": ("planq.providers.memory.InMemoryBroker"),
            "APP_CLASS": "planq.SyncPlanq",
            "EAGER": True,
        }
    )
    def test_app_class_eager_passed(self) -> None:
        """APP_CLASS receives EAGER flag from settings."""
        app = configure_planq()
        assert type(app) is SyncPlanq
        assert app.eager is True

    @override_settings(
        PLANQ={
            "BROKER_CLASS": ("planq.providers.memory.InMemoryBroker"),
            "APP_CLASS": "nonexistent.module.Class",
        }
    )
    def test_app_class_invalid_path_raises(self) -> None:
        """Invalid APP_CLASS path raises ImportError."""
        with pytest.raises(ImportError):
            configure_planq()


class TestGetPlanqMiddlewares:
    """Tests for get_planq_middlewares()."""

    def test_default_includes_db_and_deadline(self) -> None:
        from planq.contrib.django.middleware import (
            DjangoDbMiddleware,
        )
        from planq.middleware import DeadlineMiddleware

        configure_planq()
        mws = get_planq_middlewares()
        assert isinstance(mws[0], DjangoDbMiddleware)
        assert isinstance(mws[1], DeadlineMiddleware)

    def test_django_db_middleware_always_first(self) -> None:
        from planq.contrib.django.middleware import (
            DjangoDbMiddleware,
        )

        configure_planq()
        mws = get_planq_middlewares()
        assert isinstance(mws[0], DjangoDbMiddleware)

    @override_settings(
        PLANQ={
            "BROKER_CLASS": ("planq.providers.memory.InMemoryBroker"),
            "MIDDLEWARE": [],
        }
    )
    def test_empty_middleware_still_has_db(self) -> None:
        from planq.contrib.django.middleware import (
            DjangoDbMiddleware,
        )

        configure_planq()
        mws = get_planq_middlewares()
        assert len(mws) == 1
        assert isinstance(mws[0], DjangoDbMiddleware)
