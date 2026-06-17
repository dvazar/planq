"""Tests for the planqworker management command."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from django.core.management import call_command
from django.test import override_settings

import planq.contrib.django.setup as _setup_mod
from planq.contrib.django.setup import configure_planq


@pytest.fixture(autouse=True)
def _reset_singleton() -> None:
    _setup_mod._app = None
    configure_planq()


class TestPlanqworkerCommand:
    @patch("planq.contrib.django.management.commands.planqworker.asyncio")
    @patch("planq.contrib.django.management.commands.planqworker.PlanqConsumer")
    def test_runs_with_single_queue(
        self,
        mock_consumer_cls: MagicMock,
        mock_asyncio: MagicMock,
    ) -> None:
        consumer_instance = MagicMock()
        consumer_instance.run_many = AsyncMock()
        mock_consumer_cls.return_value = consumer_instance

        call_command("planqworker", "default")
        mock_asyncio.run.assert_called_once()

    @patch("planq.contrib.django.management.commands.planqworker.asyncio")
    @patch("planq.contrib.django.management.commands.planqworker.PlanqConsumer")
    def test_runs_with_multiple_queues(
        self,
        mock_consumer_cls: MagicMock,
        mock_asyncio: MagicMock,
    ) -> None:
        consumer_instance = MagicMock()
        consumer_instance.run_many = AsyncMock()
        mock_consumer_cls.return_value = consumer_instance

        call_command("planqworker", "default", "emails")
        mock_asyncio.run.assert_called_once()

    @patch("planq.contrib.django.management.commands.planqworker.asyncio")
    @patch("planq.contrib.django.management.commands.planqworker.PlanqConsumer")
    @override_settings(
        PLANQ={
            "BROKER_CLASS": ("planq.providers.memory.InMemoryBroker"),
            "CONSUMER": {"concurrency": 5},
        }
    )
    def test_consumer_settings_from_django(
        self,
        mock_consumer_cls: MagicMock,
        mock_asyncio: MagicMock,
    ) -> None:
        _setup_mod._app = None
        configure_planq()

        consumer_instance = MagicMock()
        consumer_instance.run_many = AsyncMock()
        mock_consumer_cls.return_value = consumer_instance

        call_command("planqworker", "default")
        kwargs = mock_consumer_cls.call_args.kwargs
        assert kwargs["settings"].concurrency == 5

    @patch("planq.contrib.django.management.commands.planqworker.asyncio")
    @patch("planq.contrib.django.management.commands.planqworker.PlanqConsumer")
    def test_cli_concurrency_overrides_settings(
        self,
        mock_consumer_cls: MagicMock,
        mock_asyncio: MagicMock,
    ) -> None:
        consumer_instance = MagicMock()
        consumer_instance.run_many = AsyncMock()
        mock_consumer_cls.return_value = consumer_instance

        call_command("planqworker", "default", "--concurrency", "20")
        kwargs = mock_consumer_cls.call_args.kwargs
        assert kwargs["settings"].concurrency == 20

    @patch("planq.contrib.django.management.commands.planqworker.asyncio")
    @patch("planq.contrib.django.management.commands.planqworker.PlanqConsumer")
    def test_middlewares_include_django_db_first(
        self,
        mock_consumer_cls: MagicMock,
        mock_asyncio: MagicMock,
    ) -> None:
        from planq.contrib.django.middleware import (
            DjangoDbMiddleware,
        )

        consumer_instance = MagicMock()
        consumer_instance.run_many = AsyncMock()
        mock_consumer_cls.return_value = consumer_instance

        call_command("planqworker", "default")
        kwargs = mock_consumer_cls.call_args.kwargs
        middlewares = kwargs["middlewares"]
        assert isinstance(middlewares[0], DjangoDbMiddleware)

    @patch("planq.contrib.django.management.commands.planqworker.asyncio")
    @patch("planq.contrib.django.management.commands.planqworker.PlanqConsumer")
    def test_heartbeat_file_passed_to_settings(
        self,
        mock_consumer_cls: MagicMock,
        mock_asyncio: MagicMock,
    ) -> None:
        consumer_instance = MagicMock()
        mock_consumer_cls.return_value = consumer_instance

        call_command("planqworker", "default", "--heartbeat-file", "/tmp/hb")
        kwargs = mock_consumer_cls.call_args.kwargs
        assert kwargs["settings"].heartbeat_file == "/tmp/hb"

    @patch("planq.contrib.django.management.commands.planqworker.asyncio")
    @patch("planq.contrib.django.management.commands.planqworker.PlanqConsumer")
    def test_heartbeat_interval_passed_to_settings(
        self,
        mock_consumer_cls: MagicMock,
        mock_asyncio: MagicMock,
    ) -> None:
        consumer_instance = MagicMock()
        mock_consumer_cls.return_value = consumer_instance

        call_command("planqworker", "default", "--heartbeat-interval", "5")
        kwargs = mock_consumer_cls.call_args.kwargs
        assert kwargs["settings"].heartbeat_interval == 5.0

    @patch("planq.contrib.django.management.commands.planqworker.asyncio")
    @patch("planq.contrib.django.management.commands.planqworker.PlanqConsumer")
    def test_heartbeat_defaults_when_not_passed(
        self,
        mock_consumer_cls: MagicMock,
        mock_asyncio: MagicMock,
    ) -> None:
        consumer_instance = MagicMock()
        mock_consumer_cls.return_value = consumer_instance

        call_command("planqworker", "default")
        kwargs = mock_consumer_cls.call_args.kwargs
        assert kwargs["settings"].heartbeat_file is None
        assert kwargs["settings"].heartbeat_interval == 10.0

    @patch("planq.contrib.django.management.commands.planqworker.importlib")
    @patch("planq.contrib.django.management.commands.planqworker.asyncio")
    @patch("planq.contrib.django.management.commands.planqworker.PlanqConsumer")
    def test_import_module_imported_before_run(
        self,
        mock_consumer_cls: MagicMock,
        mock_asyncio: MagicMock,
        mock_importlib: MagicMock,
    ) -> None:
        consumer_instance = MagicMock()
        mock_consumer_cls.return_value = consumer_instance

        call_command("planqworker", "default", "--import-module", "some.module")
        mock_importlib.import_module.assert_called_once_with("some.module")

    @patch("planq.contrib.django.management.commands.planqworker.importlib")
    @patch("planq.contrib.django.management.commands.planqworker.asyncio")
    @patch("planq.contrib.django.management.commands.planqworker.PlanqConsumer")
    def test_import_module_repeatable(
        self,
        mock_consumer_cls: MagicMock,
        mock_asyncio: MagicMock,
        mock_importlib: MagicMock,
    ) -> None:
        consumer_instance = MagicMock()
        mock_consumer_cls.return_value = consumer_instance

        call_command(
            "planqworker",
            "default",
            "--import-module",
            "some.module",
            "--import-module",
            "another.module",
        )
        assert mock_importlib.import_module.call_count == 2
        calls = [c.args[0] for c in mock_importlib.import_module.call_args_list]
        assert "some.module" in calls
        assert "another.module" in calls

    @patch("planq.contrib.django.management.commands.planqworker.importlib")
    @patch("planq.contrib.django.management.commands.planqworker.asyncio")
    @patch("planq.contrib.django.management.commands.planqworker.PlanqConsumer")
    def test_no_import_module_imports_nothing(
        self,
        mock_consumer_cls: MagicMock,
        mock_asyncio: MagicMock,
        mock_importlib: MagicMock,
    ) -> None:
        consumer_instance = MagicMock()
        mock_consumer_cls.return_value = consumer_instance

        call_command("planqworker", "default")
        mock_importlib.import_module.assert_not_called()
