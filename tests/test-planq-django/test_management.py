"""Tests for the planqworker management command."""

from __future__ import annotations

from io import StringIO
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


# === TestPlanqstatsCommand ===


class TestPlanqstatsCommand:
    """Tests for the planqstats management command."""

    def test_planqstats_explicit_queue_json(self, monkeypatch):
        """Explicit queue name + --json outputs correct JSON."""
        import json

        import planq.contrib.django.setup as setup
        from planq.stats import QueueStats

        class FakeApp:
            routes = {}

            def get_queue_depth(self, queue):
                return QueueStats(
                    queue=queue, pending=4, scheduled=1, in_flight=0
                )

        monkeypatch.setattr(setup, "get_planq_app", lambda: FakeApp())
        out = StringIO()
        call_command("planqstats", "default", "--json", stdout=out)
        payload = json.loads(out.getvalue())
        assert payload == [
            {
                "queue": "default",
                "pending": 4,
                "scheduled": 1,
                "in_flight": 0,
                "total": 5,
            }
        ]

    def test_planqstats_no_args_uses_routes(self, monkeypatch):
        """No queue args — uses unique queue names from app.routes."""
        import json
        from dataclasses import dataclass

        import planq.contrib.django.setup as setup
        from planq.stats import QueueStats

        @dataclass
        class Route:
            queue_name: str

        class FakeApp:
            routes = {
                "a": Route("default"),
                "b": Route("imports"),
                "c": Route("default"),
            }

            def get_queue_depth(self, queue):
                return QueueStats(
                    queue=queue, pending=0, scheduled=0, in_flight=0
                )

        monkeypatch.setattr(setup, "get_planq_app", lambda: FakeApp())
        out = StringIO()
        call_command("planqstats", "--json", stdout=out)
        queues = sorted(row["queue"] for row in json.loads(out.getvalue()))
        assert queues == ["default", "imports"]  # deduped, sorted

    def test_planqstats_table_output(self, monkeypatch):
        """Without --json, emit a human-readable line per queue."""
        import planq.contrib.django.setup as setup
        from planq.stats import QueueStats

        class FakeApp:
            routes = {}

            def get_queue_depth(self, queue):
                return QueueStats(
                    queue=queue, pending=4, scheduled=1, in_flight=2
                )

        monkeypatch.setattr(setup, "get_planq_app", lambda: FakeApp())
        out = StringIO()
        call_command("planqstats", "default", stdout=out)
        text = out.getvalue()
        assert "default" in text
        assert "pending=4" in text
        assert "in_flight=2" in text
        assert "total=7" in text
