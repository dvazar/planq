"""Tests for planq.contrib.django.apps."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from django.test import override_settings

import planq.contrib.django.setup as _setup_mod


@pytest.fixture(autouse=True)
def _reset_singleton() -> None:
    _setup_mod._app = None


class TestPlanqConfig:
    @patch("planq.contrib.django.apps.autodiscover_modules")
    def test_ready_calls_configure(self, mock_discover) -> None:
        from planq.contrib.django.apps import PlanqConfig

        config = PlanqConfig("planq.contrib.django", __import__("planq"))
        config.ready()
        assert _setup_mod._app is not None

    @patch("planq.contrib.django.apps.autodiscover_modules")
    def test_ready_autodiscovers_default_tasks(self, mock_discover) -> None:
        from planq.contrib.django.apps import PlanqConfig

        config = PlanqConfig("planq.contrib.django", __import__("planq"))
        config.ready()
        mock_discover.assert_called_once_with("tasks")

    @override_settings(
        PLANQ={
            "BROKER_CLASS": ("planq.providers.memory.InMemoryBroker"),
            "AUTODISCOVER_MODULES": ["tasks", "jobs"],
        }
    )
    @patch("planq.contrib.django.apps.autodiscover_modules")
    def test_ready_autodiscovers_custom_modules(self, mock_discover) -> None:
        from planq.contrib.django.apps import PlanqConfig

        config = PlanqConfig("planq.contrib.django", __import__("planq"))
        config.ready()
        assert mock_discover.call_count == 2
        mock_discover.assert_any_call("tasks")
        mock_discover.assert_any_call("jobs")
