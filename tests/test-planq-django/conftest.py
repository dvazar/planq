"""Django test configuration for planq.contrib.django."""

from __future__ import annotations

import django
from django.conf import settings


def pytest_configure() -> None:
    """Configure Django settings for the test suite."""
    settings.configure(
        INSTALLED_APPS=["planq.contrib.django"],
        PLANQ={
            "BROKER_CLASS": ("planq.providers.memory.InMemoryBroker"),
        },
        DATABASES={
            "default": {
                "ENGINE": "django.db.backends.sqlite3",
                "NAME": ":memory:",
            }
        },
    )
    django.setup()
