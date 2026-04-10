"""Django application configuration for PlanQ."""

from __future__ import annotations

from django.apps import AppConfig
from django.utils.module_loading import autodiscover_modules


class PlanqConfig(AppConfig):
    """Django AppConfig that configures PlanQ on startup."""

    name = "planq.contrib.django"
    label = "planq"

    def ready(self) -> None:
        """Initialize PlanQ and run task auto-discovery.

        Calls ``configure_planq()`` to build the broker and Planq
        instance from Django settings, then calls
        ``autodiscover_modules`` for each module name listed in
        ``settings.PLANQ["AUTODISCOVER_MODULES"]`` (defaults to
        ``["tasks"]``).
        """
        from django.conf import settings

        from .setup import configure_planq

        configure_planq()
        config = getattr(settings, "PLANQ", {})
        for module_name in config.get("AUTODISCOVER_MODULES", ("tasks",)):
            autodiscover_modules(module_name)
