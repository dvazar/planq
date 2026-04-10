"""PlanQ configuration from Django settings."""

from __future__ import annotations

from typing import TYPE_CHECKING

from planq import Planq

if TYPE_CHECKING:
    from planq.middleware import Middleware

_app: Planq | None = None


def get_planq_app() -> Planq:
    """Get the configured Planq application instance.

    Raises:
        RuntimeError: If Planq has not been configured yet.
    """
    if _app is None:
        raise RuntimeError(
            "Planq is not configured. "
            "Add 'planq.contrib.django' to INSTALLED_APPS."
        )
    return _app


def configure_planq() -> Planq:
    """Create and configure a Planq instance from Django settings.

    Reads the ``PLANQ`` dict from ``django.conf.settings`` and
    constructs a :class:`~planq.Planq` instance with the specified
    broker. Safe to call multiple times -- replaces the previous
    instance.

    Returns:
        The configured Planq instance.
    """
    global _app
    from django.conf import settings
    from django.utils.module_loading import import_string

    config = getattr(settings, "PLANQ", {})

    broker_class = import_string(config["BROKER_CLASS"])
    broker = broker_class(**config.get("BROKER_OPTIONS", {}))

    _app = Planq(broker=broker, eager=config.get("EAGER", False))
    return _app


def get_planq_middlewares() -> list[Middleware]:
    """Build the middleware list from Django settings.

    Prepends :class:`DjangoDbMiddleware` automatically.

    Returns:
        Ordered list of middleware instances.
    """
    from django.conf import settings
    from django.utils.module_loading import import_string

    from planq.contrib.django.middleware import (
        DjangoDbMiddleware,
    )

    config = getattr(settings, "PLANQ", {})
    middleware_paths = config.get(
        "MIDDLEWARE",
        ("planq.middleware.DeadlineMiddleware",),
    )

    middlewares: list[Middleware] = [DjangoDbMiddleware()]
    for path in middleware_paths:
        mw_class = import_string(path)
        middlewares.append(mw_class())
    return middlewares
