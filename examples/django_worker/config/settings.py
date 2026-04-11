"""Django settings for the django_worker example project."""
from __future__ import annotations

from pathlib import Path

from planq.providers.redis import RedisConsumerConfig

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = "dev-only-do-not-use-in-production"  # noqa: S105
DEBUG = True
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "planq.contrib.django",
    "images",
]

MIDDLEWARE: list[str] = []

ROOT_URLCONF = "config.urls"

WSGI_APPLICATION = "config.wsgi.application"
ASGI_APPLICATION = "config.asgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    },
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

USE_TZ = True
TIME_ZONE = "UTC"

# --- PlanQ configuration ---
#
# BROKER_OPTIONS is passed verbatim to RedisBroker, so it includes
# the consumer config. In a production deployment you'd split
# producer and consumer roles via an environment variable (e.g.,
# PLANQ_ROLE=worker) so the web process doesn't spawn the
# background scheduler. This example keeps the single config for
# simplicity.
PLANQ = {
    "BROKER_CLASS": "planq.providers.redis.RedisBroker",
    "BROKER_OPTIONS": {
        "dsn": "redis://localhost:6379/0",
        "consumer": RedisConsumerConfig(
            group_name="images-workers",
            consumer_name="worker-1",
        ),
    },
    "CONSUMER": {
        "concurrency": 10,
        "max_retries": 3,
    },
    "MIDDLEWARE": ("planq.middleware.DeadlineMiddleware",),
}
