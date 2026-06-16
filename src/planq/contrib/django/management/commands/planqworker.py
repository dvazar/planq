"""Management command to run the PlanQ consumer worker."""

from __future__ import annotations

import asyncio

from django.core.management.base import BaseCommand

from planq.consumer import PlanqConsumer
from planq.models import ConsumerSettings


class Command(BaseCommand):
    """Run the PlanQ consumer worker."""

    help = "Start a PlanQ consumer worker for the given queues."

    def add_arguments(self, parser) -> None:  # type: ignore[no-untyped-def]
        parser.add_argument(
            "queues",
            nargs="+",
            type=str,
            help="Queue names to consume from.",
        )
        parser.add_argument(
            "--concurrency",
            type=int,
            default=None,
            help="Override consumer concurrency.",
        )
        parser.add_argument(
            "--process-workers",
            type=int,
            default=None,
            help="Number of process pool workers.",
        )
        parser.add_argument(
            "--heartbeat-file",
            type=str,
            default=None,
            help=(
                "Path to a liveness heartbeat file the worker updates "
                "periodically (for supervisors like systemd, draug, k8s)."
            ),
        )
        parser.add_argument(
            "--heartbeat-interval",
            type=float,
            default=None,
            help="Heartbeat update period in seconds (default: 10.0).",
        )

    def handle(self, *args: object, **options: object) -> None:
        from planq.contrib.django.setup import (
            get_planq_app,
            get_planq_middlewares,
        )

        app = get_planq_app()
        queues: list[str] = options["queues"]  # type: ignore[assignment]
        concurrency: int | None = options["concurrency"]  # type: ignore[assignment]
        process_workers: int | None = options["process_workers"]  # type: ignore[assignment]
        heartbeat_file: str | None = options["heartbeat_file"]  # type: ignore[assignment]
        heartbeat_interval: float | None = options["heartbeat_interval"]  # type: ignore[assignment]

        consumer_config = self._build_consumer_settings(
            concurrency, heartbeat_file, heartbeat_interval
        )
        middlewares = get_planq_middlewares()

        consumer = PlanqConsumer(
            app=app,
            settings=consumer_config,
            middlewares=middlewares,
            process_workers=process_workers,
        )

        asyncio.run(consumer.run(*queues))

    def _build_consumer_settings(
        self,
        concurrency_override: int | None,
        heartbeat_file: str | None = None,
        heartbeat_interval: float | None = None,
    ) -> ConsumerSettings:
        from django.conf import settings

        config = getattr(settings, "PLANQ", {})
        consumer_config = dict(config.get("CONSUMER", {}))

        if concurrency_override is not None:
            consumer_config["concurrency"] = concurrency_override
        if heartbeat_file is not None:
            consumer_config["heartbeat_file"] = heartbeat_file
        if heartbeat_interval is not None:
            consumer_config["heartbeat_interval"] = heartbeat_interval

        return ConsumerSettings(**consumer_config)
