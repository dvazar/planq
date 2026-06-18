"""Print PlanQ queue-depth statistics (read-only)."""

from __future__ import annotations

import json
from typing import Any

from django.core.management.base import BaseCommand

from planq.contrib.django import setup


class Command(BaseCommand):
    help = "Show PlanQ queue-depth statistics."

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument(
            "queues",
            nargs="*",
            help="Queue names to inspect. Omit to use all queues "
            "declared in the app's task routes.",
        )
        parser.add_argument(
            "--json",
            action="store_true",
            dest="as_json",
            help="Emit machine-readable JSON instead of a table.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        app = setup.get_planq_app()
        queues = options["queues"] or sorted(
            {route.queue_name for route in app.routes.values()}
        )
        rows = []
        for queue in queues:
            stats = app.get_queue_depth(queue)
            rows.append(
                {
                    "queue": stats.queue,
                    "pending": stats.pending,
                    "scheduled": stats.scheduled,
                    "in_flight": stats.in_flight,
                    "total": stats.total,
                }
            )

        if options["as_json"]:
            self.stdout.write(json.dumps(rows))
            return

        for row in rows:
            self.stdout.write(
                f"{row['queue']}: pending={row['pending']} "
                f"scheduled={row['scheduled']} "
                f"in_flight={row['in_flight']} "
                f"total={row['total']}"
            )
