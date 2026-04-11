"""Run the PlanQ consumer worker."""
from __future__ import annotations

import asyncio

from planq import ConsumerSettings, PlanqConsumer
from tasks import app


async def main() -> None:
    consumer = PlanqConsumer(
        app=app,
        settings=ConsumerSettings(concurrency=10, max_retries=3),
    )
    await consumer.run(
        "http://localhost:4566/000000000000/emails",
    )


if __name__ == "__main__":
    asyncio.run(main())
