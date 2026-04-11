"""Embedded PlanQ consumer lifecycle helpers for ASGI lifespan.

Called from ``config/asgi.py`` when uvicorn dispatches
``lifespan.startup`` and ``lifespan.shutdown`` messages. The
consumer runs as a background ``asyncio.Task`` inside the same
process as the Django web application.
"""
from __future__ import annotations

import asyncio
import logging

from planq import PlanqConsumer
from planq.contrib.django.setup import (
    get_planq_app,
    get_planq_middlewares,
)

logger = logging.getLogger(__name__)

_consumer: PlanqConsumer | None = None
_consumer_task: asyncio.Task[None] | None = None


async def startup() -> None:
    """Start the embedded PlanQ consumer as a background task.

    Called from ASGI ``lifespan.startup``. Any exception raised
    here causes uvicorn to abort startup with
    ``lifespan.startup.failed``.
    """
    global _consumer, _consumer_task

    app = get_planq_app()

    _consumer = PlanqConsumer(
        app=app,
        middlewares=get_planq_middlewares(),
        install_signal_handlers=False,
    )
    _consumer_task = asyncio.create_task(
        _consumer.run("images"),
        name="planq-consumer",
    )
    logger.info("PlanQ consumer started in embedded mode")


async def shutdown() -> None:
    """Gracefully stop the embedded PlanQ consumer.

    Called from ASGI ``lifespan.shutdown``. Signals shutdown via
    :meth:`PlanqConsumer.stop`, then awaits the background task
    to drain in-flight messages. Shutdown latency is bounded by
    the broker's poll interval — typically sub-second under
    load, longer on a completely idle consumer.
    """
    global _consumer, _consumer_task

    if _consumer is None or _consumer_task is None:
        return

    await _consumer.stop()
    await _consumer_task

    logger.info("PlanQ consumer stopped")
