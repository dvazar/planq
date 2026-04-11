"""ASGI entrypoint with embedded PlanQ consumer via lifespan.

Run with:
    uvicorn config.asgi:application --lifespan on --reload
"""
import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()  # must run BEFORE importing anything that touches models

from django.core.asgi import get_asgi_application  # noqa: E402

from planq_lifespan import shutdown, startup  # noqa: E402

_django_app = get_asgi_application()


async def application(scope, receive, send):
    """Dispatch ASGI scopes to Django or lifespan handlers."""
    if scope["type"] == "lifespan":
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                try:
                    await startup()
                    await send({"type": "lifespan.startup.complete"})
                except Exception as exc:
                    await send(
                        {
                            "type": "lifespan.startup.failed",
                            "message": repr(exc),
                        },
                    )
                    return
            elif message["type"] == "lifespan.shutdown":
                await shutdown()
                await send({"type": "lifespan.shutdown.complete"})
                return
    else:
        await _django_app(scope, receive, send)
