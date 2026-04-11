"""PlanQ task definitions for the images app."""
from __future__ import annotations

from planq import ExecutionMode
from planq.contrib.django import get_planq_app

from .models import Image

app = get_planq_app()


@app.task(
    name="images.resize",
    queue_name="images",
    mode=ExecutionMode.THREAD,
)
def resize_image(image_id: int) -> dict:
    """Resize an image and update its DB row.

    Runs in THREAD mode because the Django ORM is synchronous.
    DjangoDbMiddleware (registered automatically by
    planq.contrib.django) ensures the DB connection is closed
    cleanly after each task invocation.
    """
    image = Image.objects.get(id=image_id)
    image.status = "processing"
    image.save(update_fields=["status"])

    image.resized_url = (
        f"{image.url}?resized={image.width}x{image.height}"
    )
    image.status = "done"
    image.save(update_fields=["resized_url", "status"])

    return {"image_id": image_id, "url": image.resized_url}
