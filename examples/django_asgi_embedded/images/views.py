"""HTTP views for the images app."""
from __future__ import annotations

from asgiref.sync import sync_to_async
from django.http import HttpRequest, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from .models import Image
from .tasks import resize_image


@csrf_exempt
@require_POST
async def submit_resize(request: HttpRequest) -> JsonResponse:
    """Create an Image row and enqueue a resize task."""
    image = await sync_to_async(Image.objects.create)(
        url="https://example.com/cat.jpg",
        width=200,
        height=200,
    )
    msg_id = await resize_image.send(image_id=image.id)
    return JsonResponse(
        {
            "image_id": image.id,
            "message_id": msg_id,
            "status": "queued",
        },
    )


@require_GET
async def get_image(
    request: HttpRequest, image_id: int,
) -> JsonResponse:
    """Return the current state of an Image row."""
    image = await sync_to_async(Image.objects.get)(id=image_id)
    return JsonResponse(
        {
            "id": image.id,
            "url": image.url,
            "status": image.status,
            "resized_url": image.resized_url,
        },
    )
