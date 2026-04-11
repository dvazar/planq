"""URL patterns for the images app."""
from django.urls import path

from . import views

urlpatterns = [
    path("resize", views.submit_resize, name="submit-resize"),
    path("images/<int:image_id>", views.get_image, name="get-image"),
]
