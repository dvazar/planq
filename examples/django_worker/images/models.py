"""Image model."""
from django.db import models


class Image(models.Model):
    """A stored image awaiting or having completed resize."""

    url = models.URLField()
    width = models.IntegerField()
    height = models.IntegerField()
    status = models.CharField(max_length=20, default="pending")
    resized_url = models.URLField(blank=True, null=True)
