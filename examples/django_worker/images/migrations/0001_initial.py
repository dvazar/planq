"""Initial migration for the images app."""
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies: list[tuple[str, str]] = []

    operations = [
        migrations.CreateModel(
            name="Image",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("url", models.URLField()),
                ("width", models.IntegerField()),
                ("height", models.IntegerField()),
                (
                    "status",
                    models.CharField(default="pending", max_length=20),
                ),
                (
                    "resized_url",
                    models.URLField(blank=True, null=True),
                ),
            ],
        ),
    ]
