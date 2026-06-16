# Django Worker Example

Django + PlanQ using the **standalone worker** pattern: the web
app runs in one process (uvicorn), and the PlanQ consumer runs
in a separate process via `python manage.py planqworker`. This
is the traditional Celery-style deployment pattern.

Uses Redis as the broker and SQLite for Django's database.
Demonstrates:

- `planq.contrib.django` setup via `settings.PLANQ`
- `get_planq_app()` from `@app.task` decorators
- `planqworker` management command
- `DjangoDbMiddleware` (automatic DB-connection management)
- Django ORM inside a task handler (`ExecutionMode.THREAD`)
- Async Django views with `sync_to_async` for ORM access

## Prerequisites

- Python 3.12+
- Docker (for Redis)

```bash
pip install -r requirements.txt
```

## Run

**Step 1: start Redis**

```bash
docker compose up -d
```

**Step 2: apply migrations**

```bash
python manage.py migrate
```

**Step 3: start the web app (terminal 1)**

```bash
uvicorn config.asgi:application --reload
```

**Step 4: start the worker (terminal 2)**

```bash
python manage.py planqworker images --concurrency 10
```

> **Liveness heartbeat (optional).** Pass `--heartbeat-file PATH` to have the
> worker update that file's mtime every `--heartbeat-interval` seconds
> (default 10s). A process supervisor (systemd `WatchdogSec`, draug, a k8s
> file-based liveness probe) can then restart a wedged worker:
>
> ```bash
> python manage.py planqworker images --heartbeat-file /tmp/worker.heartbeat
> ```
>
> For a non-file signal (e.g. systemd `sd_notify`, a metrics push), register a
> callback with the `@consumer.on_heartbeat()` decorator instead.

**Step 5: submit a resize job (terminal 3)**

```bash
curl -X POST http://localhost:8000/resize
# => {"image_id": 1, "message_id": "...", "status": "queued"}
```

The worker will process the task and update the `Image` row.
Check the result:

```bash
curl http://localhost:8000/images/1
# => {"id": 1, "url": "https://example.com/cat.jpg",
#     "status": "done",
#     "resized_url": "https://example.com/cat.jpg?resized=200x200"}
```

## Production note

This example keeps a single `settings.PLANQ` configuration for
both web and worker processes. Because the broker is Redis, the
web process also spawns the background scheduler task (harmless
but wasteful). In production you'd split producer and consumer
roles via an environment variable:

```python
# In production settings.py:
import os

_is_worker = os.environ.get("PLANQ_ROLE") == "worker"
PLANQ = {
    "BROKER_CLASS": "planq.providers.redis.RedisBroker",
    "BROKER_OPTIONS": {
        "dsn": "redis://...",
        "consumer": (
            RedisConsumerConfig(
                group_name="images-workers",
                consumer_name=os.environ["HOSTNAME"],
            )
            if _is_worker
            else None
        ),
    },
    ...
}
```

## Stop

`Ctrl+C` the worker and web processes. `docker compose down` the
Redis container.

## See also

- [`../django_asgi_embedded/`](../django_asgi_embedded/) — the
  same application deployed as one process with the consumer
  embedded in uvicorn via ASGI lifespan.
- [`../../docs/reference/`](../../docs/reference/) — API reference
  (when written).
