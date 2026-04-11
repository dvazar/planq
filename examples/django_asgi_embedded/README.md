# Django ASGI-Embedded Example

Django + PlanQ using the **embedded consumer** pattern: the
PlanQ consumer runs as a background asyncio task inside the same
uvicorn process that serves the web application. There is no
separate worker process.

Uses Redis as the broker and SQLite for Django's database.
Demonstrates:

- ASGI lifespan protocol (`lifespan.startup` / `lifespan.shutdown`)
- `PlanqConsumer(install_signal_handlers=False)` for embedding
- `consumer.stop()` called from the lifespan shutdown handler
- `lifespan.startup.failed` handling on broker errors
- One-process deployment of web + background worker

This is the **same application** as `../django_worker/`. The
only differences are `config/asgi.py` (lifespan dispatch),
`planq_lifespan.py` (consumer lifecycle), and `config/settings.py`
(different `consumer_name` so both examples can consume from
the same Redis group simultaneously without name collisions).

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

**Step 3: start the one combined process**

```bash
uvicorn config.asgi:application --lifespan on --reload
```

The `--lifespan on` flag is required — it tells uvicorn to send
lifespan startup/shutdown events to the ASGI application. The
PlanQ consumer starts as a background task during
`lifespan.startup`.

**Step 4: submit a resize job**

```bash
curl -X POST http://localhost:8000/resize
# => {"image_id": 1, "message_id": "...", "status": "queued"}
```

The embedded consumer processes the task in the same process.
Check the result:

```bash
curl http://localhost:8000/images/1
# => {"id": 1, "status": "done", "resized_url": "..."}
```

## Stop

`Ctrl+C` uvicorn. Uvicorn sends `lifespan.shutdown`, which calls
`consumer.stop()`, which signals the background task to drain
in-flight messages. Then `docker compose down`.

## Shutdown timing

`consumer.stop()` signals the consumer loop but does not
forcibly unblock a currently-blocked `broker.consume()` call.
The actual exit happens when:

- A new message arrives and the shutdown check fires before
  the message is handed to a handler, or
- The broker's current poll cycle returns.

For Redis, `XREADGROUP` has a configurable block timeout; for
SQS, long-polling blocks up to 20 seconds. Under load (messages
flowing), shutdown latency is sub-second. On a completely idle
consumer, it is bounded by the broker poll interval. Uvicorn
gives applications a shutdown grace period that covers this
window.

## When to use this pattern

**Use embedded mode when:**
- Your app is small-to-medium and doesn't need independent
  worker scaling.
- You want fewer processes in production (one uvicorn instead
  of `web + worker`).
- Dev loop matters: `uvicorn --reload` restarts both web and
  consumer together.

**Use standalone worker (`../django_worker/`) when:**
- You need to scale workers independently of web.
- You run high-throughput workloads where web traffic and
  worker CPU contend for resources.
- You need per-role monitoring or deployment.

## See also

- [`../django_worker/`](../django_worker/) — the same application
  deployed as two processes (web + worker).
- [`../../docs/explanation/`](../../docs/explanation/) — architecture
  and design notes (when written).
