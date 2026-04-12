# Quickstart: Your First Task Queue with Redis

In this tutorial, we will build a working PlanQ task queue backed by
Redis Streams. In about ten minutes you will have a worker process
running on your machine, picking up tasks that you publish from a
separate terminal, and printing their results as they arrive.

You will write three files:

- `tasks.py` — the task definitions, shared by the worker and the
  producer.
- `worker.py` — a script that starts a consumer and keeps it running.
- `send.py` — a script that publishes tasks to the queue.

You will need Python 3.12 or newer, Docker, and a terminal.

## Step 1: Create the project

Create an empty directory and a virtual environment:

```bash
mkdir planq-redis-quickstart
cd planq-redis-quickstart
python3.12 -m venv .venv
source .venv/bin/activate
```

Install PlanQ with the Redis extra:

```bash
pip install "planq[redis]"
```

## Step 2: Start Redis

Start a Redis container in the background:

```bash
docker run --rm -d --name planq-redis -p 6379:6379 redis:7-alpine
```

Confirm it is running:

```bash
docker ps --filter name=planq-redis
```

You will see a single container named `planq-redis` listed.

## Step 3: Define a task

Create a file called `tasks.py` with the following contents:

```python
from planq import Planq
from planq.providers.redis import RedisBroker, RedisConsumerConfig

broker = RedisBroker(
    dsn="redis://localhost:6379",
    consumer=RedisConsumerConfig(
        group_name="quickstart",
        consumer_name="worker-1",
    ),
)

app = Planq(broker=broker)


@app.task(name="greet", queue_name="greetings")
async def greet(name: str) -> dict:
    message = f"Hello, {name}!"
    print(f"[worker] {message}")
    return {"message": message}
```

You now have one task, `greet`, bound to a queue called `greetings`.
Both the worker and the producer will import `app` and `greet` from
this single file.

## Step 4: Write the worker

Create a file called `worker.py`:

```python
import asyncio

from planq import ConsumerSettings, PlanqConsumer

from tasks import app


async def main() -> None:
    consumer = PlanqConsumer(
        app,
        settings=ConsumerSettings(concurrency=5),
    )
    await consumer.run("greetings")


if __name__ == "__main__":
    asyncio.run(main())
```

Open a terminal, activate the virtual environment, and start the
worker:

```bash
python worker.py
```

The worker connects to Redis and waits. Leave this terminal open.

## Step 5: Send your first task

Create a second file called `send.py`:

```python
import asyncio

from tasks import app, greet


async def main() -> None:
    async with app.broker:
        msg_id = await greet.send(name="Alice")
        print(f"[producer] sent {msg_id}")


if __name__ == "__main__":
    asyncio.run(main())
```

Open a second terminal, activate the same virtual environment, and
run the producer:

```bash
python send.py
```

The producer prints the message ID that Redis assigned:

```
[producer] sent 1712847600000-0
```

Switch back to the worker terminal. You will see the worker pick up
the task and print the greeting:

```
[worker] Hello, Alice!
```

You just processed your first PlanQ task through Redis.

## Step 6: Send a batch of tasks

Edit `send.py` and replace the body of `main()` with the following:

```python
async def main() -> None:
    async with app.broker:
        for name in ["Alice", "Bob", "Charlie", "Dana"]:
            msg_id = await greet.send(name=name)
            print(f"[producer] sent {name}: {msg_id}")
```

Run it again:

```bash
python send.py
```

The producer prints four message IDs. In the worker terminal, four
new lines appear almost instantly:

```
[worker] Hello, Alice!
[worker] Hello, Bob!
[worker] Hello, Charlie!
[worker] Hello, Dana!
```

The worker processed all four tasks concurrently because you set
`ConsumerSettings(concurrency=5)` in Step 4.

## Step 7: Shut down cleanly

In the worker terminal, press `Ctrl+C`. The consumer drains any
in-flight messages and exits.

Stop the Redis container:

```bash
docker stop planq-redis
```

## What you built

You installed PlanQ, started a Redis server, defined a task in a
shared `tasks.py` module, ran a worker that listened to the
`greetings` queue, and sent tasks from a separate producer script —
first one, then a batch of four. You watched the worker process each
one and print its result.
