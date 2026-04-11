"""Run the PlanQ consumer worker against Redis.

RedisBroker splits producer and consumer configuration: the
consumer needs a RedisConsumerConfig (group_name,
consumer_name), which spawns the background scheduler that
migrates delayed messages from the ZSET to the stream. The
producer should NOT have this config -- it would spawn a useless
scheduler task on the web process.

We reuse the task registry from tasks.py but swap the broker
here before starting the consumer loop.
"""
from __future__ import annotations

import asyncio

from planq import ConsumerSettings, PlanqConsumer
from planq.providers.redis import RedisBroker, RedisConsumerConfig
from tasks import app


async def main() -> None:
    app.broker = RedisBroker(
        dsn="redis://localhost:6379/0",
        consumer=RedisConsumerConfig(
            group_name="emails-workers",
            consumer_name="worker-1",
        ),
    )

    consumer = PlanqConsumer(
        app=app,
        settings=ConsumerSettings(concurrency=10, max_retries=3),
    )
    await consumer.run("emails")


if __name__ == "__main__":
    asyncio.run(main())
