"""Shared task registry for the Redis quickstart.

The broker is constructed without a RedisConsumerConfig so that
the producer can import this module without spawning the
background scheduler. The worker reconstructs the broker with a
consumer config before calling PlanqConsumer.run().
"""
from __future__ import annotations

from pydantic import BaseModel

from planq import Planq
from planq.providers.redis import RedisBroker


class EmailPayload(BaseModel):
    """Payload for the send_email task."""

    to: str
    subject: str
    body: str


app = Planq(broker=RedisBroker(dsn="redis://localhost:6379/0"))


@app.task(name="email.send", queue_name="emails")
async def send_email(payload: EmailPayload) -> dict:
    """Simulate sending a transactional email."""
    print(
        f"[worker] sending email to {payload.to}: {payload.subject}"
    )
    return {"status": "sent", "to": payload.to}
