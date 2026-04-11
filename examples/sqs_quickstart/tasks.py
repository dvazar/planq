"""Shared task registry for the SQS quickstart."""
from __future__ import annotations

from pydantic import BaseModel

from planq import Planq
from planq.providers.sqs import SqsBroker


class EmailPayload(BaseModel):
    """Payload for the send_email task."""

    to: str
    subject: str
    body: str


app = Planq(broker=SqsBroker(dsn="http://localhost:4566"))


@app.task(name="email.send", queue_name="emails")
async def send_email(payload: EmailPayload) -> dict:
    """Simulate sending a transactional email."""
    print(
        f"[worker] sending email to {payload.to}: {payload.subject}"
    )
    return {"status": "sent", "to": payload.to}
