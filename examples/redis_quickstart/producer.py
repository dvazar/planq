"""Send a few test emails to the Redis queue."""
from __future__ import annotations

import asyncio
import time

from tasks import EmailPayload, app, send_email


async def main() -> None:
    async with app.broker:
        # Immediate delivery
        msg_id = await send_email.send(
            payload=EmailPayload(
                to="alice@example.com",
                subject="Welcome",
                body="Hi Alice",
            ),
        )
        print(f"[producer] sent immediate: {msg_id}")

        # Delayed 5 seconds
        msg_id = await send_email.options(delay=5).send(
            payload=EmailPayload(
                to="bob@example.com",
                subject="Follow-up",
                body="Hi Bob",
            ),
        )
        print(f"[producer] sent delayed: {msg_id}")

        # With TTL (60s from now)
        msg_id = await send_email.options(
            expire_at=time.time() + 60,
        ).send(
            payload=EmailPayload(
                to="carol@example.com",
                subject="Reminder",
                body="Hi Carol",
            ),
        )
        print(f"[producer] sent with TTL: {msg_id}")


if __name__ == "__main__":
    asyncio.run(main())
