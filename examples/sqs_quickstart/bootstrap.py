"""Create SQS queues in LocalStack for the quickstart."""
from __future__ import annotations

import asyncio

from aiobotocore.session import AioSession


async def main() -> None:
    session = AioSession()
    async with session.create_client(
        "sqs",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    ) as sqs:
        resp = await sqs.create_queue(QueueName="emails")
        print(f"Created queue: {resp['QueueUrl']}")


if __name__ == "__main__":
    asyncio.run(main())
