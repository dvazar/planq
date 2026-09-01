"""Redis integration tests for Django worker event-loop ownership."""

from __future__ import annotations

import asyncio
from contextlib import suppress

import pytest

from planq import ExecutionMode, SyncPlanq
from planq.consumer import PlanqConsumer
from planq.contrib.django.management.commands.planqworker import _run_worker
from planq.models import ConsumerSettings, JsonRpcRequest
from planq.providers.redis import RedisBroker, RedisConsumerConfig


@pytest.mark.integration
@pytest.mark.asyncio
async def test_thread_handler_can_chain_without_cross_loop_redelivery() -> None:
    """A THREAD handler publishes and both messages settle exactly once."""
    dsn = "redis://localhost:16379"
    queue = "django-worker-loop-affinity"
    group = "django-worker-loop-affinity-group"
    consumer_name = "django-worker-loop-affinity-consumer"

    broker = RedisBroker(
        dsn=dsn,
        consumer=RedisConsumerConfig(
            group_name=group,
            consumer_name=consumer_name,
            claim_idle_ms=0,
            scheduler_interval=0.05,
        ),
    )
    app = SyncPlanq(broker=broker)
    consumer = PlanqConsumer(
        app,
        settings=ConsumerSettings(concurrency=1),
        middlewares=[],
        install_signal_handlers=False,
    )
    producer = RedisBroker(dsn=dsn)
    executions: list[str] = []
    done = asyncio.Event()
    loop = asyncio.get_running_loop()
    worker_task: asyncio.Task[None] | None = None

    @app.task("loop-affinity.leaf", queue_name=queue, mode=ExecutionMode.THREAD)
    def leaf(message: str) -> None:
        executions.append(f"leaf:{message}")
        loop.call_soon_threadsafe(done.set)

    @app.task(
        "loop-affinity.chain",
        queue_name=queue,
        mode=ExecutionMode.THREAD,
    )
    def chain(message: str) -> None:
        executions.append(f"chain:{message}")
        leaf.send(f"{message}-leaf")

    try:
        await producer.connect()
        assert producer._client is not None
        await producer._client.delete(queue, f"{queue}:delayed")
        await producer.publish(
            queue,
            JsonRpcRequest(
                method="loop-affinity.chain",
                params=["message-1"],
                id=None,
            ),
        )

        worker_task = asyncio.create_task(_run_worker(app, consumer, [queue]))
        async with asyncio.timeout(5.0):
            await done.wait()

        await consumer.stop()
        await worker_task

        assert executions == ["chain:message-1", "leaf:message-1-leaf"]
        pending = await producer._client.xpending(queue, group)
        assert pending["pending"] == 0
    finally:
        if worker_task is not None and not worker_task.done():
            await consumer.stop()
            with suppress(Exception):
                await worker_task
        if producer._client is not None:
            await producer._client.delete(queue, f"{queue}:delayed")
        await producer.disconnect()
