"""In-memory broker for testing.

Provides a broker implementation that stores messages in
``asyncio.Queue`` instances. No external dependencies required.
All data is lost when the process exits.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import TYPE_CHECKING, Any, override

from planq.broker import BaseBroker
from planq.enums import Header
from planq.message import BrokerMessage
from planq.models import JsonRpcRequest
from planq.stats import QueueStats

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from planq.models import JsonRpcResponse
    from planq.types import Headers, Seconds


class InMemoryMessage(BrokerMessage):
    """Message backed by an in-memory envelope dict."""

    def __init__(
        self,
        raw: dict[str, Any],
        body: JsonRpcRequest,
        headers: Headers,
        received_at: Seconds,
        queue_name: str,
        broker: InMemoryBroker,
    ) -> None:
        """Initialize the in-memory message.

        Args:
            raw: Envelope dict with message metadata and body.
            body: Validated JSON-RPC request.
            headers: Flat string-to-string header mapping.
            received_at: Unix timestamp when the message was
                received.
            queue_name: Queue this message was consumed from.
            broker: Parent broker for nack requeue.
        """
        super().__init__(raw, body, headers, received_at, queue_name)
        self._broker = broker

    @property
    @override
    def message_id(self) -> str:
        return self.raw["message_id"]

    @property
    @override
    def enqueued_at(self) -> float:
        return self.raw["enqueued_at"]

    @property
    @override
    def delivery_count(self) -> int:
        return self.raw["delivery_count"]

    @property
    @override
    def reply_to(self) -> str | None:
        return self.raw["reply_to"]

    @override
    async def _ack(self) -> None:
        pass

    @override
    async def _reject(self) -> None:
        pass

    @override
    async def _nack(self, delay: Seconds) -> None:
        self.raw["delivery_count"] += 1
        if delay > 0:
            task = asyncio.create_task(self._delayed_requeue(delay))
            self._broker._delayed_tasks.add(task)
            task.add_done_callback(self._broker._delayed_tasks.discard)
        else:
            await self._broker._get_queue(self.queue_name).put(self.raw)

    async def _delayed_requeue(self, delay: Seconds) -> None:
        await asyncio.sleep(delay)
        await self._broker._get_queue(self.queue_name).put(self.raw)


_SENTINEL = object()


class InMemoryBroker(BaseBroker):
    """In-memory broker for testing.

    Args:
        dsn: Ignored. Accepted for interface compatibility.
    """

    def __init__(self, dsn: str = "memory://") -> None:
        """Initialize the in-memory broker.

        Args:
            dsn: Ignored. Accepted for interface compatibility.
        """
        super().__init__(dsn)
        self._queues: dict[str, asyncio.Queue[Any]] = {}
        self._delayed_tasks: set[asyncio.Task[None]] = set()

    def _get_queue(self, name: str) -> asyncio.Queue[Any]:
        """Return the queue for *name*, creating it if needed."""
        if name not in self._queues:
            self._queues[name] = asyncio.Queue()
        return self._queues[name]

    @override
    async def connect(self) -> None:
        """No-op — in-memory broker needs no connection."""

    @override
    async def disconnect(self) -> None:
        """Cancel delayed tasks and unblock waiting consumers.

        Sends a sentinel value into every queue so that any
        ``consume()`` coroutine blocked on ``Queue.get()`` returns
        cleanly.
        """
        for task in self._delayed_tasks:
            task.cancel()
        self._delayed_tasks.clear()
        for q in self._queues.values():
            await q.put(_SENTINEL)
        self._queues.clear()

    @override
    async def publish(
        self,
        queue: str,
        rpc: JsonRpcRequest | JsonRpcResponse,
        *,
        delay: Seconds | None = None,
        expire_at: float | None = None,
        reply_to: str | None = None,
        headers: Headers | None = None,
    ) -> str:
        """Wrap *rpc* in an envelope and put it on the named queue.

        Args:
            queue: Destination queue name.
            rpc: JSON-RPC request or response to publish.
            delay: Seconds to wait before the message appears.
            expire_at: Unix timestamp mapped to ``x-expire-at``
                header.
            reply_to: Queue name for the consumer's response.
            headers: Additional user-defined headers.

        Returns:
            A UUID4 message identifier string.
        """
        message_id = str(uuid.uuid4())
        merged_headers: Headers = dict(headers) if headers else {}
        if expire_at is not None:
            merged_headers[Header.EXPIRE_AT] = str(expire_at)

        envelope: dict[str, Any] = {
            "message_id": message_id,
            "enqueued_at": time.time(),
            "delivery_count": 1,
            "reply_to": reply_to,
            "headers": merged_headers,
            "body": rpc.model_dump(),
        }

        q = self._get_queue(queue)
        if delay and delay > 0:
            task = asyncio.create_task(self._delayed_put(q, envelope, delay))
            self._delayed_tasks.add(task)
            task.add_done_callback(self._delayed_tasks.discard)
        else:
            await q.put(envelope)
        return message_id

    @override
    async def get_queue_depth(self, queue: str) -> QueueStats:
        """Return pending message count for an in-memory queue.

        ``scheduled`` and ``in_flight`` are always 0 for the in-memory
        broker — it has no delayed-message support and no consumer groups.

        Args:
            queue: Logical queue name.

        Returns:
            A :class:`~planq.stats.QueueStats` snapshot.
        """
        name = self.get_queue_name(queue)
        q = self._queues.get(name)
        pending = q.qsize() if q is not None else 0
        return QueueStats(
            queue=name, pending=pending, scheduled=0, in_flight=0
        )

    @override
    async def consume(
        self,
        queue: str,
        *,
        prefetch: int = 10,
    ) -> AsyncIterator[InMemoryMessage]:
        """Yield messages from *queue* as they arrive.

        Blocks on an empty queue until a message is published or
        :meth:`disconnect` sends a sentinel.

        Args:
            queue: Source queue name.
            prefetch: Ignored (present for interface compatibility).

        Yields:
            :class:`InMemoryMessage` instances.
        """
        q = self._get_queue(queue)
        while True:
            envelope = await q.get()
            if envelope is _SENTINEL:
                return
            body = JsonRpcRequest.model_validate(envelope["body"])
            received_at = time.time()
            yield InMemoryMessage(
                raw=envelope,
                body=body,
                headers=envelope["headers"],
                received_at=received_at,
                queue_name=queue,
                broker=self,
            )

    async def _delayed_put(
        self,
        q: asyncio.Queue[Any],
        envelope: dict[str, Any],
        delay: Seconds,
    ) -> None:
        await asyncio.sleep(delay)
        await q.put(envelope)
