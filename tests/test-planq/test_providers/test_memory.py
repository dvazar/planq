"""Tests for InMemoryBroker."""

from __future__ import annotations

import asyncio

import pytest

from planq.models import JsonRpcRequest
from planq.providers.memory import InMemoryBroker, InMemoryMessage


@pytest.fixture
def broker() -> InMemoryBroker:
    return InMemoryBroker()


def _make_request(method: str = "test.task") -> JsonRpcRequest:
    return JsonRpcRequest(
        jsonrpc="2.0",
        method=method,
        params={"key": "value"},
        id="req-1",
    )


class TestInMemoryBrokerPublish:
    @pytest.mark.asyncio
    async def test_publish_returns_message_id(
        self, broker: InMemoryBroker
    ) -> None:
        msg_id = await broker.publish("q", _make_request())
        assert isinstance(msg_id, str)
        assert len(msg_id) > 0

    @pytest.mark.asyncio
    async def test_publish_returns_unique_ids(
        self, broker: InMemoryBroker
    ) -> None:
        id1 = await broker.publish("q", _make_request())
        id2 = await broker.publish("q", _make_request())
        assert id1 != id2


class TestInMemoryBrokerConsume:
    @pytest.mark.asyncio
    async def test_publish_then_consume(self, broker: InMemoryBroker) -> None:
        request = _make_request()
        await broker.publish("q", request)

        async for msg in broker.consume("q"):
            assert isinstance(msg, InMemoryMessage)
            assert msg.body.method == "test.task"
            assert msg.body.params == {"key": "value"}
            assert msg.delivery_count == 1
            assert msg.queue_name == "q"
            break

    @pytest.mark.asyncio
    async def test_message_id_matches_published(
        self, broker: InMemoryBroker
    ) -> None:
        msg_id = await broker.publish("q", _make_request())

        async for msg in broker.consume("q"):
            assert msg.message_id == msg_id
            break

    @pytest.mark.asyncio
    async def test_enqueued_at_is_set(self, broker: InMemoryBroker) -> None:
        await broker.publish("q", _make_request())

        async for msg in broker.consume("q"):
            assert isinstance(msg.enqueued_at, float)
            assert msg.enqueued_at > 0
            break

    @pytest.mark.asyncio
    async def test_reply_to_preserved(self, broker: InMemoryBroker) -> None:
        await broker.publish("q", _make_request(), reply_to="reply-q")

        async for msg in broker.consume("q"):
            assert msg.reply_to == "reply-q"
            break

    @pytest.mark.asyncio
    async def test_reply_to_default_none(self, broker: InMemoryBroker) -> None:
        await broker.publish("q", _make_request())

        async for msg in broker.consume("q"):
            assert msg.reply_to is None
            break

    @pytest.mark.asyncio
    async def test_headers_preserved(self, broker: InMemoryBroker) -> None:
        await broker.publish(
            "q",
            _make_request(),
            headers={"x-custom": "val"},
        )

        async for msg in broker.consume("q"):
            assert msg.headers["x-custom"] == "val"
            break

    @pytest.mark.asyncio
    async def test_expire_at_becomes_header(
        self, broker: InMemoryBroker
    ) -> None:
        await broker.publish("q", _make_request(), expire_at=1234567890.0)

        async for msg in broker.consume("q"):
            assert msg.headers["x-expire-at"] == "1234567890.0"
            break

    @pytest.mark.asyncio
    async def test_queue_isolation(self, broker: InMemoryBroker) -> None:
        await broker.publish("q1", _make_request("task.a"))
        await broker.publish("q2", _make_request("task.b"))

        async for msg in broker.consume("q1"):
            assert msg.body.method == "task.a"
            break

        async for msg in broker.consume("q2"):
            assert msg.body.method == "task.b"
            break

    @pytest.mark.asyncio
    async def test_consume_blocks_until_message(
        self, broker: InMemoryBroker
    ) -> None:
        received: list[InMemoryMessage] = []

        async def consumer() -> None:
            async for msg in broker.consume("q"):
                received.append(msg)
                break

        task = asyncio.create_task(consumer())
        await asyncio.sleep(0.05)
        assert len(received) == 0

        await broker.publish("q", _make_request())
        await asyncio.sleep(0.05)
        assert len(received) == 1
        await task


class TestInMemoryMessageAck:
    @pytest.mark.asyncio
    async def test_ack_succeeds(self, broker: InMemoryBroker) -> None:
        await broker.publish("q", _make_request())
        async for msg in broker.consume("q"):
            await msg.ack()
            break

    @pytest.mark.asyncio
    async def test_ack_is_idempotent(self, broker: InMemoryBroker) -> None:
        await broker.publish("q", _make_request())
        async for msg in broker.consume("q"):
            await msg.ack()
            await msg.ack()  # second ack is no-op
            break


class TestInMemoryMessageReject:
    @pytest.mark.asyncio
    async def test_reject_succeeds(self, broker: InMemoryBroker) -> None:
        await broker.publish("q", _make_request())
        async for msg in broker.consume("q"):
            await msg.reject()
            break

    @pytest.mark.asyncio
    async def test_rejected_message_not_redelivered(
        self, broker: InMemoryBroker
    ) -> None:
        await broker.publish("q", _make_request())
        async for msg in broker.consume("q"):
            await msg.reject()
            break

        # Queue should be empty — second consume should block
        received = False

        async def try_consume() -> None:
            nonlocal received
            async for _msg in broker.consume("q"):
                received = True
                break

        task = asyncio.create_task(try_consume())
        await asyncio.sleep(0.05)
        assert not received
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


class TestInMemoryMessageNack:
    @pytest.mark.asyncio
    async def test_nack_requeues_message(self, broker: InMemoryBroker) -> None:
        await broker.publish("q", _make_request())
        async for msg in broker.consume("q"):
            assert msg.delivery_count == 1
            await msg.nack(delay=0)
            break

        async for msg in broker.consume("q"):
            assert msg.delivery_count == 2
            break

    @pytest.mark.asyncio
    async def test_nack_with_delay(self, broker: InMemoryBroker) -> None:
        await broker.publish("q", _make_request())
        async for msg in broker.consume("q"):
            await msg.nack(delay=0.1)
            break

        received = False

        async def try_consume() -> None:
            nonlocal received
            async for _msg in broker.consume("q"):
                received = True
                break

        task = asyncio.create_task(try_consume())
        await asyncio.sleep(0.05)
        assert not received  # not yet — delay not elapsed

        await asyncio.sleep(0.1)
        assert received
        await task

    @pytest.mark.asyncio
    async def test_nack_increments_delivery_count_each_time(
        self, broker: InMemoryBroker
    ) -> None:
        await broker.publish("q", _make_request())

        for expected_count in (1, 2, 3):
            async for msg in broker.consume("q"):
                assert msg.delivery_count == expected_count
                await msg.nack(delay=0)
                break


class TestInMemoryBrokerDisconnect:
    @pytest.mark.asyncio
    async def test_disconnect_cancels_delayed_tasks(
        self, broker: InMemoryBroker
    ) -> None:
        await broker.publish("q", _make_request(), delay=10.0)
        assert len(broker._delayed_tasks) == 1

        await broker.disconnect()
        assert len(broker._delayed_tasks) == 0

    @pytest.mark.asyncio
    async def test_disconnect_unblocks_consumer(
        self, broker: InMemoryBroker
    ) -> None:
        consumed: list[InMemoryMessage] = []

        async def consumer() -> None:
            async for msg in broker.consume("q"):
                consumed.append(msg)

        task = asyncio.create_task(consumer())
        await asyncio.sleep(0.05)

        await broker.disconnect()
        await asyncio.sleep(0.05)
        assert task.done()
        assert len(consumed) == 0

    @pytest.mark.asyncio
    async def test_connect_disconnect_are_noop(
        self, broker: InMemoryBroker
    ) -> None:
        await broker.connect()
        await broker.disconnect()
        # No errors — connect/disconnect are safe to call

    @pytest.mark.asyncio
    async def test_publish_with_delay(self, broker: InMemoryBroker) -> None:
        await broker.publish("q", _make_request(), delay=0.1)

        received = False

        async def try_consume() -> None:
            nonlocal received
            async for _msg in broker.consume("q"):
                received = True
                break

        task = asyncio.create_task(try_consume())
        await asyncio.sleep(0.05)
        assert not received

        await asyncio.sleep(0.1)
        assert received
        await task
