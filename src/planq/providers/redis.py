"""Redis Streams broker implementation using ``redis.asyncio``."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from contextlib import suppress
from typing import TYPE_CHECKING, Final, override
from uuid import uuid4

from redis.asyncio import Redis
from redis.exceptions import ConnectionError, ResponseError, TimeoutError

from planq.backoff import full_jitter
from planq.broker import BaseBroker
from planq.enums import Header, LogEvent
from planq.log import get_planq_logger
from planq.message import BrokerMessage
from planq.models import JsonRpcRequest, JsonRpcResponse

if TYPE_CHECKING:
    from redis.commands.core import AsyncScript

    from planq.types import Headers, Seconds

_MIGRATE_BATCH_SIZE: Final[int] = 100
"""Max messages migrated per scheduler Lua script run."""

_DEFAULT_BLOCK_MS: Final[int] = 2000
"""XREADGROUP block timeout in milliseconds (backpressure)."""

_DEFAULT_MAX_CONNECTIONS: Final[int] = 100
"""Maximum Redis connections in the pool."""

_DEFAULT_SCHEDULER_INTERVAL: Final[Seconds] = 1.0
"""Seconds between delayed-queue scheduler polls."""

_DELAYED_SUFFIX: Final[str] = ":delayed"
"""ZSET key suffix for delayed messages."""

_DEFAULT_MAX_STREAM_LEN: Final[int] = 100_000
"""Approximate cap for XADD MAXLEN ~ to prevent OOM."""

_DEFAULT_CLAIM_IDLE_MS: Final[int] = 300_000
"""XAUTOCLAIM min idle time in ms (5 minutes)."""

_DEFAULT_CLAIM_INTERVAL: Final[Seconds] = 60.0
"""Seconds between XAUTOCLAIM checks in consume loop."""

_DEFAULT_SOCKET_TIMEOUT: Final[Seconds] = 5.0
"""TCP socket timeout in seconds."""

_DEFAULT_HEALTH_CHECK_INTERVAL: Final[Seconds] = 30
"""Seconds between connection health checks."""

_RECONNECT_BASE_DELAY: Final[Seconds] = 0.5
"""Initial backoff delay in seconds for transient connection errors."""

_RECONNECT_MAX_DELAY: Final[Seconds] = 30.0
"""Maximum backoff delay in seconds for transient connection errors."""

_TRANSIENT_ERRORS = (ConnectionError, TimeoutError)
"""Redis exceptions safe to retry in the consume loop.
ConnectionError: connection lost/refused/DNS failure.
TimeoutError: socket timeout (after redis-py internal retries)."""

MIGRATE_LUA: Final[str] = """
local delayed_key = KEYS[1]
local stream_key = KEYS[2]
local now = ARGV[1]
local batch = tonumber(ARGV[2])
local max_len = tonumber(ARGV[3])

local items = redis.call(
    'ZRANGE', delayed_key, '-inf', now, 'BYSCORE', 'LIMIT', 0, batch
)
if #items == 0 then return 0 end

redis.call('ZREM', delayed_key, unpack(items))

local migrated = 0

for _, raw in ipairs(items) do
    local ok, data = pcall(cjson.decode, raw)

    if not ok then
        redis.log(
            redis.LOG_WARNING,
            'planq: skipped corrupt delayed entry in ' .. delayed_key
        )
    else
        local args = {'XADD', stream_key}

        if max_len > 0 then
            args[#args + 1] = 'MAXLEN'
            args[#args + 1] = '~'
            args[#args + 1] = max_len
        end

        args[#args + 1] = '*'
        args[#args + 1] = 'body'
        args[#args + 1] = tostring(data.body)

        if data.reply_to and data.reply_to ~= "" then
            args[#args + 1] = 'reply_to'
            args[#args + 1] = tostring(data.reply_to)
        end

        if data.expire_at and data.expire_at ~= "" then
            args[#args + 1] = 'expire_at'
            args[#args + 1] = tostring(data.expire_at)
        end

        if data.delivery_count then
            args[#args + 1] = 'delivery_count'
            args[#args + 1] = tostring(data.delivery_count)
        end

        if data.headers and data.headers ~= "{}" and data.headers ~= "" then
            args[#args + 1] = 'headers'
            args[#args + 1] = tostring(data.headers)
        end

        redis.call(unpack(args))
        migrated = migrated + 1
    end
end

return migrated
"""

logger = get_planq_logger(__name__)


class RedisMessage(BrokerMessage):
    """Redis Streams message wrapper.

    Acknowledgement and rejection ``XACK`` the entry.
    Negative acknowledgement removes the entry and re-publishes to the
    delayed ZSET with an incremented ``delivery_count``.

    Attributes:
        raw: Raw stream entry fields dict.
        body: Parsed JSON-RPC request.
        headers: Normalised planq headers.
    """

    def __init__(
        self,
        raw: dict[str, str],
        body: JsonRpcRequest,
        headers: Headers,
        received_at: Seconds,
        queue_name: str,
        redis_client: Redis,
        stream_key: str,
        group_name: str,
        entry_id: str,
    ) -> None:
        """Store Redis-specific fields alongside common message data.

        Args:
            raw: Stream entry fields dict (string keys and values).
            body: Validated JSON-RPC request parsed from the body field.
            headers: Normalised planq headers.
            received_at: Unix timestamp when the message was received.
            queue_name: Name of the stream this message was received from.
            redis_client: Active ``redis.asyncio.Redis`` client.
            stream_key: Redis stream name (same as queue).
            group_name: Consumer group name for XACK.
            entry_id: Stream entry ID (e.g. ``"1234567890123-0"``).
        """
        super().__init__(raw, body, headers, received_at, queue_name)
        self._redis_client = redis_client
        self._stream_key = stream_key
        self._group_name = group_name
        self._entry_id = entry_id

    @property
    @override
    def message_id(self) -> str:
        """Redis stream entry ID."""
        return self._entry_id

    @property
    @override
    def enqueued_at(self) -> float:
        """Unix timestamp derived from the stream entry ID."""
        return int(self._entry_id.split("-")[0]) / 1000.0

    @property
    @override
    def delivery_count(self) -> int:
        """Number of times this message has been delivered."""
        return int(self.raw.get("delivery_count", "1"))

    @property
    @override
    def reply_to(self) -> str | None:
        """Reply-to queue name, or ``None``."""
        return self.raw.get("reply_to") or None

    @override
    async def _ack(self) -> None:
        """XACK to acknowledge and remove from stream."""
        await self._redis_client.xack(
            self._stream_key, self._group_name, self._entry_id
        )

    @override
    async def _reject(self) -> None:
        """XACK to reject and remove from stream."""
        await self._redis_client.xack(
            self._stream_key, self._group_name, self._entry_id
        )

    @override
    async def _nack(self, delay: Seconds) -> None:
        """Remove from stream and re-publish to delayed ZSET.

        Args:
            delay: Seconds before the message becomes visible again.
        """
        delayed_payload = json.dumps(
            {
                "body": self.raw.get("body", ""),
                "reply_to": self.raw.get("reply_to", ""),
                "expire_at": self.raw.get("expire_at", ""),
                "delivery_count": str(self.delivery_count + 1),
                "headers": self.raw.get("headers", "{}"),
                "delayed_id": str(uuid4()),
            }
        )
        async with self._redis_client.pipeline(transaction=True) as pipe:
            await pipe.zadd(
                f"{self._stream_key}{_DELAYED_SUFFIX}",
                {delayed_payload: time.time() + delay},
            )
            await pipe.xack(self._stream_key, self._group_name, self._entry_id)
            await pipe.execute()


class RedisBroker(BaseBroker):
    """Redis Streams broker with ZSET-based delayed message scheduling.

    Uses Redis Streams with consumer groups for immediate message
    delivery and Redis Sorted Sets for delayed messages. A background
    scheduler atomically migrates ready messages from ZSETs to streams
    using a Lua script.

    Note:
        Requires Redis Server 6.2 or higher for delayed messages migration.

    Attributes:
        dsn: Redis connection URL (e.g. ``redis://localhost:6379``).
    """

    def __init__(
        self,
        dsn: str,
        *,
        group_name: str,
        consumer_name: str,
        max_connections: int = _DEFAULT_MAX_CONNECTIONS,
        scheduler_interval: Seconds = _DEFAULT_SCHEDULER_INTERVAL,
        max_stream_len: int | None = _DEFAULT_MAX_STREAM_LEN,
        claim_idle_ms: int = _DEFAULT_CLAIM_IDLE_MS,
        claim_interval: Seconds = _DEFAULT_CLAIM_INTERVAL,
        socket_timeout: Seconds = _DEFAULT_SOCKET_TIMEOUT,
        health_check_interval: int = _DEFAULT_HEALTH_CHECK_INTERVAL,
        retry_on_timeout: bool = True,
    ) -> None:
        """Initialize the Redis broker.

        Args:
            dsn: Redis connection URL passed to ``Redis.from_url()``.
            group_name: Consumer group name for XREADGROUP.
            consumer_name: Unique consumer name within the consumer group.
                It is highly recommended to use a deterministic and stable
                identifier (e.g., `socket.gethostname()`, a Kubernetes Pod
                name, or a fixed string) rather than a random UUID.
                A stable name allows the broker to instantly recover and
                resume processing its own pending messages (PEL) upon
                restart, bypassing the slow `XAUTOCLAIM` timeout.
            max_connections: Maximum Redis connections in the pool.
            scheduler_interval: Seconds between delayed-queue scheduler
                polls. Defaults to ``_DEFAULT_SCHEDULER_INTERVAL``.
            max_stream_len: Approximate MAXLEN cap for XADD.
                ``None`` disables the cap.
            claim_idle_ms: XAUTOCLAIM minimum idle time in
                milliseconds. ``0`` disables claiming.
            claim_interval: Seconds between XAUTOCLAIM checks.
            socket_timeout: TCP socket timeout in seconds.
            health_check_interval: Seconds between connection
                health checks.
            retry_on_timeout: Retry commands on timeout errors.
        """
        super().__init__(dsn)
        self._group_name = group_name
        self._consumer_name = consumer_name
        self._max_connections = max_connections
        self._scheduler_interval = scheduler_interval
        self._max_stream_len = max_stream_len
        self._claim_idle_ms = claim_idle_ms
        self._claim_interval = claim_interval
        self._socket_timeout = socket_timeout
        self._health_check_interval = health_check_interval
        self._retry_on_timeout = retry_on_timeout
        self._client: Redis | None = None
        self._scheduler_task: asyncio.Task[None] | None = None
        self._delayed_queues: set[str] = set()
        self._migrate_script: AsyncScript | None = None

    @override
    async def connect(self) -> None:
        """Create a Redis client and start the scheduler task."""
        self._client = Redis.from_url(
            self.dsn,
            decode_responses=True,
            socket_timeout=self._socket_timeout,
            health_check_interval=self._health_check_interval,
            retry_on_timeout=self._retry_on_timeout,
            max_connections=self._max_connections,
        )
        self._migrate_script = self._client.register_script(MIGRATE_LUA)
        self._scheduler_task = asyncio.create_task(self._run_scheduler())

    @override
    async def disconnect(self) -> None:
        """Cancel the scheduler and close the Redis client."""
        if self._scheduler_task is not None:
            self._scheduler_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._scheduler_task
            self._scheduler_task = None

        if self._client is not None:
            await self._client.aclose()
            self._client = None
            self._migrate_script = None

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
        """Publish a JSON-RPC message to a Redis stream.

        Args:
            queue: Destination stream name.
            rpc: JSON-RPC request or response to send.
            delay: Seconds to defer delivery. ``None`` or ``<= 0`` means
                immediate. Delayed messages go to a ZSET and are migrated
                by the scheduler.
            expire_at: Unix timestamp stored as the ``expire_at`` field.
            reply_to: Optional queue name for the consumer's response.
            headers: Optional user-defined headers serialized as JSON.

        Returns:
            The stream entry ID for immediate messages, or a UUID string
            for delayed messages.
        """
        assert self._client is not None

        fields: dict[str, str] = {
            "body": rpc.model_dump_json(),
            "delivery_count": "1",
        }

        if reply_to:
            fields["reply_to"] = reply_to

        if expire_at is not None:
            fields["expire_at"] = str(expire_at)

        if headers:
            fields["headers"] = json.dumps(headers)

        if delay is not None and delay > 0:
            delayed_id = str(uuid4())
            delayed_payload = json.dumps(
                {
                    **fields,
                    "delayed_id": delayed_id,
                }
            )
            delayed_key = f"{queue}{_DELAYED_SUFFIX}"
            score = time.time() + delay
            await self._client.zadd(delayed_key, {delayed_payload: score})
            self._delayed_queues.add(queue)
            return delayed_id

        entry_id = await self._client.xadd(
            queue,
            fields,
            maxlen=self._max_stream_len,
            approximate=True,
        )
        return entry_id

    @override
    async def consume(
        self,
        queue: str,
        *,
        prefetch: int = 10,
        block_ms: int = _DEFAULT_BLOCK_MS,
    ) -> AsyncIterator[RedisMessage]:
        """Read messages from a Redis stream via consumer groups.

        Creates the consumer group if it does not exist. Poison messages
        (unparseable bodies) are logged and deleted. Periodically runs
        XAUTOCLAIM to recover messages stuck with crashed consumers.

        Args:
            queue: Source stream name.
            prefetch: Maximum messages per XREADGROUP call.
            block_ms: Milliseconds to block waiting for new messages.
                Use ``0`` in tests to avoid blocking.

        Yields:
            :class:`RedisMessage` instances ready for processing.
        """
        assert self._client is not None

        queue_name = self.get_queue_name(queue)

        try:
            await self._client.xgroup_create(
                queue, self._group_name, "0", mkstream=True
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

        self._delayed_queues.add(queue)

        # Recovery: re-deliver own pending messages from PEL
        try:
            last_id = "0-0"
            while True:
                recovery_entries = await self._client.xreadgroup(
                    self._group_name,
                    self._consumer_name,
                    {queue: last_id},
                    count=prefetch,
                    block=None,
                )
                if not recovery_entries or not recovery_entries[0][1]:
                    break

                received_at = time.time()
                for _stream_name, messages in recovery_entries:
                    for entry_id, fields in messages:
                        last_id = entry_id
                        msg = await self._parse_entry(
                            entry_id,
                            fields,
                            queue,
                            queue_name,
                            received_at,
                        )
                        if msg is not None:
                            yield msg
        except Exception:
            logger.warning(
                "Failed to read pending messages on startup for %r.",
                queue,
                exc_info=True,
            )

        last_claim_at = time.monotonic()
        claim_start_id = "0-0"
        consecutive_errors = 0

        while True:
            now_mono = time.monotonic()
            has_more_to_claim = False

            if (
                self._claim_idle_ms > 0
                and now_mono - last_claim_at >= self._claim_interval
            ):
                try:
                    result = await self._client.xautoclaim(
                        queue,
                        self._group_name,
                        self._consumer_name,
                        min_idle_time=self._claim_idle_ms,
                        start_id=claim_start_id,
                        count=prefetch,
                    )
                    next_id = result[0]
                    claimed_messages = result[1]
                    if claimed_messages:
                        received_at = time.time()
                        for entry_id, fields in claimed_messages:
                            msg = await self._parse_entry(
                                entry_id, fields, queue, queue_name, received_at
                            )
                            if msg is not None:
                                yield msg

                    if next_id == "0-0":
                        last_claim_at = now_mono
                        claim_start_id = "0-0"
                    else:
                        claim_start_id = next_id
                        has_more_to_claim = True
                except Exception:
                    last_claim_at = now_mono
                    claim_start_id = "0-0"
                    logger.warning(
                        "XAUTOCLAIM failed for queue %r.",
                        queue,
                        exc_info=True,
                    )

            current_block = None if has_more_to_claim else block_ms
            try:
                entries = await self._client.xreadgroup(
                    self._group_name,
                    self._consumer_name,
                    {queue: ">"},
                    count=prefetch,
                    block=current_block,
                )
            except _TRANSIENT_ERRORS:
                consecutive_errors += 1
                delay = full_jitter(
                    consecutive_errors,
                    _RECONNECT_BASE_DELAY,
                    _RECONNECT_MAX_DELAY,
                )
                log_ctx = {
                    "event": LogEvent.CONSUME_CONNECTION_ERROR,
                    "queue_name": queue_name,
                    "attempt": consecutive_errors,
                    "delay_seconds": round(delay, 2),
                }
                logger.warning(
                    "Redis connection error in consume loop"
                    " for %(queue_name)r, retrying in"
                    " %(delay_seconds)ss (attempt %(attempt)d).",
                    log_ctx,
                    extra=log_ctx,
                    exc_info=True,
                )
                await asyncio.sleep(delay)
                continue

            consecutive_errors = 0

            if not entries:
                continue

            received_at = time.time()

            for _stream_name, messages in entries:
                for entry_id, fields in messages:
                    msg = await self._parse_entry(
                        entry_id,
                        fields,
                        queue,
                        queue_name,
                        received_at,
                    )
                    if msg is not None:
                        yield msg

    async def _parse_entry(
        self,
        entry_id: str,
        fields: dict[str, str],
        queue: str,
        queue_name: str,
        received_at: float,
    ) -> RedisMessage | None:
        """Parse a stream entry into a RedisMessage.

        Returns ``None`` if the entry is a poison message (logged and
        deleted).

        Args:
            entry_id: Stream entry ID.
            fields: Raw stream entry fields dict.
            queue: Redis stream key.
            queue_name: Logical queue name for the message.
            received_at: Unix timestamp when the entry was received.

        Returns:
            A :class:`RedisMessage` or ``None`` for poison messages.
        """
        assert self._client is not None

        raw_body = fields.get("body", "")

        try:
            body = JsonRpcRequest.model_validate_json(raw_body)
        except Exception as exc:
            try:
                await self.on_poison_message(entry_id, raw_body, queue, exc)
            except Exception as inner_exc:
                log_ctx = {
                    "event": LogEvent.POISON_MESSAGE_HANDLING_FAILED,
                    "message_id": entry_id,
                    "queue_name": queue_name,
                }
                logger.error(
                    "Failed to handle poison message."
                    " Message ID: %(message_id)s.",
                    log_ctx,
                    extra=log_ctx,
                    exc_info=inner_exc,
                )
            finally:
                with suppress(Exception):
                    await self._client.xack(queue, self._group_name, entry_id)
            return None

        msg_headers: Headers = {}

        if expire_at_val := fields.get("expire_at", ""):
            msg_headers[Header.EXPIRE_AT] = expire_at_val

        user_headers_raw = fields.get("headers", "{}")
        if user_headers_raw and user_headers_raw != "{}":
            try:
                user_headers = json.loads(user_headers_raw)
                msg_headers.update(user_headers)
            except json.JSONDecodeError:
                pass

        return RedisMessage(
            raw=fields,
            body=body,
            headers=msg_headers,
            received_at=received_at,
            queue_name=queue_name,
            redis_client=self._client,
            stream_key=queue,
            group_name=self._group_name,
            entry_id=entry_id,
        )

    async def _migrate_one_queue(self, queue: str) -> None:
        """Migrate ready delayed messages for one queue."""
        delayed_key = f"{queue}{_DELAYED_SUFFIX}"
        try:
            while True:
                migrated = await self._migrate_script(
                    keys=(delayed_key, queue),
                    args=(
                        str(time.time()),
                        _MIGRATE_BATCH_SIZE,
                        self._max_stream_len or 0,
                    ),
                )
                if migrated < _MIGRATE_BATCH_SIZE:
                    break
        except Exception:
            logger.warning(
                "Scheduler failed to migrate delayed messages for queue %r.",
                queue,
                exc_info=True,
            )

    async def _run_scheduler(self) -> None:
        """Background loop that migrates ready delayed messages."""
        while True:
            await asyncio.sleep(self._scheduler_interval)
            if queues := tuple(self._delayed_queues):
                await asyncio.gather(
                    *(self._migrate_one_queue(q) for q in queues)
                )
