"""Redis Streams broker implementation using ``redis.asyncio``."""

from __future__ import annotations

import asyncio
import json
import math
import time
from collections.abc import AsyncIterator
from contextlib import suppress
from typing import TYPE_CHECKING, Final, override
from uuid import uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    ValidationInfo,
    field_validator,
)
from redis.asyncio import Redis
from redis.exceptions import ConnectionError, ResponseError, TimeoutError

from planq.backoff import full_jitter
from planq.broker import BaseBroker
from planq.enums import Header, LogEvent
from planq.log import get_planq_logger
from planq.message import BrokerMessage
from planq.models import JsonRpcRequest, JsonRpcResponse
from planq.types import Seconds

if TYPE_CHECKING:
    from redis.commands.core import AsyncScript

    from planq.types import Headers

_MIGRATE_BATCH_SIZE: Final[int] = 100
"""Max messages migrated per scheduler Lua script run."""

_MIGRATE_CONCURRENCY: Final[int] = 10
"""Max concurrent queue migrations per scheduler tick.

Caps the fan-out of ``asyncio.gather`` inside the scheduler loop to
prevent one tick from saturating the Redis connection pool and
starving the main consume loop."""

_DEFAULT_BLOCK_MS: Final[int] = 2000
"""XREADGROUP block timeout in milliseconds (backpressure)."""

_DEFAULT_MAX_CONNECTIONS: Final[int] = 100
"""Maximum Redis connections in the pool."""

_DELAYED_SUFFIX: Final[str] = ":delayed"
"""ZSET key suffix for delayed messages."""

_DELAYED_QUEUES_KEY: Final[str] = "planq:delayed_queues"
"""ZSET key that registers every queue with at least one delayed
message. Members are stream names; scores are Unix timestamps of
first registration (kept for debugging visibility). The scheduler
reads this registry to discover which queues need migration, so
delayed messages published by one process are still scheduled by
another — replacing the per-process ``_delayed_queues`` set that
was invisible across processes and lost on restart."""

_DEFAULT_MAX_STREAM_LEN: Final[int] = 100_000
"""Approximate cap for XADD MAXLEN ~ to prevent OOM."""

_DEFAULT_SOCKET_TIMEOUT: Final[Seconds] = 5.0
"""TCP socket timeout in seconds."""

_DEFAULT_HEALTH_CHECK_INTERVAL: Final[Seconds] = 30
"""Seconds between connection health checks."""

_RECONNECT_BASE_DELAY: Final[Seconds] = 0.5
"""Initial backoff delay in seconds for transient connection errors."""

_RECONNECT_MAX_DELAY: Final[Seconds] = 30.0
"""Maximum backoff delay in seconds for transient connection errors."""

_MAX_RECOVERY_ATTEMPTS: Final[int] = 5
"""Max retries for the PEL recovery pass on startup. After this many
consecutive transient errors, fall through to the main consume loop;
remaining PEL entries will be picked up by XAUTOCLAIM once
``claim_idle_ms`` elapses."""

_TRANSIENT_ERRORS = (ConnectionError, TimeoutError)
"""Redis exceptions safe to retry in the consume loop.
ConnectionError: connection lost/refused/DNS failure.
TimeoutError: socket timeout (after redis-py internal retries)."""

_NOT_CONNECTED_MSG: Final[str] = (
    "RedisBroker is not connected; "
    "call 'await broker.connect()' first "
    "or use 'async with broker:'."
)
"""User-facing error raised when a broker method is called on a
broker that has not been connected yet. Shared between
:meth:`RedisBroker.publish` and :meth:`RedisBroker.consume`."""

_STREAM_SCHEMA_VERSION: Final[str] = "1"
"""Stream entry format version. Written to the ``v`` field of each
stream entry and delayed-message payload. Future format changes
should bump this value and add parse-time migration logic to
:meth:`RedisBroker._parse_entry`. Entries without the ``v`` field
are treated as legacy v1 for backward compatibility."""

#: Lua script that atomically migrates ready messages from the
#: delayed ZSET to the stream. Runs inside Redis to avoid
#: race conditions between scheduler polls.
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

        if data.v and data.v ~= "" then
            args[#args + 1] = 'v'
            args[#args + 1] = tostring(data.v)
        end

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


def _build_delayed_payload(
    *,
    body: str,
    reply_to: str,
    expire_at: str,
    delivery_count: str,
    headers: str,
    delayed_id: str,
    version: str,
) -> str:
    """Serialize a delayed-message payload for ZSET storage.

    Shared by :meth:`RedisBroker.publish` and
    :meth:`RedisMessage._nack` to guarantee identical field layout —
    the Lua migration script relies on these exact key names.

    ``delayed_id`` is passed in rather than generated inside the
    helper so callers control the UUID (``publish()`` returns it to
    the user for later tracking; ``_nack`` discards it). The UUID
    also ensures uniqueness of the JSON string inside the ZSET —
    identical payloads with the same score would otherwise collide.

    ``version`` is copied to the ``v`` field and migrated through
    XADD by the Lua script so consumers can detect schema drift.
    """
    return json.dumps(
        {
            "v": version,
            "body": body,
            "reply_to": reply_to,
            "expire_at": expire_at,
            "delivery_count": delivery_count,
            "headers": headers,
            "delayed_id": delayed_id,
        }
    )


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
        broker: RedisBroker,
        stream_key: str,
        group_name: str,
        entry_id: str,
        *,
        pel_delivery_count: int = 1,
    ) -> None:
        """Store Redis-specific fields alongside common message data.

        Args:
            raw: Stream entry fields dict (string keys and values).
            body: Validated JSON-RPC request parsed from the body field.
            headers: Normalised planq headers.
            received_at: Unix timestamp when the message was received.
            queue_name: Name of the stream this message was received from.
            broker: The owning :class:`RedisBroker`. Stored as a strong
                reference so that :meth:`_ack`, :meth:`_reject` and
                :meth:`_nack` read the current ``_client`` dynamically —
                if the broker ever swaps its client (reconnect), the
                in-flight message still settles against the live one
                instead of a captured stale reference. The resulting
                circular reference (broker → in-flight message → broker)
                is collected by Python's cycle GC.
            stream_key: Redis stream name (same as queue).
            group_name: Consumer group name for XACK.
            entry_id: Stream entry ID (e.g. ``"1234567890123-0"``).
            pel_delivery_count: Redis PEL ``times_delivered`` counter
                for this entry. Defaults to ``1`` (first delivery).
                Set by the consume loop when re-delivering entries
                from PEL recovery or XAUTOCLAIM so that
                :attr:`delivery_count` reflects crash-driven
                redeliveries in addition to nack-driven retries.
        """
        super().__init__(raw, body, headers, received_at, queue_name)
        self._broker = broker
        self._stream_key = stream_key
        self._group_name = group_name
        self._entry_id = entry_id
        self._pel_delivery_count = pel_delivery_count

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
        """Number of times this message has been handed to a worker.

        Combines the stream-entry ``delivery_count`` field (advanced
        by nack-driven retries via :meth:`_nack`) with the Redis PEL
        ``times_delivered`` counter (advanced by Redis itself on
        every XREADGROUP delivery, including crash recovery and
        XAUTOCLAIM). The formula is
        ``stream_field + (pel_delivery_count - 1)``: NEL starts at
        ``1`` for the first delivery, so the ``-1`` avoids
        double-counting.
        """
        stream_field = int(self.raw.get("delivery_count", "1"))
        return stream_field + (self._pel_delivery_count - 1)

    @property
    @override
    def reply_to(self) -> str | None:
        """Reply-to queue name, or ``None``."""
        return self.raw.get("reply_to") or None

    @override
    async def _ack(self) -> None:
        """XACK to acknowledge and remove from stream."""
        await self._broker._client.xack(
            self._stream_key, self._group_name, self._entry_id
        )

    @override
    async def _reject(self) -> None:
        """XACK to reject and remove from stream."""
        await self._broker._client.xack(
            self._stream_key, self._group_name, self._entry_id
        )

    @override
    async def _nack(self, delay: Seconds) -> None:
        """Remove from stream and re-publish to delayed ZSET.

        Args:
            delay: Seconds before the message becomes visible again.
        """
        delayed_payload = _build_delayed_payload(
            body=self.raw.get("body", ""),
            reply_to=self.raw.get("reply_to", ""),
            expire_at=self.raw.get("expire_at", ""),
            delivery_count=str(self.delivery_count + 1),
            headers=self.raw.get("headers", "{}"),
            delayed_id=str(uuid4()),
            version=self.raw.get("v", _STREAM_SCHEMA_VERSION),
        )
        async with self._broker._client.pipeline(transaction=True) as pipe:
            await pipe.zadd(
                f"{self._stream_key}{_DELAYED_SUFFIX}",
                {delayed_payload: time.time() + delay},
            )
            await pipe.zadd(
                _DELAYED_QUEUES_KEY,
                {self._stream_key: time.time()},
                nx=True,
            )
            await pipe.xack(self._stream_key, self._group_name, self._entry_id)
            await pipe.execute()


class RedisConsumerConfig(BaseModel):
    """Consumer-only configuration for :class:`RedisBroker`.

    Holds Redis Streams parameters that are meaningful only on the
    consumer side: consumer group membership, XAUTOCLAIM tuning, and
    the delayed-message scheduler interval. Producer-only instances of
    :class:`RedisBroker` omit this object entirely.
    """

    model_config = ConfigDict(frozen=True, strict=True)

    # Redis Streams consumer group name (XREADGROUP / XGROUP CREATE).
    group_name: str

    # Unique consumer name within the group. Use a stable identifier
    # (hostname, pod name) so PEL recovery bypasses XAUTOCLAIM on
    # restart.
    consumer_name: str

    # XAUTOCLAIM min idle time in milliseconds. ``0`` disables claiming.
    claim_idle_ms: int = 300_000

    # Seconds between XAUTOCLAIM passes in the consume loop.
    claim_interval: Seconds = 60.0

    # Seconds between delayed-queue scheduler polls. The scheduler
    # migrates ready messages from the ZSET to the stream.
    scheduler_interval: Seconds = 1.0

    @field_validator("group_name", "consumer_name")
    @classmethod
    def validate_non_empty(cls, v: str, info: ValidationInfo) -> str:
        """Reject empty or whitespace-only identifiers."""
        if not v.strip():
            raise ValueError(f"{info.field_name} must not be empty")
        return v

    @field_validator("claim_idle_ms")
    @classmethod
    def validate_claim_idle_ms(cls, v: int) -> int:
        """Ensure claim_idle_ms is non-negative; ``0`` disables claiming."""
        if v < 0:
            raise ValueError(
                "claim_idle_ms must be non-negative (0 disables claiming)"
            )
        return v

    @field_validator("claim_interval")
    @classmethod
    def validate_claim_interval(cls, v: float) -> float:
        """Ensure claim_interval is a finite non-negative number.

        ``0`` is allowed and means "claim on every iteration" (no
        throttling between XAUTOCLAIM passes).
        """
        if math.isnan(v):
            raise ValueError("claim_interval cannot be NaN")
        if math.isinf(v):
            raise ValueError("claim_interval cannot be infinite")
        if v < 0:
            raise ValueError("claim_interval must be non-negative")
        return v

    @field_validator("scheduler_interval")
    @classmethod
    def validate_scheduler_interval(cls, v: float) -> float:
        """Ensure scheduler_interval is a finite positive number.

        ``0`` is not allowed because it would turn the scheduler into
        a busy-loop without any yield point between ticks.
        """
        if math.isnan(v):
            raise ValueError("scheduler_interval cannot be NaN")
        if math.isinf(v):
            raise ValueError("scheduler_interval cannot be infinite")
        if v <= 0:
            raise ValueError("scheduler_interval must be positive")
        return v


class RedisBroker(BaseBroker):
    """Redis Streams broker with ZSET-based delayed message scheduling.

    Uses Redis Streams with consumer groups for immediate message
    delivery and Redis Sorted Sets for delayed messages. A background
    scheduler atomically migrates ready messages from ZSETs to streams
    using a Lua script.

    The broker is usable from both producer and consumer processes. A
    producer-only instance omits the ``consumer`` argument; the delayed
    message scheduler only runs when ``consumer`` is provided, so
    producers incur no extra background task.

    Note:
        Requires Redis Server 6.2 or higher for delayed messages
        migration.

    Example:
        Producer (no consumer config)::

            broker = RedisBroker(dsn="redis://localhost:6379")

        Consumer::

            broker = RedisBroker(
                dsn="redis://localhost:6379",
                consumer=RedisConsumerConfig(
                    group_name="workers",
                    consumer_name=socket.gethostname(),
                ),
            )

    Attributes:
        dsn: Redis connection URL (e.g. ``redis://localhost:6379``).
    """

    def __init__(
        self,
        dsn: str,
        *,
        consumer: RedisConsumerConfig | None = None,
        max_connections: int = _DEFAULT_MAX_CONNECTIONS,
        max_stream_len: int | None = _DEFAULT_MAX_STREAM_LEN,
        socket_timeout: Seconds = _DEFAULT_SOCKET_TIMEOUT,
        health_check_interval: Seconds = _DEFAULT_HEALTH_CHECK_INTERVAL,
        retry_on_timeout: bool = True,
    ) -> None:
        """Initialize the Redis broker.

        Args:
            dsn: Redis connection URL passed to ``Redis.from_url()``.
            consumer: Consumer-side configuration (group name, consumer
                name, XAUTOCLAIM tuning, scheduler interval). Required
                when the broker is used to ``consume()`` messages;
                omit for producer-only instances.
            max_connections: Maximum Redis connections in the pool.
            max_stream_len: Approximate MAXLEN cap for XADD.
                ``None`` disables the cap.
            socket_timeout: TCP socket timeout in seconds.
            health_check_interval: Seconds between connection
                health checks.
            retry_on_timeout: Retry commands on timeout errors.
        """
        super().__init__(dsn)
        self._consumer = consumer
        self._max_connections = max_connections
        self._max_stream_len = max_stream_len
        self._socket_timeout = socket_timeout
        self._health_check_interval = health_check_interval
        self._retry_on_timeout = retry_on_timeout
        self._client: Redis | None = None
        self._scheduler_task: asyncio.Task[None] | None = None
        self._migrate_script: AsyncScript | None = None

    @override
    async def connect(self) -> None:
        """Create a Redis client and start the scheduler task.

        Idempotent: calling :meth:`connect` on an already-connected
        broker is a no-op and reuses the existing client.

        The delayed-message scheduler and the ``MIGRATE_LUA`` script
        run only when a :class:`RedisConsumerConfig` was provided.
        Producer-only instances skip both and rely on some other
        consumer-equipped process to migrate delayed messages.
        """
        if self._client is not None:
            return
        self._client = Redis.from_url(
            self.dsn,
            decode_responses=True,
            socket_timeout=self._socket_timeout,
            health_check_interval=self._health_check_interval,
            retry_on_timeout=self._retry_on_timeout,
            max_connections=self._max_connections,
        )
        if self._consumer is not None:
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

        Raises:
            RuntimeError: If the broker has not been connected yet.
        """
        if self._client is None:
            raise RuntimeError(_NOT_CONNECTED_MSG)

        body_json = rpc.model_dump_json()
        reply_to_str = reply_to or ""
        expire_at_str = str(expire_at) if expire_at is not None else ""
        headers_str = json.dumps(headers) if headers else "{}"

        if delay is not None and delay > 0:
            delayed_id = str(uuid4())
            delayed_payload = _build_delayed_payload(
                body=body_json,
                reply_to=reply_to_str,
                expire_at=expire_at_str,
                delivery_count="1",
                headers=headers_str,
                delayed_id=delayed_id,
                version=_STREAM_SCHEMA_VERSION,
            )
            delayed_key = f"{queue}{_DELAYED_SUFFIX}"
            score = time.time() + delay
            # MULTI/EXEC the payload ZADD together with the
            # registry ZADD so a process crash between the two
            # cannot leave a queue orphaned in the ZSET without
            # being discoverable by the scheduler.
            async with self._client.pipeline(transaction=True) as pipe:
                pipe.zadd(delayed_key, {delayed_payload: score})
                pipe.zadd(
                    _DELAYED_QUEUES_KEY,
                    {queue: time.time()},
                    nx=True,
                )
                await pipe.execute()
            return delayed_id

        fields: dict[str, str] = {
            "v": _STREAM_SCHEMA_VERSION,
            "body": body_json,
            "delivery_count": "1",
        }
        if reply_to:
            fields["reply_to"] = reply_to
        if expire_at is not None:
            fields["expire_at"] = str(expire_at)
        if headers:
            fields["headers"] = json.dumps(headers)

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
            block_ms: Milliseconds XREADGROUP blocks waiting for new
                messages. ``0`` means block *indefinitely* (Redis
                semantics), which is safe only in unit tests where
                the client is mocked. In real usage pass a positive
                value; the default is 2000 ms.

        Yields:
            :class:`RedisMessage` instances ready for processing.

        Raises:
            RuntimeError: If the broker has not been connected yet, or
                if it was constructed without a ``consumer`` config
                (producer-only instance).
        """
        if self._client is None:
            raise RuntimeError(_NOT_CONNECTED_MSG)
        if self._consumer is None:
            raise RuntimeError(
                "RedisBroker.consume() requires a RedisConsumerConfig; "
                "pass consumer=... to the RedisBroker constructor."
            )
        consumer_cfg = self._consumer

        queue_name = self.get_queue_name(queue)

        try:
            await self._client.xgroup_create(
                queue, consumer_cfg.group_name, "0", mkstream=True
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

        # Recovery: re-deliver own pending messages from PEL.
        # Retries on transient Redis errors with full-jitter backoff;
        # ``last_id`` is advanced before each yield so a retry
        # resumes from the next PEL entry instead of re-yielding one
        # that was already handed to the consumer.
        recovery_attempts = 0
        last_id = "0-0"
        while True:
            try:
                while True:
                    recovery_entries = await self._client.xreadgroup(
                        consumer_cfg.group_name,
                        consumer_cfg.consumer_name,
                        {queue: last_id},
                        count=prefetch,
                        block=None,
                    )
                    if not recovery_entries or not recovery_entries[0][1]:
                        break

                    received_at = time.time()
                    batch_ids = [
                        entry_id
                        for _stream_name, messages in recovery_entries
                        for entry_id, _fields in messages
                    ]
                    pel_counts = await self._get_pel_counts(queue, batch_ids)
                    for _stream_name, messages in recovery_entries:
                        for entry_id, fields in messages:
                            last_id = entry_id
                            msg = await self._parse_entry(
                                entry_id,
                                fields,
                                queue,
                                queue_name,
                                received_at,
                                pel_delivery_count=pel_counts.get(entry_id, 1),
                            )
                            if msg is not None:
                                yield msg
                break
            except _TRANSIENT_ERRORS:
                recovery_attempts += 1
                if recovery_attempts > _MAX_RECOVERY_ATTEMPTS:
                    logger.error(
                        "PEL recovery failed after %d attempts for %r;"
                        " falling through to main loop. Remaining"
                        " entries will be picked up by XAUTOCLAIM.",
                        recovery_attempts,
                        queue,
                        exc_info=True,
                    )
                    break
                delay = full_jitter(
                    recovery_attempts,
                    _RECONNECT_BASE_DELAY,
                    _RECONNECT_MAX_DELAY,
                )
                logger.warning(
                    "PEL recovery transient error for %r;"
                    " retrying in %.2fs (attempt %d/%d).",
                    queue,
                    delay,
                    recovery_attempts,
                    _MAX_RECOVERY_ATTEMPTS,
                    exc_info=True,
                )
                await asyncio.sleep(delay)
            except Exception:
                logger.warning(
                    "Failed to read pending messages on startup for %r.",
                    queue,
                    exc_info=True,
                )
                break

        last_claim_at = time.monotonic()
        claim_start_id = "0-0"
        claim_consecutive_errors = 0
        claim_backoff_until: float = 0.0
        consecutive_errors = 0

        while True:
            now_mono = time.monotonic()
            has_more_to_claim = False

            if (
                consumer_cfg.claim_idle_ms > 0
                and now_mono - last_claim_at >= consumer_cfg.claim_interval
                and now_mono >= claim_backoff_until
            ):
                try:
                    result = await self._client.xautoclaim(
                        queue,
                        consumer_cfg.group_name,
                        consumer_cfg.consumer_name,
                        min_idle_time=consumer_cfg.claim_idle_ms,
                        start_id=claim_start_id,
                        count=prefetch,
                    )
                    claim_consecutive_errors = 0
                    claim_backoff_until = 0.0
                    next_id = result[0]
                    claimed_messages = result[1]
                    if claimed_messages:
                        received_at = time.time()
                        claimed_ids = [
                            entry_id for entry_id, _ in claimed_messages
                        ]
                        pel_counts = await self._get_pel_counts(
                            queue, claimed_ids
                        )
                        for entry_id, fields in claimed_messages:
                            msg = await self._parse_entry(
                                entry_id,
                                fields,
                                queue,
                                queue_name,
                                received_at,
                                pel_delivery_count=pel_counts.get(entry_id, 1),
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
                    claim_consecutive_errors += 1
                    claim_delay = full_jitter(
                        claim_consecutive_errors,
                        _RECONNECT_BASE_DELAY,
                        _RECONNECT_MAX_DELAY,
                    )
                    claim_backoff_until = now_mono + claim_delay
                    last_claim_at = now_mono
                    claim_start_id = "0-0"
                    logger.warning(
                        "XAUTOCLAIM failed for queue %r;"
                        " backing off %.2fs (attempt %d).",
                        queue,
                        claim_delay,
                        claim_consecutive_errors,
                        exc_info=True,
                    )

            current_block = None if has_more_to_claim else block_ms
            try:
                entries = await self._client.xreadgroup(
                    consumer_cfg.group_name,
                    consumer_cfg.consumer_name,
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
        pel_delivery_count: int = 1,
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
            pel_delivery_count: Redis PEL ``times_delivered`` counter
                for this entry. Defaults to ``1`` (first delivery).
                The recovery and claim paths pass a higher value to
                reflect crash-driven redeliveries.

        Returns:
            A :class:`RedisMessage` or ``None`` for poison messages.
        """
        assert self._client is not None
        # Invariant: only reached from consume(), which has already
        # verified self._consumer is not None.
        assert self._consumer is not None
        group_name = self._consumer.group_name

        version = fields.get("v", _STREAM_SCHEMA_VERSION)
        if version != _STREAM_SCHEMA_VERSION:
            logger.warning(
                "Unknown stream schema version %r in queue %r"
                " (entry %s); attempting to parse as v%s.",
                version,
                queue_name,
                entry_id,
                _STREAM_SCHEMA_VERSION,
            )

        raw_body = fields.get("body", "")

        try:
            body = JsonRpcRequest.model_validate_json(raw_body)
        except Exception as exc:
            hook_ok = False
            try:
                await self.on_poison_message(entry_id, raw_body, queue, exc)
                hook_ok = True
            except Exception as inner_exc:
                log_ctx = {
                    "event": LogEvent.POISON_MESSAGE_HANDLING_FAILED,
                    "message_id": entry_id,
                    "queue_name": queue_name,
                }
                logger.error(
                    "Failed to handle poison message %(message_id)s in"
                    " %(queue_name)r. Leaving entry in PEL for next"
                    " recovery pass — ensure your on_poison_message hook"
                    " is idempotent and robust, or you will see infinite"
                    " redelivery loops.",
                    log_ctx,
                    extra=log_ctx,
                    exc_info=inner_exc,
                )

            if hook_ok:
                try:
                    await self._client.xack(queue, group_name, entry_id)
                except Exception:
                    logger.warning(
                        "Failed to XACK poison message %r in queue %r;"
                        " it will be re-delivered on next recovery.",
                        entry_id,
                        queue,
                        exc_info=True,
                    )
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
            broker=self,
            stream_key=queue,
            group_name=group_name,
            entry_id=entry_id,
            pel_delivery_count=pel_delivery_count,
        )

    async def _get_pel_counts(
        self, queue: str, entry_ids: list[str]
    ) -> dict[str, int]:
        """Batch-read PEL ``times_delivered`` for given entry IDs.

        Uses a pipelined ``XPENDING_RANGE`` call per entry so the
        roundtrip cost scales with the batch size rather than with the
        number of entries in the full PEL. Entries that are no longer
        in the PEL (e.g. acknowledged between the recovery read and
        this call) are omitted from the result.

        Args:
            queue: Stream key.
            entry_ids: Stream entry IDs to look up.

        Returns:
            Dict mapping ``entry_id`` → ``times_delivered``. Missing
            entries are absent from the dict.
        """
        assert self._client is not None
        assert self._consumer is not None
        if not entry_ids:
            return {}
        async with self._client.pipeline(transaction=False) as pipe:
            for eid in entry_ids:
                pipe.xpending_range(
                    name=queue,
                    groupname=self._consumer.group_name,
                    min=eid,
                    max=eid,
                    count=1,
                )
            results = await pipe.execute()
        out: dict[str, int] = {}
        for eid, res in zip(entry_ids, results):
            if res:
                out[eid] = int(res[0]["times_delivered"])
        return out

    async def _list_delayed_queues(self) -> list[str]:
        """Return all queue names registered in the delayed-queues ZSET.

        Reads ``planq:delayed_queues`` so the scheduler can migrate
        delayed messages regardless of which process published them —
        producers register their queue here atomically with the first
        delayed publish, and the (possibly different) consumer process
        reads the registry on each scheduler tick.
        """
        assert self._client is not None
        return await self._client.zrange(_DELAYED_QUEUES_KEY, 0, -1)

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
        """Background loop that migrates ready delayed messages.

        Auto-restarts on unexpected exceptions with full-jitter
        backoff so a transient Redis error (e.g., a pool reconnect
        burst) does not leave delayed messages permanently
        unmigrated. :class:`asyncio.CancelledError` is re-raised so
        :meth:`disconnect` can shut the task down cleanly.
        """
        assert self._consumer is not None
        consumer_cfg = self._consumer
        sem = asyncio.Semaphore(_MIGRATE_CONCURRENCY)
        consecutive_errors = 0

        async def _bounded_migrate(q: str) -> None:
            async with sem:
                await self._migrate_one_queue(q)

        while True:
            try:
                await asyncio.sleep(consumer_cfg.scheduler_interval)
                queues = await self._list_delayed_queues()
                if queues:
                    await asyncio.gather(*(_bounded_migrate(q) for q in queues))
                consecutive_errors = 0
            except asyncio.CancelledError:
                raise
            except Exception:
                consecutive_errors += 1
                delay = full_jitter(
                    consecutive_errors,
                    _RECONNECT_BASE_DELAY,
                    _RECONNECT_MAX_DELAY,
                )
                logger.warning(
                    "Scheduler loop error; retrying in %.2fs (attempt %d).",
                    delay,
                    consecutive_errors,
                    exc_info=True,
                )
                await asyncio.sleep(delay)
