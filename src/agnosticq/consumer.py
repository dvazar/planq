from __future__ import annotations

import asyncio
import logging
import signal
import time
from concurrent.futures import ProcessPoolExecutor
from random import uniform
from typing import TYPE_CHECKING, Any, Callable

from agnosticq.base import BaseBroker
from agnosticq.enums import ExecutionMode, Header, JsonRpcError
from agnosticq.models import (
    ConsumerSettings,
    JsonRpcErrorDetail,
    JsonRpcResponse,
)

if TYPE_CHECKING:
    from agnosticq.message import BrokerMessage
    from agnosticq.types import Seconds

logger = logging.getLogger(__name__)


class AgnosticConsumer:
    def __init__(
        self,
        broker: BaseBroker,
        process_workers: int | None = None,
        settings: ConsumerSettings | None = None,
    ) -> None:
        self.broker = broker
        self.routes: dict[str, tuple[Callable[..., Any], ExecutionMode]] = {}
        self._pool: ProcessPoolExecutor | None = (
            ProcessPoolExecutor(max_workers=process_workers)
            if process_workers
            else None
        )
        self._settings = (
            settings if settings is not None else ConsumerSettings()
        )

    def task(
        self,
        name: str,
        mode: ExecutionMode = ExecutionMode.ASYNC,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.routes[name] = (func, mode)
            return func

        return decorator

    def _calculate_backoff(self, delivery_count: int) -> Seconds:
        s = self._settings
        return min(
            s.retry_base_delay * 2 ** (delivery_count - 1)
            + uniform(0, s.retry_jitter),
            s.retry_max_delay,
        )

    async def _execute(
        self,
        handler: Callable[..., Any],
        mode: ExecutionMode,
        params: Any,
    ) -> Any:
        match mode:
            case ExecutionMode.ASYNC:
                return await handler(params)
            case ExecutionMode.THREAD:
                return await asyncio.to_thread(handler, params)
            case ExecutionMode.PROCESS:
                if self._pool is None:
                    raise RuntimeError(
                        "ProcessPoolExecutor not configured; "
                        "set process_workers in AgnosticConsumer"
                    )
                loop = asyncio.get_running_loop()
                return await loop.run_in_executor(self._pool, handler, params)

    async def _process_message(self, msg: BrokerMessage) -> None:
        # 1. Check TTL
        expire_at = msg.headers.get(Header.EXPIRE_AT)
        if expire_at is not None and time.time() > float(expire_at):
            logger.debug("Message expired, rejecting: %s", msg.body.method)
            await msg.reject()
            return

        # 2. Check retries
        max_retries = msg.headers.get(Header.MAX_RETRIES)
        if max_retries is not None and msg.delivery_count > int(max_retries):
            logger.debug("Max retries exceeded, rejecting: %s", msg.body.method)
            await msg.reject()
            return

        # 3. Route
        route = self.routes.get(msg.body.method)
        if route is None:
            logger.warning("No route for method: %s", msg.body.method)
            await msg.reject()
            return

        handler, mode = route

        # 4. Execute handler
        payload: Any = None
        handler_exc: Exception | None = None
        try:
            payload = await self._execute(handler, mode, msg.body.params)
        except Exception as exc:
            handler_exc = exc

        # 5. Notification flow (no id → fire-and-forget)
        if msg.correlation_id is None:
            if handler_exc is not None:
                backoff = self._calculate_backoff(msg.delivery_count)
                logger.error(
                    "Handler %s failed, nacking with %.1fs delay",
                    msg.body.method,
                    backoff,
                    exc_info=handler_exc,
                )
                await msg.nack(backoff)
                return
            await msg.ack()
            return

        # 6. Request flow (has id → send response to reply_to)
        if msg.reply_to is None:
            logger.warning(
                "Request %s has id but no reply_to, acking without reply",
                msg.body.method,
            )
            await msg.ack()
            return

        if handler_exc is not None:
            response = JsonRpcResponse(
                id=msg.correlation_id,
                error=JsonRpcErrorDetail(
                    code=JsonRpcError.INTERNAL_ERROR,
                    message=str(handler_exc),
                ),
            )
        else:
            response = JsonRpcResponse(id=msg.correlation_id, result=payload)

        try:
            await self.broker.publish(msg.reply_to, response)
        except Exception:
            backoff = self._calculate_backoff(msg.delivery_count)
            logger.exception(
                "Failed to publish response for %s, nacking with %.1fs delay",
                msg.body.method,
                backoff,
            )
            await msg.nack(backoff)
            return

        await msg.ack()

    async def _guarded_process(
        self,
        msg: BrokerMessage,
        sem: asyncio.Semaphore,
    ) -> None:
        try:
            await self._process_message(msg)
        finally:
            sem.release()

    async def run(self, queue: str) -> None:
        shutdown_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, shutdown_event.set)

        try:
            async with self.broker:
                async with asyncio.TaskGroup() as tg:
                    sem = asyncio.Semaphore(self._settings.concurrency)
                    async for msg in self.broker.consume(
                        queue,
                        prefetch=self._settings.concurrency,
                    ):
                        if shutdown_event.is_set():
                            break
                        await sem.acquire()
                        tg.create_task(self._guarded_process(msg, sem))
        finally:
            if self._pool is not None:
                self._pool.shutdown(wait=True)
