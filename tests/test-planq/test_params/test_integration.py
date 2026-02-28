"""End-to-end integration tests: register handler -> convert params."""

import dataclasses
import time
from typing import Annotated, Literal
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel, Field

from planq.consumer import PlanqConsumer
from planq.enums import ExecutionMode, JsonRpcError
from planq.exceptions import RejectMessage
from planq.message import BrokerMessage
from planq.models import (
    ConsumerSettings,
    JsonRpcRequest,
)


@pytest.fixture
def consumer():
    """Consumer with no middleware for clean testing."""
    broker = MagicMock()
    return PlanqConsumer(broker, middlewares=[])


@pytest.fixture
def mock_message():
    """Factory for creating mock BrokerMessage instances."""

    def _create(
        method: str,
        params=None,
        msg_id: str | None = "test-123",
        delivery_count: int = 1,
        reply_to: str | None = "reply-queue",
    ):
        msg = MagicMock(spec=BrokerMessage)
        msg.body = JsonRpcRequest(method=method, params=params, id=msg_id)
        msg.correlation_id = msg_id
        msg.headers = {}
        msg.delivery_count = delivery_count
        msg.reply_to = reply_to
        msg.message_id = "test-msg-id"
        msg.queue_name = "test-queue"
        msg.enqueued_at = time.time() - 0.1
        msg.received_at = time.time()
        msg.ack = AsyncMock()
        msg.nack = AsyncMock()
        msg.reject = AsyncMock()
        return msg

    return _create


class TestIntegrationPrimitiveHandlers:
    """End-to-end: primitive type handlers."""

    @pytest.mark.asyncio
    async def test_dict_params_with_typed_handler(self, consumer, mock_message):
        """Dict params validated and passed to handler."""
        received = {}

        @consumer.task("test.typed")
        async def handler(name: str, age: int):
            received["name"] = name
            received["age"] = age
            return "ok"

        msg = mock_message(
            "test.typed",
            params={"name": "Alice", "age": 30},
        )
        response = await consumer._router_endpoint(msg)
        assert received == {"name": "Alice", "age": 30}
        assert response.result == "ok"

    @pytest.mark.asyncio
    async def test_list_params_with_typed_handler(self, consumer, mock_message):
        """List params mapped to named params."""
        received = {}

        @consumer.task("test.positional")
        async def handler(x: int, y: str):
            received["x"] = x
            received["y"] = y
            return "ok"

        msg = mock_message(
            "test.positional",
            params=[42, "hello"],
        )
        await consumer._router_endpoint(msg)
        assert received == {"x": 42, "y": "hello"}

    @pytest.mark.asyncio
    async def test_default_values_work(self, consumer, mock_message):
        """Handler defaults used when params omitted."""
        received = {}

        @consumer.task("test.defaults")
        async def handler(name: str, greeting: str = "Hello"):
            received["name"] = name
            received["greeting"] = greeting
            return "ok"

        msg = mock_message("test.defaults", params={"name": "Alice"})
        await consumer._router_endpoint(msg)
        assert received["name"] == "Alice"
        assert received["greeting"] == "Hello"

    @pytest.mark.asyncio
    async def test_none_params_for_no_arg_handler(self, consumer, mock_message):
        """None params works for handlers with no args."""

        @consumer.task("test.no_args")
        async def handler():
            return "ok"

        msg = mock_message("test.no_args", params=None)
        response = await consumer._router_endpoint(msg)
        assert response.result == "ok"


class TestIntegrationPydanticModels:
    """End-to-end: Pydantic model handlers."""

    @pytest.mark.asyncio
    async def test_pydantic_model_parsed(self, consumer, mock_message):
        """Pydantic model auto-parsed from dict param."""

        class UserModel(BaseModel):
            name: str
            age: int

        received_user = None

        @consumer.task("test.pydantic")
        async def handler(user: UserModel):
            nonlocal received_user
            received_user = user
            return "ok"

        msg = mock_message(
            "test.pydantic",
            params={"user": {"name": "Alice", "age": 30}},
        )
        await consumer._router_endpoint(msg)
        assert isinstance(received_user, UserModel)
        assert received_user.name == "Alice"
        assert received_user.age == 30

    @pytest.mark.asyncio
    async def test_pydantic_validation_returns_error(
        self, consumer, mock_message
    ):
        """Invalid Pydantic params return -32602 error."""

        class UserModel(BaseModel):
            name: str
            age: int

        @consumer.task("test.pydantic_invalid")
        async def handler(user: UserModel):
            return "should not reach"

        msg = mock_message(
            "test.pydantic_invalid",
            params={
                "user": {
                    "name": "Alice",
                    "age": "not_int",
                }
            },
        )
        response = await consumer._router_endpoint(msg)
        assert response.error is not None
        assert response.error.code == JsonRpcError.INVALID_PARAMS

    @pytest.mark.asyncio
    async def test_mixed_pydantic_and_primitives(self, consumer, mock_message):
        """Handler with Pydantic model + primitive params."""

        class Config(BaseModel):
            timeout: float

        received = {}

        @consumer.task("test.mixed")
        async def handler(name: str, config: Config, count: int = 1):
            received["name"] = name
            received["config"] = config
            received["count"] = count
            return "ok"

        msg = mock_message(
            "test.mixed",
            params={
                "name": "test",
                "config": {"timeout": 5.0},
                "count": 10,
            },
        )
        await consumer._router_endpoint(msg)
        assert received["name"] == "test"
        assert isinstance(received["config"], Config)
        assert received["config"].timeout == 5.0
        assert received["count"] == 10


class TestIntegrationDataclasses:
    """End-to-end: dataclass handlers."""

    @pytest.mark.asyncio
    async def test_dataclass_parsed(self, consumer, mock_message):
        """Dataclass auto-parsed from dict param."""

        @dataclasses.dataclass
        class Point:
            x: int
            y: int

        received_point = None

        @consumer.task("test.dataclass")
        async def handler(p: Point):
            nonlocal received_point
            received_point = p
            return "ok"

        msg = mock_message(
            "test.dataclass",
            params={"p": {"x": 1, "y": 2}},
        )
        await consumer._router_endpoint(msg)
        assert isinstance(received_point, Point)
        assert received_point.x == 1

    @pytest.mark.asyncio
    async def test_dataclass_with_global_parser(self, mock_message):
        """Global dataclass_parser in ConsumerSettings."""

        @dataclasses.dataclass
        class Data:
            items: list[int]

        def my_parser(cls, data):
            return cls(items=sorted(data["items"]))

        settings = ConsumerSettings(dataclass_parser=my_parser)
        broker = MagicMock()
        consumer = PlanqConsumer(broker, settings=settings, middlewares=[])

        received_data = None

        @consumer.task("test.parser")
        async def handler(d: Data):
            nonlocal received_data
            received_data = d
            return "ok"

        msg = mock_message(
            "test.parser",
            params={"d": {"items": [3, 1, 2]}},
        )
        await consumer._router_endpoint(msg)
        assert received_data.items == [1, 2, 3]


class TestIntegrationVarArgs:
    """End-to-end: *args and **kwargs handlers."""

    @pytest.mark.asyncio
    async def test_var_args_handler(self, consumer, mock_message):
        """Handler with *args receives extra positional."""
        received_args = None

        @consumer.task("test.varargs")
        async def handler(x: int, *args):
            nonlocal received_args
            received_args = args
            return x

        msg = mock_message(
            "test.varargs",
            params=[42, "extra1", "extra2"],
        )
        response = await consumer._router_endpoint(msg)
        assert response.result == 42
        assert received_args == ("extra1", "extra2")

    @pytest.mark.asyncio
    async def test_var_kwargs_handler(self, consumer, mock_message):
        """Handler with **kwargs receives extra named."""
        received_kwargs = None

        @consumer.task("test.varkw")
        async def handler(x: int, **kwargs):
            nonlocal received_kwargs
            received_kwargs = kwargs
            return x

        msg = mock_message(
            "test.varkw",
            params={"x": 42, "extra": "value"},
        )
        response = await consumer._router_endpoint(msg)
        assert response.result == 42
        assert received_kwargs == {"extra": "value"}


class TestIntegrationAnnotatedConstraints:
    """End-to-end: Annotated types with constraints."""

    @pytest.mark.asyncio
    async def test_annotated_constraint_passes(self, consumer, mock_message):
        """Valid Annotated[int, Field(gt=0)] passes."""

        @consumer.task("test.constrained")
        async def handler(
            count: Annotated[int, Field(gt=0)],
        ):
            return count

        msg = mock_message("test.constrained", params={"count": 5})
        response = await consumer._router_endpoint(msg)
        assert response.result == 5

    @pytest.mark.asyncio
    async def test_annotated_constraint_fails(self, consumer, mock_message):
        """Invalid Annotated[int, Field(gt=0)] returns error."""

        @consumer.task("test.constrained_fail")
        async def handler(
            count: Annotated[int, Field(gt=0)],
        ):
            return count

        msg = mock_message(
            "test.constrained_fail",
            params={"count": -1},
        )
        response = await consumer._router_endpoint(msg)
        assert response.error is not None
        assert response.error.code == JsonRpcError.INVALID_PARAMS


class TestIntegrationErrorHandling:
    """End-to-end: InvalidParamsError handling."""

    @pytest.mark.asyncio
    async def test_invalid_params_returns_json_rpc_error(
        self, consumer, mock_message
    ):
        """InvalidParamsError returns -32602 for requests."""

        @consumer.task("test.invalid")
        async def handler(x: int, y: str):
            return "should not reach"

        msg = mock_message(
            "test.invalid",
            params={},
            msg_id="req-1",
            reply_to="reply-q",
        )
        response = await consumer._router_endpoint(msg)
        assert response.error is not None
        assert response.error.code == JsonRpcError.INVALID_PARAMS
        assert response.error.data is not None
        assert len(response.error.data) == 2

    @pytest.mark.asyncio
    async def test_invalid_params_rejects_notification(
        self, consumer, mock_message
    ):
        """InvalidParamsError raises RejectMessage for
        notifications."""

        @consumer.task("test.notify_invalid")
        async def handler(x: int, y: str):
            return "should not reach"

        msg = mock_message(
            "test.notify_invalid",
            params={},
            msg_id=None,
        )
        with pytest.raises(RejectMessage):
            await consumer._router_endpoint(msg)

    @pytest.mark.asyncio
    async def test_invalid_params_request_no_reply_to(
        self, consumer, mock_message
    ):
        """InvalidParamsError raises RejectMessage when
        no reply_to."""

        @consumer.task("test.no_reply")
        async def handler(x: int):
            pass

        msg = mock_message(
            "test.no_reply",
            params={},
            msg_id="req-1",
            reply_to=None,
        )
        with pytest.raises(RejectMessage):
            await consumer._router_endpoint(msg)


class TestIntegrationConsumerProperty:
    """Tests for consumer task registration."""

    def test_param_meta_stored_in_route(self, consumer):
        """task() decorator stores param_meta in route."""

        @consumer.task("test.meta")
        async def handler(x: int, y: str):
            pass

        route = consumer.routes["test.meta"]
        assert route.param_meta is not None
        assert len(route.param_meta.params) == 2


class TestIntegrationThreadMode:
    """End-to-end: THREAD mode with param conversion."""

    @pytest.mark.asyncio
    async def test_thread_mode_with_typed_params(self, consumer, mock_message):
        """THREAD mode works with type conversion."""
        received = {}

        @consumer.task("test.thread_typed", mode=ExecutionMode.THREAD)
        def handler(name: str, count: int):
            received["name"] = name
            received["count"] = count
            return "ok"

        msg = mock_message(
            "test.thread_typed",
            params={"name": "Alice", "count": 5},
        )
        response = await consumer._router_endpoint(msg)
        assert received == {"name": "Alice", "count": 5}
        assert response.result == "ok"

    @pytest.mark.asyncio
    async def test_thread_mode_pydantic_model(self, consumer, mock_message):
        """THREAD mode with Pydantic model param."""

        class Task(BaseModel):
            title: str
            priority: int

        received_task = None

        @consumer.task(
            "test.thread_pydantic",
            mode=ExecutionMode.THREAD,
        )
        def handler(task: Task):
            nonlocal received_task
            received_task = task
            return "ok"

        msg = mock_message(
            "test.thread_pydantic",
            params={
                "task": {
                    "title": "Fix bug",
                    "priority": 1,
                }
            },
        )
        await consumer._router_endpoint(msg)
        assert isinstance(received_task, Task)
        assert received_task.title == "Fix bug"


class TestIntegrationUnionOptional:
    """End-to-end: Union and Optional types."""

    @pytest.mark.asyncio
    async def test_optional_param(self, consumer, mock_message):
        """Optional[int] param accepts None."""
        received = {}

        @consumer.task("test.optional")
        async def handler(x: int, y: int | None = None):
            received["x"] = x
            received["y"] = y
            return "ok"

        msg = mock_message(
            "test.optional",
            params={"x": 42, "y": None},
        )
        await consumer._router_endpoint(msg)
        assert received["x"] == 42
        assert received["y"] is None

    @pytest.mark.asyncio
    async def test_union_type_param(self, consumer, mock_message):
        """Union[int, str] param accepts both types."""
        received = {}

        @consumer.task("test.union")
        async def handler(value: int | str):
            received["value"] = value
            return "ok"

        msg = mock_message("test.union", params={"value": "hello"})
        await consumer._router_endpoint(msg)
        assert received["value"] == "hello"

    @pytest.mark.asyncio
    async def test_literal_type_param(self, consumer, mock_message):
        """Literal type validates allowed values."""

        @consumer.task("test.literal")
        async def handler(
            status: Literal["active", "inactive"],
        ):
            return status

        msg = mock_message("test.literal", params={"status": "active"})
        response = await consumer._router_endpoint(msg)
        assert response.result == "active"

    @pytest.mark.asyncio
    async def test_literal_rejects_invalid(self, consumer, mock_message):
        """Literal type rejects invalid values."""

        @consumer.task("test.literal_bad")
        async def handler(
            status: Literal["active", "inactive"],
        ):
            return status

        msg = mock_message(
            "test.literal_bad",
            params={"status": "unknown"},
        )
        response = await consumer._router_endpoint(msg)
        assert response.error is not None
        assert response.error.code == JsonRpcError.INVALID_PARAMS
