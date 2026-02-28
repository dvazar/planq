"""Tests exposing edge-case gaps in JSON-RPC params conversion.

These tests target three root causes:
1. DataclassResolver is shallow (no recursive nested conversion)
2. Positional-only params always placed in kwargs (TypeError)
3. Generic dataclasses fall to wrong resolver (GenericAlias)
"""

import functools
from dataclasses import dataclass
from enum import Enum
from typing import Generic, NamedTuple, TypeVar

import dacite
import pytest
from pydantic import BaseModel

from planq.params.analyzer import analyze_signature
from planq.params.converter import ParamsConverter

METHOD = "test.edge"


@pytest.fixture
def converter():
    """Fresh ParamsConverter."""
    return ParamsConverter()


def _make_sig(handler):
    """Helper to analyse a handler's signature."""
    return analyze_signature(handler)


# === Category 1: Nested dataclass structures ===


class TestNestedDataclasses:
    """Tests for nested dataclass field conversion."""

    def test_nested_dataclass_field(self, converter):
        """Nested dict should be converted to dataclass instance."""

        @dataclass
        class Address:
            city: str
            zip_code: str

        @dataclass
        class User:
            name: str
            address: Address

        def handler(user: User):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(
            sig,
            {
                "user": {
                    "name": "Alice",
                    "address": {"city": "NY", "zip_code": "10001"},
                },
            },
            METHOD,
            dataclass_parser=dacite.from_dict,
        )
        assert isinstance(kwargs["user"], User)
        assert isinstance(kwargs["user"].address, Address)
        assert kwargs["user"].address.city == "NY"

    def test_list_of_dataclasses(self, converter):
        """List of dicts should be converted to list of dataclass instances."""

        @dataclass
        class Tag:
            name: str

        @dataclass
        class Post:
            title: str
            tags: list[Tag]

        def handler(post: Post):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(
            sig,
            {
                "post": {
                    "title": "Hello",
                    "tags": [
                        {"name": "python"},
                        {"name": "async"},
                    ],
                },
            },
            METHOD,
            dataclass_parser=dacite.from_dict,
        )
        assert isinstance(kwargs["post"], Post)
        for tag in kwargs["post"].tags:
            assert isinstance(tag, Tag)

    def test_optional_nested_dataclass(self, converter):
        """Optional nested dataclass dict should be converted."""

        @dataclass
        class RetryConfig:
            max_retries: int
            delay: float

        @dataclass
        class TaskConfig:
            name: str
            retry: RetryConfig | None

        def handler(config: TaskConfig):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(
            sig,
            {
                "config": {
                    "name": "task",
                    "retry": {
                        "max_retries": 3,
                        "delay": 1.0,
                    },
                },
            },
            METHOD,
            dataclass_parser=dacite.from_dict,
        )
        assert isinstance(kwargs["config"], TaskConfig)
        assert isinstance(kwargs["config"].retry, RetryConfig)
        assert kwargs["config"].retry.max_retries == 3

    def test_enum_field_in_dataclass(self, converter):
        """Enum string value should be coerced to Enum member."""

        class Priority(str, Enum):
            HIGH = "high"
            LOW = "low"

        @dataclass
        class Job:
            name: str
            priority: Priority

        def handler(job: Job):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(
            sig,
            {"job": {"name": "job1", "priority": "high"}},
            METHOD,
            dataclass_parser=functools.partial(
                dacite.from_dict,
                config=dacite.Config(type_hooks={Priority: Priority}),
            ),
        )
        assert isinstance(kwargs["job"], Job)
        assert kwargs["job"].priority is Priority.HIGH

    def test_dict_of_dataclasses(self, converter):
        """Dict values that are dicts should be converted to dataclass."""

        @dataclass
        class Score:
            value: float

        @dataclass
        class Report:
            title: str
            scores: dict[str, Score]

        def handler(report: Report):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(
            sig,
            {
                "report": {
                    "title": "Q1",
                    "scores": {
                        "math": {"value": 95.0},
                        "eng": {"value": 88.0},
                    },
                },
            },
            METHOD,
            dataclass_parser=dacite.from_dict,
        )
        assert isinstance(kwargs["report"], Report)
        for score in kwargs["report"].scores.values():
            assert isinstance(score, Score)

    def test_pydantic_model_nested_in_dataclass(self, converter):
        """Pydantic model nested in dataclass should be converted."""

        class Metadata(BaseModel):
            created_at: str
            version: int

        @dataclass
        class Document:
            title: str
            metadata: Metadata

        def handler(doc: Document):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(
            sig,
            {
                "doc": {
                    "title": "doc",
                    "metadata": {
                        "created_at": "2024-01-01",
                        "version": 1,
                    },
                },
            },
            METHOD,
            dataclass_parser=functools.partial(
                dacite.from_dict,
                config=dacite.Config(
                    type_hooks={Metadata: Metadata.model_validate},
                ),
            ),
        )
        assert isinstance(kwargs["doc"], Document)
        assert isinstance(kwargs["doc"].metadata, Metadata)
        assert kwargs["doc"].metadata.version == 1

    def test_deeply_nested_three_levels(self, converter):
        """Three-level nesting should all be properly typed."""

        @dataclass
        class Street:
            name: str
            number: int

        @dataclass
        class Address:
            street: Street
            city: str

        @dataclass
        class Person:
            name: str
            address: Address

        def handler(person: Person):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(
            sig,
            {
                "person": {
                    "name": "Alice",
                    "address": {
                        "street": {
                            "name": "Main",
                            "number": 42,
                        },
                        "city": "NY",
                    },
                },
            },
            METHOD,
            dataclass_parser=dacite.from_dict,
        )
        assert isinstance(kwargs["person"], Person)
        assert isinstance(kwargs["person"].address, Address)
        assert isinstance(kwargs["person"].address.street, Street)
        assert kwargs["person"].address.street.number == 42

    def test_union_of_dataclasses(self, converter):
        """Union of dataclasses should resolve to correct variant."""

        @dataclass
        class Cat:
            meow_volume: int

        @dataclass
        class Dog:
            bark_volume: int

        @dataclass
        class Owner:
            name: str
            pet: Cat | Dog

        def handler(owner: Owner):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(
            sig,
            {"owner": {"name": "Alice", "pet": {"meow_volume": 5}}},
            METHOD,
            dataclass_parser=dacite.from_dict,
        )
        assert isinstance(kwargs["owner"], Owner)
        assert isinstance(kwargs["owner"].pet, Cat)
        assert kwargs["owner"].pet.meow_volume == 5


# === Category 2: Positional-only parameters ===


class TestPositionalOnlyParams:
    """Tests for positional-only parameter handling."""

    def test_single_positional_only_dict_params(self, converter):
        """Single positional-only param from dict params."""

        def handler(x: int, /):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(sig, {"x": 42}, METHOD)
        # Must be callable without TypeError
        handler(*args, **kwargs)

    def test_single_positional_only_list_params(self, converter):
        """Single positional-only param from list params."""

        def handler(x: int, /):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(sig, [42], METHOD)
        handler(*args, **kwargs)

    def test_mixed_positional_only_and_keyword(self, converter):
        """Positional-only + regular keyword param."""

        def handler(x: int, /, y: str):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(sig, {"x": 1, "y": "hello"}, METHOD)
        handler(*args, **kwargs)

    def test_positional_only_and_keyword_only(self, converter):
        """Positional-only + keyword-only params."""

        def handler(x: int, /, *, y: str):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(sig, {"x": 1, "y": "hello"}, METHOD)
        handler(*args, **kwargs)


# === Category 3: Generic dataclass ===


class TestGenericDataclass:
    """Tests for generic (parameterized) dataclass handling."""

    def test_parameterized_generic_dataclass(self, converter):
        """Container[int] should be resolved as a dataclass."""
        T = TypeVar("T")

        @dataclass
        class Container(Generic[T]):
            value: T
            label: str

        def handler(data: Container[int]):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(
            sig,
            {"data": {"value": 42, "label": "test"}},
            METHOD,
        )
        assert isinstance(kwargs["data"], Container)
        assert kwargs["data"].value == 42
        assert kwargs["data"].label == "test"

    def test_generic_consistency_with_bare(self, converter):
        """Bare and parameterized generic should produce same result."""
        T = TypeVar("T")

        @dataclass
        class Container(Generic[T]):
            value: T
            label: str

        def handler_bare(data: Container):
            pass

        def handler_param(data: Container[int]):
            pass

        sig_bare = _make_sig(handler_bare)
        sig_param = _make_sig(handler_param)

        params = {"data": {"value": 42, "label": "test"}}

        _, kwargs_bare = converter.convert(sig_bare, params, METHOD, )
        _, kwargs_param = converter.convert(sig_param, params, METHOD)

        assert isinstance(kwargs_bare["data"], Container)
        assert isinstance(kwargs_param["data"], Container)


# === Category 4: Other edge-case annotations ===


class TestOtherEdgeCaseAnnotations:
    """Tests for NamedTuple, typed tuple, set, standalone Enum."""

    def test_namedtuple_from_dict(self, converter):
        """NamedTuple should be constructable from dict params."""

        class Point(NamedTuple):
            x: int
            y: int

        def handler(point: Point):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(
            sig,
            {"point": {"x": 1, "y": 2}},
            METHOD,
        )
        assert isinstance(kwargs["point"], Point)
        assert kwargs["point"].x == 1
        assert kwargs["point"].y == 2

    def test_typed_tuple_from_list(self, converter):
        """tuple[int, str] should be coerced from JSON array."""

        def handler(pair: tuple[int, str]):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(
            sig,
            {"pair": [42, "hello"]},
            METHOD,
        )
        assert isinstance(kwargs["pair"], tuple)
        assert kwargs["pair"] == (42, "hello")

    def test_set_from_json_array(self, converter):
        """set[str] should be coerced from JSON array with dedup."""

        def handler(tags: set[str]):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(
            sig,
            {"tags": ["python", "async", "python"]},
            METHOD,
        )
        assert isinstance(kwargs["tags"], set)
        assert kwargs["tags"] == {"python", "async"}

    def test_standalone_enum_param(self, converter):
        """Enum as standalone param should coerce from string."""

        class Color(str, Enum):
            RED = "red"
            GREEN = "green"

        def handler(color: Color):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(
            sig,
            {"color": "red"},
            METHOD,
        )
        assert isinstance(kwargs["color"], Color)
        assert kwargs["color"] is Color.RED
