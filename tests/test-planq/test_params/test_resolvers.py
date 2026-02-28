"""Tests for individual resolver strategies."""

import dataclasses
from typing import Annotated, Any, Literal, Union

import pytest
from pydantic import BaseModel, Field, TypeAdapter, ValidationError

from planq.params.resolvers import (
    DataclassResolver,
    ParamResolver,
    PassthroughResolver,
    PrimitiveResolver,
    PydanticResolver,
)

# === ParamResolver base ===


class TestParamResolver:
    """Tests for ParamResolver base class."""

    def test_resolve_raises_not_implemented(self):
        """Base resolve() raises NotImplementedError."""
        resolver = ParamResolver()
        with pytest.raises(NotImplementedError):
            resolver.resolve("value", int)


# === PydanticResolver ===


class TestPydanticResolver:
    """Tests for PydanticResolver."""

    def test_validates_pydantic_model(self):
        """Validates dict into Pydantic model."""

        class UserModel(BaseModel):
            name: str
            age: int

        adapter = TypeAdapter(UserModel)
        resolver = PydanticResolver(adapter)
        result = resolver.resolve({"name": "Alice", "age": 30}, UserModel)
        assert isinstance(result, UserModel)
        assert result.name == "Alice"
        assert result.age == 30

    def test_invalid_data_raises_validation_error(self):
        """Invalid data raises ValidationError."""

        class UserModel(BaseModel):
            name: str
            age: int

        adapter = TypeAdapter(UserModel)
        resolver = PydanticResolver(adapter)
        with pytest.raises(ValidationError):
            resolver.resolve(
                {"name": "Alice", "age": "not_int"},
                UserModel,
            )

    def test_annotated_pydantic_model(self):
        """Handles Annotated[Model, ...] correctly."""

        class Item(BaseModel):
            price: float

        adapter = TypeAdapter(Item)
        resolver = PydanticResolver(adapter)
        result = resolver.resolve({"price": 9.99}, Item)
        assert result.price == 9.99

    def test_nested_model(self):
        """Handles nested Pydantic models."""

        class Address(BaseModel):
            city: str

        class Person(BaseModel):
            name: str
            address: Address

        adapter = TypeAdapter(Person)
        resolver = PydanticResolver(adapter)
        result = resolver.resolve(
            {
                "name": "Alice",
                "address": {"city": "NYC"},
            },
            Person,
        )
        assert result.address.city == "NYC"


# === DataclassResolver ===


class TestDataclassResolver:
    """Tests for DataclassResolver."""

    def test_fallback_construction(self):
        """Falls back to Model(**data)."""

        @dataclasses.dataclass
        class Point:
            x: int
            y: int

        resolver = DataclassResolver()
        result = resolver.resolve({"x": 1, "y": 2}, Point)
        assert result.x == 1
        assert result.y == 2

    def test_from_dict_classmethod(self):
        """Uses from_dict if available."""

        @dataclasses.dataclass
        class Config:
            value: str

            @classmethod
            def from_dict(cls, data):
                return cls(value=data["value"].upper())

        resolver = DataclassResolver()
        result = resolver.resolve({"value": "hello"}, Config)
        assert result.value == "HELLO"

    def test_global_parser(self):
        """Uses global dataclass_parser when set."""

        @dataclasses.dataclass
        class Data:
            items: list[int]

        def mock_parser(cls, data):
            return cls(items=list(data["items"]))

        resolver = DataclassResolver()
        result = resolver.resolve(
            {"items": [1, 2, 3]},
            Data,
            dataclass_parser=mock_parser,
        )
        assert result.items == [1, 2, 3]

    def test_from_dict_priority_over_parser(self):
        """from_dict takes priority over global parser."""

        @dataclasses.dataclass
        class Obj:
            v: str

            @classmethod
            def from_dict(cls, data):
                return cls(v="from_dict")

        def parser(cls, data):
            return cls(v="parser")

        resolver = DataclassResolver()
        result = resolver.resolve(
            {"v": "raw"},
            Obj,
            dataclass_parser=parser,
        )
        assert result.v == "from_dict"

    def test_non_dict_raises_type_error(self):
        """Non-dict input raises TypeError."""

        @dataclasses.dataclass
        class Point:
            x: int
            y: int

        resolver = DataclassResolver()
        with pytest.raises(TypeError, match="Expected dict"):
            resolver.resolve([1, 2], Point)


# === PrimitiveResolver ===


class TestPrimitiveResolver:
    """Tests for PrimitiveResolver."""

    def test_passthrough_without_adapter(self):
        """Passes through values when no adapter."""
        resolver = PrimitiveResolver(adapter=None)
        assert resolver.resolve(42, int) == 42
        assert resolver.resolve("hello", str) == "hello"
        assert resolver.resolve(3.14, float) == 3.14

    def test_validates_with_adapter(self):
        """Validates when adapter is provided."""
        adapter = TypeAdapter(Annotated[int, Field(gt=0)])
        resolver = PrimitiveResolver(adapter=adapter)
        assert resolver.resolve(5, int) == 5

    def test_adapter_rejects_invalid(self):
        """Adapter rejects invalid values."""
        adapter = TypeAdapter(Annotated[int, Field(gt=0)])
        resolver = PrimitiveResolver(adapter=adapter)
        with pytest.raises(ValidationError):
            resolver.resolve(-1, int)

    def test_union_type_with_adapter(self):
        """Handles Union types via TypeAdapter."""
        adapter = TypeAdapter(Union[int, str])
        resolver = PrimitiveResolver(adapter=adapter)
        assert resolver.resolve(42, Any) == 42
        assert resolver.resolve("hello", Any) == "hello"

    def test_literal_type_with_adapter(self):
        """Handles Literal types via TypeAdapter."""
        adapter = TypeAdapter(Literal["a", "b", "c"])
        resolver = PrimitiveResolver(adapter=adapter)
        assert resolver.resolve("a", Any) == "a"

    def test_literal_rejects_invalid(self):
        """Literal adapter rejects invalid values."""
        adapter = TypeAdapter(Literal["a", "b"])
        resolver = PrimitiveResolver(adapter=adapter)
        with pytest.raises(ValidationError):
            resolver.resolve("z", Any)


# === PassthroughResolver ===


class TestPassthroughResolver:
    """Tests for PassthroughResolver."""

    def test_returns_value_unchanged(self):
        """Returns raw value unchanged."""
        resolver = PassthroughResolver()
        data = {"nested": [1, 2, 3]}
        assert resolver.resolve(data, Any) is data

    def test_handles_none(self):
        """Handles None value."""
        resolver = PassthroughResolver()
        assert resolver.resolve(None, Any) is None
