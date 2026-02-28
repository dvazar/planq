"""Tests for ParamsConverter."""

import dataclasses
import inspect
from typing import Annotated

import pytest
from pydantic import BaseModel, Field

from planq.exceptions import InvalidParamsError
from planq.params.analyzer import analyze_signature
from planq.params.converter import ParamsConverter
from planq.params.resolvers import PassthroughResolver
from planq.params.types import HandlerSignature, ParamMeta

METHOD = "test.method"


@pytest.fixture
def converter():
    """Fresh ParamsConverter."""
    return ParamsConverter()


def _make_sig(handler):
    """Helper to analyse a handler's signature."""
    return analyze_signature(handler)


# === Dict params (keyword) ===


class TestConverterDictParams:
    """Tests for dict params conversion."""

    def test_simple_dict_params(self, converter):
        """Dict params passed as kwargs."""

        def handler(name: str, age: int):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(
            sig, {"name": "Alice", "age": 30}, METHOD
        )
        assert args == ()
        assert kwargs == {"name": "Alice", "age": 30}

    def test_missing_required_param(self, converter):
        """Missing required param raises InvalidParamsError."""

        def handler(name: str, age: int):
            pass

        sig = _make_sig(handler)
        with pytest.raises(InvalidParamsError) as exc_info:
            converter.convert(sig, {"name": "Alice"}, METHOD)

        errors = exc_info.value.errors
        assert any(
            e["loc"] == ("age",) and e["type"] == "missing" for e in errors
        )

    def test_default_values_used(self, converter):
        """Params with defaults are not required."""

        def handler(name: str, greeting: str = "Hello"):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(sig, {"name": "Alice"}, METHOD)
        assert kwargs == {"name": "Alice"}
        # greeting not in kwargs (handler uses its default)

    def test_extra_keys_rejected(self, converter):
        """Extra keys raise InvalidParamsError."""

        def handler(name: str):
            pass

        sig = _make_sig(handler)
        with pytest.raises(InvalidParamsError) as exc_info:
            converter.convert(
                sig,
                {"name": "Alice", "extra": "bad"},
                METHOD,
            )

        errors = exc_info.value.errors
        assert any(
            e["loc"] == ("extra",) and e["type"] == "unexpected_keyword"
            for e in errors
        )

    def test_extra_keys_accepted_with_var_keyword(self, converter):
        """Extra keys passed through with **kwargs."""

        def handler(name: str, **kwargs):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(
            sig,
            {"name": "Alice", "extra": "ok"},
            METHOD,
        )
        assert kwargs == {"name": "Alice", "extra": "ok"}

    def test_none_params(self, converter):
        """None params works for no-arg handlers."""

        def handler():
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(sig, None, METHOD)
        assert args == ()
        assert kwargs == {}


# === List params (positional) ===


class TestConverterListParams:
    """Tests for list params conversion."""

    def test_list_params_mapped_to_names(self, converter):
        """List params mapped to param names by position."""

        def handler(x: int, y: str):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(sig, [42, "hello"], METHOD)
        assert args == ()
        assert kwargs == {"x": 42, "y": "hello"}

    def test_list_params_with_defaults(self, converter):
        """Partial list params use defaults for remaining."""

        def handler(x: int, y: str = "world", z: float = 1.0):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(sig, [42], METHOD)
        assert kwargs == {"x": 42}

    def test_extra_positional_rejected(self, converter):
        """Extra positional args raise InvalidParamsError."""

        def handler(x: int):
            pass

        sig = _make_sig(handler)
        with pytest.raises(InvalidParamsError) as exc_info:
            converter.convert(sig, [42, "extra"], METHOD)

        errors = exc_info.value.errors
        assert any(e["type"] == "unexpected_positional" for e in errors)

    def test_extra_positional_accepted_with_var_args(self, converter):
        """Extra positional passed through with *args."""

        def handler(x: int, *args):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(sig, [42, "extra1", "extra2"], METHOD)
        # x moved to positional to avoid double-binding
        assert args == (42, "extra1", "extra2")
        assert kwargs == {}

    def test_extra_args_rebuild_skips_defaulted_param(self, converter):
        """Positional rebuild skips params not present in kwargs."""
        sig = HandlerSignature(
            params=(
                ParamMeta(
                    name="x",
                    position=0,
                    annotation=int,
                    resolver=PassthroughResolver(),
                    default=inspect.Parameter.empty,
                    kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                ),
                ParamMeta(
                    name="y",
                    position=1,
                    annotation=str,
                    resolver=PassthroughResolver(),
                    default="fallback",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                ),
            ),
            has_var_positional=True,
            has_var_keyword=False,
            positional_names=("x",),
        )
        args, kwargs = converter.convert(sig, [42, "extra"], METHOD)
        assert args == (42, "extra")
        assert kwargs == {}

    def test_empty_list_with_required_param(self, converter):
        """Empty list with required param fails."""

        def handler(x: int):
            pass

        sig = _make_sig(handler)
        with pytest.raises(InvalidParamsError):
            converter.convert(sig, [], METHOD)


# === Pydantic model params ===


class TestConverterPydanticModels:
    """Tests for Pydantic BaseModel parameter conversion."""

    def test_pydantic_model_from_dict(self, converter):
        """Dict auto-parsed into Pydantic model."""

        class UserModel(BaseModel):
            name: str
            age: int

        def handler(user: UserModel):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(
            sig,
            {"user": {"name": "Alice", "age": 30}},
            METHOD,
        )
        assert isinstance(kwargs["user"], UserModel)
        assert kwargs["user"].name == "Alice"

    def test_pydantic_validation_error(self, converter):
        """Invalid Pydantic data collects errors."""

        class UserModel(BaseModel):
            name: str
            age: int

        def handler(user: UserModel):
            pass

        sig = _make_sig(handler)
        with pytest.raises(InvalidParamsError) as exc_info:
            converter.convert(
                sig,
                {
                    "user": {
                        "name": "Alice",
                        "age": "not_int",
                    }
                },
                METHOD,
            )
        errors = exc_info.value.errors
        assert any("user" in e["loc"] for e in errors)


# === Dataclass params ===


class TestConverterDataclasses:
    """Tests for dataclass parameter conversion."""

    def test_dataclass_from_dict(self, converter):
        """Dict auto-parsed into dataclass."""

        @dataclasses.dataclass
        class Point:
            x: int
            y: int

        def handler(p: Point):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(sig, {"p": {"x": 1, "y": 2}}, METHOD)
        assert isinstance(kwargs["p"], Point)
        assert kwargs["p"].x == 1

    def test_dataclass_with_from_dict(self, converter):
        """Dataclass with from_dict uses it."""

        @dataclasses.dataclass
        class Config:
            value: str

            @classmethod
            def from_dict(cls, data):
                return cls(value=data["value"].upper())

        def handler(c: Config):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(sig, {"c": {"value": "hello"}}, METHOD)
        assert kwargs["c"].value == "HELLO"

    def test_dataclass_with_global_parser(self, converter):
        """Global dataclass_parser is used."""

        @dataclasses.dataclass
        class Data:
            items: list[int]

        def handler(d: Data):
            pass

        def my_parser(cls, data):
            return cls(items=sorted(data["items"]))

        sig = _make_sig(handler)
        args, kwargs = converter.convert(
            sig,
            {"d": {"items": [3, 1, 2]}},
            METHOD,
            dataclass_parser=my_parser,
        )
        assert kwargs["d"].items == [1, 2, 3]


# === Annotated with constraints ===


class TestConverterAnnotatedConstraints:
    """Tests for Annotated types with Pydantic constraints."""

    def test_annotated_int_gt_validates(self, converter):
        """Annotated[int, Field(gt=0)] validates."""

        def handler(count: Annotated[int, Field(gt=0)]):
            pass

        sig = _make_sig(handler)
        args, kwargs = converter.convert(sig, {"count": 5}, METHOD)
        assert kwargs["count"] == 5

    def test_annotated_int_gt_rejects_invalid(self, converter):
        """Annotated[int, Field(gt=0)] rejects <= 0."""

        def handler(count: Annotated[int, Field(gt=0)]):
            pass

        sig = _make_sig(handler)
        with pytest.raises(InvalidParamsError):
            converter.convert(sig, {"count": -1}, METHOD)


# === Error collection ===


class TestConverterErrorCollection:
    """Tests for error collection behavior."""

    def test_multiple_errors_collected(self, converter):
        """Multiple errors collected in single raise."""

        def handler(x: int, y: str, z: float):
            pass

        sig = _make_sig(handler)
        with pytest.raises(InvalidParamsError) as exc_info:
            converter.convert(sig, {}, METHOD)

        errors = exc_info.value.errors
        # All three missing
        assert len(errors) == 3
        locs = {e["loc"] for e in errors}
        assert ("x",) in locs
        assert ("y",) in locs
        assert ("z",) in locs

    def test_non_validation_error_collected(self, converter):
        """Non-ValidationError from resolver collected as error."""

        @dataclasses.dataclass
        class Point:
            x: int
            y: int

        def handler(p: Point):
            pass

        sig = _make_sig(handler)
        with pytest.raises(InvalidParamsError) as exc_info:
            converter.convert(sig, {"p": "not_a_dict"}, METHOD)

        errors = exc_info.value.errors
        assert any(
            e["loc"] == ("p",) and e["type"] == "TypeError"
            for e in errors
        )

    def test_error_includes_method_name(self, converter):
        """InvalidParamsError includes method name."""

        def handler(x: int):
            pass

        sig = _make_sig(handler)
        with pytest.raises(InvalidParamsError) as exc_info:
            converter.convert(sig, {}, "my.method")
        assert exc_info.value.method == "my.method"
        assert "my.method" in str(exc_info.value)
