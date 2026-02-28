"""Tests for analyze_signature."""

import dataclasses
import inspect
from typing import Annotated, Any

from pydantic import BaseModel, Field

from planq.params.analyzer import analyze_signature
from planq.params.resolvers import (
    DataclassResolver,
    PassthroughResolver,
    PrimitiveResolver,
    PydanticResolver,
)


class TestAnalyzeSignatureBasic:
    """Tests for basic signature analysis."""

    def test_simple_primitives(self):
        """Analyses handler with primitive params."""

        def handler(name: str, age: int):
            pass

        sig = analyze_signature(handler)
        assert len(sig.params) == 2
        assert sig.positional_names == ("name", "age")
        assert not sig.has_var_positional
        assert not sig.has_var_keyword

    def test_param_meta_names_and_positions(self):
        """ParamMeta has correct names and positions."""

        def handler(x: int, y: str, z: float):
            pass

        sig = analyze_signature(handler)
        assert sig.params[0].name == "x"
        assert sig.params[0].position == 0
        assert sig.params[1].name == "y"
        assert sig.params[1].position == 1
        assert sig.params[2].name == "z"
        assert sig.params[2].position == 2

    def test_default_values_preserved(self):
        """Default values are stored in ParamMeta."""

        def handler(x: int, y: str = "hello", z: float = 3.14):
            pass

        sig = analyze_signature(handler)
        assert sig.params[0].default is inspect.Parameter.empty
        assert sig.params[1].default == "hello"
        assert sig.params[2].default == 3.14

    def test_no_params_handler(self):
        """Analyses handler with no parameters."""

        def handler():
            pass

        sig = analyze_signature(handler)
        assert len(sig.params) == 0
        assert sig.positional_names == ()

    def test_unannotated_param(self):
        """Unannotated param gets PassthroughResolver."""

        def handler(x):
            pass

        sig = analyze_signature(handler)
        assert isinstance(sig.params[0].resolver, PassthroughResolver)

    def test_any_annotation(self):
        """Any-annotated param gets PassthroughResolver."""

        def handler(x: Any):
            pass

        sig = analyze_signature(handler)
        assert isinstance(sig.params[0].resolver, PassthroughResolver)


class TestAnalyzeSignatureVarArgs:
    """Tests for *args and **kwargs detection."""

    def test_var_positional_detected(self):
        """*args is detected."""

        def handler(x: int, *args):
            pass

        sig = analyze_signature(handler)
        assert sig.has_var_positional
        assert not sig.has_var_keyword
        # *args is not in params
        assert len(sig.params) == 1

    def test_var_keyword_detected(self):
        """**kwargs is detected."""

        def handler(x: int, **kwargs):
            pass

        sig = analyze_signature(handler)
        assert not sig.has_var_positional
        assert sig.has_var_keyword
        assert len(sig.params) == 1

    def test_both_var_args_detected(self):
        """Both *args and **kwargs detected."""

        def handler(x: int, *args, **kwargs):
            pass

        sig = analyze_signature(handler)
        assert sig.has_var_positional
        assert sig.has_var_keyword
        assert len(sig.params) == 1


class TestAnalyzeSignatureResolverSelection:
    """Tests for resolver selection logic."""

    def test_unresolvable_type_gets_passthrough(self):
        """Type that fails TypeAdapter gets PassthroughResolver."""
        sentinel = object()

        def handler(x: int):
            pass

        handler.__annotations__["x"] = sentinel
        sig = analyze_signature(handler)
        assert isinstance(sig.params[0].resolver, PassthroughResolver)

    def test_primitive_gets_primitive_resolver(self):
        """Bare primitive types get PrimitiveResolver."""

        def handler(x: int, y: str, z: float, b: bool):
            pass

        sig = analyze_signature(handler)
        for param in sig.params:
            assert isinstance(param.resolver, PrimitiveResolver)
            # Bare primitives have no adapter
            assert param.resolver._adapter is None

    def test_pydantic_model_gets_pydantic_resolver(self):
        """Pydantic BaseModel gets PydanticResolver."""

        class UserModel(BaseModel):
            name: str

        def handler(user: UserModel):
            pass

        sig = analyze_signature(handler)
        assert isinstance(sig.params[0].resolver, PydanticResolver)

    def test_dataclass_gets_dataclass_resolver(self):
        """Dataclass gets DataclassResolver."""

        @dataclasses.dataclass
        class Point:
            x: int
            y: int

        def handler(p: Point):
            pass

        sig = analyze_signature(handler)
        assert isinstance(sig.params[0].resolver, DataclassResolver)

    def test_annotated_with_field_gets_adapter(self):
        """Annotated[int, Field(gt=0)] gets TypeAdapter."""

        def handler(
            x: Annotated[int, Field(gt=0)],
        ):
            pass

        sig = analyze_signature(handler)
        assert isinstance(sig.params[0].resolver, PrimitiveResolver)
        assert sig.params[0].resolver._adapter is not None

    def test_union_type_gets_adapter(self):
        """Union types get PrimitiveResolver with adapter."""

        def handler(x: int | str):
            pass

        sig = analyze_signature(handler)
        assert isinstance(sig.params[0].resolver, PrimitiveResolver)
        assert sig.params[0].resolver._adapter is not None

    def test_optional_type_gets_adapter(self):
        """Optional types get PrimitiveResolver with adapter."""

        def handler(x: int | None = None):
            pass

        sig = analyze_signature(handler)
        assert isinstance(sig.params[0].resolver, PrimitiveResolver)
        assert sig.params[0].resolver._adapter is not None

    def test_list_type_gets_adapter(self):
        """list[int] gets PrimitiveResolver with adapter."""

        def handler(items: list[int]):
            pass

        sig = analyze_signature(handler)
        assert isinstance(sig.params[0].resolver, PrimitiveResolver)
        assert sig.params[0].resolver._adapter is not None


class TestAnalyzeSignatureHintsFallback:
    """Tests for get_type_hints failure fallback."""

    def test_unresolvable_string_annotation_fallback(self):
        """Falls back when get_type_hints can't resolve."""

        def handler(x):
            pass

        # String annotation that get_type_hints can't resolve
        handler.__annotations__["x"] = "UndefinedType"

        sig = analyze_signature(handler)
        assert len(sig.params) == 1
        # String annotation filtered out -> Any -> Passthrough
        assert isinstance(sig.params[0].resolver, PassthroughResolver)

    def test_fallback_preserves_resolvable_annotations(self):
        """Fallback keeps non-string annotations that exist."""

        def handler(x, y=5):
            pass

        handler.__annotations__["x"] = "UndefinedType"
        handler.__annotations__["y"] = int

        sig = analyze_signature(handler)
        assert len(sig.params) == 2
        # x: string annotation filtered -> Any -> Passthrough
        assert isinstance(sig.params[0].resolver, PassthroughResolver)
        # y: int annotation preserved -> PrimitiveResolver
        assert isinstance(sig.params[1].resolver, PrimitiveResolver)


class TestAnalyzeSignatureAnnotatedUnwrap:
    """Tests for Annotated type unwrapping."""

    def test_annotated_unwraps_base_type(self):
        """Annotated[T, ...] unwraps to T in ParamMeta."""

        def handler(
            x: Annotated[int, Field(gt=0)],
        ):
            pass

        sig = analyze_signature(handler)
        # annotation stores the unwrapped base type
        assert sig.params[0].annotation is int

    def test_bare_type_not_unwrapped(self):
        """Bare types are passed through unchanged."""

        def handler(x: int):
            pass

        sig = analyze_signature(handler)
        assert sig.params[0].annotation is int


class TestAnalyzeSignatureMixed:
    """Tests for handlers with mixed parameter types."""

    def test_mixed_pydantic_primitive(self):
        """Handler with Pydantic and primitive params."""

        class Config(BaseModel):
            value: str

        def handler(
            name: str,
            config: Config,
            count: int = 0,
        ):
            pass

        sig = analyze_signature(handler)
        assert len(sig.params) == 3

        # name is primitive
        assert isinstance(sig.params[0].resolver, PrimitiveResolver)

        # config is Pydantic
        assert isinstance(sig.params[1].resolver, PydanticResolver)

        # count has default
        assert sig.params[2].default == 0

        assert sig.positional_names == (
            "name",
            "config",
            "count",
        )

    def test_async_handler(self):
        """Async handler is analysed correctly."""

        async def handler(x: int, y: str) -> str:
            return f"{x}-{y}"

        sig = analyze_signature(handler)
        assert len(sig.params) == 2
        assert sig.params[0].annotation is int
        assert sig.params[1].annotation is str
