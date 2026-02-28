"""Signature analysis for handler parameter introspection."""

from __future__ import annotations

import dataclasses
import inspect
from typing import (
    Annotated,
    Any,
    Callable,
    get_args,
    get_origin,
    get_type_hints,
)

from pydantic import BaseModel, TypeAdapter

from planq.params.resolvers import (
    DataclassResolver,
    PassthroughResolver,
    PrimitiveResolver,
    PydanticResolver,
)
from planq.params.types import (
    HandlerSignature,
    ParamMeta,
)

#: Primitive types that need no TypeAdapter validation.
_PRIMITIVE_TYPES: frozenset[type] = frozenset(
    {int, str, float, bool, type(None)}
)


def _unwrap_annotated(annotation: Any) -> Any:
    """Unwrap ``Annotated[T, ...]`` to its base type.

    Args:
        annotation: The raw type hint (may be
            ``Annotated[T, ...]``).

    Returns:
        The base type if ``Annotated``, otherwise the
        original annotation.
    """
    origin = get_origin(annotation)
    if origin is Annotated:
        return get_args(annotation)[0]
    return annotation


def _is_pydantic_model(tp: Any) -> bool:
    """Check if a type is a Pydantic BaseModel subclass."""
    return isinstance(tp, type) and issubclass(tp, BaseModel)


def _find_resolver(
    full_annotation: Any,
    base_type: Any,
) -> (
    PydanticResolver
    | DataclassResolver
    | PrimitiveResolver
    | PassthroughResolver
):
    """Select the appropriate resolver for a parameter.

    Args:
        full_annotation: The original type hint (may be
            ``Annotated[T, ...]``).
        base_type: The unwrapped base type.

    Returns:
        A resolver instance, possibly with a pre-built
        ``TypeAdapter``.
    """
    # Pydantic BaseModel
    if _is_pydantic_model(base_type):
        adapter = TypeAdapter(full_annotation)
        return PydanticResolver(adapter)

    # Dataclass
    if dataclasses.is_dataclass(base_type) and isinstance(base_type, type):
        return DataclassResolver()

    # Any or unannotated
    if base_type is Any or (base_type is inspect.Parameter.empty):
        return PassthroughResolver()

    # Bare primitive (no Annotated wrapper)
    if base_type in _PRIMITIVE_TYPES and full_annotation is base_type:
        return PrimitiveResolver(adapter=None)

    # Complex type or Annotated with constraints
    try:
        adapter = TypeAdapter(full_annotation)
        return PrimitiveResolver(adapter=adapter)
    except Exception:
        return PassthroughResolver()


def analyze_signature(handler: Callable[..., Any]) -> HandlerSignature:
    """Analyze a handler's signature and build param metadata.

    Produces a :class:`~planq.params.types.HandlerSignature` with
    pre-built resolvers for each parameter. Called once in the
    ``@consumer.task()`` decorator, so runtime conversion has zero
    introspection overhead.

    Args:
        handler: The task handler function to analyze.

    Returns:
        Cached signature metadata with pre-built resolvers.
    """
    sig = inspect.signature(handler)
    try:
        hints = get_type_hints(handler, include_extras=True)
    except Exception:
        # Fallback when annotations can't be resolved
        # (e.g. handler defined in local scope with
        # `from __future__ import annotations`).
        hints = {
            n: p.annotation
            for n, p in sig.parameters.items()
            if p.annotation is not inspect.Parameter.empty
            and not isinstance(p.annotation, str)
        }

    params_meta: list[ParamMeta] = []
    positional_names: list[str] = []
    has_var_positional = False
    has_var_keyword = False
    position = 0

    for param_name, param in sig.parameters.items():
        kind = param.kind

        if kind == inspect.Parameter.VAR_POSITIONAL:
            has_var_positional = True
            continue
        if kind == inspect.Parameter.VAR_KEYWORD:
            has_var_keyword = True
            continue

        # Full annotation (preserves Annotated wrapper)
        full_annotation = hints.get(param_name, Any)
        base_type = _unwrap_annotated(full_annotation)

        resolver = _find_resolver(full_annotation, base_type)
        positional_names.append(param_name)

        params_meta.append(
            ParamMeta(
                name=param_name,
                position=position,
                annotation=base_type,
                resolver=resolver,
                default=param.default,
                kind=kind,
            )
        )
        position += 1

    return HandlerSignature(
        params=tuple(params_meta),
        has_var_positional=has_var_positional,
        has_var_keyword=has_var_keyword,
        positional_names=tuple(positional_names),
    )
