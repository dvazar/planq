"""Type definitions for the parameter introspection engine."""

from __future__ import annotations

import dataclasses
import inspect
from typing import Any

from planq.params.resolvers import ParamResolver


@dataclasses.dataclass(frozen=True, slots=True)
class ParamMeta:
    """Metadata for a single handler parameter.

    Attributes:
        name: Parameter name.
        position: Positional index in the handler signature.
        annotation: Unwrapped type annotation (base type from
            ``Annotated``, or original annotation).
        resolver: Pre-built resolver instance for this parameter.
        default: Default value, or ``inspect.Parameter.empty``
            if required.
        kind: Parameter kind from ``inspect.Parameter``.
    """

    # Parameter name from the function signature
    name: str
    # Positional index
    position: int
    # Unwrapped type annotation
    annotation: type | Any
    # Pre-built resolver instance
    resolver: ParamResolver
    # Default value or inspect.Parameter.empty
    default: Any
    # inspect.Parameter kind (POSITIONAL_ONLY, etc.)
    kind: inspect._ParameterKind


@dataclasses.dataclass(frozen=True, slots=True)
class HandlerSignature:
    """Cached analysis of a handler's parameter signature.

    Attributes:
        params: Ordered parameter metadata.
        has_var_positional: Whether the handler accepts ``*args``.
        has_var_keyword: Whether the handler accepts ``**kwargs``.
        positional_names: Parameter names in declaration order,
            used to map list params to names.
    """

    # Ordered parameter metadata.
    params: tuple[ParamMeta, ...]
    # Whether handler accepts *args.
    has_var_positional: bool
    # Whether handler accepts **kwargs.
    has_var_keyword: bool
    # Param names in order.
    positional_names: tuple[str, ...]
