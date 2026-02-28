"""Resolver strategies for converting raw values to typed objects."""

from __future__ import annotations

from typing import Any

from pydantic import TypeAdapter


class ParamResolver:
    """Base class for parameter resolvers.

    Subclasses implement strategy-specific conversion logic.
    Follows the project's ``NotImplementedError`` pattern.
    """

    def resolve(
        self,
        raw_value: Any,
        annotation: Any,
        **ctx: Any,
    ) -> Any:
        """Convert a raw value to the target type.

        Args:
            raw_value: The raw value from JSON-RPC params.
            annotation: The target type annotation.
            **ctx: Additional context (``dataclass_parser``, etc.).

        Returns:
            The converted value.

        Raises:
            NotImplementedError: Must be overridden.
        """
        raise NotImplementedError


class PydanticResolver(ParamResolver):
    """Resolves Pydantic BaseModel parameters.

    Uses a pre-built ``TypeAdapter`` for zero-overhead validation
    at runtime. The adapter is created once during signature
    analysis.
    """

    def __init__(self, adapter: TypeAdapter) -> None:  # type: ignore[type-arg]
        """Initialize with a pre-built TypeAdapter.

        Args:
            adapter: TypeAdapter created during signature analysis,
                bound to the Pydantic model type.
        """
        self._adapter = adapter

    def resolve(
        self,
        raw_value: Any,
        annotation: Any,
        **ctx: Any,
    ) -> Any:
        """Validate and convert using the pre-built TypeAdapter.

        Args:
            raw_value: The raw value from JSON-RPC params.
            annotation: Unused (adapter already knows the type).
            **ctx: Unused.

        Returns:
            The validated Pydantic model instance.

        Raises:
            pydantic.ValidationError: If validation fails.
        """
        return self._adapter.validate_python(raw_value)


class DataclassResolver(ParamResolver):
    """Resolves dataclass parameters with 3-tier priority.

    Resolution order:

    1. ``Model.from_dict(raw_data)`` if classmethod exists.
    2. ``dataclass_parser(Model, raw_data)`` if global parser set.
    3. ``Model(**raw_data)`` fallback.
    """

    def resolve(
        self,
        raw_value: Any,
        annotation: Any,
        **ctx: Any,
    ) -> Any:
        """Convert a dict to a dataclass instance.

        Args:
            raw_value: Dict of field values.
            annotation: The dataclass type to instantiate.
            **ctx: May contain ``dataclass_parser``.

        Returns:
            The dataclass instance.

        Raises:
            TypeError: If raw_value is not a dict.
        """
        if not isinstance(raw_value, dict):
            raise TypeError(
                f"Expected dict for dataclass"
                f" {annotation.__name__},"
                f" got {type(raw_value).__name__}"
            )

        # Step A: from_dict classmethod
        from_dict = getattr(annotation, "from_dict", None)
        if callable(from_dict):
            return from_dict(raw_value)

        # Step B: global dataclass parser
        parser = ctx.get("dataclass_parser")
        if parser is not None:
            return parser(annotation, raw_value)

        # Step C: direct construction
        return annotation(**raw_value)


class PrimitiveResolver(ParamResolver):
    """Resolves primitive and complex non-model types.

    Passes through primitive types (``int``, ``str``, ``float``,
    ``bool``, ``None``) directly. For complex types (``Union``,
    ``Literal``, ``Annotated`` with constraints, etc.), uses a
    pre-built ``TypeAdapter``.
    """

    def __init__(
        self,
        adapter: TypeAdapter | None = None,  # type: ignore[type-arg]
    ) -> None:
        """Initialize with an optional TypeAdapter.

        Args:
            adapter: Pre-built TypeAdapter for complex types. ``None``
                for bare primitives (pass-through without validation).
        """
        self._adapter = adapter

    def resolve(
        self,
        raw_value: Any,
        annotation: Any,
        **ctx: Any,
    ) -> Any:
        """Validate with TypeAdapter if available, else pass through.

        Args:
            raw_value: The raw value from JSON-RPC params.
            annotation: Unused.
            **ctx: Unused.

        Returns:
            The validated or passed-through value.

        Raises:
            pydantic.ValidationError: If adapter validation fails.
        """
        if self._adapter is not None:
            return self._adapter.validate_python(raw_value)
        return raw_value


class PassthroughResolver(ParamResolver):
    """Catch-all resolver for ``Any`` or unannotated parameters.

    Returns the raw value without any conversion.
    """

    def resolve(
        self,
        raw_value: Any,
        annotation: Any,
        **ctx: Any,
    ) -> Any:
        """Return raw_value unchanged.

        Args:
            raw_value: The raw value from JSON-RPC params.
            annotation: Unused.
            **ctx: Unused.

        Returns:
            The raw value as-is.
        """
        return raw_value
