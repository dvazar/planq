"""Payload conversion from JSON-RPC params to handler arguments."""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from planq.exceptions import InvalidParamsError
from planq.params.types import HandlerSignature

if TYPE_CHECKING:
    from planq.types import DataclassParser, JsonRpcParams


class ParamsConverter:
    """Converts JSON-RPC params into typed handler arguments.

    Called per request. Normalizes list params to named params
    via ``positional_names``, resolves each parameter through its
    pre-assigned resolver, and collects all errors for a single
    ``InvalidParamsError``.
    """

    def convert(
        self,
        signature: HandlerSignature,
        params: JsonRpcParams,
        method: str,
        *,
        dataclass_parser: DataclassParser[Any] | None = None,
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        """Convert JSON-RPC params to (args, kwargs).

        Args:
            signature: Pre-analyzed handler signature.
            params: Raw JSON-RPC params (list, dict, or None).
            method: JSON-RPC method name for error context.
            dataclass_parser: Optional global dataclass parser.

        Returns:
            A ``(args, kwargs)`` tuple ready for
            ``handler(*args, **kwargs)``.

        Raises:
            InvalidParamsError: If any parameter fails
                validation or required params are missing.
        """
        errors: list[dict[str, Any]] = []
        kwargs: dict[str, Any] = {}
        extra_args: tuple[Any, ...] = ()

        # 1. Normalize list -> dict via positional_names
        if isinstance(params, list):
            params_dict: dict[str, Any] = {}
            pos_names = signature.positional_names
            for i, value in enumerate(params):
                if i < len(pos_names):
                    params_dict[pos_names[i]] = value
                else:
                    break
            remaining = params[len(pos_names) :]
            if remaining:
                if signature.has_var_positional:
                    extra_args = tuple(remaining)
                else:
                    for i in range(len(pos_names), len(params)):
                        errors.append(
                            {
                                "loc": (i,),
                                "msg": "Unexpected positional argument",
                                "type": "unexpected_positional",
                            }
                        )
        elif isinstance(params, dict):
            params_dict = dict(params)
        else:
            params_dict = {}

        # 2. Resolve each parameter
        consumed: set[str] = set()
        for meta in signature.params:
            if meta.name in params_dict:
                consumed.add(meta.name)
                try:
                    kwargs[meta.name] = meta.resolver.resolve(
                        params_dict[meta.name],
                        meta.annotation,
                        dataclass_parser=dataclass_parser,
                    )
                except ValidationError as exc:
                    for err in exc.errors():
                        errors.append(
                            {
                                "loc": (
                                    meta.name,
                                    *err.get("loc", ()),
                                ),
                                "msg": err["msg"],
                                "type": err["type"],
                            }
                        )
                except Exception as exc:
                    errors.append(
                        {
                            "loc": (meta.name,),
                            "msg": str(exc),
                            "type": type(exc).__name__,
                        }
                    )
            elif meta.default is not inspect.Parameter.empty:
                continue
            else:
                errors.append(
                    {
                        "loc": (meta.name,),
                        "msg": "Missing required parameter",
                        "type": "missing",
                    }
                )

        # 3. Check extra keys
        extra_keys = set(params_dict) - consumed
        if extra_keys:
            if signature.has_var_keyword:
                for key in extra_keys:
                    kwargs[key] = params_dict[key]
            else:
                for key in sorted(extra_keys):
                    errors.append(
                        {
                            "loc": (key,),
                            "msg": "Unexpected parameter",
                            "type": "unexpected_keyword",
                        }
                    )

        if errors:
            raise InvalidParamsError(errors, method)

        # 4. When *args are present, rebuild as positional
        # to avoid double-binding (positional + keyword).
        if extra_args:
            ordered: list[Any] = []
            for meta in signature.params:
                if meta.name in kwargs:
                    ordered.append(kwargs.pop(meta.name))
            return tuple(ordered) + extra_args, kwargs

        # 5. Move positional-only params from kwargs to args
        positional_args: list[Any] = []
        for meta in signature.params:
            if (
                meta.kind == inspect.Parameter.POSITIONAL_ONLY
                and meta.name in kwargs
            ):
                positional_args.append(kwargs.pop(meta.name))

        return tuple(positional_args), kwargs
