"""Shared type aliases for the planq package.

All aliases use Python 3.12 ``type`` statement syntax.

+-----------------+---------------------------------------------------+
| Alias           | Concrete type                                     |
+=================+===================================================+
| JsonRpcId       | ``str | int | None``                              |
+-----------------+---------------------------------------------------+
| JsonRpcVersion  | ``Literal["2.0"]``                                |
+-----------------+---------------------------------------------------+
| JsonRpcParams   | ``dict | list | None``                            |
+-----------------+---------------------------------------------------+
| Headers         | ``dict[str, str]``                                |
+-----------------+---------------------------------------------------+
| Seconds         | ``float``                                         |
+-----------------+---------------------------------------------------+
| RetryCondition  | ``Type[Exception] | Callable[[Exception], bool]`` |
+-----------------+---------------------------------------------------+
"""

from __future__ import annotations

from typing import Any, Callable, Literal, Protocol, Type, TypeVar

#: JSON-RPC request/response identifier; ``None`` denotes a notification.
type JsonRpcId = str | int | None

#: Allowed JSON-RPC protocol version string.
type JsonRpcVersion = Literal["2.0"]

#: JSON-RPC ``params`` field; positional (list), named (dict), or absent.
type JsonRpcParams = dict[str, Any] | list[Any] | None

#: Broker message headers as a flat string-to-string mapping.
type Headers = dict[str, str]

#: A duration expressed in fractional seconds.
type Seconds = float

#: An exception type or predicate function used to determine whether an
#: exception should trigger a retry attempt.
RetryCondition = Type[Exception] | Callable[[Exception], bool]


T = TypeVar("T")


class DataclassParser(Protocol[T]):
    """Pluggable parser for deserializing dicts into dataclasses.

    Implement this protocol to integrate third-party libraries like
    ``dacite`` or ``cattrs`` with the parameter conversion engine.
    """

    def __call__(self, cls: type[T], data: dict[str, Any], /) -> T:
        """Convert a raw dict into a dataclass instance.

        Args:
            cls: The target dataclass type.
            data: Raw dict of field values.

        Returns:
            An instance of ``cls`` populated from ``data``.
        """
        ...
