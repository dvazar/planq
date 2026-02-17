"""Shared type aliases for the agnosticq package.

All aliases use Python 3.12 ``type`` statement syntax.

+-----------------+------------------------------+
| Alias           | Concrete type                |
+=================+==============================+
| JsonRpcId       | ``str | int | None``         |
+-----------------+------------------------------+
| JsonRpcVersion  | ``Literal["2.0"]``           |
+-----------------+------------------------------+
| JsonRpcParams   | ``dict | list | None``       |
+-----------------+------------------------------+
| Headers         | ``dict[str, str]``           |
+-----------------+------------------------------+
| Seconds         | ``float``                    |
+-----------------+------------------------------+
"""

from __future__ import annotations

from typing import Any, Literal

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
