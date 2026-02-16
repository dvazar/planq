from __future__ import annotations

from typing import Any, Literal

type JsonRpcId = str | int | None
type JsonRpcVersion = Literal["2.0"]
type JsonRpcParams = dict[str, Any] | list[Any] | None

type Headers = dict[str, str]
type Seconds = float
