"""Enumerations used throughout qanat."""

from enum import IntEnum, StrEnum


class ExecutionMode(StrEnum):
    """Strategy used to run a registered task handler.

    Choose based on the nature of the work:

    - CPU-bound → ``PROCESS``
    - Sync I/O  → ``THREAD``
    - Async I/O → ``ASYNC`` (default)
    """

    ASYNC = "async"
    """Run the handler as a native coroutine in the event loop."""

    THREAD = "thread"
    """Run the handler in a thread via ``asyncio.to_thread``."""

    PROCESS = "process"
    """Run the handler in a ``ProcessPoolExecutor`` worker."""


class Header(StrEnum):
    """Broker-level message headers."""

    EXPIRE_AT = "x-expire-at"
    """Unix timestamp (float) after which the message is considered expired."""


class JsonRpcError(IntEnum):
    """Standard JSON-RPC 2.0 error codes.

    See: https://www.jsonrpc.org/specification#error_object
    """

    PARSE_ERROR = -32700
    """Invalid JSON was received by the server."""

    INVALID_REQUEST = -32600
    """The JSON sent is not a valid Request object."""

    METHOD_NOT_FOUND = -32601
    """The method does not exist or is not available."""

    INVALID_PARAMS = -32602
    """Invalid method parameter(s)."""

    INTERNAL_ERROR = -32603
    """Internal JSON-RPC error."""

    DEADLINE_EXCEEDED = -32001
    """Message deadline exceeded before processing could begin."""
