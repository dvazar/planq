from enum import IntEnum, StrEnum


class ExecutionMode(StrEnum):
    ASYNC = "async"
    THREAD = "thread"
    PROCESS = "process"


class Header(StrEnum):
    """Broker-level message headers."""

    MAX_RETRIES = "x-max-retries"
    EXPIRE_AT = "x-expire-at"


class JsonRpcError(IntEnum):
    """Standard JSON-RPC 2.0 error codes.

    See: https://www.jsonrpc.org/specification#error_object
    """

    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603
