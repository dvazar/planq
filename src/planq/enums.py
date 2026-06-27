"""Enumerations used throughout planq."""

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

    TRACEPARENT = "traceparent"
    """W3C Trace Context traceparent header for distributed tracing."""


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


class LogEvent(StrEnum):
    """Stable identifiers for log events emitted by the library.

    Each member corresponds to a single log site. Use these values
    in ``extra={"event": LogEvent.XXX}`` to enable reliable
    aggregation and alerting on structured log output.
    """

    RETRY_PREDICATE_ERROR = "retry_predicate_error"
    """Callable retry predicate raised an unexpected exception."""

    PROCESS_MONITOR_ERROR = "process_monitor_error"
    """Process pool monitor thread encountered an error."""

    REJECT_CALLBACK_ERROR = "reject_callback_error"
    """An on_reject callback raised an exception."""

    HEARTBEAT_CALLBACK_ERROR = "heartbeat_callback_error"
    """An on_heartbeat callback raised an exception."""

    HANDLER_RETRYING = "handler_retrying"
    """Handler failed but retries remain; message will be requeued."""

    PUBLISH_RESPONSE_FAILED = "publish_response_failed"
    """Failed to publish JSON-RPC response to reply_to queue."""

    MESSAGE_REQUEUEING = "message_requeueing"
    """Message is being nacked for redelivery after RetryMessage."""

    WORKER_SHUTDOWN_REQUEUEING = "worker_shutdown_requeueing"
    """In-flight message requeued because the worker is shutting down."""

    MESSAGE_REJECTING = "message_rejecting"
    """Message is being permanently rejected."""

    PIPELINE_ERROR = "pipeline_error"
    """Unhandled exception escaped the middleware pipeline."""

    BROKER_OPERATION_FAILED = "broker_operation_failed"
    """Broker ack/nack/reject call failed after pipeline."""

    MESSAGE_DEADLINE_EXCEEDED = "message_deadline_exceeded"
    """Message dropped because its deadline had expired."""

    DEADLINE_LEEWAY_WARNING = "deadline_leeway_warning"
    """DeadlineMiddleware configured with unusually large leeway."""

    POISON_MESSAGE = "poison_message"
    """Message body could not be parsed as valid JSON-RPC."""

    POISON_MESSAGE_HANDLING_FAILED = "poison_message_handling_failed"
    """Failed to handle (log + delete) a poison message."""

    MESSAGE_ALREADY_SETTLED = "message_already_settled"
    """Duplicate ack/reject/nack skipped on already-settled message."""

    CONSUME_CONNECTION_ERROR = "consume_connection_error"
    """Transient Redis error in consume loop; retrying with backoff."""
