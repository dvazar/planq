import logging
from typing import Any

from qanat.context import QanatContextFilter

logger = logging.getLogger("qanat")
logger.addFilter(QanatContextFilter())

# Prevent "No handlers found" warning if user doesn't configure logging
logger.addHandler(logging.NullHandler())


def instrument_logging(default_value: str | None = "-") -> None:
    """Configure global LogRecordFactory to inject QanatContext fields.

    Wraps the standard log record factory with QanatContextFilter to
    automatically add message metadata, route information, and latency
    metrics to all log records created after this call.

    Args:
        default_value: Placeholder string for missing context fields.
            Defaults to ``"-"``.

    Example:
        >>> from qanat.log import instrument_logging
        >>> instrument_logging(default_value="-")
        >>> logging.info("Processing task")  # Auto-includes context
    """
    old_factory = logging.getLogRecordFactory()

    qanat_filter = QanatContextFilter(default_value)

    def qanat_record_factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
        """Create a LogRecord with QanatContext fields injected.

        Internal factory function used by ``instrument_logging()``. Wraps
        the original factory to apply ``qanat_filter`` to every record.

        Args:
            *args: Positional arguments forwarded to original factory.
            **kwargs: Keyword arguments forwarded to original factory.

        Returns:
            LogRecord with QanatContext attributes attached.
        """
        record = old_factory(*args, **kwargs)
        qanat_filter.filter(record)
        return record

    logging.setLogRecordFactory(qanat_record_factory)
