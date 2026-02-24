import logging
from typing import Any

from planq.context import PlanqContextFilter

logger = logging.getLogger("planq")
logger.addFilter(PlanqContextFilter())

# Prevent "No handlers found" warning if user doesn't configure logging
logger.addHandler(logging.NullHandler())


def instrument_logging(default_value: str | None = "-") -> None:
    """Configure global LogRecordFactory to inject PlanqContext fields.

    Wraps the standard log record factory with PlanqContextFilter to
    automatically add message metadata, route information, and latency
    metrics to all log records created after this call.

    Args:
        default_value: Placeholder string for missing context fields.
            Defaults to ``"-"``.

    Example:
        >>> from planq.log import instrument_logging
        >>> instrument_logging(default_value="-")
        >>> logging.info("Processing task")  # Auto-includes context
    """
    old_factory = logging.getLogRecordFactory()

    planq_filter = PlanqContextFilter(default_value)

    def planq_record_factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
        """Create a LogRecord with PlanqContext fields injected.

        Internal factory function used by ``instrument_logging()``. Wraps
        the original factory to apply ``planq_filter`` to every record.

        Args:
            *args: Positional arguments forwarded to original factory.
            **kwargs: Keyword arguments forwarded to original factory.

        Returns:
            LogRecord with PlanqContext attributes attached.
        """
        record = old_factory(*args, **kwargs)
        planq_filter.filter(record)
        return record

    logging.setLogRecordFactory(planq_record_factory)
