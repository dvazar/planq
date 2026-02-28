"""Structured logging integration for planq.

Provides :func:`get_planq_logger` for per-logger context enrichment
and :func:`instrument_logging` for opt-in global enrichment.
"""

from __future__ import annotations

import logging
from typing import Any, Final

from planq.context import PlanqContextFilter

logger = logging.getLogger("planq")

# Prevent "No handlers found" warning if user doesn't configure logging
logger.addHandler(logging.NullHandler())

_original_factory: Any = None
_global_filter: PlanqContextFilter | None = None
_default_filter: Final[PlanqContextFilter] = PlanqContextFilter()


def get_planq_logger(name: str) -> logging.Logger:
    """Return a logger with :class:`PlanqContextFilter` attached.

    Safe to call multiple times for the same ``name``; the filter is
    added only once.

    Args:
        name: Logger name, typically ``__name__``.

    Returns:
        A :class:`logging.Logger` enriched with PlanqContext fields.
    """
    log = logging.getLogger(name)
    if not any(isinstance(f, PlanqContextFilter) for f in log.filters):
        if _global_filter is not None:
            log.addFilter(_global_filter)
        else:
            log.addFilter(_default_filter)
    return log


def instrument_logging(default_value: str | None = None) -> None:
    """Extend context enrichment to all loggers, not just ``planq.*``.

    After this call every log record created by *any* logger will
    carry PlanqContext fields (``message_id``, ``method``, etc.).

    This function is idempotent: calling it multiple times simply
    updates the ``default_value`` used for missing context fields.

    Args:
        default_value: Placeholder used when a context field is not
            available (e.g. outside handler execution). Defaults to
            ``None``.

    Example:
        >>> from planq.log import instrument_logging
        >>> instrument_logging()
        >>> logging.info("Processing task")  # Auto-includes context
    """
    global _original_factory, _global_filter

    if _global_filter is None:
        _global_filter = PlanqContextFilter(default_value)
        _original_factory = logging.getLogRecordFactory()

        def _enriched_factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
            record = _original_factory(*args, **kwargs)
            _global_filter.filter(record)
            return record

        logging.setLogRecordFactory(_enriched_factory)
