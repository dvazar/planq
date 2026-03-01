"""W3C Trace Context (traceparent) parsing and span generation."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Final

#: Regex for the W3C traceparent header format:
#: ``{version}-{trace-id}-{parent-id}-{trace-flags}``
TRACEPARENT_PATTERN: re.Pattern[str] = re.compile(
    r"^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$"
)

# Invalid trace_id and parent_id values (all zeros) are reserved by the W3C spec
_INVALID_TRACE_ID: Final[str] = "0" * 32
_INVALID_PARENT_ID: Final[str] = "0" * 16


@dataclass(frozen=True, slots=True)
class TraceContext:
    """Immutable trace context for distributed tracing.

    Attributes:
        trace_id: 32 hex-character trace identifier.
        span_id: 16 hex-character span identifier for this invocation.
        parent_span_id: 16 hex-character parent span identifier,
            or ``None`` for root spans.
        trace_flags: 2 hex-character trace flags (e.g. ``"01"`` for
            sampled).
    """

    # 32 hex-character trace identifier
    trace_id: str
    # 16 hex-character span identifier for this invocation
    span_id: str
    # 16 hex-character parent span identifier, or None for root spans
    parent_span_id: str | None = None
    # 2 hex-character trace flags
    trace_flags: str = "00"

    def to_traceparent(self) -> str:
        """Format as a W3C ``traceparent`` header value.

        Returns:
            Header string in
            ``{version}-{trace_id}-{span_id}-{trace_flags}`` format.
        """
        return f"00-{self.trace_id}-{self.span_id}-{self.trace_flags}"


def _generate_trace_id() -> str:
    return os.urandom(16).hex()


def _generate_span_id() -> str:
    return os.urandom(8).hex()


def parse_traceparent_and_generate_span(
    traceparent: str | None,
) -> TraceContext:
    """Parse a W3C traceparent header and generate a new child span.

    If the header is absent, malformed, or contains invalid IDs
    (all-zero trace_id or parent_id), a brand-new root trace is
    generated instead.

    Args:
        traceparent: Raw ``traceparent`` header value, or ``None``.

    Returns:
        A :class:`TraceContext` with a freshly generated ``span_id``.
    """
    if traceparent is not None:
        match = TRACEPARENT_PATTERN.match(traceparent.strip().lower())
        if match is not None:
            _version, trace_id, parent_id, trace_flags = match.groups()
            if (
                trace_id != _INVALID_TRACE_ID
                and parent_id != _INVALID_PARENT_ID
            ):
                return TraceContext(
                    trace_id=trace_id,
                    span_id=_generate_span_id(),
                    parent_span_id=parent_id,
                    trace_flags=trace_flags,
                )

    return TraceContext(
        trace_id=_generate_trace_id(),
        span_id=_generate_span_id(),
    )
