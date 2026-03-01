"""Comprehensive tests for W3C Trace Context support."""

from __future__ import annotations

import re

import pytest

from planq.tracing import (
    TRACEPARENT_PATTERN,
    TraceContext,
    parse_traceparent_and_generate_span,
)

# Valid traceparent components for reuse
VALID_TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
VALID_PARENT_ID = "b7ad6b7169203331"
VALID_TRACEPARENT = f"00-{VALID_TRACE_ID}-{VALID_PARENT_ID}-01"


# === Layer 1: TraceContext Dataclass ===


class TestTraceContext:
    """Test TraceContext construction and properties."""

    def test_construction_with_required_fields(self):
        """TraceContext requires trace_id and span_id."""
        ctx = TraceContext(trace_id="a" * 32, span_id="b" * 16)

        assert ctx.trace_id == "a" * 32
        assert ctx.span_id == "b" * 16
        assert ctx.parent_span_id is None
        assert ctx.trace_flags == "00"

    def test_construction_with_all_fields(self):
        """TraceContext accepts all fields."""
        ctx = TraceContext(
            trace_id="a" * 32,
            span_id="b" * 16,
            parent_span_id="c" * 16,
            trace_flags="01",
        )

        assert ctx.trace_id == "a" * 32
        assert ctx.span_id == "b" * 16
        assert ctx.parent_span_id == "c" * 16
        assert ctx.trace_flags == "01"

    def test_frozen_immutability(self):
        """TraceContext is frozen (immutable)."""
        ctx = TraceContext(trace_id="a" * 32, span_id="b" * 16)

        with pytest.raises(AttributeError):
            ctx.trace_id = "x" * 32  # type: ignore[misc]

    def test_slots(self):
        """TraceContext uses __slots__ for memory efficiency."""
        ctx = TraceContext(trace_id="a" * 32, span_id="b" * 16)

        assert hasattr(ctx, "__slots__")
        with pytest.raises((AttributeError, TypeError)):
            ctx.extra_field = "nope"  # type: ignore[attr-defined]

    def test_equality(self):
        """Two TraceContext with same fields are equal."""
        ctx1 = TraceContext(
            trace_id="a" * 32, span_id="b" * 16, trace_flags="01"
        )
        ctx2 = TraceContext(
            trace_id="a" * 32, span_id="b" * 16, trace_flags="01"
        )

        assert ctx1 == ctx2

    def test_inequality(self):
        """Two TraceContext with different fields are not equal."""
        ctx1 = TraceContext(trace_id="a" * 32, span_id="b" * 16)
        ctx2 = TraceContext(trace_id="a" * 32, span_id="c" * 16)

        assert ctx1 != ctx2

    def test_to_traceparent_format(self):
        """to_traceparent() returns W3C header format."""
        ctx = TraceContext(
            trace_id=VALID_TRACE_ID,
            span_id=VALID_PARENT_ID,
            trace_flags="01",
        )

        assert ctx.to_traceparent() == (
            f"00-{VALID_TRACE_ID}-{VALID_PARENT_ID}-01"
        )

    def test_to_traceparent_with_custom_flags(self):
        """to_traceparent() preserves custom trace flags."""
        ctx = TraceContext(
            trace_id="a" * 32,
            span_id="b" * 16,
            trace_flags="ff",
        )

        result = ctx.to_traceparent()

        assert result.endswith("-ff")
        assert result == f"00-{'a' * 32}-{'b' * 16}-ff"

    def test_to_traceparent_roundtrip(self):
        """Roundtrip: to_traceparent() output can be parsed back."""
        original = TraceContext(
            trace_id=VALID_TRACE_ID,
            span_id=VALID_PARENT_ID,
            parent_span_id="d" * 16,
            trace_flags="01",
        )

        child = parse_traceparent_and_generate_span(original.to_traceparent())

        # trace_id and flags preserved
        assert child.trace_id == original.trace_id
        assert child.trace_flags == original.trace_flags
        # original span_id becomes child's parent_span_id
        assert child.parent_span_id == original.span_id
        # child gets a new span_id
        assert child.span_id != original.span_id


# === Layer 2: TRACEPARENT_PATTERN Regex ===


class TestTraceparentPattern:
    """Test the TRACEPARENT_PATTERN regex."""

    def test_matches_valid_traceparent(self):
        """Regex matches valid traceparent format."""
        match = TRACEPARENT_PATTERN.match(VALID_TRACEPARENT)

        assert match is not None
        version, trace_id, parent_id, flags = match.groups()
        assert version == "00"
        assert trace_id == VALID_TRACE_ID
        assert parent_id == VALID_PARENT_ID
        assert flags == "01"

    def test_rejects_short_trace_id(self):
        """Regex rejects trace_id shorter than 32 hex chars."""
        assert TRACEPARENT_PATTERN.match("00-abc-b7ad6b7169203331-01") is None

    def test_rejects_short_parent_id(self):
        """Regex rejects parent_id shorter than 16 hex chars."""
        assert TRACEPARENT_PATTERN.match(f"00-{VALID_TRACE_ID}-abc-01") is None

    def test_rejects_non_hex_characters(self):
        """Regex rejects non-hex characters."""
        bad_trace = "00-" + "g" * 32 + f"-{VALID_PARENT_ID}-01"
        assert TRACEPARENT_PATTERN.match(bad_trace) is None

    def test_rejects_missing_fields(self):
        """Regex rejects traceparent with missing fields."""
        assert TRACEPARENT_PATTERN.match(f"00-{VALID_TRACE_ID}") is None


# === Layer 3: parse_traceparent_and_generate_span ===


class TestParseTraceparentAndGenerateSpan:
    """Test parse_traceparent_and_generate_span()."""

    def test_valid_traceparent_continues_trace(self):
        """Valid traceparent continues the existing trace."""
        result = parse_traceparent_and_generate_span(VALID_TRACEPARENT)

        assert result.trace_id == VALID_TRACE_ID
        assert result.parent_span_id == VALID_PARENT_ID
        assert result.trace_flags == "01"
        # span_id should be newly generated (16 hex chars)
        assert len(result.span_id) == 16
        assert re.fullmatch(r"[0-9a-f]{16}", result.span_id)

    def test_none_generates_new_trace(self):
        """None traceparent generates a new root trace."""
        result = parse_traceparent_and_generate_span(None)

        assert len(result.trace_id) == 32
        assert re.fullmatch(r"[0-9a-f]{32}", result.trace_id)
        assert len(result.span_id) == 16
        assert re.fullmatch(r"[0-9a-f]{16}", result.span_id)
        assert result.parent_span_id is None
        assert result.trace_flags == "00"

    def test_empty_string_generates_new_trace(self):
        """Empty string generates a new root trace."""
        result = parse_traceparent_and_generate_span("")

        assert result.parent_span_id is None
        assert result.trace_flags == "00"

    def test_invalid_format_generates_new_trace(self):
        """Invalid format generates a new root trace."""
        result = parse_traceparent_and_generate_span("not-a-traceparent")

        assert result.parent_span_id is None
        assert len(result.trace_id) == 32

    def test_all_zero_trace_id_generates_new_trace(self):
        """All-zero trace_id generates a new root trace."""
        traceparent = f"00-{'0' * 32}-{VALID_PARENT_ID}-01"
        result = parse_traceparent_and_generate_span(traceparent)

        assert result.parent_span_id is None
        assert result.trace_id != "0" * 32

    def test_all_zero_parent_id_generates_new_trace(self):
        """All-zero parent_id generates a new root trace."""
        traceparent = f"00-{VALID_TRACE_ID}-{'0' * 16}-01"
        result = parse_traceparent_and_generate_span(traceparent)

        assert result.parent_span_id is None

    def test_whitespace_stripped(self):
        """Leading/trailing whitespace is stripped."""
        result = parse_traceparent_and_generate_span(f"  {VALID_TRACEPARENT}  ")

        assert result.trace_id == VALID_TRACE_ID
        assert result.parent_span_id == VALID_PARENT_ID

    def test_case_normalized(self):
        """Uppercase hex characters are normalized to lowercase."""
        upper = f"00-{VALID_TRACE_ID.upper()}-{VALID_PARENT_ID.upper()}-01"
        result = parse_traceparent_and_generate_span(upper)

        assert result.trace_id == VALID_TRACE_ID
        assert result.parent_span_id == VALID_PARENT_ID

    def test_trace_flags_preserved(self):
        """Trace flags from traceparent are preserved."""
        traceparent = f"00-{VALID_TRACE_ID}-{VALID_PARENT_ID}-ff"
        result = parse_traceparent_and_generate_span(traceparent)

        assert result.trace_flags == "ff"

    def test_span_id_is_16_hex(self):
        """Generated span_id is exactly 16 hex characters."""
        result = parse_traceparent_and_generate_span(VALID_TRACEPARENT)

        assert re.fullmatch(r"[0-9a-f]{16}", result.span_id)

    def test_trace_id_format_for_new_trace(self):
        """Generated trace_id is exactly 32 hex characters."""
        result = parse_traceparent_and_generate_span(None)

        assert re.fullmatch(r"[0-9a-f]{32}", result.trace_id)

    def test_each_call_generates_unique_span_id(self):
        """Each call generates a different span_id."""
        result1 = parse_traceparent_and_generate_span(VALID_TRACEPARENT)
        result2 = parse_traceparent_and_generate_span(VALID_TRACEPARENT)

        assert result1.span_id != result2.span_id

    def test_wrong_version_still_parsed(self):
        """Non-00 version is still parsed (forward compatibility).

        The W3C spec says implementations should parse and use the
        trace_id/parent_id even if the version is unknown.
        """
        traceparent = f"ff-{VALID_TRACE_ID}-{VALID_PARENT_ID}-01"
        result = parse_traceparent_and_generate_span(traceparent)

        assert result.trace_id == VALID_TRACE_ID
        assert result.parent_span_id == VALID_PARENT_ID
