"""Comprehensive tests for ConsumerSettings model."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from planq.models import ConsumerSettings

from .conftest import (
    invalid_concurrency,
    invalid_floats,
    invalid_max_retries,
    valid_consumer_settings_kwargs,
)

# === Layer 1: Parametrized Edge Cases ===


class TestConsumerSettingsValidation:
    """Explicit edge case tests for each field validator."""

    # Concurrency validation tests

    @pytest.mark.parametrize(
        "concurrency",
        [0, -1, -999],
    )
    def test_concurrency_validation_fails(self, concurrency):
        """Concurrency must be positive (> 0)."""
        with pytest.raises(ValidationError) as exc_info:
            ConsumerSettings(concurrency=concurrency)

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("concurrency",)
        assert "concurrency must be positive" in str(errors[0]["msg"])

    @pytest.mark.parametrize(
        "concurrency",
        [1, 10, 100, 1000, 10000],
    )
    def test_concurrency_validation_succeeds(self, concurrency):
        """Valid concurrency values are accepted."""
        settings = ConsumerSettings(concurrency=concurrency)
        assert settings.concurrency == concurrency

    # max_retries validation tests

    @pytest.mark.parametrize(
        "max_retries",
        [-1, -999],
    )
    def test_max_retries_validation_fails(self, max_retries):
        """max_retries must be non-negative or None."""
        with pytest.raises(ValidationError) as exc_info:
            ConsumerSettings(max_retries=max_retries)

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("max_retries",)
        assert "max_retries must be non-negative" in str(errors[0]["msg"])

    @pytest.mark.parametrize(
        "max_retries",
        [0, 1, 10, 100, None],
    )
    def test_max_retries_validation_succeeds(self, max_retries):
        """Valid max_retries values are accepted (0, positive, None)."""
        settings = ConsumerSettings(max_retries=max_retries)
        assert settings.max_retries == max_retries

    # Float field validation tests (retry_base_delay, retry_max_delay,
    # process_timeout_grace_period)

    @pytest.mark.parametrize(
        "field_name",
        ["retry_base_delay", "retry_max_delay", "process_timeout_grace_period"],
    )
    def test_float_field_rejects_nan(self, field_name):
        """Float fields reject NaN values."""
        with pytest.raises(ValidationError) as exc_info:
            ConsumerSettings(**{field_name: float("nan")})

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == (field_name,)
        assert "cannot be NaN" in str(errors[0]["msg"])

    @pytest.mark.parametrize(
        "field_name",
        ["retry_base_delay", "retry_max_delay", "process_timeout_grace_period"],
    )
    def test_float_field_rejects_positive_infinity(self, field_name):
        """Float fields reject positive infinity."""
        with pytest.raises(ValidationError) as exc_info:
            ConsumerSettings(**{field_name: float("inf")})

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == (field_name,)
        assert "cannot be infinite" in str(errors[0]["msg"])

    @pytest.mark.parametrize(
        "field_name",
        ["retry_base_delay", "retry_max_delay", "process_timeout_grace_period"],
    )
    def test_float_field_rejects_negative_infinity(self, field_name):
        """Float fields reject negative infinity."""
        with pytest.raises(ValidationError) as exc_info:
            ConsumerSettings(**{field_name: float("-inf")})

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == (field_name,)
        assert "cannot be infinite" in str(errors[0]["msg"])

    @pytest.mark.parametrize(
        "field_name,value",
        [
            ("retry_base_delay", 0.0),
            ("retry_base_delay", -1.0),
            ("retry_max_delay", 0.0),
            ("retry_max_delay", -1.0),
            ("process_timeout_grace_period", 0.0),
            ("process_timeout_grace_period", -1.0),
        ],
    )
    def test_float_field_rejects_non_positive(self, field_name, value):
        """Float fields reject zero and negative values."""
        with pytest.raises(ValidationError) as exc_info:
            ConsumerSettings(**{field_name: value})

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == (field_name,)
        assert "must be positive" in str(errors[0]["msg"])

    @pytest.mark.parametrize(
        "field_name,value",
        [
            ("retry_base_delay", 0.001),
            ("retry_base_delay", 1.0),
            ("retry_base_delay", 300.0),
            ("retry_max_delay", 1.0),
            ("retry_max_delay", 300.0),
            ("retry_max_delay", 1000.0),
            ("process_timeout_grace_period", 0.1),
            ("process_timeout_grace_period", 5.0),
            ("process_timeout_grace_period", 60.0),
        ],
    )
    def test_float_field_accepts_positive_values(self, field_name, value):
        """Float fields accept positive values."""
        settings = ConsumerSettings(**{field_name: value})
        assert getattr(settings, field_name) == value


class TestConsumerSettingsConstruction:
    """Valid construction scenarios."""

    def test_default_values(self):
        """All fields have correct defaults per CLAUDE.md."""
        settings = ConsumerSettings()
        assert settings.concurrency == 10
        assert settings.max_retries is None
        assert settings.retry_base_delay == 1.0
        assert settings.retry_max_delay == 300.0
        assert settings.process_timeout_grace_period == 5.0

    def test_custom_values(self):
        """Can construct with custom values for all fields."""
        settings = ConsumerSettings(
            concurrency=50,
            max_retries=5,
            retry_base_delay=2.0,
            retry_max_delay=600.0,
            process_timeout_grace_period=10.0,
        )
        assert settings.concurrency == 50
        assert settings.max_retries == 5
        assert settings.retry_base_delay == 2.0
        assert settings.retry_max_delay == 600.0
        assert settings.process_timeout_grace_period == 10.0

    def test_partial_custom_values(self):
        """Can override some fields while using defaults for others."""
        settings = ConsumerSettings(
            concurrency=25,
            retry_base_delay=5.0,
        )
        assert settings.concurrency == 25
        assert settings.max_retries is None  # default
        assert settings.retry_base_delay == 5.0
        assert settings.retry_max_delay == 300.0  # default
        assert settings.process_timeout_grace_period == 5.0  # default

    def test_max_retries_zero_is_valid(self):
        """max_retries=0 means one attempt with no retries."""
        settings = ConsumerSettings(max_retries=0)
        assert settings.max_retries == 0


class TestConsumerSettingsImmutability:
    """Frozen=True enforcement via ConfigDict."""

    def test_cannot_modify_concurrency(self, default_consumer_settings):
        """Cannot modify concurrency after construction."""
        with pytest.raises(ValidationError) as exc_info:
            default_consumer_settings.concurrency = 20

        assert "frozen" in str(exc_info.value).lower()

    def test_cannot_modify_max_retries(self, default_consumer_settings):
        """Cannot modify max_retries after construction."""
        with pytest.raises(ValidationError) as exc_info:
            default_consumer_settings.max_retries = 5

        assert "frozen" in str(exc_info.value).lower()

    def test_cannot_modify_retry_base_delay(self, default_consumer_settings):
        """Cannot modify retry_base_delay after construction."""
        with pytest.raises(ValidationError) as exc_info:
            default_consumer_settings.retry_base_delay = 2.0

        assert "frozen" in str(exc_info.value).lower()

    def test_cannot_modify_retry_max_delay(self, default_consumer_settings):
        """Cannot modify retry_max_delay after construction."""
        with pytest.raises(ValidationError) as exc_info:
            default_consumer_settings.retry_max_delay = 600.0

        assert "frozen" in str(exc_info.value).lower()

    def test_cannot_modify_grace_period(self, default_consumer_settings):
        """Cannot modify process_timeout_grace_period after construction."""
        with pytest.raises(ValidationError) as exc_info:
            default_consumer_settings.process_timeout_grace_period = 10.0

        assert "frozen" in str(exc_info.value).lower()


class TestConsumerSettingsStrictMode:
    """Pydantic strict mode enforcement (no type coercion)."""

    def test_concurrency_rejects_string(self):
        """Strict mode: concurrency must be int, not string."""
        with pytest.raises(ValidationError) as exc_info:
            ConsumerSettings(concurrency="10")

        errors = exc_info.value.errors()
        assert any(error["loc"] == ("concurrency",) for error in errors)

    def test_max_retries_rejects_string(self):
        """Strict mode: max_retries must be int or None, not string."""
        with pytest.raises(ValidationError) as exc_info:
            ConsumerSettings(max_retries="5")

        errors = exc_info.value.errors()
        assert any(error["loc"] == ("max_retries",) for error in errors)

    def test_retry_base_delay_rejects_string(self):
        """Strict mode: retry_base_delay must be float, not string."""
        with pytest.raises(ValidationError) as exc_info:
            ConsumerSettings(retry_base_delay="1.0")

        errors = exc_info.value.errors()
        assert any(error["loc"] == ("retry_base_delay",) for error in errors)


# === Layer 2: Hypothesis Fuzzing ===


class TestConsumerSettingsFuzz:
    """Property-based testing with hypothesis."""

    @pytest.mark.hypothesis
    @given(st.integers(min_value=1, max_value=10000))
    def test_valid_concurrency_always_succeeds(self, concurrency):
        """Any positive concurrency value is valid."""
        settings = ConsumerSettings(concurrency=concurrency)
        assert settings.concurrency == concurrency

    @pytest.mark.hypothesis
    @given(invalid_concurrency)
    def test_invalid_concurrency_always_fails(self, concurrency):
        """Non-positive concurrency values always fail."""
        with pytest.raises(ValidationError) as exc_info:
            ConsumerSettings(concurrency=concurrency)

        errors = exc_info.value.errors()
        assert any(
            error["loc"] == ("concurrency",)
            and "concurrency must be positive" in str(error["msg"])
            for error in errors
        )

    @pytest.mark.hypothesis
    @given(
        st.one_of(
            st.none(),
            st.integers(min_value=0, max_value=1000),
        )
    )
    def test_valid_max_retries_always_succeeds(self, max_retries):
        """None or non-negative max_retries is valid."""
        settings = ConsumerSettings(max_retries=max_retries)
        assert settings.max_retries == max_retries

    @pytest.mark.hypothesis
    @given(invalid_max_retries)
    def test_invalid_max_retries_always_fails(self, max_retries):
        """Negative max_retries values always fail."""
        with pytest.raises(ValidationError) as exc_info:
            ConsumerSettings(max_retries=max_retries)

        errors = exc_info.value.errors()
        assert any(
            error["loc"] == ("max_retries",)
            and "max_retries must be non-negative" in str(error["msg"])
            for error in errors
        )

    @pytest.mark.hypothesis
    @given(st.floats(min_value=0.001, max_value=1000.0))
    def test_valid_retry_base_delay_always_succeeds(self, retry_base_delay):
        """Positive retry_base_delay is valid."""
        settings = ConsumerSettings(retry_base_delay=retry_base_delay)
        assert settings.retry_base_delay == retry_base_delay

    @pytest.mark.hypothesis
    @given(invalid_floats)
    def test_invalid_retry_base_delay_always_fails(self, retry_base_delay):
        """NaN, Inf, or non-positive retry_base_delay always fails."""
        with pytest.raises(ValidationError) as exc_info:
            ConsumerSettings(retry_base_delay=retry_base_delay)

        errors = exc_info.value.errors()
        assert any(error["loc"] == ("retry_base_delay",) for error in errors)

    @pytest.mark.hypothesis
    @given(st.floats(min_value=0.001, max_value=10000.0))
    def test_valid_retry_max_delay_always_succeeds(self, retry_max_delay):
        """Positive retry_max_delay is valid."""
        settings = ConsumerSettings(retry_max_delay=retry_max_delay)
        assert settings.retry_max_delay == retry_max_delay

    @pytest.mark.hypothesis
    @given(invalid_floats)
    def test_invalid_retry_max_delay_always_fails(self, retry_max_delay):
        """NaN, Inf, or non-positive retry_max_delay always fails."""
        with pytest.raises(ValidationError) as exc_info:
            ConsumerSettings(retry_max_delay=retry_max_delay)

        errors = exc_info.value.errors()
        assert any(error["loc"] == ("retry_max_delay",) for error in errors)

    @pytest.mark.hypothesis
    @given(st.floats(min_value=0.001, max_value=1000.0))
    def test_valid_grace_period_always_succeeds(self, grace_period):
        """Positive process_timeout_grace_period is valid."""
        settings = ConsumerSettings(process_timeout_grace_period=grace_period)
        assert settings.process_timeout_grace_period == grace_period

    @pytest.mark.hypothesis
    @given(invalid_floats)
    def test_invalid_grace_period_always_fails(self, grace_period):
        """NaN, Inf, or non-positive grace_period always fails."""
        with pytest.raises(ValidationError) as exc_info:
            ConsumerSettings(process_timeout_grace_period=grace_period)

        errors = exc_info.value.errors()
        assert any(
            error["loc"] == ("process_timeout_grace_period",)
            for error in errors
        )

    @pytest.mark.hypothesis
    @given(valid_consumer_settings_kwargs())
    def test_valid_combinations_always_succeed(self, kwargs):
        """Any combination of valid field values succeeds."""
        settings = ConsumerSettings(**kwargs)
        assert settings.concurrency == kwargs["concurrency"]
        assert settings.max_retries == kwargs["max_retries"]
        assert settings.retry_base_delay == kwargs["retry_base_delay"]
        assert settings.retry_max_delay == kwargs["retry_max_delay"]
        assert (
            settings.process_timeout_grace_period
            == kwargs["process_timeout_grace_period"]
        )
