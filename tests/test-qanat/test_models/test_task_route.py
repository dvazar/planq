"""Comprehensive tests for TaskRoute model."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from qanat import enums as qanat_enums
from qanat.enums import ExecutionMode
from qanat.models import TaskRoute
from .conftest import (
    invalid_floats,
    invalid_max_retries,
    valid_task_route_kwargs,
)

# Rebuild the model with proper type namespace
TaskRoute.model_rebuild(_types_namespace=qanat_enums.__dict__)


# === Layer 1: Parametrized Edge Cases ===


class TestTaskRouteValidation:
    """Explicit edge case tests for each field validator."""

    # max_retries validation tests

    @pytest.mark.parametrize(
        "max_retries",
        [-1, -999],
    )
    def test_max_retries_validation_fails(self, max_retries, mock_handler):
        """max_retries must be non-negative or None."""
        with pytest.raises(ValidationError) as exc_info:
            TaskRoute(
                handler=mock_handler,
                mode=ExecutionMode.ASYNC,
                max_retries=max_retries,
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("max_retries",)
        assert "max_retries must be non-negative" in str(errors[0]["msg"])

    @pytest.mark.parametrize(
        "max_retries",
        [0, 1, 10, 100, None],
    )
    def test_max_retries_validation_succeeds(self, max_retries, mock_handler):
        """Valid max_retries values are accepted (0, positive, None)."""
        route = TaskRoute(
            handler=mock_handler,
            mode=ExecutionMode.ASYNC,
            max_retries=max_retries,
        )
        assert route.max_retries == max_retries

    # time_limit validation tests

    def test_time_limit_rejects_nan(self, mock_handler):
        """time_limit rejects NaN."""
        with pytest.raises(ValidationError) as exc_info:
            TaskRoute(
                handler=mock_handler,
                mode=ExecutionMode.ASYNC,
                time_limit=float("nan"),
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("time_limit",)
        assert "cannot be NaN" in str(errors[0]["msg"])

    def test_time_limit_rejects_positive_infinity(self, mock_handler):
        """time_limit rejects positive infinity."""
        with pytest.raises(ValidationError) as exc_info:
            TaskRoute(
                handler=mock_handler,
                mode=ExecutionMode.ASYNC,
                time_limit=float("inf"),
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("time_limit",)
        assert "cannot be infinite" in str(errors[0]["msg"])

    def test_time_limit_rejects_negative_infinity(self, mock_handler):
        """time_limit rejects negative infinity."""
        with pytest.raises(ValidationError) as exc_info:
            TaskRoute(
                handler=mock_handler,
                mode=ExecutionMode.ASYNC,
                time_limit=float("-inf"),
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("time_limit",)
        assert "cannot be infinite" in str(errors[0]["msg"])

    @pytest.mark.parametrize(
        "time_limit",
        [0.0, -1.0],
    )
    def test_time_limit_rejects_non_positive(self, time_limit, mock_handler):
        """time_limit rejects zero and negative values."""
        with pytest.raises(ValidationError) as exc_info:
            TaskRoute(
                handler=mock_handler,
                mode=ExecutionMode.ASYNC,
                time_limit=time_limit,
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("time_limit",)
        assert "must be positive when specified" in str(errors[0]["msg"])

    @pytest.mark.parametrize(
        "time_limit",
        [None, 0.1, 1.0, 60.0, 3600.0],
    )
    def test_time_limit_accepts_none_or_positive(
        self, time_limit, mock_handler
    ):
        """time_limit accepts None or positive values."""
        route = TaskRoute(
            handler=mock_handler,
            mode=ExecutionMode.ASYNC,
            time_limit=time_limit,
        )
        assert route.time_limit == time_limit

    # grace_period validation tests

    def test_grace_period_rejects_nan(self, mock_handler):
        """grace_period rejects NaN."""
        with pytest.raises(ValidationError) as exc_info:
            TaskRoute(
                handler=mock_handler,
                mode=ExecutionMode.PROCESS,
                grace_period=float("nan"),
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("grace_period",)
        assert "cannot be NaN" in str(errors[0]["msg"])

    def test_grace_period_rejects_positive_infinity(self, mock_handler):
        """grace_period rejects positive infinity."""
        with pytest.raises(ValidationError) as exc_info:
            TaskRoute(
                handler=mock_handler,
                mode=ExecutionMode.PROCESS,
                grace_period=float("inf"),
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("grace_period",)
        assert "cannot be infinite" in str(errors[0]["msg"])

    def test_grace_period_rejects_negative_infinity(self, mock_handler):
        """grace_period rejects negative infinity."""
        with pytest.raises(ValidationError) as exc_info:
            TaskRoute(
                handler=mock_handler,
                mode=ExecutionMode.PROCESS,
                grace_period=float("-inf"),
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("grace_period",)
        assert "cannot be infinite" in str(errors[0]["msg"])

    @pytest.mark.parametrize(
        "grace_period",
        [0.0, -1.0],
    )
    def test_grace_period_rejects_non_positive(
        self, grace_period, mock_handler
    ):
        """grace_period rejects zero and negative values."""
        with pytest.raises(ValidationError) as exc_info:
            TaskRoute(
                handler=mock_handler,
                mode=ExecutionMode.PROCESS,
                grace_period=grace_period,
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("grace_period",)
        assert "must be positive when specified" in str(errors[0]["msg"])

    @pytest.mark.parametrize(
        "grace_period",
        [None, 0.1, 1.0, 5.0, 30.0],
    )
    def test_grace_period_accepts_none_or_positive(
        self, grace_period, mock_handler
    ):
        """grace_period accepts None or positive values."""
        route = TaskRoute(
            handler=mock_handler,
            mode=ExecutionMode.PROCESS,
            grace_period=grace_period,
        )
        assert route.grace_period == grace_period


class TestTaskRouteConstruction:
    """Valid construction scenarios."""

    def test_minimal_construction(self, mock_handler):
        """Can construct with only required fields."""
        route = TaskRoute(handler=mock_handler, mode=ExecutionMode.ASYNC)
        assert route.handler == mock_handler
        assert route.mode == ExecutionMode.ASYNC
        assert route.max_retries is None
        assert route.time_limit is None
        assert route.grace_period is None

    def test_all_fields_custom(self, mock_handler):
        """Can construct with all fields customized."""
        route = TaskRoute(
            handler=mock_handler,
            mode=ExecutionMode.PROCESS,
            max_retries=5,
            time_limit=60.0,
            grace_period=10.0,
        )
        assert route.handler == mock_handler
        assert route.mode == ExecutionMode.PROCESS
        assert route.max_retries == 5
        assert route.time_limit == 60.0
        assert route.grace_period == 10.0

    @pytest.mark.parametrize(
        "mode",
        [ExecutionMode.ASYNC, ExecutionMode.THREAD, ExecutionMode.PROCESS],
    )
    def test_all_execution_modes(self, mode, mock_handler):
        """Can construct with any ExecutionMode."""
        route = TaskRoute(handler=mock_handler, mode=mode)
        assert route.mode == mode

    def test_sync_handler(self, mock_handler):
        """Can use synchronous function as handler."""
        route = TaskRoute(handler=mock_handler, mode=ExecutionMode.THREAD)
        assert callable(route.handler)
        assert route.handler() == "success"

    def test_async_handler(self, async_mock_handler):
        """Can use asynchronous function as handler."""
        route = TaskRoute(handler=async_mock_handler, mode=ExecutionMode.ASYNC)
        assert callable(route.handler)

    def test_callable_class_handler(self, callable_class_handler):
        """Can use callable class instance as handler."""
        route = TaskRoute(
            handler=callable_class_handler, mode=ExecutionMode.ASYNC
        )
        assert callable(route.handler)
        assert route.handler() == "callable class success"

    def test_lambda_handler(self):
        """Can use lambda as handler."""

        def handler(x):
            return x * 2

        route = TaskRoute(handler=handler, mode=ExecutionMode.ASYNC)
        assert callable(route.handler)
        assert route.handler(5) == 10

    def test_max_retries_zero_is_valid(self, mock_handler):
        """max_retries=0 means one attempt with no retries."""
        route = TaskRoute(
            handler=mock_handler,
            mode=ExecutionMode.ASYNC,
            max_retries=0,
        )
        assert route.max_retries == 0


class TestTaskRouteImmutability:
    """Frozen=True enforcement via ConfigDict."""

    def test_cannot_modify_handler(self, mock_handler):
        """Cannot modify handler after construction."""
        route = TaskRoute(handler=mock_handler, mode=ExecutionMode.ASYNC)

        def new_handler():
            return "new"

        with pytest.raises(ValidationError) as exc_info:
            route.handler = new_handler

        assert "frozen" in str(exc_info.value).lower()

    def test_cannot_modify_mode(self, mock_handler):
        """Cannot modify mode after construction."""
        route = TaskRoute(handler=mock_handler, mode=ExecutionMode.ASYNC)
        with pytest.raises(ValidationError) as exc_info:
            route.mode = ExecutionMode.THREAD

        assert "frozen" in str(exc_info.value).lower()

    def test_cannot_modify_max_retries(self, mock_handler):
        """Cannot modify max_retries after construction."""
        route = TaskRoute(handler=mock_handler, mode=ExecutionMode.ASYNC)
        with pytest.raises(ValidationError) as exc_info:
            route.max_retries = 5

        assert "frozen" in str(exc_info.value).lower()

    def test_cannot_modify_time_limit(self, mock_handler):
        """Cannot modify time_limit after construction."""
        route = TaskRoute(handler=mock_handler, mode=ExecutionMode.ASYNC)
        with pytest.raises(ValidationError) as exc_info:
            route.time_limit = 60.0

        assert "frozen" in str(exc_info.value).lower()

    def test_cannot_modify_grace_period(self, mock_handler):
        """Cannot modify grace_period after construction."""
        route = TaskRoute(handler=mock_handler, mode=ExecutionMode.PROCESS)
        with pytest.raises(ValidationError) as exc_info:
            route.grace_period = 10.0

        assert "frozen" in str(exc_info.value).lower()


class TestTaskRouteStrictMode:
    """Pydantic strict mode enforcement (no type coercion)."""

    def test_mode_rejects_string(self, mock_handler):
        """Strict mode: mode must be ExecutionMode, not string."""
        with pytest.raises(ValidationError) as exc_info:
            TaskRoute(handler=mock_handler, mode="async")

        errors = exc_info.value.errors()
        assert any(error["loc"] == ("mode",) for error in errors)

    def test_max_retries_rejects_string(self, mock_handler):
        """Strict mode: max_retries must be int or None, not string."""
        with pytest.raises(ValidationError) as exc_info:
            TaskRoute(
                handler=mock_handler,
                mode=ExecutionMode.ASYNC,
                max_retries="5",
            )

        errors = exc_info.value.errors()
        assert any(error["loc"] == ("max_retries",) for error in errors)

    def test_time_limit_rejects_string(self, mock_handler):
        """Strict mode: time_limit must be float or None, not string."""
        with pytest.raises(ValidationError) as exc_info:
            TaskRoute(
                handler=mock_handler,
                mode=ExecutionMode.ASYNC,
                time_limit="60.0",
            )

        errors = exc_info.value.errors()
        assert any(error["loc"] == ("time_limit",) for error in errors)

    def test_grace_period_rejects_string(self, mock_handler):
        """Strict mode: grace_period must be float or None, not string."""
        with pytest.raises(ValidationError) as exc_info:
            TaskRoute(
                handler=mock_handler,
                mode=ExecutionMode.PROCESS,
                grace_period="5.0",
            )

        errors = exc_info.value.errors()
        assert any(error["loc"] == ("grace_period",) for error in errors)


# === Layer 2: Hypothesis Fuzzing ===


class TestTaskRouteFuzz:
    """Property-based testing with hypothesis."""

    @pytest.mark.hypothesis
    @given(
        max_retries=st.one_of(
            st.none(),
            st.integers(min_value=0, max_value=1000),
        )
    )
    def test_valid_max_retries_always_succeeds(self, max_retries):
        """None or non-negative max_retries is valid."""

        def handler():
            return "success"

        route = TaskRoute(
            handler=handler,
            mode=ExecutionMode.ASYNC,
            max_retries=max_retries,
        )
        assert route.max_retries == max_retries

    @pytest.mark.hypothesis
    @given(max_retries=invalid_max_retries)
    def test_invalid_max_retries_always_fails(self, max_retries):
        """Negative max_retries values always fail."""

        def handler():
            return "success"

        with pytest.raises(ValidationError) as exc_info:
            TaskRoute(
                handler=handler,
                mode=ExecutionMode.ASYNC,
                max_retries=max_retries,
            )

        errors = exc_info.value.errors()
        assert any(
            error["loc"] == ("max_retries",)
            and "max_retries must be non-negative" in str(error["msg"])
            for error in errors
        )

    @pytest.mark.hypothesis
    @given(
        time_limit=st.one_of(
            st.none(),
            st.floats(min_value=0.001, max_value=10000.0),
        )
    )
    def test_valid_time_limit_always_succeeds(self, time_limit):
        """None or positive time_limit is valid."""

        def handler():
            return "success"

        route = TaskRoute(
            handler=handler,
            mode=ExecutionMode.ASYNC,
            time_limit=time_limit,
        )
        assert route.time_limit == time_limit

    @pytest.mark.hypothesis
    @given(time_limit=invalid_floats)
    def test_invalid_time_limit_always_fails(self, time_limit):
        """NaN, Inf, or non-positive time_limit always fails."""

        def handler():
            return "success"

        with pytest.raises(ValidationError) as exc_info:
            TaskRoute(
                handler=handler,
                mode=ExecutionMode.ASYNC,
                time_limit=time_limit,
            )

        errors = exc_info.value.errors()
        assert any(error["loc"] == ("time_limit",) for error in errors)

    @pytest.mark.hypothesis
    @given(
        grace_period=st.one_of(
            st.none(),
            st.floats(min_value=0.001, max_value=1000.0),
        )
    )
    def test_valid_grace_period_always_succeeds(self, grace_period):
        """None or positive grace_period is valid."""

        def handler():
            return "success"

        route = TaskRoute(
            handler=handler,
            mode=ExecutionMode.PROCESS,
            grace_period=grace_period,
        )
        assert route.grace_period == grace_period

    @pytest.mark.hypothesis
    @given(grace_period=invalid_floats)
    def test_invalid_grace_period_always_fails(self, grace_period):
        """NaN, Inf, or non-positive grace_period always fails."""

        def handler():
            return "success"

        with pytest.raises(ValidationError) as exc_info:
            TaskRoute(
                handler=handler,
                mode=ExecutionMode.PROCESS,
                grace_period=grace_period,
            )

        errors = exc_info.value.errors()
        assert any(error["loc"] == ("grace_period",) for error in errors)

    @pytest.mark.hypothesis
    @given(valid_task_route_kwargs())
    def test_valid_combinations_always_succeed(self, kwargs):
        """Any combination of valid field values succeeds."""
        route = TaskRoute(**kwargs)
        assert route.handler == kwargs["handler"]
        assert route.mode == kwargs["mode"]
        assert route.max_retries == kwargs["max_retries"]
        assert route.time_limit == kwargs["time_limit"]
        assert route.grace_period == kwargs["grace_period"]
