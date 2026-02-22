"""Shared fixtures and hypothesis strategies for models tests."""

from __future__ import annotations

import pytest
from hypothesis import settings
from hypothesis import strategies as st

from qanat.enums import ExecutionMode

# Configure hypothesis for consistent test behavior
settings.register_profile("default", max_examples=100, deadline=1000)
settings.load_profile("default")


# === Valid Value Strategies ===


@st.composite
def valid_consumer_settings_kwargs(draw):
    """Generate valid ConsumerSettings constructor kwargs."""
    return {
        "concurrency": draw(st.integers(min_value=1, max_value=1000)),
        "max_retries": draw(
            st.one_of(
                st.none(),
                st.integers(min_value=0, max_value=100),
            )
        ),
        "retry_base_delay": draw(st.floats(min_value=0.1, max_value=100.0)),
        "retry_max_delay": draw(st.floats(min_value=1.0, max_value=1000.0)),
        "process_timeout_grace_period": draw(
            st.floats(min_value=0.1, max_value=60.0)
        ),
    }


@st.composite
def valid_task_route_kwargs(draw):
    """Generate valid TaskRoute constructor kwargs."""

    def sync_handler(*args, **kwargs):
        return "success"

    async def async_handler(*args, **kwargs):
        return "async success"

    handler = draw(st.sampled_from([sync_handler, async_handler]))
    mode = draw(st.sampled_from(list(ExecutionMode)))

    return {
        "handler": handler,
        "mode": mode,
        "max_retries": draw(
            st.one_of(
                st.none(),
                st.integers(min_value=0, max_value=100),
            )
        ),
        "time_limit": draw(
            st.one_of(
                st.none(),
                st.floats(min_value=0.1, max_value=3600.0),
            )
        ),
        "grace_period": draw(
            st.one_of(
                st.none(),
                st.floats(min_value=0.1, max_value=60.0),
            )
        ),
    }


# === Invalid Value Strategies ===

invalid_floats = st.one_of(
    st.just(float("nan")),
    st.just(float("inf")),
    st.just(float("-inf")),
    st.floats(max_value=0.0),
    st.floats(min_value=-1000.0, max_value=-0.001),
)

invalid_max_retries = st.integers(max_value=-1)

invalid_concurrency = st.integers(max_value=0)


# === Fixtures ===


@pytest.fixture
def default_consumer_settings():
    """ConsumerSettings with all default values."""
    from qanat.models import ConsumerSettings

    return ConsumerSettings()


@pytest.fixture
def mock_handler():
    """Simple synchronous handler for TaskRoute."""

    def handler(*args, **kwargs):
        return "success"

    return handler


@pytest.fixture
def async_mock_handler():
    """Simple asynchronous handler for TaskRoute."""

    async def handler(*args, **kwargs):
        return "async success"

    return handler


@pytest.fixture
def callable_class_handler():
    """Callable class instance for TaskRoute."""

    class CallableHandler:
        def __call__(self, *args, **kwargs):
            return "callable class success"

    return CallableHandler()
