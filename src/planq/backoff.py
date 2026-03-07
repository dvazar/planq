"""Backoff strategies for retry and reconnect loops."""

from __future__ import annotations

from random import uniform
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from planq.types import Seconds


def full_jitter(
    attempt: int,
    base_delay: float,
    max_delay: float,
) -> Seconds:
    """Full Jitter exponential backoff.

    Returns a random delay in
    ``[0, min(max_delay, base_delay * 2^(attempt-1))]``.

    Args:
        attempt: Current attempt number (1-based).
        base_delay: Initial delay in seconds; doubles each attempt.
        max_delay: Upper bound for the delay in seconds.

    Returns:
        Backoff delay in seconds.
    """
    cap = min(max_delay, base_delay * (2 ** (attempt - 1)))
    return uniform(0, cap)
