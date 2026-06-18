"""Queue introspection value objects."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class QueueStats:
    """Point-in-time depth breakdown for a single queue.

    Field semantics are uniform across brokers, but population depends on
    the backend and connection role:

    - ``pending``: messages ready to be consumed. Always populated.
    - ``scheduled``: delayed messages not yet due. Always populated.
    - ``in_flight``: claimed-but-unacked messages. Best-effort — Redis
      reports this only from a connection that knows the consumer group;
      a producer-only connection reports 0. SQS always reports it.
    """

    queue: str
    pending: int
    scheduled: int
    in_flight: int

    @property
    def total(self) -> int:
        """Sum of all buckets."""
        return self.pending + self.scheduled + self.in_flight
