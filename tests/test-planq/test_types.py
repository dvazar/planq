"""Tests for shared type aliases and protocols."""

from dataclasses import dataclass
from typing import Any

from planq.types import DataclassParser


class TestDataclassParser:
    """Tests for DataclassParser protocol."""

    def test_protocol_callable(self):
        """Protocol __call__ body is reachable."""
        result = DataclassParser.__call__(
            DataclassParser, int, {}
        )
        assert result is None

    def test_conforming_function(self):
        """A conforming function works as DataclassParser."""

        @dataclass
        class Point:
            x: int
            y: int

        def parser(cls: type, data: dict[str, Any], /) -> Any:
            return cls(**data)

        p: DataclassParser[Any] = parser
        result = p(Point, {"x": 1, "y": 2})
        assert result == Point(x=1, y=2)
