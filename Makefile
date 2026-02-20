test-qanat:
	uv run pytest tests/test-qanat/

test-qanat-with-cov:
	uv run pytest tests/test-qanat/ \
      --cov=qanat \
      --cov-report=html \
      --cov-report=term-missing \
      --cov-branch
