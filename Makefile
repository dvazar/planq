test-planq:
	uv run pytest tests/test-planq/

test-planq-with-cov:
	uv run pytest tests/test-planq/ \
      --cov=planq \
      --cov-report=html \
      --cov-report=term-missing \
      --cov-branch
