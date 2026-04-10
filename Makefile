test-all:
	uv run pytest tests/

test-all-with-coverage:
	uv run pytest tests/ \
      --cov=planq \
      --cov-report=html \
      --cov-report=term-missing \
      --cov-branch

test-planq:
	uv run pytest tests/test-planq/

test-planq-with-cov:
	uv run pytest tests/test-planq/ \
      --cov=planq \
      --cov-report=html \
      --cov-report=term-missing \
      --cov-branch
