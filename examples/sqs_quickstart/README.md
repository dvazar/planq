# SQS Quickstart

Minimal end-to-end PlanQ example against AWS SQS using LocalStack.
One task, `email.send`, takes a Pydantic `EmailPayload` and
simulates sending a transactional email.

## Prerequisites

- Python 3.12+
- Docker (for LocalStack)
- `planq[sqs]` installed

```bash
pip install 'planq[sqs]'
```

## Run

Open three terminals.

**Terminal 1: start LocalStack**

```bash
docker run --rm -p 4566:4566 localstack/localstack
```

Wait until you see `Ready.` in the LocalStack output.

**Terminal 2: create the queue and start the worker**

```bash
cd examples/sqs_quickstart
python bootstrap.py
# => Created queue: http://localhost:4566/000000000000/emails

python worker.py
# => (blocks, waiting for messages)
```

**Terminal 3: send a few messages**

```bash
cd examples/sqs_quickstart
python producer.py
# => [producer] sent immediate: <msg-id>
# => [producer] sent delayed: <msg-id>
# => [producer] sent with TTL: <msg-id>
```

Watch Terminal 2 — you should see three `[worker] sending email
to ...` lines. The delayed one arrives ~5 seconds later.

## Stop

`Ctrl+C` in each terminal. The worker drains in-flight messages
before exiting.

## See also

- [`../redis_quickstart/`](../redis_quickstart/) — the same
  example running on Redis Streams.
- [`../../docs/howto/`](../../docs/howto/) — prose how-to guides
  for individual features (delayed messages, retry policies,
  custom middleware, etc.).
