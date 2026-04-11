# Redis Quickstart

Minimal end-to-end PlanQ example against Redis Streams. The same
`email.send` task shown in the SQS quickstart, running against a
local Redis container instead of LocalStack SQS. Use this to
compare the two brokers — the handler code is identical.

## Prerequisites

- Python 3.12+
- Docker (for Redis)
- `planq[redis]` installed

```bash
pip install 'planq[redis]'
```

## Run

Open three terminals.

**Terminal 1: start Redis**

```bash
cd examples/redis_quickstart
docker compose up
```

**Terminal 2: start the worker**

```bash
cd examples/redis_quickstart
python worker.py
# => (blocks, waiting for messages)
```

**Terminal 3: send a few messages**

```bash
cd examples/redis_quickstart
python producer.py
# => [producer] sent immediate: <msg-id>
# => [producer] sent delayed: <msg-id>
# => [producer] sent with TTL: <msg-id>
```

Watch Terminal 2 — you should see three `[worker] sending email
to ...` lines, with the delayed one arriving ~5 seconds later.

## Producer vs consumer broker config

Redis splits the broker configuration between producer and
consumer. `tasks.py` constructs `RedisBroker` without a
`consumer` argument so the producer can import it without
spawning the background scheduler. `worker.py` reconstructs the
broker with a `RedisConsumerConfig` before calling
`PlanqConsumer.run()`. This is the idiomatic pattern for real
deployments where producer and consumer run in separate
processes.

## Stop

`Ctrl+C` the worker; `docker compose down` the Redis container.

## See also

- [`../sqs_quickstart/`](../sqs_quickstart/) — the same example
  on AWS SQS.
- [`../../docs/reference/providers/redis.md`](../../docs/reference/providers/redis.md) —
  full Redis broker reference (when written).
