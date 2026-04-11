# PlanQ Examples

Runnable reference applications demonstrating PlanQ across
brokers and deployment patterns. For prose guides and how-to
articles, see [`../docs/howto/`](../docs/howto/) and
[`../docs/tutorial/`](../docs/tutorial/).

Each example directory is self-contained: it has its own
`README.md` with setup and run instructions, its own
infrastructure manifest (`docker-compose.yml` or a bootstrap
script), and its own complete working code. Clone the repo,
`cd` into any example, and follow that directory's README.

## Choose your starting point

| I want to... | Example | Broker | Infrastructure |
|---|---|---|---|
| See PlanQ end-to-end on SQS | [`sqs_quickstart/`](sqs_quickstart/) | SQS | LocalStack |
| See PlanQ end-to-end on Redis | [`redis_quickstart/`](redis_quickstart/) | Redis | docker-compose |
| Integrate PlanQ into a Django app with a standalone worker | [`django_worker/`](django_worker/) | Redis | docker-compose |
| Run a PlanQ consumer embedded in uvicorn via ASGI lifespan | [`django_asgi_embedded/`](django_asgi_embedded/) | Redis | docker-compose |

## Django deployment patterns compared

Both Django examples ship the **same application** (image
resize service) and differ only in how the PlanQ consumer is
deployed. Use this table to pick the right pattern for your
project:

| | `django_worker/` | `django_asgi_embedded/` |
|---|---|---|
| Processes | 2 (web + worker) | 1 (uvicorn) |
| Worker scaling | independent | coupled to web |
| CPU/memory isolation | yes | no |
| Signal handling | native SIGINT/SIGTERM | ASGI lifespan |
| Ops complexity | higher | lower |
| Recommended for | production, high-throughput | dev, prototypes, small apps |

## Prerequisites for all examples

- Python 3.12+
- `uv` or `pip`
- Docker (for infrastructure containers)
