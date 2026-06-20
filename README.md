<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./docs/images/logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="./docs/images/logo-light.svg">
    <img alt="PlanQ" src="./docs/images/logo-light.svg" width="400px">
  </picture>
</p>

<p align="center">
  <a href="https://github.com/dvazar/planq/actions/workflows/ci.yml"><img src="https://github.com/dvazar/planq/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://codecov.io/gh/dvazar/planq"><img src="https://codecov.io/gh/dvazar/planq/graph/badge.svg" alt="codecov"></a>
  <a href="https://pypi.org/project/planq/"><img src="https://img.shields.io/pypi/v/planq.svg" alt="PyPI"></a>
  <a href="https://pypi.org/project/planq/"><img src="https://img.shields.io/pypi/pyversions/planq.svg" alt="Python versions"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License"></a>
</p>

---

**PlanQ** is a transport-agnostic async task queue for Python. Define a task
once and run it over an in-memory, Redis, or SQS broker — the wire format is
JSON-RPC 2.0, so producers and consumers stay decoupled from the transport.

## Installation

```bash
pip install planq            # in-memory only
pip install "planq[redis]"   # Redis broker
pip install "planq[sqs]"     # AWS SQS broker
```

## Example

```python
from planq import Planq
from planq.providers.memory import InMemoryBroker

app = Planq(broker=InMemoryBroker())

@app.task()
async def greet(name: str, say: str = "hi") -> None:
    print(f"{say}, {name}!")

await greet.send("world")   # enqueue
```

## License

Apache-2.0
