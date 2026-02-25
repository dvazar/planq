<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./docs/images/logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="./docs/images/logo-light.svg">
    <img alt="PlanQ Logo" src="./docs/images/logo-light.svg" width="400px">
  </picture>
</p>

---

**Documentation**: <a href="https://" target="_blank">https://</a>

**Source Code**: <a href="https://github.com/dvazar/planq" target="_blank">https://github.com/dvazar/planq</a>

---

# planq

## Installation

```bash
pip install planq
```

## Example

```python
from planq import App

app = App("memory://", queue="my_queue")

@app.task
async def greeting(name: str, say: str = "hi") -> None:
    print(f"{say}, {name}!")
```
