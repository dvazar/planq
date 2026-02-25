
<br/>
<img alt="planq-logo" src="docs/images/logo.svg" style="display: block; width:100%; max-height:150px; height:auto; margin-left: auto; margin-right: auto;" />

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
