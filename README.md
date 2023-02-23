# Quick 'n Dirty Locust <-> Neo4j integration

[Locust](https://locust.io) is a Python-based load generator designed
specifically for load testing web applications.

HOWEVER! It _can_ be adapted to non-HTTP systems.

## Install

```
$ python3 -m venv venv
$ . venv/bin/activate
$ pip install -r requirements.txt
```

## Run

```
(venv) $ python neo4j_locust.py
```


### Subclassing `Neo4jUser`

If you're writing a Locust test, you should start by subclassing the
`Neo4jUser`. It exposes simple `read()` and `write()` methods to make
it easier to configure your Cypher transactions.

An example (also in [neo4j_locust.py](./neo4j_locust.py)):

```python
from locust import task
from locust.env import Environment

class DumbUser(Neo4jUser):
    """Simply slams the target Neo4j system with a silly Cypher read."""

    def __init__(self, environment: Environment,
                 auth: Tuple[str, str] = ("neo4j", "password")):
        super().__init__(environment, auth=auth)

    @task
    def hello_world(self):
        cnt, delta = self.read("UNWIND ['Hello', 'World'] AS x RETURN x")
```
