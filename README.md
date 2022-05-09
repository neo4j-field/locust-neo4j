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
(venv) $ locust -f neo4j_locust.py DumbUser
```

Open your browser to [http://localhost:8089].

## Caveats

- Currently assumes a localhost Neo4j with password of `password`.
  + You can modify or re-write the `DumbUser` class to change this.
  + Probably needs env vars or a Locust arg.
- I've tested with multiple users simulated, but not multiple _worker_
  nodes. Try it out!

## Design

The code is a bit messy and probably should be split out into a few
files to make typing easier, but in short it looks to address the
following gotchas adapting a TCP-based, stateful protocol like Bolt to
Locust (which prefers stateless HTTP protocols):

### Driver pool

`Neo4jPool` manages multiple `Neo4jClient` (see below) instances,
which each contain a Neo4j Driver. The `Neo4jPool` is responsible for
reference counting of the `Neo4jClient` instances and organizing them
based off a key defined like:

```
key = f"{user}@{bolt_uri}"
```

That is, one `Neo4jClient` per user/uri combo. (Since each Neo4j
Driver can only currently use one identity.)

> Note: the `Neo4jPool` is designed to be a static instance per Python
> interpreter. It only exposes class methods.

### Neo4j Client

`Neo4jClient` wraps the Neo4j Python Driver and implements the event
reporting required by Locust for observing the load test performance.

It exposes two simple helper methods: `read()` and `write()`. They
expect to take a _weak_ reference to the `Neo4jUser` (see below)
performing the transaction along with the cypher and any params.


### Neo4j User

The `Neo4jUser` is an abstract Locust `User` that has some lifecycle
hooks for acquiring a `Neo4jClient` and using it to perform
transactions. It talks directly with the local `Neo4jPool` to acquire
or release the `Neo4jClient` it uses under the covers.

> Note: one funky hack is we need to hook into the `on_stop()` event
> handler and kill the underlying `greenlet` instance. This is
> annoying, but I've found no better way to solve the problem of
> runaway users after a test ends. You can thank the use of
> monkey-patching in Locust, I bet.

#### Subclassing `Neo4jUser`

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
