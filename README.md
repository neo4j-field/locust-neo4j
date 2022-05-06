# Quick 'n Dirty Locust <-> Neo4j integration

[Locust](https://locust.io) is a Python-based load generator designed specifically for load testing web applications.

HOWEVER! It _can_ be adapted to non-HTTP systems.

## Install

```
$ python3 -m venv venv
$ . venv/bin/activate
$ pip install -r requirements.txt
```

## Run

```
(venv) $ locust -f neo4j_locst.py DumbUser
```

Open your browser to [http://localhost:8089].

## Caveats

- Currently assumes a localhost Neo4j with password of `password`.
  + You can modify or re-write the `DumbUser` class to change this.
- I've tested with multiple users simulated, but not multiple _worker_ nodes.
