from enum import Enum
import time
import uuid

from collections.abc import Callable
from typing import Any, Dict, Tuple

from locust import between, User, task
from locust.env import Environment
from neo4j import Driver, GraphDatabase

class Request(Enum):
    READ = "Cypher Read"
    WRITE = "Cypher Write"

class Neo4jClient: ... # this is here just as a forward declaration

class Neo4jPool:
    """Manages Neo4j Driver state. Acts as a 'static' instance, so 1 per Python interpreter."""
    client_map: Dict[str, Neo4jClient] = {}

    @classmethod
    def get_client(cls, uri: str, auth: Tuple[str, str]) -> Neo4jClient:
        if uri in cls.client_map:
            return cls.client_map[uri]
        client = Neo4jClient(uri, auth)
        cls.client_map.update({uri: client})
        return client


class Neo4jClient:
    """Wrapper around a Driver instance to make a Neo4jUser simpler."""
    def __init__(self, uri: str, auth: Tuple[str, str]):
        self.driver: Driver = GraphDatabase.driver(uri, auth=auth) # type: ignore
        self.client_id = str(uuid.uuid4())

    @classmethod
    def _do_work(cls, cypher) -> Callable:
        def _work(tx, **params) -> Tuple[int, Any]:
            result = tx.run(cypher, params)
            # brute force through all results
            cnt = sum(1 for _ in iter(result))
            return cnt, result.consume()
        return _work

    def _run_tx(self, req: Request, env: Environment, cypher: str, **params) -> Tuple[int, int]:
        err: Exception = None
        delta, cnt = 0, 0
        skip_report = False

        start = time.perf_counter()
        try:
            with self.driver.session() as session:
                if req is Request.READ:
                    cnt, _ = session.read_transaction(self._do_work(cypher), **params)
                elif req is Request.WRITE:
                    cnt, _ = session.write_transaction(self._do_work(cypher), **params)
                else:
                    raise Exception("oh crap")
            delta = (time.perf_counter() - start) * 1000 # todo: is this correct? (millis?)
        except StopIteration as e:
            # someone pulled the plug, just ignore for now
            skip_report = True
        except Exception as e:
            err = e

        if not skip_report:
            env.events.request.fire(request_type=str(req),
                                    name=cypher,
                                    response_time=delta,
                                    response_length=cnt, # should be bytes, but we're using rows
                                    exception=err,
                                    context = { "client_id": self.client_id })
        return cnt, delta

    def read(self, env: Environment, cypher: str, **params):
        return self._run_tx(Request.READ, env, cypher, **params)

    def write(self, env: Environment, cypher: str, **params):
        return self._run_tx(Request.WRITE, env, cypher, **params)

    def __del__(self):
        self._driver.close()


class Neo4jUser(User):
    """
    Represents an end-user that may perform a transaction to the database.

    Subclass this puppy and create some @task methods! (See DumbUser for an
    example of this.)
    """
    abstract = True

    def __init__(self, environment: Environment, auth: Tuple[str, str]):
        super().__init__(environment)
        self.host = environment.host
        self.auth = auth

    def on_start(self):
        self.client: Neo4jClient = Neo4jPool.get_client(self.host, self.auth)
        print(f"Neo4jUser({self.auth[0]}) starting")

    def on_stop(self):
        print(f"Neo4jUser({self.auth[0]}) stopped")
        # need a better cleanup hook
        del self.client


class DumbUser(Neo4jUser):
    """Simply slams the target Neo4j system with a silly Cypher read."""

    def __init__(self, environment: Environment):
        super().__init__(environment, auth=("neo4j", "password")) # <-- need to plug your auth here

    @task
    def hello_world(self):
        cnt, delta = self.client.read(self.environment,
                                      "UNWIND ['Hello', 'World'] AS x RETURN x")
