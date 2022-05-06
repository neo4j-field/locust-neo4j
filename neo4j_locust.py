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

class Neo4jUser: ... # forward declaration
class Neo4jClient: ... # forward declaration


class Neo4jPool:
    """
    Manages Neo4j Driver state. Acts as a 'static' instance, so 1 per Python
    interpreter.
    """
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
        self.driver: Driver = GraphDatabase.driver(uri, auth=auth) #type: ignore
        self.client_id = str(uuid.uuid4())

    @classmethod
    def _do_work(cls, cypher) -> Callable:
        def _work(tx, **params) -> Tuple[int, Any]:
            result = tx.run(cypher, params)
            # brute force through all results
            cnt = sum(1 for _ in iter(result))
            return cnt, result.consume()
        return _work

    def _run_tx(self, req: Request, user: Neo4jUser,
                cypher: str, **params) -> Tuple[int, int]:
        err: Exception = None
        delta, cnt = 0, 0
        send_report = True
        fire: Callable = user.environment.events.request.fire

        start = time.perf_counter()
        try:
            with self.driver.session() as session:
                if req is Request.READ:
                    cnt, _ = session.read_transaction(
                        self._do_work(cypher), **params)
                elif req is Request.WRITE:
                    cnt, _ = session.write_transaction(
                        self._do_work(cypher), **params)
                else:
                    raise Exception("oh crap")
            delta = (time.perf_counter() - start) * 1000
        except (KeyboardInterrupt, StopIteration) as e:
            # someone pulled the plug, just ignore for now
            send_report = False
        except Exception as e:
            err = e

        if send_report:
            fire(request_type=str(req),
                 name=cypher,
                 response_time=delta,
                 response_length=cnt, # should be bytes, but we're using rows
                 exception=err,
                 context = {
                     "user_id": user.user_id,
                     "client_id": self.client_id
                 })
        return cnt, delta

    def read(self, user: Neo4jUser, cypher: str, **params):
        return self._run_tx(Request.READ, user, cypher, **params)

    def write(self, user: Neo4jUser, cypher: str, **params):
        return self._run_tx(Request.WRITE, user, cypher, **params)

    def close(self):
        self.driver.close()

    def __del__(self):
        self.driver.close()


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
        if not self.host:
            self.host = "neo4j://localhost"
        self.auth = auth
        self.user_id = str(uuid.uuid4())

    def read(self, cypher: str, **params):
        """Higher order wrapper around Neo4jClient.read()"""
        return self.client.read(self, cypher, **params)

    def write(self, cypher: str, **params):
        """Higher order wrapper around Neo4jClient.write()"""
        return self.client.write(self, cypher, **params)

    def on_start(self):
        self.client: Neo4jClient = Neo4jPool.get_client(self.host, self.auth)
        print(f"{self} starting")

    def on_stop(self):
        self.client.close()
        print(f"{self} stopped")
        # todo: need a better cleanup hook...this pulls the plug on multiple
        # users at once

    def __str__(self):
        return f"Neo4jUser({self.user_id})"


class DumbUser(Neo4jUser):
    """Simply slams the target Neo4j system with a silly Cypher read."""

    def __init__(self, environment: Environment,
                 auth: Tuple[str, str] = ("neo4j", "password")):
        super().__init__(environment, auth=auth)

    @task
    def hello_world(self):
        cnt, delta = self.read("UNWIND ['Hello', 'World'] AS x RETURN x")
