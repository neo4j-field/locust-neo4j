from enum import Enum
from time import perf_counter
from uuid import uuid4
from weakref import ref

from collections.abc import Callable
from typing import Any, Dict, Tuple

from locust import events, User, task
from locust.env import Environment
from locust.user.wait_time import constant
from neo4j import Driver, GraphDatabase


class Request(Enum):
    READ = "CypherRead"
    WRITE = "CypherWrite"

# using a global for now
DRIVER_CONFIG = {
    "user_agent": "neo4j_locust/1.0 (yolo edition)",
    "max_connection_lifetime": 60, # seconds
    "max_connection_pool_size": 10, # XXX
    "connection_acquisition_timeout": 10, # seconds
}

class Neo4jClient:
    """Wrapper around a Driver instance to make a Neo4jUser simpler."""
    def __init__(self, uri: str, auth: Tuple[str, str]):
        self.driver: Driver = GraphDatabase \
            .driver(uri, auth=auth, **DRIVER_CONFIG) #type: ignore

        self.client_id = str(uuid4())
        self.pool_key = f"{auth[0]}@{uri}"

    @classmethod
    def _do_work(cls, cypher) -> Callable:
        def _work(tx, **params) -> Tuple[int, Any]:
            result = tx.run(cypher, params)
            # brute force through all results
            cnt = sum(1 for _ in iter(result))
            return cnt, result.consume()
        return _work

    def _run_tx(self, req: Request, user_ref: ref,
                cypher: str, **params) -> Tuple[int, int, bool]:
        err = None
        delta, cnt, abort = 0, 0, False
        send_report = True
        # todo: set env from pool during client creation
        fire: Callable = user_ref().environment.events.request.fire

        start = perf_counter()
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
            delta = int((perf_counter() - start) * 1000)
        except (KeyboardInterrupt, StopIteration) as e:
            # someone pulled the plug, just ignore for now
            send_report = False
            abort = True
        except Exception as e:
            err = e

        if send_report:
            fire(request_type=str(req.value),
                 name=cypher,
                 response_time=delta,
                 response_length=cnt, # should be bytes, but we're using rows
                 exception=err,
                 context = {
                     "user_id": user_ref().user_id,
                     "client_id": self.client_id
                 })
        return cnt, delta, abort

    def read(self, user_ref: ref, cypher: str, **params):
        return self._run_tx(Request.READ, user_ref, cypher, **params)

    def write(self, user_ref: ref, cypher: str, **params):
        return self._run_tx(Request.WRITE, user_ref, cypher, **params)

    def close(self):
        print(f"{self} closing driver")
        self.driver.close()

    def __str__(self):
        return f"Neo4jClient({self.client_id})"

    def __del__(self):
        self.close()
        print(f"{self} destroyed")


class Neo4jPool:
    """
    Manages Neo4j Driver state. Acts as a 'static' instance, so 1 per Python
    interpreter.
    """
    client_map: Dict[str, Neo4jClient] = dict()  # key -> client
    refcnt_map: Dict[str, int] = dict()          # client_id -> refcnt
    environment = None

    @classmethod
    def on_test_start(cls, environment: Environment):
        print(f"Neo4jPool: on_test_start")
        cls.environment = environment

    @classmethod
    def on_test_stop(cls, environment: Environment):
        print(f"Neo4jPool: on_test_stop")
        # todo: release all clients, but we need to hand out weakrefs

    @classmethod
    def acquire(cls, uri: str, auth: Tuple[str, str]) -> Neo4jClient:
        key = f"{auth[0]}@{uri}"

        if key in cls.client_map:
            client = cls.client_map[key]
        else:
            client = Neo4jClient(uri, auth)
            cls.client_map.update({key: client})
            print(f"Neo4jPool: added driver for {key}")

        cnt = cls.refcnt_map.get(client.client_id, 0) + 1
        cls.refcnt_map[client.client_id] = cnt
        print(f"Neo4jPool.acquire: client {client.client_id} refcnt = {cnt}")

        return client

    @classmethod
    def release(cls, client: Neo4jClient) -> None:
        client_id: str = client.client_id
        key: str = client.pool_key

        if not client_id in cls.refcnt_map:
            print(f"Neo4jPool: unknown client_id {client_id}")
            return

        # XXX there's possibly a race condition here
        cnt = cls.refcnt_map[client_id] - 1
        print(f"Neo4jPool.release: client {client_id} refcnt = {cnt}")
        if cnt < 1:
            cls.refcnt_map.pop(client_id)
            cls.client_map.pop(key).close()
        else:
            cls.refcnt_map[client_id] = cnt


@events.test_start.add_listener
def init_pool(environment, **kwargs):
    Neo4jPool.on_test_start(environment)


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    Neo4jPool.on_test_stop(environment)


@events.user_error.add_listener
def on_user_error(user_instance, exception, tb):
    if not isinstance(exception, LocustSucksException):
        print(f"error from {user_instance}: {exception}\n{tb}")
    else:
        print(f"user {user_instance} hard stopping.")
        user_instance.interrupt(reschedule=False)

@events.quit.add_listener
def on_quit(exit_code):
    print(f"exiting... {exit_code}")


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
        self.user_id = str(uuid4())
        self.client = None

    def read(self, cypher: str, **params) -> Tuple[int, int]:
        """Higher order wrapper around Neo4jClient.read()"""
        if not self.client:
            # bailout
            return -1, 0

        cnt, delta, abort = self.client.read(ref(self), cypher, **params)
        if abort:
            print(f"{self} aborting")
            self.on_stop()
        return cnt, delta

    def write(self, cypher: str, **params) -> Tuple[int, int]:
        """Higher order wrapper around Neo4jClient.write()"""
        if not self.client:
            # bailout
            return -1, 0

        cnt, delta, abort = self.client.write(ref(self), cypher, **params)
        if abort:
            print(f"{self} aborting")
            self.on_stop()
        return cnt, delta

    def on_start(self):
        if not self.client:
            self.client: Neo4jClient = Neo4jPool.acquire(self.host, self.auth)
        print(f"{self} starting")

    def on_stop(self):
        if self.client:
            Neo4jPool.release(self.client)
            self.client = None
        self.greenlet.kill()     # XXX this is silly
        print(f"{self} stopped")

    def __str__(self):
        return f"Neo4jUser({self.user_id})"

    def __del__(self):
        if self.client:
            self.on_stop()
        print(f"{self} destroyed")


class DumbUser(Neo4jUser):
    """Simply slams the target Neo4j system with a silly Cypher read."""
    #wait_time = between(0.01, 0.02)

    def __init__(self, environment: Environment,
                 auth: Tuple[str, str] = ("neo4j", "password")):
        super().__init__(environment, auth=auth)

    @task
    def hello_world(self):
        cnt, delta = self.read("UNWIND ['Hello', 'World'] AS x RETURN x")
