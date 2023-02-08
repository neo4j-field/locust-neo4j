"""
Foundational classes and types.
"""
import logging

from enum import Enum
from time import perf_counter
from uuid import uuid4
from weakref import ref

from locust import events, User
from locust.env import Environment
from neo4j import Driver, GraphDatabase, ManagedTransaction

from collections.abc import Callable
from typing import cast, Any, Dict, Optional, Tuple


class Request(Enum):
    READ = "CypherRead"
    WRITE = "CypherWrite"


# using a global for now
DRIVER_CONFIG = {
    "user_agent": "neo4j_locust/1.0 (yolo edition)",
    "max_connection_lifetime": 60 * 30, # seconds
    "max_connection_pool_size": 100, # XXX
    "connection_acquisition_timeout": 10, # seconds
}


class Neo4jClient:
    """
    Wrapper around a Driver instance to make a Neo4jUser simpler by
    encapsulating any Neo4j Driver nuances (like tx handling).
    """

    def __init__(self, uri: str, auth: Tuple[str, str]):
        self.driver: Driver = GraphDatabase \
            .driver(uri, auth=auth, **DRIVER_CONFIG) #type: ignore
        self.client_id = str(uuid4())
        self.pool_key = f"{auth[0]}@{uri}"

    @classmethod
    def _do_work(cls, cypher: str) -> Callable[..., Any]:
        def _work(tx: ManagedTransaction, **kwargs: Any) -> Tuple[int, Any]:
            result = tx.run(cypher, kwargs)
            # brute force through all results
            cnt = sum(1 for _ in iter(result))
            return cnt, result.consume()
        return _work

    def _run_tx(self, req: Request, user_ref: ref[User],
                cypher: str, **kwargs: Any) -> Tuple[int, int, bool]:
        err = None
        delta, cnt, abort = 0, 0, False
        send_report = True
        # todo: set env from pool during client creation
        fire: Callable[..., Any] = (
            user_ref().environment.events.request.fire #type: ignore
        )

        start = perf_counter()
        try:
            with self.driver.session() as session:
                if req is Request.READ:
                    cnt, _ = session.read_transaction(
                        self._do_work(cypher), **kwargs)
                elif req is Request.WRITE:
                    cnt, _ = session.write_transaction(
                        self._do_work(cypher), **kwargs)
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
                     "user_id": user_ref().user_id, # type: ignore
                     "client_id": self.client_id
                 })
        return cnt, delta, abort

    def read(self, user_ref: ref[User], cypher: str, **kwargs: Any) \
            -> Tuple[int, int, bool]:
        return self._run_tx(Request.READ, user_ref, cypher, **kwargs)

    def write(self, user_ref: ref[User], cypher: str, **kwargs: Any) \
            -> Tuple[int, int, bool]:
        return self._run_tx(Request.WRITE, user_ref, cypher, **kwargs)

    def close(self) -> None:
        logging.info(f"{self} closing driver")
        self.driver.close()

    def __str__(self) -> str:
        return f"Neo4jClient({self.client_id})"

    def __del__(self) -> None:
        self.close()
        logging.debug(f"{self} destroyed")


@events.test_start.add_listener #type: ignore
def init_pool(environment: Environment, **kwargs: Any) -> None:
    Neo4jPool.on_test_start(environment)


@events.test_stop.add_listener #type: ignore
def on_test_stop(environment: Environment, **kwargs: Any) -> None:
    Neo4jPool.on_test_stop(environment)


@events.user_error.add_listener
def on_user_error(user_instance, exception, tb): # type: ignore
    logging.info(f"user {user_instance} hard stopping.")
    user_instance.stop(force=True)


class Neo4jPool:
    """
    Manages Neo4j Driver state. Acts as a 'static' instance, so 1 per Python
    interpreter.
    """
    client_map: Dict[str, Neo4jClient] = dict()  # key -> client
    refcnt_map: Dict[str, int] = dict()          # client_id -> refcnt
    environment = None

    @classmethod
    def on_test_start(cls, environment: Environment) -> None:
        cls.environment = environment

    @classmethod
    def on_test_stop(cls, environment: Environment) -> None:
        pass
        # todo: release all clients?, but we need to hand out weakrefs

    @classmethod
    def acquire(cls, uri: str, auth: Tuple[str, str]) -> Neo4jClient:
        key = f"{auth[0]}@{uri}"

        if key in cls.client_map:
            client = cls.client_map[key]
        else:
            client = Neo4jClient(uri, auth)
            cls.client_map.update({key: client})
            logging.info(f"Neo4jPool: added driver for {key}")

        cnt = cls.refcnt_map.get(client.client_id, 0) + 1
        cls.refcnt_map[client.client_id] = cnt
        logging.debug(f"Neo4jPool.acquire: {client} refcnt = {cnt}")

        return client

    @classmethod
    def release(cls, client: Neo4jClient) -> None:
        client_id: str = client.client_id
        key: str = client.pool_key

        if not client_id in cls.refcnt_map:
            logging.error(f"Neo4jPool: unknown client_id {client_id}")
            return

        # XXX there's possibly a race condition here
        cnt = cls.refcnt_map[client_id] - 1
        logging.debug(f"Neo4jPool.release: {client} refcnt = {cnt}")
        if cnt < 1:
            cls.refcnt_map.pop(client_id)
            cls.client_map.pop(key).close()
        else:
            cls.refcnt_map[client_id] = cnt


class Neo4jUser(User):
    """
    Represents an end-user that may perform a transaction to the database.

    Subclass this puppy and create some @task methods! (See DumbUser for an
    example of this.)
    """
    abstract = True

    def __init__(self, env: Environment):
        super().__init__(env)
        if not env.host:
            raise ValueError("host cannot be empty!")
        if not env.parsed_options:
            raise ValueError("missing parsed options!")
        self.auth = (
            env.parsed_options.neo4j_user,
            env.parsed_options.neo4j_pass
        )
        self.user_id = str(uuid4())
        self.client: Optional[Neo4jClient] = None

    def read(self, cypher: str, **kwargs: Any) -> Tuple[int, int]:
        """Higher order wrapper around Neo4jClient.read()"""
        if not self.client:
            # bailout
            return -1, 0

        cnt, delta, abort = self.client.read(ref(self), cypher, **kwargs)
        if abort:
            logging.debug(f"{self} aborting")
            self.on_stop()
        return cnt, delta

    def write(self, cypher: str, **kwargs: Any) -> Tuple[int, int]:
        """Higher order wrapper around Neo4jClient.write()"""
        if not self.client:
            # bailout
            return -1, 0

        cnt, delta, abort = self.client.write(ref(self), cypher, **kwargs)
        if abort:
            logging.debug(f"{self} aborting")
            self.on_stop()
        return cnt, delta

    def on_start(self) -> None:
        if not self.client:
            self.client = Neo4jPool.acquire(cast(str, self.host), self.auth)
        logging.info(f"{self} starting")

    def on_stop(self) -> None:
        if self.client:
            Neo4jPool.release(self.client)
            self.client = None
        # self.greenlet.kill()     # XXX this is silly
        logging.info(f"{self} stopped")

    def __str__(self) -> str:
        return f"Neo4jUser({self.user_id})"

    def __del__(self) -> None:
        if self.client:
            self.on_stop()
        logging.debug(f"{self} destroyed")
