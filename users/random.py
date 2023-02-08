from random import uniform
from typing import Tuple

from locust import task
from locust.env import Environment

from . import Neo4jUser


class RandomReader(Neo4jUser):
    """
    Randomly selects an anchor node in the database and traverses a number of
    hops.
    """

    def __init__(self, env: Environment):
        super().__init__(env)
        self.max_node_id = -1

    def find_max_node_id(self) -> None:
        if self.client is None:
            raise RuntimeError("failed to find a valid Neo4j client")

        with self.client.driver.session() as session:
            res = session.run(
                "MATCH (n) WITH id(n) AS nodeId RETURN max(nodeId)"
            )
            record = res.single()
            if record is None:
                raise RuntimeError("failed to find max node id")
            value = record.value()
            res.consume()
            self.max_node_id = int(value)

    @task
    def random_read(self) -> None:
        if self.max_node_id < 0:
            self.find_max_node_id()

        target = int(uniform(0, self.max_node_id))
        self.read(
            """
            MATCH (n) WHERE id(n) = $nodeId
            MATCH p=(n)-[*1..3]-()
            RETURN p LIMIT 5
            """,
            nodeId=target
        )


class RandomWriter(Neo4jUser):
    """
    Randomly selects an anchor node in the database, traverses a few hops,
    and sets properties on all touched nodes to make them "dirty".
    """

    def __init__(self, env: Environment):
        super().__init__(env)
        self.max_node_id = -1

    def find_max_node_id(self) -> None:
        if self.client is None:
            raise RuntimeError("failed to find a valid Neo4j client")

        with self.client.driver.session() as session:
            res = session.run(
                "MATCH (n) WITH id(n) AS nodeId RETURN max(nodeId)"
            )
            record = res.single()
            if record is None:
                raise RuntimeError("failed to find max node id")
            value = record.value()
            res.consume()
            self.max_node_id = int(value)

    @task
    def random_write(self) -> None:
        if self.max_node_id < 0:
            self.find_max_node_id()

        target = int(uniform(0, self.max_node_id))
        self.write(
            """
            MATCH p=(n)-[*0..3]-() WHERE id(n) = $nodeId
            WITH p, localdatetime() as now LIMIT 5
            UNWIND nodes(p) AS n
            SET n.touched = now
            RETURN count(*) AS touched
            """,
            nodeId=target
        )
