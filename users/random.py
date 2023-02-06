from . import Neo4jUser

from locust import on_start, task
from locust.env import Environment

from typing import Tuple


class RandomReader(Neo4jUser):
    """
    Randomly selects an anchor node in the database and traverses a number of
    hops.
    """

    def __init__(self, environment: Environment,
                 auth: Tuple[str, str] = ("neo4j", "password")):
        super().__init__(environment, auth=auth)
        self.max_node_id = 0

    @on_start
    def find_max_node_id(self) -> None:
        with self.driver.session() as session:
            res = session.run(
                "MATCH (n) WITH id(n) AS nodeId RETURN max(nodeId)"
            )
            self.max_node_id = int(res.single())

    @task
    def random_read(self) -> None:
        self.read(
            """
            MATCH (n) WHERE id(n) = $nodeId
            MATCH p=(n)-[*1..3]-()
            RETURN p LIMIT 5
            """,
            nodeId=self.max_node_id
        )
