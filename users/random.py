from . import Neo4jUser

from locust import task
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

    @task
    def random_read(self) -> None:
        self.read("MATCH 1 RETURN 1")
