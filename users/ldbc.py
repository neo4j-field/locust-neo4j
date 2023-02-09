"""
LDBC-like users
"""
from random import uniform
from typing import Tuple

from locust import task
from locust.env import Environment

from . import Neo4jUser


LDBC_I_C_6 = """
// Tag co-occurrence
MATCH (knownTag:Tag {name: "Tag-" + $tagId})
MATCH (person:Person {id:$personId})-[:KNOWS*1..2]-(friend)
WHERE NOT person=friend
WITH DISTINCT friend, knownTag

MATCH (friend)<-[:HAS_CREATOR]-(post)
WHERE (post)-[:HAS_TAG]->(knownTag)
WITH post, knownTag

MATCH (post)-[:HAS_TAG]->(commonTag)
WHERE NOT commonTag=knownTag
WITH commonTag, count(post) AS postCount

RETURN commonTag.name AS tagName, postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 10;
"""

class LDBCUser(Neo4jUser):
    """
    Implements (currently a single) test case(s) from the LDBC Social Network
    Benchmark.
    """
    def __init__(self, env: Environment):
        super().__init__(env)
        self.max_person_id = -1
        self.max_tag = 16080

    def find_max_person_id(self) -> None:
        if self.client is None:
            raise RuntimeError("failed to find a valid Neo4j client")

        with self.client.driver.session() as session:
            res = session.run(
                "MATCH (p:Person) RETURN max(p.id) AS maxId"
            )
            record = res.single()
            if record is None:
                raise RuntimeError("failed to find max person id")
            value = record.value()
            res.consume()
            self.max_person_id = int(value)

    @task
    def ldbc_tag_cooccurrence(self) -> None:
        if self.max_node_id < 1:
            self.find_max_node_id()

        target = int(uniform(1, self.max_person_id))
        self.read(LDBC_I_C_6, personId=target)
