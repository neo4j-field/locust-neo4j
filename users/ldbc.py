"""
LDBC-like users
"""
import logging
from random import uniform
from typing import Tuple

from locust import tag, task
from locust.env import Environment

from . import Neo4jUser


LDBC_I_C_2 = """
// Recent messages by your friends
MATCH (:Person {id: $personId})-[:KNOWS]-(friend),
      (friend)<-[:HAS_CREATOR]-(message)
WHERE message.creationDate <= date({year: 2010, month:10, day:10})
RETURN friend.id AS personId,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName,
       message.id AS messageId,
       coalesce(message.content, message.imageFile) AS messageContent,
       message.creationDate AS messageDate
ORDER BY messageDate DESC, messageId ASC
LIMIT 20
"""

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

LDBC_I_C_9 = """
// Recent messages by Friends and Friends of friends
MATCH (person:Person {id:$personId})-[:KNOWS*1..2]-(otherPerson)
MATCH (otherPerson)<-[:HAS_CREATOR]-(message:Message)
WHERE message.creationDate < date({year: 2010, month:10, day:10})
RETURN otherPerson.id as personId,
       otherPerson.firstName as firstName,
       otherPerson.lastName as lastName,
       message.id as messageId,
       coalesce(message.content, message.imageFile) as messageContent,
       message.creationDate as messageDate
ORDER BY messageDate DESC, messageId ASC
LIMIT 20
"""


LDBC_I_C_10 = """
// Friend recommendation
MATCH (person:Person {id:$personId})-[:KNOWS*2..2]-(friend),
       (friend)-[:IS_LOCATED_IN]->(city)
WHERE NOT friend=person AND
      NOT (friend)-[:KNOWS]-(person) AND
            ( (friend.birthday.month=$birthdayMonth AND friend.birthday.day>=21) OR
        (friend.birthday.month=($birthdayMonth%12)+1 AND friend.birthday.day<22) )
WITH DISTINCT friend, city, person
OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)
WITH friend, city, collect(post) AS posts, person
WITH friend,
     city,
     size(posts) AS postCount,
     size([p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)]) AS commonPostCount
RETURN friend.id AS personId,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName,
       friend.gender AS personGender,
       city.name AS personCityName,
       commonPostCount - (postCount - commonPostCount) AS commonInterestScore
ORDER BY commonInterestScore DESC, personId ASC
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
        self.max_tag_id = 16080

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
            if value is None:
                logging.error("failed to find person id...is this an LDBC graph?")
                self.environment.runner.quit()
            self.max_person_id = int(value)

    @tag("ldbc_ic2")
    @task(37)
    def ldbc_recent_messages_by_friends(self) -> None:
        if self.max_person_id < 1:
            self.find_max_person_id()

        person_id = int(uniform(1, self.max_person_id))
        self.read(LDBC_I_C_2, personId=person_id)

    @tag("ldbc_ic6")
    @task(129)
    def ldbc_tag_cooccurrence(self) -> None:
        if self.max_person_id < 1:
            self.find_max_person_id()

        person_id = int(uniform(1, self.max_person_id))
        tag_id = str(uniform(1, self.max_tag_id))
        self.read(LDBC_I_C_6, personId=person_id, tagId=tag_id)

    @tag("ldbc_ic9")
    @task(157)
    def ldbc_recent_messages_by_fofs(self) -> None:
        if self.max_person_id < 1:
            self.find_max_person_id()

        person_id = int(uniform(1, self.max_person_id))
        self.read(LDBC_I_C_9, personId=person_id)

    @tag("ldbc_ic10")
    @task(30)
    def ldbc_friend_recommendation(self) -> None:
        if self.max_person_id < 1:
            self.find_max_person_id()

        person_id = int(uniform(1, self.max_person_id))
        birthday = int(uniform(1, 13))
        self.read(LDBC_I_C_10, personId=person_id, birthdayMonth=birthday)
