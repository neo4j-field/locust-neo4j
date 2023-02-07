#!/usr/bin/env python3
import argparse
from os import cpu_count, environ, getpid

from locust import User
from locust.env import Environment
from locust.log import setup_logging

from typing import List, Optional, Tuple, Type


def worker(neo4j_uri: str, args: argparse.Namespace,
           user_classes: Optional[List[Type[User]]] = []):
    from locust.env import Environment
    from locust.log import setup_logging

    setup_logging("INFO", None)

    pid = getpid()
    env = Environment(host=neo4j_uri, parsed_options=args,
                      user_classes=user_classes)

    host, port = args.master_host, args.master_port
    print(f"worker({pid}): connecting to parent @ {host}:{port}")

    runner = env.create_worker_runner(host, port)
    print(f"worker({pid}): created runner")

    runner.greenlet.join()
    print(f"worker({pid}): STOPPING!!!!!!!")


if __name__ == "__main__":
    import multiprocessing as mp

    from locust.argument_parser import LocustArgumentParser, setup_parser_arguments
    from users import RandomReader

    mp.set_start_method("spawn")

    parser = LocustArgumentParser()
    #parser = argparse.ArgumentParser(
    #    prog="neo4j-locust",
    #    description="Swarm a Neo4j deployment using Locust"
    #)
    parser.prog = "neo4j-locust"
    parser.description = "Swarm Neo4j!"
    neo4j_group = parser.add_argument_group("Neo4j options")
    neo4j_group.add_argument("--neo4j-uri", default="neo4j://localhost:7687")
    neo4j_group.add_argument("--neo4j-user", default="neo4j")
    neo4j_group.add_argument("--neo4j-pass", default="password")
    neo4j_group.add_argument("--debug", action="store_true")
    setup_parser_arguments(parser)
    args = parser.parse_args()

    if args.debug:
        setup_logging("DEBUG", None)
    else:
        setup_logging("INFO", None)

    env = Environment(user_classes=[RandomReader],
                      host=args.neo4j_uri,
                      parsed_options=args)
    parent = env.create_master_runner()

    num_workers = 1 or cpu_count() or 1
    workers = [
        mp.Process(target=worker, args=(args.neo4j_uri, args, [RandomReader]))
        for _ in range(num_workers)
    ]
    print(f"Starting {len(workers)} workers")
    for w in workers:
        w.start()

    import time
    time.sleep(2)
    parent.start(1, spawn_rate=1)

    for w in workers:
        w.join()
