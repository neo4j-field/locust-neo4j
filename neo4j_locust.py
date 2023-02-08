#!/usr/bin/env python3
import argparse
import logging
from os import cpu_count, environ, getpid

from locust import User
from locust.env import Environment
from locust.log import setup_logging

from typing import cast, List, Optional, Tuple, Type


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

    try:
        runner.greenlet.join()
    except Exception as e:
        print(f"worker({pid}): caught {e}")


if __name__ == "__main__":
    import multiprocessing as mp
    import gevent

    from locust.argument_parser import LocustArgumentParser, setup_parser_arguments
    from users import RandomReader

    # Required on Linux
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
    neo4j_group.add_argument("--workers", default=cpu_count(), type=int)
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
    runner = env.create_master_runner()

    num_workers = cast(int, args.workers or cpu_count())
    workers = [
        mp.Process(target=worker, args=(args.neo4j_uri, args, [RandomReader]))
        for _ in range(num_workers)
    ]
    print(f"Starting {len(workers)} workers")
    for w in workers:
        w.start()

    import time
    time.sleep(2)
    try:
        runner.start(args.num_users or 1, spawn_rate=args.spawn_rate or 0.1)
    except Exception as e:
        logging.info(f"exception caught: {e}")

    gevent.spawn_later(90 * 60, lambda: runner.quit())

    for w in workers:
        w.join()
