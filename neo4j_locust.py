#!/usr/bin/env python3
import argparse
import logging
from os import cpu_count, environ, getpid

import gevent
from locust import User
from locust.env import Environment
from locust.log import setup_logging
from locust.stats import stats_printer

from users import RandomReader

from typing import cast, List, Optional, Tuple, Type


def worker(neo4j_uri: str, args: argparse.Namespace,
           user_classes: Optional[List[Type[User]]] = []):
    """
    Worker code. Needs to reimport in case being spawned in new process.
    """
    import logging
    from locust.env import Environment
    from locust.log import setup_logging

    setup_logging("INFO", None)

    pid = getpid()
    env = Environment(host=neo4j_uri, parsed_options=args,
                      user_classes=user_classes)

    host, port = args.master_host, args.master_port
    logging.info(f"worker({pid}) connecting to parent @ {host}:{port}")

    runner = env.create_worker_runner(host, port)
    logging.info(f"worker({pid}) created runner")

    try:
        runner.greenlet.join()
    except KeyboardInterrupt:
        logging.info(f"worker({pid}) stopping")
        runner.greenlet.killall()


if __name__ == "__main__":
    import multiprocessing as mp
    from locust.argument_parser import (
        LocustArgumentParser, setup_parser_arguments
    )

    # Required at least on Linux to get Workers in a fresh context
    mp.set_start_method("spawn")

    # Locust assumes particular runtime parameters, so use their arg parser
    parser = LocustArgumentParser()
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

    # Debug logging is super noisy, so gate it behind a flag.
    if args.debug:
        setup_logging("DEBUG", None)
    else:
        setup_logging("INFO", None)

    # Create our "master runner"
    env = Environment(user_classes=[RandomReader],
                      host=args.neo4j_uri,
                      parsed_options=args)
    runner = env.create_master_runner()

    # Spin up enough workers to saturate the cpus or whatever is requested
    num_workers = cast(int, args.workers or cpu_count())
    workers = [
        mp.Process(target=worker, args=(args.neo4j_uri, args, [RandomReader]))
        for _ in range(num_workers)
    ]
    print(f"Starting {len(workers)} workers")
    for w in workers:
        w.daemon = True
        w.start()

    import time
    time.sleep(2)

    # greenlets for orchestration
    gevent.spawn(stats_printer(env.stats))
    gevent.spawn_later(90 * 60, lambda: runner.quit())

    # kick off the test...this doesn't return until spawn is complete.
    try:
        runner.start(args.num_users or 1, spawn_rate=args.spawn_rate or 0.1)
    except KeyboardInterrupt:
        logging.info("aborting test")

    # wait for workers to finish up
    try:
        for w in workers:
            w.join()
    except KeyboardInterrupt:
        logging.info("aborting test")
        for w in workers:
            if w.is_alive():
                w.kill()
