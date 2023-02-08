#!/usr/bin/env python3
import argparse
import logging
import sys
from os import cpu_count, environ, getpid

import gevent
from locust import User
from locust.env import Environment
from locust.log import setup_logging
from locust.stats import stats_printer
from locust.util.timespan import parse_timespan

from users import RandomReader, RandomWriter, RandomReaderWriter

from typing import cast, List, Optional, Tuple, Type


def worker(neo4j_uri: str, args: argparse.Namespace,
           user_classes: Optional[List[Type[User]]] = []):
    """
    Worker code. Needs to reimport in case being spawned in new process.
    """
    import logging
    import gevent
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
    sys.exit(0)


def stop_test(runner) -> None:
    logging.info("stopping test")
    try:
        runner.quit()
    except KeyboardInterrupt:
        pass


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

    # Check if we have a set runtime. Needs parsing.
    if args.run_time:
        try:
            args.run_time = parse_timespan(args.run_time)
        except ValueError:
            logging.error("invalid run time")
            sys.exit(1)

    # Create our "master runner"
    env = Environment(user_classes=[RandomReaderWriter],
                      host=args.neo4j_uri,
                      parsed_options=args)
    runner = env.create_master_runner()

    # Spin up enough workers to saturate the cpus or whatever is requested
    num_workers = cast(int, args.workers or cpu_count())
    workers = [
        mp.Process(target=worker,
                   args=(args.neo4j_uri, args, env.user_classes))
        for _ in range(num_workers)
    ]
    print(f"Starting {len(workers)} workers")
    for w in workers:
        w.daemon = True
        w.start()

    import time
    time.sleep(2)

    # spin up some stats printing
    gevent.spawn(stats_printer(env.stats))

    # kick off the test...this doesn't return until spawn is complete.
    try:
        runner.start(args.num_users or 1, spawn_rate=args.spawn_rate or 0.1)
    except KeyboardInterrupt:
        logging.info("aborting test")

    # now that we're ramped up, schedule termination
    if args.run_time:
        logging.info(f"stopping test in {args.run_time} seconds")
        gevent.spawn_later(args.run_time, stop_test, runner)

    # wait for workers to finish up
    try:
        runner.greenlet.join()
        logging.info(f"waiting on {len(workers)} to finish up")
        for w in workers:
            w.join(15)
            if w.exitcode is None:
                logging.info(f"killing worker {w}")
                w.kill()
    except KeyboardInterrupt:
        logging.info("aborting test")
        for w in workers:
            if w.is_alive():
                w.kill()
