#!/usr/bin/env python3
from os import cpu_count, environ, getpid
import multiprocessing as mp

import gevent

from locust.env import Environment
from locust.log import setup_logging
from locust.runners import MasterRunner, WorkerRunner
from locust.stats import stats_printer


setup_logging("INFO", None)


def worker(host: str, port: int, neo4j_uri: str):
    from locust.env import Environment
    from locust.log import setup_logging
    from locust.runners import MasterRunner, WorkerRunner
    from users import RandomReader

    setup_logging("INFO", None)

    """TBD"""
    pid = getpid()
    env = Environment(host=neo4j_uri, user_classes=[RandomReader])
    print(f"worker({pid}): connecting to parent @ {host}:{port}")

    runner = env.create_worker_runner(host, port)
    print(f"worker({pid}): created runner")

    if runner.environment.host != env.host:
        print(f"UHHH host not set?!")
        runner.greenlet.kill()

    runner.greenlet.join()
    print(f"worker({pid}): STOPPING!!!!!!!")


if __name__ == "__main__":
    from users import RandomReader

    neo4j_host = environ.get("NEO4J_HOST", "localhost")
    neo4j_port = environ.get("NEO4J_PORT", "7687")
    neo4j_tls = environ.get("NEO4J_TLS", "")
    tls_prefix = "" if not neo4j_tls else f"+{neo4j_tls}"
    neo4j_uri = f"neo4j{tls_prefix}://{neo4j_host}:{neo4j_port}"

    host = environ.get("HOST", "127.0.0.1")
    port = int(environ.get("PORT", "5557"))

    env = Environment(user_classes=[RandomReader])
    parent = env.create_master_runner(host, port)

    mp.set_start_method("spawn")
    num_workers = cpu_count() or 1
    workers = [
        mp.Process(target=worker, args=(host, port, neo4j_uri))
        for _ in range(num_workers)
    ]
    print(f"Starting {len(workers)} workers")
    for w in workers:
        w.start()

    import time
    time.sleep(5)
    parent.start(10, spawn_rate=5)

    for w in workers:
        w.join()
