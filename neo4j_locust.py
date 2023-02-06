#!/usr/bin/env python3
from os import cpu_count

import gevent

from locust.env import Environment
from locust.log import setup_logging
from locust.stats import stats_printer


setup_logging("INFO", None)


if __name__ == "__main__":
    from users import RandomReader

    env = Environment(user_classes=[RandomReader])
    runner = env.create_local_runner()
    web_ui = env.create_web_ui("127.0.0.1", 8089)

    env.events.init.fire(environment=env, runner=runner, web_ui=web_ui)

    gevent.spawn(stats_printer(env.stats))

    runner.start(cpu_count(), spawn_rate=10)

    gevent.spawn_later(60, lambda: runner.quit())

    runner.greenlet.join()
    web_ui.stop()
