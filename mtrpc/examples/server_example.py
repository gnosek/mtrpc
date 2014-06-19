#!/usr/bin/env python

import threading
import time

from mtrpc.server.amqp import AmqpServer
from mtrpc.server.server_config import ServerConfig


CONFIG_PATH = 'server_example_conf.json'


restart_lock = threading.Lock()
final_callback = restart_lock.release
# (^ to restart the server when the service threads are stopped)

server = None

try:
    # no inner server loop needed, we have the outer one here
    while True:
        if restart_lock.acquire(False):   # (<- non-blocking)
            server = ServerConfig([CONFIG_PATH], AmqpServer)
            server.run(final_callback=final_callback)
        time.sleep(0.5)
except KeyboardInterrupt:
    if server is not None:
        server.stop()
