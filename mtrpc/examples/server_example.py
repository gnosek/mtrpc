#!/usr/bin/env python
import json

import sys
import threading
import time

from mtrpc.server.amqp import AmqpServer


CONFIG_PATH = 'server_example_conf.json'


cmdline_args = set(sys.argv[1:])

force_daemon = ('-d' in cmdline_args) or ('--daemon' in cmdline_args)


restart_lock = threading.Lock()
final_callback = restart_lock.release
# (^ to restart the server when the service threads are stopped)

server = None

try:
    # no inner server loop needed, we have the outer one here
    while True:
        if restart_lock.acquire(False):   # (<- non-blocking)
            server = AmqpServer.configure_and_start(
                    config_dict=json.load(open(CONFIG_PATH)),
                    force_daemon=force_daemon,
                    final_callback=final_callback,
            )
        time.sleep(0.5)
except KeyboardInterrupt:
    if server is not None:
        server.stop()
