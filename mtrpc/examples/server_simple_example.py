#!/usr/bin/env python

import signal

from mtrpc.server.amqp import AmqpServer
from mtrpc.server.server_config import ServerConfig


# configure and start the server -- then wait for KeyboardInterrupt
# exception or OS signals specified in the config file...

server = None

try:
    # no inner server loop needed, we have the outer one here
    while True:
        server = ServerConfig(['server_simple_example_conf.json'], AmqpServer)
        server.run()
        signal.pause()
except KeyboardInterrupt:
    if server is not None:
        server.stop()

