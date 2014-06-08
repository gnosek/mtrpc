#!/usr/bin/env python

from mtrpc.server.amqp import AmqpServer
import signal

# configure and start the server -- then wait for KeyboardInterrupt
# exception or OS signals specified in the config file...

server = None

try:
    # no inner server loop needed, we have the outer one here
    while True:
        server = AmqpServer.configure_and_start(
                config_path='server_simple_example_conf.json',
                final_callback=AmqpServer.restart_on,
        )
        signal.pause()
except KeyboardInterrupt:
    if server is not None:
        server.stop()

