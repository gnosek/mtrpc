#!/usr/local/python2.6/bin/python2.6

import sys
import threading
import signal
import os
from optparse import OptionParser
from mtrpc.server import daemonize
from mtrpc.server.amqp import AmqpServer

sys.path.insert(0, '/usr/local/megiteam/python2.6')

from mtrpc.server.config import loader

directory, name = os.path.split(sys.argv[0])

if name.endswith("_agent"):
    name = name[:-6]  # strip _agent

CONFIG_DIR = '/etc/megiteam/mtrpc'
CONFIG_PATH = os.path.join(CONFIG_DIR, name + ".json")

parser = OptionParser(usage='%prog [options]')
parser.add_option('-d', '--daemon', dest='daemon', action='store_true', default=False, help='Daemonize')
parser.add_option('-c', '--config', dest='config', default=CONFIG_PATH, help='Path to config file', metavar='FILE')

(o, a) = parser.parse_args(sys.argv[1:])

server = None
restart_lock = threading.Lock()
final_callback = restart_lock.release
# (^ to restart the server when the service threads are stopped)

if o.daemon:
    daemonize.daemonize()

try:
    # no inner server loop needed, we have the outer one here
    while True:
        if restart_lock.acquire(False):   # (<- non-blocking)
            config_dict = loader.load_props(open(o.config))
            server = AmqpServer.configure_and_start(
                config_dict=config_dict,
                final_callback=final_callback,
            )
        signal.pause()
except KeyboardInterrupt:
    if server is not None:
        server.stop()
