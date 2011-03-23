#!/usr/local/python2.6/bin/python2.6

import sys
import threading
import time
import signal
import os
from optparse import OptionParser

sys.path.insert(0, '/usr/local/megiteam/python2.6')

from mtrpc.server import MTRPCServerInterface
from mtrpc.server.config import loader

dir, name = os.path.split(sys.argv[0])

if name.endswith("_agent"):
    name = name[:-6] # strip _agent

CONFIG_DIR = '/etc/megiteam/mtrpc'
CONFIG_PATH = os.path.join(CONFIG_DIR, name + ".json")

parser = OptionParser(usage='%prog [options]')
parser.add_option('-d', '--daemon', dest='daemon', action='store_true', default=False, help='Daemonize')
parser.add_option('-c', '--config', dest='config', default=CONFIG_PATH, help='Path to config file', metavar='FILE')

(o, a) = parser.parse_args(sys.argv[1:])

force_daemon = o.daemon

restart_lock = threading.Lock()
final_callback = restart_lock.release
# (^ to restart the server when the service threads are stopped)
try:
    # no inner server loop needed, we have the outer one here
    while True:
        if restart_lock.acquire(False):   # (<- non-blocking)
            config_dict = loader.load_props(open(o.config))
            server = MTRPCServerInterface.configure_and_start(
                    config_dict=config_dict,
                    force_daemon=force_daemon,
                    loop_mode=False,  # <- return immediately
                    final_callback=final_callback,
            )
        signal.pause()
except KeyboardInterrupt:
    server.stop()
