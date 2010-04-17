#!/usr/bin/env python

import sys
import threading
import time

from mtrpc.server import MTRPCServerInterface


CONFIG_PATH = 'server_example_conf.json'   # (<- look at that file)

cmdline_args = set(sys.argv[1:])
# loop mode/non-loop mode * daemon/non-daemon mode == four possibilities :-)
loop_mode = ('-l' in cmdline_args) or ('--loop-mode' in cmdline_args)
force_daemon = ('-d' in cmdline_args) or ('--daemon' in cmdline_args)


if loop_mode:
    final_callback = MTRPCServerInterface.restart_on
    # (^ to restart the server when the service threads are stopped)
    MTRPCServerInterface.configure_and_start(
            CONFIG_PATH,
            force_daemon=force_daemon,
            loop_mode=True,       # <- stay in the inner server loop
            final_callback=final_callback,
    )
else:
    restart_lock = threading.Lock()
    final_callback = restart_lock.release
    # (^ to restart the server when the service threads are stopped)
    try:
        # no inner server loop needed, we have the outer one here
        while True:
            if restart_lock.acquire(False):   # (<- non-blocking)
                server = MTRPCServerInterface.configure_and_start(
                        CONFIG_PATH,
                        force_daemon=force_daemon,
                        loop_mode=False,  # <- return immediately
                        final_callback=final_callback,
                )
            time.sleep(0.5)
    except KeyboardInterrupt:
        server.stop()
