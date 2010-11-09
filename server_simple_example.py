#!/usr/bin/env python

from mtrpc.server import MTRPCServerInterface

# configure and start the server -- then wait for KeyboardInterrupt
# exception or OS signals specified in the config file...

MTRPCServerInterface.configure_and_start(
        config_path='server_simple_example_conf.json',
        loop_mode=True,    # <- stay there and wait for OS signals
        final_callback=MTRPCServerInterface.restart_on,
)
