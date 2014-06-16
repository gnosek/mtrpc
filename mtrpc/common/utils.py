# mtrpc/common/utils.py
#
# Author: Jan Kaliszewski (zuo)
# Copyright (c) 2010, MegiTeam

"""MTRPC common utility classes and functions"""



import logging
import logging.handlers
import socket
import sys

from .const import *



#
# Standard module post-init callable
#

def basic_postinit(mod, full_name, logging_settings, mod_globals):

    """Initialize the module logger and add custom globals

    Arguments:

    * mod -- the Python module object;

    * full_name -- an absolute dot.separated.name;

    * logging_settings -- a dict with all or some of the keys:
      'mod_logger_pattern', 'level', 'handlers', 'propagate',
      'custom_mod_loggers' (see: the fragment of mtrpc.server
      documentation about configuration file structure and content);

    * mod_globals -- a dict of variables to be set as module globals.

    """

    # configure the module logger
    log_config = logging_settings.copy()
    log_config.update(log_config.get('custom_mod_loggers', {})
                      .get(full_name, {}))
    log_name = log_config.get('mod_logger')
    if log_name is None:
        log_name = (log_config.get('mod_logger_pattern', '')
                                   .format(full_name=full_name))
    new_log = logging.getLogger(log_name)
    prev_log = getattr(mod, RPC_LOG, None)
    if prev_log is None:
        log_handlers = []
        setattr(mod, RPC_LOG_HANDLERS, log_handlers)
    else:
        log_handlers = getattr(mod, RPC_LOG_HANDLERS)
    configure_logging(new_log, prev_log, log_handlers, log_config)
    setattr(mod, RPC_LOG, new_log)

    # set custom module globals
    mod.__dict__.update(mod_globals.get(full_name, {}))



#
# Other functions
#

def configure_logging(log, prev_log, log_handlers, log_config):

    """Configure logging for a particular logger, using given settings.

    Arguments:

    * log (logging.Logger instance) -- a new logger (to configure);

    * prev_log (logging.Logger instance) -- the previous logger;

    * log_handlers -- auxiliary list of logger handlers being in use;

    * log_config -- a dict with all or some of the keys: 'level' (str),
      'handlers' (dict), 'propagate' (bool); see: the fragment of
      mtrpc.server documentation about configuration file structure
      and content.

    """

    while log_handlers:  # when restarting -- disable old handlers
        prev_log.removeHandler(log_handlers.pop())

    # set some attributes of the new logger
    log.propagate = log_config.get('propagate', False)
    level = log_config.get('level', 'info').upper()
    log.setLevel(getattr(logging, level))

    # configure logger handlers
    default_hprops = DEFAULT_LOG_HANDLER_SETTINGS
    for handler_props in log_config['handlers']:
        class_name = handler_props['cls']
        if '.' in class_name:
            package, class_name = class_name.rsplit('.', 1)
            __import__(package)
            handler_package = sys.modules[package]
            HandlerClass = getattr(handler_package, class_name)
        else:
            try:
                HandlerClass = getattr(logging, class_name)
            except AttributeError:
                HandlerClass = getattr(logging.handlers, class_name)

        kwargs = handler_props.get('kwargs', default_hprops['kwargs'])
        level = handler_props.get('level', default_hprops['level']).upper()
        format = handler_props.get('format', default_hprops['format'])

        handler = HandlerClass(**kwargs)
        handler.setLevel(getattr(logging, level))
        handler.setFormatter(logging.Formatter(format))

        log_handlers.append(handler)
        log.addHandler(handler)

    log.debug('Logger %s configured', log.name)


def setkeepalives(sck, enabled=True, keepcnt=5, keepintvl=120, keepidle=300):
    if enabled:
        sck.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sck.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, keepcnt)
        sck.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, keepintvl)
        sck.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, keepidle)
    else:
        sck.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 0)

