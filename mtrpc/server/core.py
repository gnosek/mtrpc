import logging

from mtrpc.common import utils
from mtrpc.common.const import DEFAULT_LOG_HANDLER_SETTINGS
from mtrpc.server import schema


class MTRPCServerInterface(object):
    """

    Public instance methods
    ^^^^^^^^^^^^^^^^^^^^^^^

    * configure_logging() -- set up the server logger;
    * load_rpc_tree() -- load RPC-module/methods;
    * start() -- create and start service threads (manager and responder),
    * stop() -- stop these service threads.

    By default, most of these methods base on the instance attributes
    (see below: "Public instance attributes"), but ignore them if gets
    adequate objects as arguments.

    See documentation and signatures of that methods for more details.

    Public static/class methods
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * restart_on() -- set the instance [sic] attribute `_restart' to True;
      in loop-mode (see: configure_and_start()) it causes break of waiting
      for KeyboardInterrupt/OS signals and is especially useful as
      manager's final callback (=> auto-restarting after a fatal failure).

    * validate_and_complete_config(),
    * make_config_stub(),
    * write_config_skeleton()
    -- config-manipulation-related static/class methods, accessible also
    as module-level functions.

    See documentation and signatures of that methods for more details.

    Public instance attributes
    ^^^^^^^^^^^^^^^^^^^^^^^^^^

    * manager (mtrpc.server.thread.RPCManager instance, set at server start,
      initially set to None) -- the service thread responsible for starting
      and stopping other threads as well as for receiving RPC requests;
      see also: mtrpc.server.thread.RPCManager documentation;

    * responder (mtrpc.server.thread.RPCResponder instance, set at server
      start, initially set to None) -- a service thread responsible for
      sending RPC-responses after getting them from task threads
      (mtrpc.server.thread.RPCTaskThread instances);
      see also: mtrpc.server.thread.RPCResponder documentation;

    * config -- a dict with content read from config file (see: the next
      section: "Server configuration file..."; initially set to None),
      complemented with default values and slightly adjusted (e.g. some
      sub-dicts transformed into named tuples);

    * daemonized: True or False (set when OS settings are made, initially set
      to False; definitely should not be set manually) -- informs whether
      the server process has been daemonized;

    * log (logging.Logger instance) -- the main server logger object (initially
      set to the root logger, with logging.getLogger(), and configured with
      logging.basicConfig; it may change after config is read, if other
      settings are specified in config);

    * rpc_tree (mtrpc.server.methodtree.RPCTree instance) -- populated with
      RPC-modules and RPC-methods defined in modules whose names or paths are
      specified in config + their submodules (set on tree load -- after config
      load; initially set to None);

    * task_dict (a dict),
    * result_fifo (Queue.Queue instance),
    * mutex (threading.Lock instance)
    -- objects (set on init) that are passed both into the manager and
    responder constructors when server is started.

    """

    CONFIG_DEFAULTS = {
        'logging_settings': {
            'server_logger': 'mtrpc.server.rpc_log',
            'level': 'info',
            'handlers': [DEFAULT_LOG_HANDLER_SETTINGS],
            'propagate': False,
        }
    }

    CONFIG_SCHEMAS = [schema.by_example(CONFIG_DEFAULTS)]

    RPC_MODE = None
    SIGNAL_STOP_TIMEOUT = 45

    def __init__(self, config_dict):
        self.config = config_dict
        self._log_handlers = []
        self.log = None
        self.log = self.configure_logging()

    def configure_logging(self, log_config=None):
        """Configure server logger and its handlers"""

        prev_log = self.log
        if log_config is None:
            log_config = self.config['logging_settings']

        # get the logger
        logger = logging.getLogger(log_config.get('server_logger', ''))

        # configure it
        utils.configure_logging(logger, prev_log, self._log_handlers, log_config)
        return logger

    #
    # The actual server management

    def start(self, rpc_tree, final_callback=None):

        raise NotImplementedError()
