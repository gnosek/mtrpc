import functools
import logging
import threading
import signal
import sys

from mtrpc.common import utils
from mtrpc.common.const import DEFAULT_LOG_HANDLER_SETTINGS
from mtrpc.server import methodtree


class MTRPCServerInterface(object):
    """

    Instantiation
    ^^^^^^^^^^^^^

    MTRPCServerInterface is a singleton type, i.e. it can have at most one
    instance -- and it should not be instantiated directly but with one of
    three alternative constructors (being class methods):

    * get_instance() -- get (create if it does not exist) the
      MTRPCServerInterface instance; it doesn't do anything else, so
      after creating the instance your script is supposed to call
      configure_logging(), start()...

    * configure() -- get the instance, read config file (see: above config
      file structure/content description), set up logging, OS-related stuff
      (signal handlers, optional daemonization and some other things...)
      and loads RPC-module/method definitions building the RPC-tree;
      the only thing left to do by your script to run the server is to
      call the start() method.

    * configure_and_start() -- do the same what configure() does *plus* start
      the server

    See documentation of these methods for detailed info about arguments.

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

    CONFIG_SECTION_TYPES = dict(
        rpc_tree_init=dict,
        logging_settings=dict,
    )
    # allowed sections of a config file and their default content
    CONFIG_SECTION_FIELDS = dict(
        rpc_tree_init=dict(
            paths=[],
            imports=['mtrpc.server.sysmethods as system'],
            postinit_kwargs=dict(
                logging_settings=dict(
                    mod_logger_pattern='mtrpc.server.rpc_log.{full_name}',
                    level='warning',
                    handlers=[DEFAULT_LOG_HANDLER_SETTINGS],
                    propagate=False,
                    custom_mod_loggers=dict(
                        # maps RPC-module full names to logging settings dicts
                        # with 'mod_logger' key pointing at a logger name;
                        # omitted items will be substituted with general ones
                    ),
                ),
                mod_globals=dict(
                    # maps RPC-module full names to dicts of attributes
                ),
            ),
        ),
        logging_settings=dict(
            server_logger='mtrpc.server',
            level='info',
            handlers=[DEFAULT_LOG_HANDLER_SETTINGS],
            propagate=False
        ),
    )
    RPC_MODE = None

    _instance = None
    _server_iface_rlock = threading.RLock()

    def __init__(self):

        """Attention: MTRPCServerInterface is a singleton class.

        Use one of the alternavice constructor methods: get_instance(),
        configure() or configure_and_start() -- rather than instantiate
        the class directly.

        """

        with self.__class__._server_iface_rlock:
            if self.__class__._instance is not None:
                raise TypeError("{0} is a singleton class and its instance "
                                "has been already created; use "
                                "get_instance() class method to obtain it"
                                .format(self.__class__.__name__))

            self.__class__._instance = self

        self.config = None
        self._signal_handlers = {}

        # to be used in configure_and_start() and restart_on()
        self._restart = False

        # the actual logger to be configured and set in configure_logging()
        logging.basicConfig(format="%(asctime)s %(levelno)s "
                                   "@%(threadName)s: %(message)s")
        self.log = logging.getLogger()
        self._log_handlers = []

        # the RPC-tree -- to be set in load_rpc_tree()
        self.rpc_tree = None

    @classmethod
    def get_instance(cls):
        """Get (the only) class instance; create it if it does not exist yet"""
        with cls._server_iface_rlock:
            if cls._instance is None:
                return cls()
            else:
                return cls._instance

    @classmethod
    def configure(cls, config_dict=None,
                  default_postinit_callable=utils.basic_postinit):

        """Get the instance, load config + configure (don't start) the server.

        Obligatory argument:  config_dict -- parsed config

        Optional arguments:

        * default_postinit_callable (callable object) -- to be passed to
          methodtree.RPCTree.build_new(); default: common.utils.basic_postinit.

        * rpc_mode -- either 'server' or 'cli'; not used directly in mtrpc but
          modules may wish to differentiate, e.g. not spawn extra threads

        """

        self = cls.get_instance()
        self.config = self.validate_and_complete_config(config_dict)
        self.configure_logging()
        self.do_os_settings()
        self.rpc_tree = self.load_rpc_tree(default_postinit_callable=default_postinit_callable, rpc_mode=self.RPC_MODE)
        return self

    @classmethod
    def configure_and_start(cls, config_dict=None,
                            default_postinit_callable=utils.basic_postinit,
                            final_callback=None):

        """The same what configure() does, then run the server.

        Obligatory argument: config_path -- path of a config file.

        Optional arguments:

        * default_postinit_callable
        -- see: configure();

        * final_callback (callable object) -- to be called from the
          manager thread before it terminates.

        """

        self = cls.configure(config_dict, default_postinit_callable)
        try:
            self.start(final_callback=final_callback)
        except Exception:
            self.log.critical('Error during server start. '
                              'Raising exception...', exc_info=True)
            raise

        return self

    # it is useful as final_callback in loop mode
    @classmethod
    def restart_on(cls):
        cls._instance._restart = True

    @classmethod
    def validate_and_complete_config(cls, config):

        """Check and supplement a given config dict.

        Check item types (specified in CONFIG_SECTION_TYPES); check presence
        of obligatory items (specified in OBLIGATORY_CONFIG_SECTIONS)
        and complete the rest with default content (defined in
        CONFIG_SECTION_FIELDS and CONFIG_SECTION_TYPES).

        Adjust 'bindings' item -- transforming it from a list of lists into
        a list of threads.BindingProps (namedtuple) instances.

        Return the same -- but modified -- config dict.

        """

        # verify section content types
        for section, sect_content in config.iteritems():
            sect_cls = cls.CONFIG_SECTION_TYPES.get(section)
            if sect_cls is None:
                continue
            if not isinstance(sect_content, sect_cls):
                raise TypeError('{0} section should be a {1.__name__}'
                                .format(section, cls.CONFIG_SECTION_TYPES[section]))

        # complement omited non-obligatory sections
        for section, section_type in cls.CONFIG_SECTION_TYPES.iteritems():
            config.setdefault(section, section_type())

        # verify section fields and complement them with default values
        for section, sect_content in cls.CONFIG_SECTION_FIELDS.iteritems():
            if sect_content is not None:
                # verify (check for illegal fields)
                used_fields = set(config[section])
                if not used_fields.issubset(sect_content):
                    bad = sorted(used_fields.difference(sect_content))
                    raise ValueError('Illegal fields in {0} section: {1}'
                                     .format(section, ', '.join(bad)))
                # complement omitted fields
                content = sect_content.copy()
                content.update(config[section])
                config[section] = content

        # verify RPC-tree-init-related settings
        for field, value in config['rpc_tree_init'].iteritems():
            if not (field in ('paths', 'imports') and isinstance(value, list)
                    or field == 'postinit_kwargs' and isinstance(value, dict)):
                raise ValueError("Illegal item in rpc_tree_init section:"
                                 " {0!r}: {1!r}".format(field, value))
        return config

    #
    # Environment-related preparations

    def configure_logging(self, log_config=None):
        """Configure server logger and its handlers"""

        prev_log = self.log
        if log_config is None:
            log_config = self.config['logging_settings']

        # get the logger
        self.log = logging.getLogger(log_config.get('server_logger', ''))

        # configure it
        utils.configure_logging(self.log, prev_log, self._log_handlers,
                                log_config)
        return self.log

    def setup_signal_handlers(self, signal_actions, sig_stopping_timeout):
        # unregister old signal handlers (when restarting)
        while self._signal_handlers:
            signal_num, handler = self._signal_handlers.popitem()
            signal.signal(signal_num, signal.SIG_DFL)

        # register signal handlers
        if signal_actions is not None:
            for signal_name, action_name in signal_actions.iteritems():
                signal_num = getattr(signal, signal_name)
                handler_func = getattr(self, '_{0}_handler'.format(action_name))
                handler = functools.partial(handler_func,
                                            stopping_timeout
                                            =sig_stopping_timeout)
                signal.signal(signal_num, handler)
                self._signal_handlers[signal_num] = handler

    def do_os_settings(self):
        """Set umask and working dir; daemonize or not; set OS signal handlers"""

        signal_actions = dict(SIGTERM='exit', SIGHUP='restart')
        sig_stopping_timeout = 45
        self.setup_signal_handlers(signal_actions, sig_stopping_timeout)

    #
    # OS signal handlers:
    def _restart_handler(self, signal_num, stack_frame, stopping_timeout):
        """"restart" action"""
        try:
            self.log.info('Signal #%s received by the process -- '
                          '"restart" action starts...', signal_num)
            if self.stop(reason='restart', timeout=stopping_timeout):
                self.restart_on()
        except Exception:
            self.log.critical('Error while restarting. Raising exception...',
                              exc_info=True)
            raise

    _reload_handler = _restart_handler

    def _exit_handler(self, signal_num, stack_frame, stopping_timeout):
        """"exit" action"""
        try:
            self.log.info('Signal #%s received by the process -- '
                          '"exit" action starts...', signal_num)
            if self.stop(reason='exit', timeout=stopping_timeout):
                sys.exit()
        except SystemExit:
            raise
        except Exception:
            self.log.critical('Error while exiting. Raising exception...',
                              exc_info=True)
            raise

    def _force_exit_handler(self, signal_num, stack_frame, stopping_timeout):
        """"force_exit" action"""
        try:
            self.log.info('Signal #%s received by the process -- '
                          '"force_exit" action starts...', signal_num)
            if self.stop(reason='force-exit', force=True,
                         timeout=stopping_timeout):
                sys.exit()
        except SystemExit:
            raise
        except Exception:
            self.log.critical('Error while force-exiting. Raising '
                              'exception...', exc_info=True)
            raise

    #
    # RPC-methods tree loading

    def load_rpc_tree(self, default_postinit_callable=utils.basic_postinit, rpc_mode='server'):
        """Load RPC-methods from modules specified by names or filesystem paths"""

        rpc_tree_init_conf = self.config.get('rpc_tree_init', {})

        paths = rpc_tree_init_conf.get('paths', [])
        imports = rpc_tree_init_conf.get('imports', [])
        postinit_kwargs = rpc_tree_init_conf.get('postinit_kwargs', {})

        return methodtree.RPCTree.load(imports, paths, default_postinit_callable, postinit_kwargs, rpc_mode)

    #
    # The actual server management

    def start(self, final_callback=None):

        raise NotImplementedError()

    def stop(self, reason='manual stop', loglevel='info', force=False, timeout=30):

        raise NotImplementedError()
