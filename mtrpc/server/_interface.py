"""MTRPC server convenience interface"""

# Author: Jan Kaliszewski (zuo)
# Copyright (c) 2010, MegiTeam


from future_builtins import filter, map, zip

import functools
import imp
import json
import logging
import logging.handlers
import os
import os.path
import Queue
import signal
import sys
import threading
import time
import traceback
import types
import warnings

from . import threads
from . import methodtree
from . import _daemon_recipe
from ..common import utils
from ..common.const import DEFAULT_JSON_ENCODING, DEFAULT_LOG_HANDLER_SETTINGS



__all__ = [
        'OBLIGATORY_CONFIG_SECTIONS',
        'CONFIG_SECTION_TYPES',
        'CONFIG_SECTION_FIELDS',
        'MTRPCServerInterface',
        'write_config_skeleton',
]



#
# Config-file-related constants
#

OBLIGATORY_CONFIG_SECTIONS = 'amqp_params', 'bindings'
CONFIG_SECTION_TYPES = dict(
        rpc_tree_init=dict,
        amqp_params=dict,
        exchange_types=dict,
        bindings=list,
        manager_settings=dict,
        manager_attributes=dict,
        responder_attributes=dict,
        logging_settings=dict,
        os_settings=dict,
)
# allowed sections of a config file and their default content
CONFIG_SECTION_FIELDS = dict(
        rpc_tree_init = dict(
            paths=[],
            imports=['mtrpc.server.sysmethods as system'],
            mod_init_kwargs=dict(
                logging_settings=dict(
                    mod_logger_pattern='mtrpc.server.rpc_log.{full_name}',
                    level='warning',
                    handlers=[DEFAULT_LOG_HANDLER_SETTINGS],
                    propagate=False,
                    custom_mod_loggers=dict(
                        # maps RPC-module full names to logging settings dicts
                        # with 'mod_logger' key pointing to a logger name;
                        # omitted items will be substituted with general ones
                    ),
                ),
                mod_globals=dict(
                    # maps RPC-module full names to dicts of attributes
                ),
            ),
        ),
        amqp_params = None,           # to be a dict with some keys...
        exchange_types = None,        # to be a dict: {exchange, its type}
        bindings = None,              # to be a list of binding props
        manager_settings = None,      # to be a dict with some keys...
        manager_attributes = None,    # to be a dict with some keys...
        responder_attributes = None,  # to be a dict with some keys...
        logging_settings = dict(
            server_logger='mtrpc.server',
            level='info',
            handlers=[DEFAULT_LOG_HANDLER_SETTINGS],
            propagate=False
        ),
        os_settings = dict(
            umask=None,
            working_dir=None,
            daemon=False,
            signal_actions=dict(
                SIGTERM='exit',
                SIGHUP='restart',
            ),
            stopping_timeout = 60,
        ),
)



#
# Server convenience interface class
#

class MTRPCServerInterface(object):

    _instance = None
    _server_iface_rlock = threading.RLock()
    
    
    def __init__(self):
        '''Attention: MTRPCServerInterface is a singleton class.

        Use get_instance() or configure() or configure_and_start() constructor
        method rather than instantiate the class directly.
        '''
        
        with self.__class__._server_iface_rlock:
            if self.__class__._instance is not None:
                raise TypeError("{0} is a singleton class and its instance "
                                "has been already created; use "
                                "get_instance() class method to obtain it"
                                .format(self.__class__.__name__))
                                
            self.__class__._instance = self
        
        self.manager = None
        self.responder = None
        self.config = None

        # to be set in do_os_settings()
        self.daemonized = False
        self._signal_handlers = {}

        # to be used in configure_and_start() and restart_on()
        self._restart = False   
        
        # the actual logger to be configured and set in configure_logging()
        logging.basicConfig(format="%(asctime)s %(levelno)s "
                                   "@%(threadName)s: %(message)s")
        self.log = logging.getLogger()
        self._log_handlers = []

        # to be used in start()
        self.task_dict = {}
        self.result_fifo = Queue.Queue()
        self.mutex = threading.Lock()


    @classmethod
    def get_instance(cls):
        "Get (the only) class instance; create it if it does not exist yet"

        with cls._server_iface_rlock:
            if cls._instance is None:
                return cls()
            else:
                return cls._instance


    @classmethod
    def configure(cls, config_path,
                  config_encoding=DEFAULT_JSON_ENCODING,
                  force_daemon=False,
                  default_mod_init_callable=utils.basic_mod_init):

        '''More interesting constructor. Load config and configure the server.

        Obligatory argument: config_path -- path of a config file.

        Optional arguments:
        
        * config_encoding (str);
        
        * force_daemon (bool) -- if True => always daemonize the OS process
          (ignore the 'os_settings'->'daemon' field);
          
        * default_mod_init_callable (callable object) --
          to be passed to methodtree.build_rpc_tree().
        '''

        try:
            self = cls.get_instance()
            self.load_config(config_path, config_encoding)
            self.configure_logging()
            self.do_os_settings(force_daemon=force_daemon)
            self.load_rpc_tree(default_mod_init_callable
                               =default_mod_init_callable)
        except:
            self.log.critical('Error during server configuration. '
                              'Raising exception...', exc_info=True)
            raise
        else:
            return self


    @classmethod
    def configure_and_start(cls, config_path,
                            config_encoding=DEFAULT_JSON_ENCODING,
                            force_daemon=False,
                            default_mod_init_callable=utils.basic_mod_init,
                            loop_mode=False,
                            final_callback=None):
                                
        '''Automagic constructor. Load config, configure and start the server.

        Obligatory argument: config_path -- path of a config file.

        Optional arguments:
        
        * config_encoding
        * loop_mode
        * force_daemon
        * default_mod_init_callable
        -- see: configure();
        
        * loop_mode (bool) --
          if True => stay here waiting for an OS signal or restart request,
          if False => return immediately after server start;
          
        * final_callback (callable object) -- to be called
          from the RPC-manager thread before it terminates.
        '''
        
        while True:
            self = cls.configure(config_path, config_encoding, force_daemon,
                                 default_mod_init_callable)
            try:
                self.start(final_callback=final_callback)
            except:
                self.log.critical('Error during server start. '
                                  'Raising exception...', exc_info=True)
                raise

            if not loop_mode:
                # non-loop mode: return the instance object immediately
                return self
                
            else:
                # loop mode: wait for a restart request or OS signals
                try:
                    while not self._restart:
                        time.sleep(0.5)
                except SystemExit:  # probably raised by a signal handler
                    self.log.debug('System exit...')
                    raise
                except KeyboardInterrupt:
                    self.log.debug('Keyboard interrupt...')
                    self.stop()  # it may be Ctrl+C -caused in non-deamon mode
                    sys.exit()   # => we must finalize the program here because
                                 # of strange effects on variable namespaces
                                 # when control gets another module :-/
                except:
                    self.log.critical('Error while handling or waiting for a '
                                      'system signal. Raising exception...',
                                      exc_info=True)
                    raise
                else:
                    self._restart = False


    # note that it can be used as final_callback for loop mode:
    @classmethod
    def restart_on(cls):
        cls._instance._restart = True


    def load_config(self, config_path, config_encoding=DEFAULT_JSON_ENCODING):
        'Parse JSON-formatted config file, prepare and return config as dict'
        
        try:
            with open(config_path) as config_file:
                config = json.load(config_file, encoding=config_encoding)
                config = self.validate_config(config)
        
        except Exception:
            raise RuntimeError("Can't load configuration -- {0}"
                               .format(traceback.format_exc()))

        self.config = config
        return config


    @staticmethod
    def validate_config(config):
        
        # verify section content types
        for section, sect_content in config.iteritems():
            if not isinstance(sect_content, CONFIG_SECTION_TYPES[section]):
                raise TypeError('{0} section should be a {1.__name__}'
                                .format(section,
                                        CONFIG_SECTION_TYPES[section]))
                                
        # verify completeness
        omitted = set(OBLIGATORY_CONFIG_SECTIONS).difference(config)
        if omitted:
            raise ValueError('Sections: {0} -- should not be omitted'
                             .format(', '.join(sorted(omitted))))
            
        # complement omited non-obligatory sections
        for section in set(CONFIG_SECTION_TYPES
                          ).difference(OBLIGATORY_CONFIG_SECTIONS):
            config.setdefault(section, CONFIG_SECTION_TYPES[section]())

        # verify section fields and complement them with default values
        for section, sect_content in CONFIG_SECTION_FIELDS.iteritems():
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

        # replace unicode-keys with str-keys in kwargs-based sections
        (config['amqp_params']
        ) = utils.kwargs_to_str(config['amqp_params'])
        (config['manager_attributes']
        ) = utils.kwargs_to_str(config['manager_attributes'])
        (config['responder_attributes']
        ) = utils.kwargs_to_str(config['responder_attributes'])

        # verify RPC-methods-related settings
        for field, value in config['rpc_tree_init'].iteritems():
            if not (field in ('paths', 'imports') and isinstance(value, list)
                    or field == 'mod_init_kwargs' and isinstance(value, dict)):
                raise ValueError("Illegal item in rpc_tree_init section:"
                                 " {0!r}: {1!r}".format(field, value))
        
        # verify and prepare exchange types
        for exchange, etype in config['exchange_types'].iteritems():
            if not (isinstance(exchange, basestring)
                    and isinstance(etype, basestring)):
                raise ValueError("Illegal item in exchange_types section:"
                                 " {0!r}: {1!r}".format(exchange, etype))

        # verify and prepare binding properties
        bindings = []
        for binding_props in config['bindings']:
            try:
                if not all(isinstance(x, basestring)
                           for x in binding_props):
                    raise TypeError
                (binding_props
                ) = threads.BindingProps._make(binding_props)
            except (ValueError, TypeError):
                raise ValueError("Illegal item in bindings section: "
                                 "{0!r}".format(q_name_props))
            else:
                bindings.append(binding_props)
        config['bindings'] = bindings

        return config


    #
    # Environment-related preparations
    
    def configure_logging(self, log_config=None):
        "Configure server logger and its handlers"

        prev_log = self.log
        if log_config is None:
            log_config = self.config['logging_settings']

        # get the logger
        self.log = logging.getLogger(log_config.get('server_logger', ''))

        # configure it
        utils.configure_logging(self.log, prev_log, self._log_handlers,
                                log_config)
        return self.log


    def do_os_settings(self, os_settings=None, force_daemon=False):

        if os_settings is None:
            os_settings = self.config['os_settings']

        umask = os_settings.get('umask')
        if umask is None:
            umask = os.umask(0)   # (os.umask() sets new, returns previous)
        os.umask(umask)

        working_dir = os_settings.get('working_dir')
        if working_dir is not None:
            os.chdir(working_dir)
            
        if (os_settings.get('daemon') or force_daemon) and not self.daemonized:
            # daemonize:
            _daemon_recipe.UMASK = umask
            _daemon_recipe.WORKDIR = os.getcwd()
            _daemon_recipe.createDaemon()
            self.daemonized = True
            
        # unregister old signal handlers (when restarting)
        while self._signal_handlers:
            signal_num, handler = self._signal_handlers.popitem()
            signal.signal(signal_num, signal.SIG_DFL)
        
        # register signal handlers
        signal_actions = os_settings.get('signal_actions',
                                         dict(SIGTERM='exit',
                                              SIGHUP='restart'))
        stopping_timeout = os_settings.get('stopping_timeout', 60)
        if signal_actions is not None:
            for signal_name, action_name in signal_actions.iteritems():
                signal_num = getattr(signal, signal_name)
                handler_func = getattr(self, '_{0}_handler'.format(action_name))
                handler = functools.partial(handler_func,
                                            stopping_timeout=stopping_timeout)
                signal.signal(signal_num, handler)
                self._signal_handlers[signal_num] = handler

    #
    # OS signal handlers:

    def _restart_handler(self, signal_num, stack_frame, stopping_timeout):
        '"restart" action'
        try:
            self.log.info('Signal #%s received by the process -- '
                          '"restart" action starts...', signal_num)
            if self.stop(reason='restart', timeout=stopping_timeout):
                self.restart_on()
        except:
            self.log.critical('Error while restarting. Raising exception...',
                              exc_info=True)
            raise

    _reload_handler = _restart_handler

    def _exit_handler(self, signal_num, stack_frame, stopping_timeout):
        '"exit" action'
        try:
            self.log.info('Signal #%s received by the process -- '
                          '"exit" action starts...', signal_num)
            if self.stop(reason='exit', timeout=stopping_timeout):
                sys.exit()
        except SystemExit:
            raise
        except:
            self.log.critical('Error while exiting. Raising exception...',
                              exc_info=True)
            raise

    def _force_exit_handler(self, signal_num, stack_frame, stopping_timeout):
        '"force_exit" action'
        try:
            self.log.info('Signal #%s received by the process -- '
                          '"force_exit" action starts...', signal_num)
            if self.stop(reason='force-exit', force=True,
                         timeout=stopping_timeout):
                sys.exit()
        except SystemExit:
            raise
        except:
            self.log.critical('Error while force-exiting. Raising '
                              'exception...', exc_info=True)
            raise


    #
    # RPC-methods tree loading

    def load_rpc_tree(self, paths=None, imports=None, mod_init_kwargs=None,
                      default_mod_init_callable=utils.basic_mod_init):
        'Load RPC-methods from modules specified by names or filesystem paths'
        
        try:
            rpc_tree_init_conf = self.config.get('rpc_tree_init', {})
            
            if paths is None:
                paths = rpc_tree_init_conf.get('paths', [])
                
            if imports is None:
                imports = rpc_tree_init_conf.get('imports', [])

            if mod_init_kwargs is None:
                mod_init_kwargs = rpc_tree_init_conf.get('mod_init_kwargs', {})

            root_mod = types.ModuleType('_MTRPC_ROOT_MODULE_')

            # load modules using absolute filesystem paths
            for path_req in paths:
                tokens = [s.strip() for s in path_req.rsplit(None, 2)]
                if len(tokens) == 3 and tokens[1] == 'as':
                    file_path = tokens[0]
                    # e.g. '/home/zuo/foo.py as bar' => dst_name='bar'
                    dst_name = tokens[2]
                else:
                    file_path = path_req
                    # e.g. '/home/zuo/foo.py' => dst_name='foo'
                    dst_name = os.path.splitext(os.path.basename(file_path))[0]
                name_owner = getattr(root_mod, dst_name, None)
                if name_owner is None:
                    module_name = 'mtrpc_pathloaded_{0}'.format(dst_name)
                    module = imp.load_source(module_name, file_path)
                    setattr(root_mod, dst_name, module)
                else:
                    self.log.warning('Cannot load module from path "{0}" as '
                                     '"{1}" -- because "{1}" name is already '
                                     'used by module {2!r}'
                                     .format(file_path, dst_name, name_owner))
                
            # import modules using module names
            for import_req in imports:
                tokens = [s.strip() for s in import_req.split()]
                if len(tokens) == 3 and tokens[1] == 'as':
                    src_name = tokens[0]
                    # e.g. 'module1.modulo2.modula3 as foo' => dst_name='foo'
                    dst_name = tokens[2]
                elif len(tokens) == 1:
                    src_name = tokens[0]
                    # e.g. 'module1.modulo2.modula3' => dst_name='modula3'
                    dst_name = tokens[0].split('.')[-1]
                else:
                    raise ValueError('Malformed import request: "{0}"'
                                     .format(import_req))
                name_owner = getattr(root_mod, dst_name, None)
                if name_owner is None:
                    module = __import__(src_name,
                                        fromlist=['__dict__'],
                                        level=0)
                    setattr(root_mod, dst_name, module)
                else:
                    self.log.warning('Cannot import module "{0}" as "{1}" -- '
                                     'because "{1}" name is already used by '
                                     'module {2!r}'
                                     .format(src_name, dst_name, name_owner))

            # (use warnings framework to log any warnings with the logger)
            with warnings.catch_warnings():
                self._set_warnings_logging_func(self.log,
                                                warnings.showwarning)

                # create RPC-tree object and walk recursively over submodules
                # of the root module to collect names and callables -- to
                # create RPC-modules and RPC-methods and populate the RPC-tree
                # with them
                rpc_tree = methodtree.build_rpc_tree(root_mod,
                                                     default_mod_init_callable,
                                                     mod_init_kwargs)
                
        except Exception:
            raise RuntimeError('Error when loading RPC-methods -- {0}'
                               .format(traceback.format_exc()))
                               
        self.rpc_tree = rpc_tree
        return rpc_tree


    @staticmethod
    def _set_warnings_logging_func(log, orig_showwarning):

        def showwarning(message, category, filename, lineno,
                        file=None, line=None):
            if issubclass(category, methodtree.LogWarning):
                log.warning(message)
            else:
                orig_showwarning(message, category, filename, lineno,
                                 file=None, line=None)
                                 
        warnings.showwarning = showwarning


    #
    # The actual server management

    def start(self, config=None, rpc_tree=None, log=None,
              wait_until_stopped=False, final_callback=None):
        "Start the server"

        self.log.info('Starting the server...')

        with self._server_iface_rlock:
            if self.manager is not None:
                time.sleep(1)  # a bit paranoic :)
                if self.manager.is_alive():
                    raise RuntimeError('The server is already started')

            if config is None:
                config = self.config

            if rpc_tree is None:
                rpc_tree = self.rpc_tree

            if log is None:
                log = self.log
                
            self.responder = threads.RPCResponder(config['amqp_params'],
                                                  self.task_dict,
                                                  self.result_fifo,
                                                  self.mutex,
                                                  log=log,
                                                  **config['responder_'
                                                           'attributes'])
                                                    
            self.manager = threads.RPCManager(config['amqp_params'],
                                              config['bindings'],
                                              config['exchange_types'],
                                              config['manager_settings']['client_id'],
                                              rpc_tree,
                                              self.responder,
                                              self.task_dict,
                                              self.result_fifo,
                                              self.mutex,
                                              final_callback,
                                              log=log,
                                              **config['manager_attributes'])
            self.manager.start()
        
        if wait_until_stopped:
            self.manager.join()
        
        return self.manager


    def stop(self, reason='manual stop', loglevel='info', force=False,
             timeout=30):
        '''Stop the server.

        force=True  => the responder will not wait to complete remaining tasks

        timeout=None  => wait until the server terminates
        timeout=<x>   => wait, but no longer than <x> seconds
        timeout=0     => don't wait, return immediately
        '''
                 
        with self._server_iface_rlock:
            if self.manager is None or not self.manager.is_alive():
                self.log.warning("Futile attempt to stop the server "
                                 "while it's not started")
                return True
            
        self.log.info('Stopping the server (reason: "%s")...', reason)
        stopped = self.manager.stop(reason, loglevel, force, timeout)
        if stopped:
            self.manager = None
        else:
            self.log.error('The server is not stopped (yet?)')

        # return True if the server has been stopped, False if it's still alive
        return stopped



#
# Additional functions
#

# (it's a static method -- useful also as a standalone function)
validate_config = MTRPCServerInterface.validate_config
                                                        


def make_config_stub():
    "Create a dict contaning (empty) obligatory config sections"
    
    return dict((section, CONFIG_SECTION_TYPES[section]())
                for section in OBLIGATORY_CONFIG_SECTIONS)


def write_config_skeleton(dest_path, config_stub=None,
                          config_encoding=DEFAULT_JSON_ENCODING):
    "A function that may help creating a proper config file"

    if config_stub is None:
        config_stub = make_config_stub()
    config = validate_config(config_stub)
    with open(dest_path, 'w') as dest_file:
        json.dump(config, dest_file, encoding=config_encoding,
                  sort_keys=True, indent=4)
