import Queue
import threading
import time
import signal
import sys
from mtrpc.server.core import MTRPCServerInterface
from mtrpc.server import threads


class AmqpServer(MTRPCServerInterface):

    CONFIG_SCHEMA = {
        'type': 'object',
        'properties': {
            'amqp_params': {'type': 'object', 'default': None},
            'exchange_types': {'type': 'object', 'default': None},
            'bindings': {'type': 'array', 'default': None},
            'manager_settings': {'type': 'object', 'default': None},
            'manager_attributes': {'type': 'object', 'default': None},
            'responder_attributes': {'type': 'object', 'default': None},
        },
        'required': ['amqp_params', 'bindings']
    }
    CONFIG_SCHEMAS = MTRPCServerInterface.CONFIG_SCHEMAS +[CONFIG_SCHEMA]

    RPC_MODE = 'server'

    def __init__(self):
        self.manager = None
        self.responder = None
        self.task_dict = {}
        self.result_fifo = Queue.Queue()
        self.mutex = threading.Lock()
        super(AmqpServer, self).__init__()

    @classmethod
    def prepare_bindings(cls, config):
        # verify and prepare binding properties (turn it into a list
        # of threads.BindingProps namedtuple instances)
        bindings = []
        for binding_props in config['bindings']:
            try:
                if not all(isinstance(x, basestring)
                           for x in binding_props):
                    raise TypeError
                binding_props = threads.BindingProps._make(binding_props)
            except (ValueError, TypeError):
                raise ValueError("Illegal item in bindings section: "
                                 "{0!r}".format(binding_props))
            else:
                bindings.append(binding_props)
        config['bindings'] = bindings
        return config

    def start(self, final_callback=None):

        """Create the manager and responder threads. Start the manager.

        Arguments:

        * final_callback (callable object or None) -- to be called by the
          manager thread directly before its termination.

        """

        self.log.info('Starting the server...')

        with self._server_iface_rlock:
            if self.manager is not None:
                time.sleep(1)  # a bit paranoic :)
                if self.manager.is_alive():
                    raise RuntimeError('The server is already started')

            config = self.prepare_bindings(self.config)
            rpc_tree = self.rpc_tree
            log = self.log

            signal.signal(signal.SIGTERM, self._exit_handler)
            signal.signal(signal.SIGHUP, self._restart_handler)

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
                                              config['manager_settings']['client_id'],  # !TODO! - inaczej...
                                              rpc_tree,
                                              self.responder,
                                              self.task_dict,
                                              self.result_fifo,
                                              self.mutex,
                                              final_callback,
                                              log=log,
                                              **config['manager_attributes'])
            self.manager.start()

        return self.manager

    #
    # OS signal handlers:
    def _restart_handler(self, signal_num, stack_frame):
        """"restart" action"""
        self.log.info('Signal #%s received by the process -- '
                      '"restart" action starts...', signal_num)
        if self.stop(reason='restart'):
            self.restart_on()

    def _exit_handler(self, signal_num, stack_frame):
        """"exit" action"""
        self.log.info('Signal #%s received by the process -- '
                      '"exit" action starts...', signal_num)
        if self.stop(reason='exit'):
            sys.exit()

    def stop(self, reason='manual stop', timeout=MTRPCServerInterface.SIGNAL_STOP_TIMEOUT):

        """Request the manager to stop the responder and then to stop itself.

        Arguments:

        * reason (str) -- an arbitrary message (to be recorded in the log);

        * timeout (int or None)
          -- timeout=None  => wait until the manager thread terminates,
          -- timeout=<i>   => wait, but no longer than <i> seconds,
          -- timeout=0     => don't wait, return immediately.

        Return True if the manager thread has been stopped successfully
        (then set the `manager' attribute to None); False if it's still
        alive.

        """

        with self._server_iface_rlock:
            if self.manager is None or not self.manager.is_alive():
                self.log.warning("Futile attempt to stop the server "
                                 "while it's not started")
                return True

        self.log.info('Stopping the server (reason: "%s")...', reason)
        stopped = self.manager.stop(reason, 'info', timeout)
        if stopped:
            self.manager = None
        else:
            self.log.warning('Server stop has been requested but the '
                             'server is not stopped (yet?)')

        return stopped

    # it is useful as final_callback in loop mode
    @classmethod
    def restart_on(cls):
        cls._instance._restart = True
