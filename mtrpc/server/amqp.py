import Queue
import threading
import time
from mtrpc.server.core import MTRPCServerInterface
from mtrpc.server import threads


class AmqpServer(MTRPCServerInterface):

    OBLIGATORY_CONFIG_SECTIONS = ('amqp_params', 'bindings')
    CONFIG_SECTION_TYPES = dict(
        MTRPCServerInterface.CONFIG_SECTION_TYPES,
        amqp_params=dict,
        exchange_types=dict,
        bindings=list,
        manager_settings=dict,  # !TODO! - inaczej...
        manager_attributes=dict,
        responder_attributes=dict,
    )
    CONFIG_SECTION_FIELDS = dict(
        MTRPCServerInterface.CONFIG_SECTION_FIELDS,
        amqp_params=None,  # to be a dict with some keys...
        exchange_types=None,  # to be a dict: {exchange, its type}
        bindings=None,  # to be a list of binding props
        manager_settings=None,  # to be a dict with some keys... !TODO! - inaczej...
        manager_attributes=None,  # to be a dict with some keys...
        responder_attributes=None,  # to be a dict with some keys...
    )
    RPC_MODE = 'server'

    def __init__(self):
        self.manager = None
        self.responder = None
        self.task_dict = {}
        self.result_fifo = Queue.Queue()
        self.mutex = threading.Lock()
        super(AmqpServer, self).__init__()

    @classmethod
    def validate_and_complete_config(cls, config):

        # verify completeness
        omitted = set(cls.OBLIGATORY_CONFIG_SECTIONS).difference(config)
        if omitted:
            raise ValueError('Section(s): {0} -- should not be omitted'
                             .format(', '.join(sorted(omitted))))

        config = super(AmqpServer, cls).validate_and_complete_config(config)

        # verify and prepare exchange types
        for exchange, etype in config['exchange_types'].iteritems():
            if not (isinstance(exchange, basestring)
                    and isinstance(etype, basestring)):
                raise ValueError("Illegal item in exchange_types section:"
                                 " {0!r}: {1!r}".format(exchange, etype))

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

            config = self.config
            rpc_tree = self.rpc_tree
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

    def stop(self, reason='manual stop', loglevel='info', force=False,
             timeout=30):

        """Request the manager to stop the responder and then to stop itself.

        Arguments:

        * reason (str) -- an arbitrary message (to be recorded in the log);

        * loglevel (str) -- one of: 'debug', 'info', 'warning', 'error',
          'critical';

        * force (bool) -- if true the server responder will not wait for
                          remaining tasks to be completed;

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
        stopped = self.manager.stop(reason, loglevel, force, timeout)
        if stopped:
            self.manager = None
        else:
            self.log.warning('Server stop has been requested but the '
                             'server is not stopped (yet?)')

        return stopped
