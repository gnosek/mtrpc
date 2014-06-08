import time
from mtrpc.server.core import MTRPCServerInterface
from mtrpc.server import threads


class AmqpServer(MTRPCServerInterface):

    OBLIGATORY_CONFIG_SECTIONS = MTRPCServerInterface.OBLIGATORY_CONFIG_SECTIONS + ('amqp_params', 'bindings')
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
