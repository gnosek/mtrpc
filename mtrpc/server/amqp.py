import time
from mtrpc.server import MTRPCServerInterface, threads


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

    def start(self, config=None, rpc_tree=None, log=None,
              wait_until_stopped=False, final_callback=None):

        """Create the manager and responder threads. Start the manager.

        Arguments:

        * config -- a dict that contains (at least) the following items:
          'amqp_params' (dict), 'bindings' (list), 'exchange_types' (dict),
          'responder_attributes' (dict), 'manager_attributes' (dict);

        * rpc_tree -- ready to use (already populated) methodtree.RPCTree
          instance;

        * log -- server logger (logging.Logger instance) or its name (str);

        [If the above arguments are set to None, the corresponding
         attributes will be used; it's typical usage]

        * wait_until_stopped (bool) -- if True don't return from this
          method until the manager thread terminates (it's rarely used);

        * final_callback (callable object or None) -- to be called by the
          manager thread directly before its termination.

        For more details about the mentioned `config' items and the
        `rpc_tree', `log' and `final_callback' arguments -- see: docs
        of __init__() of threads.RPCResponder, threads.RPCManager and
        their base classes.

        """

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

        if wait_until_stopped:
            self.manager.join()

        return self.manager


