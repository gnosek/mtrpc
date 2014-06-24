# mtrpc/common/const.py
#
# Author: Jan Kaliszewski (zuo)
# Copyright (c) 2010, MegiTeam

"""MTRPC common constants:

* Names of special attributes of modules/callables that define
RPC-modules/methods:

  * RPC_METHOD_LIST (module attribute name) -- a name of a sequence
    (list/tuple) of method names (or a singular string being a method
    name);

  * RPC_MODULE_DOC (module attribute name) -- a name of a string
    containing documentation of a particual RPC-module;

  * RPC_POSTINIT (module attribute name) -- a name of a module postinit-
    callable (to be called after RPC-module initialization, typically to
    initialize custom module logger and globals);

  * RPC_LOG (module attribute name) -- a name of a custom module logger
    object (to set by the module postinit-callable);

  * RPC_LOG_HANDLERS (module attribute name) -- a name of an auxiliary
    logger handler list (to set by the module postinit-callable).


* Names of RPC-method special keyword arguments that define access rules:

  * ACCESS_DICT_KWARG (see: the fragment of mtrpc.server documentation
    about access key/keyhole mechanism);


* Various defaults:

  * DEFAULT_RESP_EXCHANGE -- default name of AMQP exchange to be used to
    send RPC-responses by server to client (via AMQP broker);

  * DEFAULT_LOG_HANDLER_SETTINGS -- default server logger handler
    settings (see: the fragment of mtrpc.server documentation about
    configuration file structure and content).

"""


# Special attribute names
# * get when the tree is being built:
RPC_METHOD_LIST = '__rpc_methods__'
RPC_MODULE_DOC = '__rpc_doc__'
RPC_POSTINIT = '__rpc_postinit__'
# * set when the tree is being built:
RPC_LOG = '__rpc_log__'
RPC_LOG_HANDLERS = '__rpc_log_handlers__'


# Special RPC-method access-related argument names
ACCESS_DICT_KWARG = '_access_dict'

# Some defaults
DEFAULT_REQ_RK_PATTERN = '{full_name}'
DEFAULT_RESP_EXCHANGE = 'amq.direct'
DEFAULT_LOG_HANDLER_SETTINGS = dict(
        cls='StreamHandler',
        kwargs={},
        level='info',
        format='%(asctime)s %(name)s:%(levelname)s @%(threadName)s: %(message)s'
)
