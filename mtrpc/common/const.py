"""MTRPC common constants"""

# Author: Jan Kaliszewski (zuo)
# Copyright (c) 2010, MegiTeam



# Special module attribute names
RPC_METHOD_LIST = '__rpc_methods__'
RPC_MODULE_DOC = '__rpc_doc__'
RPC_TAGS = '__rpc_tags__'
RPC_INIT_CALLABLE = '__rpc_init__'
RPC_LOG = '__rpc_log__'
RPC_LOG_HANDLERS = '__rpc_log_handlers__'


# Special RPC-method access-related argument names
ACCESS_DICT_KWARG = '_access_dict'
ACCESS_KEY_KWARG = '_access_key_patt'
ACCESS_KEYHOLE_KWARG = '_access_keyhole_patt'
ACC_KWARGS = frozenset((
        ACCESS_DICT_KWARG,
        ACCESS_KEY_KWARG,
        ACCESS_KEYHOLE_KWARG,
))


# Some defaults
DEFAULT_RESP_EXCHANGE = 'MTRPCResponses'
DEFAULT_JSON_ENCODING = 'utf-8'
DEFAULT_LOG_HANDLER_SETTINGS = dict(
        cls='StreamHandler',
        kwargs={},
        level='info',
        format='%(asctime)s %(name)s:%(levelno)s @%(threadName)s: %(message)s'
)
