"""MTRPC common error classes"""

# Author: Jan Kaliszewski (zuo)
# Copyright (c) 2010, MegiTeam

import sys
import traceback


class RPCError(Exception):
    "Base MTRPC exception"


#
# RPC server exceptions

class RPCServerDeserializationError(RPCError):
    "Request message could not be deserialized by RPC server"
    
class RPCServerSerializationError(RPCError):
    "Response could not be serialized by RPC server"

class RPCInvalidRequestError(RPCError):
    "Malformed request (lack of needed field or illegal field name or type)"

class RPCNotificationsNotImplementedError(RPCError):
    "JSON-RPC notifications are not supported"

class RPCNotFoundError(RPCError):
    "No such RPC-module or method (at least for given access rights)"

class RPCMethodArgError(RPCError):
    "Bad arguments (params) for RPC-method"

class RPCInternalServerError(RPCError):
    "Bad server configuration or other internal problems"


#
# RPC client exceptions

class RPCClientError(RPCError):
    "Error detected on client side"


#
# RPC server container-exception and a convenience function to use it

class MethodExcContainer(Exception):
    "Method call exception container (the included exception will be sent)"

    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)
        self.included_exc_type, self.included_exc = sys.exc_info()[:2]
        self.formated_traceback = traceback.format_exc()


def raise_exc(exception, *args, **kwargs):
    "Raise MethodExcContainer with a particular exception"

    # `exception' argument can be an exception type or instance
    # (if it's an instance there should be no args/kwargs)
    
    rpc_log = kwargs.pop('rpc_log', None)   # optional keyword argument
    try:
        if args or kwargs:
            raise exception(*args, **kwargs)
        else:
            raise exception
    except:
        if rpc_log is not None:
            rpc_log.exception('Exception within method call:')
        raise MethodExcContainer
