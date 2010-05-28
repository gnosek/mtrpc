# mtrpc/common/errors.py
#
# Author: Jan Kaliszewski (zuo)
# Copyright (c) 2010, MegiTeam

"""MTRPC common exception classes and an exception-related function:

* RPCError -- base MTRPC exception class;

* RPCClientError -- used in mtrpc.client classes to indicate errors
  that ocurred on the client side;

* rest RPC*Error classes -- raised in mtrpc.server.* classes, sent to
  client and then re-raised; see the class docstrings for more info;

* MethodExcWrapper class and raise_exc() function -- to be used in
  RPC-method definitions; see the class/function-docstrings for more info.

"""


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
# A server wrapper-exception and a function to be used in RPC-method defs

class MethodExcWrapper(Exception):

    """RPC-method-call exception wrapper.

    If caught in threads.RPCTaskThread.run() -- the included exception
    (not the wrapper) will be sent to the client. (For security reasons,
    other exceptions -- that are not wrapped -- are treated as unexpected
    and reported to the client as RPCInternalServerError, without any
    exception details).

    """

    def __init__(self, wrapped_exc=None):

        """The wrapped exception can be:
        
        * passed explicitly into the constructor, or
        * taken from sys.exc_info (wrapper initialization should be done
          when the wrapped exception is being handled).

        """

        Exception.__init__(self, wrapped_exc)
        if wrapped_exc is not None:
            self.wrapped_exc = wrapped_exc
            self.wrapped_exc_type = type(wrapped_exc)
        else:
            self.wrapped_exc_type, self.wrapped_exc = sys.exc_info()[:2]


    def __str__(self):
        return '{0.__name__} -- {1}'.format(self.wrapped_exc_type,
                                            self.wrapped_exc)


def raise_exc(exception, *args, **kwargs):

    """Wrap a given exception with a MethodExcWrapper instance (raising it).

    It's a convenience function. The `exception' argument should be an
    exception type or instance.

    If any other arguments are given:
    * if the `exception' argument is a type -- they are used to create its
      instance,
    * otherwise -- TypeError is raised.

    """

    if not (args or kwargs):
        wrapped_exc = exception
    elif isinstance(exception, type):
        wrapped_exc = exception(*args, **kwargs)
    else:
        raise TypeError("Cannot instantiate {0!r} -- it's not "
                        "an exception type object".format(exception))
    raise MethodExcWrapper(wrapped_exc)
