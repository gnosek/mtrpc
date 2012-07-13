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

"""


import sys
import traceback
import warnings
import itertools

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


def raise_exc(exception, *args, **kwargs):

    """Simply raise an exception. This is a compat wrapper for the old
    MethodExcWrapper hiding exceptions in the name of security

    If any other arguments are given:
    * if the `exception' argument is a type -- they are used to create its
      instance,
    * otherwise -- TypeError is raised.

    """

    if args or kwargs:
        all_args = itertools.chain(map(repr, args),
                                   ('{0}={1!r}'.format(key, val)
                                    for key, val in kwargs.iteritems()))
        warnings.warn('Call to raise_exc({0}, {1})'.format(
            exception, ', '.join(all_args)), category=DeprecationWarning)
    else:
        warnings.warn('Call to raise_exc({0})'.format(
            exception), category=DeprecationWarning)

    if not (args or kwargs):
        wrapped_exc = exception
    elif isinstance(exception, type):
        wrapped_exc = exception(*args, **kwargs)
    else:
        raise TypeError("Cannot instantiate {0!r} -- it's not "
                        "an exception type object".format(exception))
    raise wrapped_exc


def wrap_exceptions(exc_base_class):

    """Dummy for API compatibility"""

    def make_wrapper(func):
        return func
    return make_wrapper

