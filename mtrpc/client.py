# mtrpc/client.py
#
# Author: Jan Kaliszewski (zuo)
# Copyright (c) 2010, MegiTeam

"""MegiTeam-RPC (MTRPC) framework -- the client part: simple RPC-proxy.

A simple example:

    from mtrpc.client import MTRPCProxy

    with MTRPCProxy(
            req_exchange='request_amqp_exchange',
            req_rk_pattern='request_amqp_routing_key',
            loglevel='info',
            host="localhost:5672",
            userid="guest",
            password="guest",
    ) as rpc:

        routing_key_info = rpc.my_module.tell_the_rk()
        rpc_tree_content = rpc.system.list_string('', deep=True)
        add_result = rpc.my_module.add(1, 2)   # -> 3

        try:
            div_result = rpc.my_module.my_submodule.div(10, 0)   # -> error
        except ZeroDivisionError as exc:
            div_result = 'ZeroDivisionError -- {0}'.format(exc)

        print
        print routing_key_info
        print '\nAccessible RPC-modules and methods:', rpc_tree_content
        print '\nAddition result:', add_result
        print '\nDivision result:', div_result

To run the above example, first start an AMQP broker and MTRPC server
-- see the example in mtrpc.server module documentation [module docstring
in mtrpc/server/__init__.py].

For more information about MTRPCProxy constructor arguments
-- see MTRPCProxy.__init__() documentation.

"""



from future_builtins import filter, map, zip

import __builtin__
import itertools
import logging
import threading
import traceback

from collections import namedtuple

from amqplib import client_0_8 as amqp

from .common import utils
from .common import errors
from .common import encoding
from .common.const import *



#
# Auxiliary types
#

Response = namedtuple('Response', 'result error id')


class _RPCModuleMethodProxy(object):

    """Auxiliary automagic-callable-co-proxy class"""

    def __init__(self, rpc_proxy, full_name):
        self._rpc_proxy = rpc_proxy
        self._full_name = full_name

    def __getattr__(self, local_name):
        if local_name.startswith('_'):
            raise AttributeError
        else:
            if not bool(self._rpc_proxy):
                raise errors.RPCClientError('MTRPCProxy instance is already closed')
            full_name = '{0}.{1}'.format(self._full_name, local_name)
            return _RPCModuleMethodProxy(self._rpc_proxy, full_name)

    def __call__(self, *args, **kwargs):
        return self._rpc_proxy._call(self._full_name, args, kwargs)

    def __nonzero__(self):
        return bool(self._rpc_proxy)



#
# The RPC-proxy class
#

class MTRPCProxy(object):

    """The actual MTRPC proxy class.

    Get RPC-modules as they were MTRPCProxy instance attributes;
    call RPC-methods as their member functions.

    """

    def __init__(self, req_exchange=None, req_rk_pattern=DEFAULT_REQ_RK_PATTERN,
                 resp_exchange=DEFAULT_RESP_EXCHANGE, custom_exceptions=None,
                 log=None, loglevel=None, **amqp_params):

        """RPC-proxy initialization.

        Arguments:

        * req_exchange (str) -- name of AMQP exchange to be used to send
          RPC-requests (optional, but must be passed either here, or to _call);

        * req_rk_pattern (str) -- pattern of routing key to be used to
          send requests (obligatory argument); it may contain some of
          the following fields to be substituted using string .format()
          method:

          * full_name -- full (absolute, dot-separated) RPC-method name;
          * local_name -- local (rightmost) part of that name;
          * parentmod_name -- full name without the local part;
          * split_name -- full name as a list of its parts (strings);
          * req_exchange -- see above: 'req_exchange' argument;
          * resp_exchange -- see below: 'resp_exchange' argument;

        * resp_exchange (str) -- name of AMQP exchange to be used to
          receive RPC-responses (default --
          see: mtrpc.common.const.DEFAULT_RESP_EXCHANGE);

        * custom_exceptions (dict or None) -- a dictionary containing
          your custom (additional) RPC-transportable exceptions; maps
          class names to class objects; (default: None)

        * log (logging.Logger instance or str or None) -- logger object
          or name (default: None => standard "basic" logging
          configuration, using the root logger);

        * loglevel (str or None) -- 'debug', 'info', 'warning', 'error'
          or 'critical' (default: None => default settings to be used);

        * amqp_params -- dict of keyword arguments for AMQP.Connection(),
          see amqplib.client_0_8.Connection.__init__() for details.

        """

        self._req_exchange = req_exchange
        self._req_rk_pattern = req_rk_pattern  # may contain {fields} used in
                                               # _prepare_routing_key()...
        if custom_exceptions is None:
            self._custom_exceptions = {}
        else:
            self._custom_exceptions = custom_exceptions
        self._resp_exchange = resp_exchange

        self._call_lock = threading.RLock()
        self._response = None
        self._closed = False

        self._logging_init(log, loglevel)
        self._amqp_init(amqp_params)


    def __getattr__(self, submod_name):
        if submod_name.startswith('_'):
            raise AttributeError
        else:
            return _RPCModuleMethodProxy(self, submod_name)


    def __nonzero__(self):
        return not self._closed


    def __enter__(self):
        return self


    def __exit__(self, type, value, tb):
        if tb is not None:
            self._log.error('An exception occurred: %s(%s)',
                            type.__name__, value)
            self._log.debug('Exception details', exc_info=True)
        self._close()


    def _close(self):
        "Close the proxy"
        try:
            try:
                try:
                    if self._amqp_channel.connection:
                        self._amqp_channel.basic_cancel(self._resp_queue)
                finally:
                    self._amqp_channel.close()
            finally:
                self._amqp_conn.close()
        finally:
            self._closed = True


    def _logging_init(self, log, loglevel):

        "Set logger"

        if log is None:
            logging.basicConfig(format="%(asctime)s %(message)s")
            self._log = logging.getLogger()
        elif isinstance(log, basestring):
            self._log = logging.getLogger(log)
        else:
            self._log = log

        if loglevel is not None:
            if isinstance(loglevel, basestring):
                loglevel = getattr(logging, loglevel.upper())
            self._log.setLevel(loglevel)



    def _amqp_init(self, amqp_params):
        "Init AMQP communication"
        self._log.info('Initializing AMQP channel and connection...')
        self._amqp_conn = amqp.Connection(**amqp_params)
        self._amqp_channel = self._amqp_conn.channel()
        self._resp_queue = self._bind_and_consume()


    def _call(self, full_name, call_args, call_kwargs, exchange=None, custom_exceptions=None):

        "Remotely call a procedure (RPC-method)"

        if exchange is None:
            exchange = self._req_exchange

        if exchange is None:
            raise errors.RPCClientError('Must specify exchange either in constructor, or in _call')

        if custom_exceptions is None:
            custom_exceptions = self._custom_exceptions

        if custom_exceptions is None:
            custom_exceptions = {}

        all_args = itertools.chain(map(repr, call_args),
                                   ('{0}={1!r}'.format(key, val)
                                    for key, val in call_kwargs.iteritems()))
        self._log.info('* remote call: %s(%s)', full_name, ', '.join(all_args))

        if self._closed:
            raise errors.RPCClientError('MTRPCProxy instance is already closed')

        with self._call_lock:
            resp_queue = self._resp_queue
            try:
                msg = self._prepare_msg(full_name, call_args,
                                        call_kwargs, resp_queue)
                routing_key = self._prepare_routing_key(full_name, exchange)
                self._amqp_channel.basic_publish(msg,
                                                 exchange=exchange,
                                                 routing_key=routing_key,
                                                 mandatory=True,
                                                 immediate=True)
                self._amqp_channel.wait()
                if not self._response:
                    reply_code, reply_text, exchange, rk, message = self._amqp_channel.returned_messages.get()
                    if message.reply_to != resp_queue:
                        raise errors.RPCClientError("It should not happen! RPC-error id "
                                             "{0!r} differs from RPC-request id {1!r}"
                                             .format(message.reply_to, resp_queue))
                    raise amqp.exceptions.AMQPChannelException(
                        reply_code, reply_text, (exchange, rk))

            finally:
                response = self._response
                self._response = None

            if response.id != resp_queue:
                raise errors.RPCClientError("It should not happen! RPC-response id "
                                     "{0!r} differs from RPC-request id {1!r}"
                                     .format(response.id, resp_queue))
            elif response.error:
                self._raise_received_error(response.error, custom_exceptions)
            else:
                return response.result


    def _bind_and_consume(self):
        self._amqp_channel.exchange_declare(
                exchange=self._resp_exchange,
                type='direct',
                durable=True,
                auto_delete=False,
        )
        resp_queue, x, x = self._amqp_channel.queue_declare(
                durable=True,
                exclusive=True,
                auto_delete=True,
        )
        self._amqp_channel.queue_bind(
                queue=resp_queue,
                exchange=self._resp_exchange,
                routing_key=resp_queue,   # (<-yes)
        )
        self._amqp_channel.basic_consume(
                queue=resp_queue,
                no_ack=False,
                callback=self._store_response,
                consumer_tag=resp_queue,  # (<-yes)
        )
        return resp_queue


    def _store_response(self, msg):
        try:
            response_dict = encoding.loads(msg.body)
            self._response = Response(**utils.kwargs_to_str(response_dict))
        except Exception:
            raise errors.RPCClientError('Could not deserialize message: {0!r}\n{1}'
                                 .format(msg.body, traceback.format_exc()))


    def _prepare_msg(self, full_name, call_args,
                     call_kwargs, resp_queue):
        request_dict = dict(
                id=resp_queue,
                method=full_name,
                params=call_args,
        )
        if call_kwargs:
            request_dict['kwparams'] = call_kwargs

        try:
            message_data = encoding.dumps(request_dict)
            return amqp.Message(
                    message_data,
                    delivery_mode=2,
                    reply_to=resp_queue,
            )
        except Exception:
            raise errors.RPCClientError('Could not serialize request dict: {0!r}\n{1}'
                                 .format(request_dict, traceback.format_exc()))


    def _prepare_routing_key(self, full_name, exchange):
        split_name = full_name.split('.')
        return self._req_rk_pattern.format(
                full_name=full_name,
                local_name=split_name[-1],
                parentmod_name='.'.join(split_name[:-1]),
                split_name=split_name,
                req_exchange=exchange,
                resp_exchange=self._resp_exchange,
        )


    def _raise_received_error(self, received_error, custom_exceptions):
        try:
            try:
                exctype_name = received_error.get('name', '')
                exc_message = received_error.get('message', '')
                exc_data = received_error.get('data', None)
            except (TypeError, AttributeError):
                exctype_name = '<UNKNOWN!>'
                exc_message = '<UNKNOWN!>'
                exc_data = None
                raise Exception

            try:
                exctype = custom_exceptions[exctype_name]
            except KeyError:
                try:
                    exctype = getattr(errors, exctype_name)
                except AttributeError:
                    exctype = getattr(__builtin__, exctype_name)

        except Exception:
            raise errors.RPCClientError(
                    'The response contains unknown/unproper '
                    'error-item: {0!r} -- with message: {1!r} and data {2!r}'
                    .format(exctype_name, exc_message, exc_data))
        else:
            exc = exctype(exc_message)
            if exc_data:
                exc.__dict__.update(exc_data)
            raise exc
