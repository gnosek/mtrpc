"""MegiTeam-RPC (MTRPC) framework -- server part"""

# Author: Jan Kaliszewski (zuo)
# Copyright (c) 2010, MegiTeam


from future_builtins import filter, map, zip

import __builtin__
import itertools
import json
import logging
import threading
import traceback

from collections import namedtuple

from amqplib import client_0_8 as amqp

from .common import utils
from .common import errors
from .common.const import *



#
# Auxiliary types
#

Response = namedtuple('Response', 'result error id')


class _RPCModuleMethodProxy(object):
    "Auxiliary automagic-callable-co-proxy class"

    def __init__(self, rpc_proxy, full_name):
        self._rpc_proxy = rpc_proxy
        self._full_name = full_name

    def __getattr__(self, local_name):
        if local_name.startswith('_'):
            raise AttributeError
        else:
            if not bool(self._rpc_proxy):
                raise RPCClientError('MTRPCProxy instance is already closed')
            full_name = '{0}.{1}'.format(self._full_name, local_name)
            return _RPCModuleMethodProxy(self._rpc_proxy, full_name)

    def __call__(self, *args, **kwargs):
        return self._rpc_proxy._call(self._full_name, args, kwargs)

    def __nonzero__(self):
        return bool(self._rpc_proxy)


class MTRPCProxy(object):
    "The actual MTRPC proxy class"

    def __init__(self, req_exchange, req_rk_pattern,
                 resp_exchange=DEFAULT_RESP_EXCHANGE, custom_exceptions=None,
                 json_encoding=DEFAULT_JSON_ENCODING,
                 log=None, loglevel=None, **amqp_params):

        self._req_exchange = req_exchange
        self._req_rk_pattern = req_rk_pattern  # may contain {fields} used in
                                               # _prepare_routing_key()...
        if custom_exceptions is None:
            self._custom_exceptions = {}
        else:
            self._custom_exceptions = custom_exceptions
        self._json_encoding = json_encoding
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
        'Init AMQP communication'
        
        self._log.info('Initializing AMQP channel and connection...')
        self._amqp_conn = amqp.Connection(**amqp_params)
        self._amqp_channel = self._amqp_conn.channel()


    def _amqp_close(self):
        'Close AMQP communication'

        self._log.info('Closing AMQP channel and connection...')
        try:
            try:
                self._amqp_channel.close()
            finally:
                self._amqp_conn.close()
        except Exception:
            self._log.error('Error when trying to close AMQP channel '
                                'or connection. Raising exception...')
            self._log.debug('Exception details', exc_info=True)
            raise


    def _call(self, full_name, call_args, call_kwargs):
        "Remotely call a procedure (RPC-method)"

        all_args = itertools.chain(map(repr, call_args),
                                   ('{0}={1!r}'.format(key, val)
                                    for key, val in call_kwargs.iteritems()))
        self._log.info('* remote call: %s(%s)', full_name, ', '.join(all_args))
        
        if self._closed:
            raise RPCClientError('MTRPCProxy instance is already closed')

        with self._call_lock:
            resp_queue = self._bind_and_consume()
            try:
                msg = self._prepare_msg(full_name, call_args,
                                        call_kwargs, resp_queue)
                routing_key = self._prepare_routing_key(full_name)
                self._amqp_channel.basic_publish(msg,
                                                 exchange=self._req_exchange,
                                                 routing_key=routing_key)
                self._amqp_channel.wait()
                
            finally:
                response = self._response
                self._response = None
                self._amqp_channel.basic_cancel(resp_queue)
                
            if response.id != resp_queue:
                raise RPCClientError("It should not happen! RPC-response id "
                                     "{0!r} differs from RPC-request id {1!r}"
                                     .format(response.id, resp_queue))
            elif response.error:
                self._raise_received_error(response.error)
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
            response_dict = json.loads(msg.body, encoding=self._json_encoding)
            self._response = Response(**utils.kwargs_to_str(response_dict))
        except Exception:
            raise RPCClientError('Could not deserialize message: {0!r}\n{1}'
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
            message_data = json.dumps(
                    request_dict,
                    encoding=self._json_encoding,
            )
            return amqp.Message(
                    message_data,
                    delivery_mode=2,
                    reply_to=resp_queue,
            )
        except Exception:
            raise RPCClientError('Could not serialize request dict: {0!r}\n{1}'
                                 .format(request_dict, traceback.format_exc()))
        

    def _prepare_routing_key(self, full_name):
        split_name = full_name.split('.')
        return self._req_rk_pattern.format(
                full_name=full_name,
                local_name=split_name[-1],
                parentmod_name='.'.join(split_name[:-1]),
                split_name=split_name,
                req_exchange=self._req_exchange,
                resp_exchange=self._resp_exchange,
        )


    def _raise_received_error(self, received_error):
        try:
            try:
                exctype_name = received_error.get('name', '')
                exc_message = received_error.get('message', '')
            except (TypeError, AttributeError):
                exctype_name = '<UNKNOWN!>'
                exc_message = '<UNKNOWN!>'
                raise Exception
                
            try:
                exctype = self._custom_exceptions[exctype_name]
            except KeyError:
                try:
                    exctype = getattr(errors, exctype_name)
                except AttributeError:
                    exctype = getattr(__builtin__, exctype_name)
                    
        except Exception:
            raise RPCClientError(
                    'The response contains unknown/unproper '
                    'error-item: {0!r} -- with message: {1!r}'
                    .format(exctype_name, exc_message))
        else:
            raise exctype(exc_message)
