# mtrpc/server/threads.py
#
# Author: Jan Kaliszewski (zuo)
# Copyright (c) 2010, MegiTeam
#
# Portions of mtrpc.server.threads module were inspired with:
# * QAM 0.2.18 (a Python RPC framework using AMQP, based on Carrot),
#   copyrighted by Christian Haintz and Karin Pichler (2009),
#   FLOSS BSD-licensed;
# * jsonrpc 0.01 (a Python JSON-RPC framework using HTTP), copyrighted
#   by Jan-Klaas Kollhof (2007), FLOSS LGPL-licensed.

"""MTRPC-server-threads classes.

-----------------------------------------------------------------------
Relations beetween RPCManager, RPCResponder and RPCTaskThread instances
-----------------------------------------------------------------------

Starting
^^^^^^^^

The RPCManager thread starts (but doesn't create) the RPCResponder thread
and RPCTaskThread threads (method-call workers).

Stopping
^^^^^^^^

Possible causes of stopping:

* RPCManager thread's .stop() called from another (e.g. the main) thread,

* a fatal error in the RPCManager or RPCResponder thread.

In typical situation the RPCManager thread requests the RPCResponder thread
to stop and waits until it terminates, then terminates itself. In case of
a fatal failure, each of them tries to request the other to terminate (but
still the RPCManager thread -- if not crashed in a strange way -- waits
until the RPCResponder thread terminates).

The RPCResponder thread, when requested (by the RPCManager thread) to stop,
normally waits until all RPCTaskThread threads complete. However it doesn't
if it encounters a fatal error or if the stop request has 'force' flag set
to True (but then suitable warnings with information about uncompleted
tasks are logged).

RPCManager stop() method -- as well as constructors of stop requests (of
`Stopping' namedtuple type) passed by one thread to the other -- take
the arguments:

* reason (str) -- an arbitrary message (to be recorded in the server log),

* loglevel (str) -- one of: 'debug', 'info', 'warning', 'error', 'critical',

* force (bool) -- if true the server responder will not wait for
                  remaining tasks to be completed.

(Additionally RPCManager's stop() takes `timeout' argument -- but it
applies to the caller thread [e.g. the main thread] and *not* the manager/
/responder/task threads themselves).

Obligatorily shared arguments/attributes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It is crucial that *the same* objects are passed to the RPCResponder and
the RPCManager constructors as the arguments:

* task_dict (a dict -- initially empty),

* result_fifo (a Queue.Queue instance -- initially empty),

* mutex (a threading.Lock instance -- initially released).

More info about the classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^

For more info about RPCManager, RPCResponder and RPCTaskThread -- see
their (and their methods) documentation.

------------------------------------------------
AMQP routing keys, exchange and queue names etc.
------------------------------------------------

Manager (consumer) exchange names and (optionally) types
-- defined in config file
(see mtrpc.server documentation [in mtrpc/server/__init__.py file])

Default exchange type:
* <RPCManager instance.default_exchange_type>

Manager (consumer) queue names format:
* <RPCManager instance.queue_prefix> + <a number (starting with 0)> # TODO doc (przejrzec...)

#Manager wake-up exchange name:
#* <RPCManager instance.wakeup_exchange>

#Manager wake-up queue name:
#* <RPCManager instance.queue_prefix>
#  + <RPCManager instance.wakeup_queue_suffix>

#Manager wake-up routing key:
#* <RPCManager instance.wakeup_exchange>

(*Note* that by default RPCManager instance attributes are not set
 -- then corresponding RPCManager class attributes are in use)

Responder (publisher) exchange name:
* <RPCResponder instance.exchange>
  (defaults to mtrpc.common.const.DEFAULT_RESP_EXCHANGE)

Responder exchange type:
* 'direct'

Responder (publisher) queue name:
* got from `reply_to' attribute of request message.

"""



from future_builtins import filter, map, zip

import abc
import functools
import hashlib
import itertools
import logging
import os
import select
import sys
import threading
import time
import traceback
from collections import namedtuple

from amqplib import client_0_8 as amqp
from amqplib.client_0_8 import transport as amqp_transport

from . import methodtree
from ..common import utils
from ..common import encoding
from ..common.const import *
from ..common.errors import *



#
# Auxiliary constants and types
#

MGR_REASON_PREFIX = 'requested by the manager'

Stopping = namedtuple('Stopping', 'reason loglevel force')
BindingProps = namedtuple('BindingProps', 'exchange routing_key '
                                          'access_key_patt '
                                          'access_keyhole_patt')
Task = namedtuple('Task', 'id request_message access_dict '
                          'access_key_patt access_keyhole_patt reply_to')
Result = namedtuple('Result', 'task_id reply_to response_message')
NoResult = namedtuple('NoResult', 'task_id')
RPCRequest = namedtuple('RPCRequest', 'id method params kwparams')



#
# Abstract base classes
#

class ServiceThread(threading.Thread):

    """Abstract class: service thread"""


    __metaclass__ = abc.ABCMeta

    instance_counter = itertools.count(1)


    class EverydayError(Exception):
        "An error that doesn't testify to a bug"


    def __init__(self, *args, **kwargs):

        """General initialization.

        Arguments:

        * <positional and/or keyword arguments> -- will be passed to init()
          method (see: init() docstrings in subclasses);

        Optional keyword-only arguments:

        * log (logging.Logger instance or str) -- server logger or its name
          (defaults to the root logger);

        * <arbitrary keyword arguments> -- that become instance attributes
          (possibly setting/overriding default attributes, e.g. daemon=True)
          after call of init() method but before start of the thread.

        """

        instance_id = next(self.instance_counter)
        if instance_id == 1:
            name = self.__class__.__name__
        else:
            name = '{0}-{1}'.format(self.__class__.__name__, instance_id)

        threading.Thread.__init__(self, name=name)
        self._set_logger(kwargs.pop('log', None))
        try:
            self.stopping = None
            attributes = kwargs.pop('attributes', {})

            # subclass-specific initialization
            self.init(*args, **kwargs)

            # optional after-init customization of instance attributes
            for key, value in attributes.iteritems():
                setattr(self, key, value)
        except:
            self.log.critical('Fatal error during initializing service '
                              'thread %s. Re-raising the exception:',
                              self, exc_info=True)
            raise


    def _set_logger(self, log_arg):
        if log_arg is None:
            logging.basicConfig(format='%(asctime)s %(levelno)s '
                                       '@%(threadName)s: %(message)s',
                                level=logging.INFO)
            self.log = logging.getLogger()
        elif isinstance(log_arg, basestring):
            self.log = logging.getLogger(log_arg)
        else:
            self.log = log_arg
        self.log.debug('The logger of service thread %s is ready '
                       '[as you see :-)]', self)


    def __str__(self):
        return '<{0}>'.format(self.name)


    @abc.abstractmethod
    def init(self):
        "Subclass-specific; called from __init__() (before the thread starts)"


    def run(self):
        "Service thread activity"
        try:
            self.log.info('Service thread started...')
            self.main()
        except:
            self.log.critical('Service thread encountered uncommon error:',
                              exc_info=True)
        finally:
            self.log.info('Service thread terminates...')


    def main(self):
        "Main part of service thread activity"
        try:
            try:
                self.starting_action()
                self.main_loop()
            except self.EverydayError as exc:
                self.log.error("Service thread activity broken with error:"
                               " %s", exc)
                self.log.debug('Exception info:', exc_info=True)
                reason = 'error: {0}'.format(exc)
                self.stopping = Stopping(reason, loglevel='error', force=False)
            except:
                self.log.critical("Service thread activity broken with "
                                  "uncommon error:", exc_info=True)
                reason = 'uncommon error: {0}'.format(sys.exc_info()[1])
                self.stopping = Stopping(reason, loglevel='critical', force=False)
            finally:
                logger_method = getattr(self.log, self.stopping.loglevel)
                logger_method('Service thread is being stopped -- reason: %s',
                              self.stopping.reason)
        finally:
            self.final_action()


    @abc.abstractmethod
    def starting_action(self):
        "Initial action (within the thread, before the main loop)"


    @abc.abstractmethod
    def main_loop(self):
        "Main activity loop, specific to a subclass"


    @abc.abstractmethod
    def final_action(self):
        "Final action (within the thread, after the main loop, at the end)"


    def join_stopping(self, timeout=None):

        """Join to the thread; to be called from another thread.

        timeout == None  =>  wait until the thread terminates
        timeout == x     =>  wait, but no longer than x seconds
        timeout == 0     =>  don't wait (return immediately)

        """

        if timeout == 0 and self.is_alive():
            self.log.info("Service thread %s was requested to stop, timeout "
                          "set to 0 => we'll not waiting for that stop", self)
            return False

        else:
            self.log.debug("Joining thread %s...", self)
            self.join(timeout)
            stopped = not self.is_alive()
            if stopped:
                self.log.info('Service thread %s is stopped', self)
            else:
                self.log.error("Timeout (%ss) encountered while waiting for "
                               "service thread %s to stop; that thread is "
                               "still alive => we'll not waiting for its stop",
                               timeout, self)
            return stopped



class AMQPClientServiceThread(ServiceThread):

    """Abstract class: service thread being AMQP client"""

    connect_attempts = 3      # attempts to (re)connect (0 means infinity)
    try_action_attempts = 2   # attempts to (re)try action (0 means infinity)
    reconnect_interval = 1    # in seconds


    class StoppingException(Exception):
        "An Exception not caught by @retry wrapper"

    class AMQPError(ServiceThread.EverydayError):
        "A problem with AMQP connection"


    def init(self, amqp_params):

        """Initialization specific to AMQP client.

        Argument:
        * amqp_params -- dict of keyword arguments for amqp.Connection(),
          see docs of amqplib.client_0_8.Connection.__init__() for details.

        """

        amqp_params.update(amqp_params.pop('kwargs', {}))
        self._amqp_params = amqp_params
        self._is_connected = False


    def starting_action(self):
        "Initial action (within the thread, before the main loop)"
        try:
            self.amqp_init()
        except self.AMQPError:
            self.log.error('AMQP initialization failed. Raising AMQPError...')
            raise


    def amqp_init(self):
        "Init AMQP communication"
        self.log.info('Initializing AMQP channel and connection...')
        self.amqp_conn = self._new_amqp_conn(self._amqp_params)
        self.amqp_channel = self.amqp_conn.channel()
        self.amqp_channel.basic_qos(prefetch_size=0, prefetch_count=1, a_global=False)
        self._is_connected = True


    def _new_amqp_conn(self, amqp_params):

        "Create and return AMQP connection, retry a number of times if failed"

        host_descr = amqp_params.get('host', '<default adress setting>')

        attempt_counter = itertools.count(1)
        while True:
            self.log.info('Connecting to AMQP broker at %s...', host_descr)
            try:
                conn = amqp.Connection(**amqp_params)
                utils.setkeepalives(conn.transport.sock)
                return conn
            except Exception as exc:
                self.log.warning('Connection failed')
                self.log.debug('Exception info:', exc_info=True)
                attempt_nr = next(attempt_counter)
                if attempt_nr == self.connect_attempts:
                    break
                time.sleep(self.reconnect_interval)
                self.log.warning('Trying to reconnect...')

        self._is_connected = False
        raise self.AMQPError('Giving up after {0} unsuccessful '
                             'attempts to connect to AMQP broker'
                             .format(attempt_nr))


    def amqp_close(self):
        "Close AMQP communication"
        if self._is_connected:
            self.log.info('Closing AMQP channel and connection...')
            try:
                try:
                    self.amqp_channel.close()
                finally:
                    self.amqp_conn.close()
            except Exception as exc:
                self.log.warning('Error when trying to close '
                                 'AMQP channel or connection: %s', exc)
                self.log.debug('Exception info:', exc_info=True)
            self._is_connected = False

    final_action = amqp_close


    @staticmethod
    def retry(action):

        "Decorate a method with AMQP-reinitializing-and-retrying wrapper"

        @functools.wraps(action)
        def reinit_retry_wrapper(self, *args, **kwargs):
            attempt_counter = itertools.count(1)
            while True:
                self.log.debug('Action %r will be run...', action)
                try:
                    return action(self, *args, **kwargs)
                except self.StoppingException:
                    "TODO: log"
                    raise
                except Exception as exc:
                    self.log.warning('Error during action %r', action)
                    self.log.warning('Exception info:', exc_info=True)
                    #self.log.debug('Exception info:', exc_info=True)  !TODO! docelowo raczej tak
                    attempt_nr = next(attempt_counter)
                    if attempt_nr == self.try_action_attempts:
                        err = exc  # (<- Py3.x compatibile way)
                        break
                    self.log.warning('Closing and re-initiating connection...')
                    self.amqp_close()
                    time.sleep(self.reconnect_interval)
                    try:
                        self.amqp_init()
                    except self.AMQPError:
                        self.log.error('Attempt to reinitialize AMQP '
                                       'communication (after problem: %r) '
                                       'failed. Raising AMQPError...', exc)
                        raise
            self.log.error('Action %r unsuccessfully tried %s times, '
                           'Re-raising the exception...', action, attempt_nr)
            raise err

        return reinit_retry_wrapper



#
# The actual MTRPC server classes
#

#
# Manager + consumer class

class RPCManager(AMQPClientServiceThread):

    """RPC manager (gets requests, spawn tasks, spawns/stops responder).

    After instance creation (see init() method docstring) call start() method
    to start a manager thread (remember to start a responder first).

    """

    default_exchange_type = 'topic'
    # !TODO! ...
    #queue_prefix = 'RPCManager'

    # !TODO! ...
    #wakeup_exchange = 'RPCManager_wakeup_exchange'
    #wakeup_exchange = 'amq.direct'
    #wakeup_queue_suffix = 'wakeup'
    #wakeup_routing_key = 'wakeup'
    sel_timeout = 60

    instance_counter = itertools.count(1)


    #
    # Auxiliary classes related to TCP-transport of AMQP method reader

    class _AbstractTransportWrapper(object):

        read_frame = amqp_transport._AbstractTransport.read_frame.__func__

        def __init__(self, manager, transport):
            self.RTW__orig_transport = transport

        def RTW__get_unwrapped(self):
            return self.RTW__orig_transport


    class SSLTransportWrapper(_AbstractTransportWrapper):

        _read = amqp_transport.SSLTransport._read.__func__

        def __init__(self, manager, transport):
            self.sslobj = RPCManager.SockWrapper(manager,
                                                 transport.sock, 'read')
            super(self.__class__, self).__init__(manager, transport)


    class TCPTransportWrapper(_AbstractTransportWrapper):

        _read = amqp_transport.TCPTransport._read.__func__

        def __init__(self, manager, transport):
            self.sock = RPCManager.SockWrapper(manager,
                                               transport.sock, 'recv')
            super(self.__class__, self).__init__(manager, transport)
            self._read_buffer = transport._read_buffer

        def RTW__get_unwrapped(self):
            transport = super(self.__class__, self).RTW__get_unwrapped()
            transport._read_buffer = self._read_buffer
            return transport


    class SockWrapper(object):

        def __init__(self, manager, sock, reading_method_name):
            self.SW__manager = manager
            self.SW__sock = sock
            self.SW__sock_reading_method = getattr(sock, reading_method_name)
            setattr(self, reading_method_name, self.SW__wrapped_reading_method)

        def SW__wrapped_reading_method(self, *args, **kwargs):
            resp_stopping_fd_r = self.SW__manager.resp_stopping_fd_r
            orig_timeout = self.SW__sock.gettimeout()
            self.SW__sock.settimeout(0)  # non-blocking
            try:
                while True:
                    sel = select.select([self.SW__sock, resp_stopping_fd_r],
                                        [], [],
                                        self.SW__manager.sel_timeout)[0]
                    if resp_stopping_fd_r in sel:
                        try:
                            os.read(resp_stopping_fd_r, 1)
                        except EnvironmentError as exc:
                            if exc.errno in (errno.EAGAIN,
                                             errno.EWOULDBLOCK):
                                # nothing really happened -> continue...
                                # (see: BUGS section of select(2) manpage
                                # about spurious readiness notifications)
                                "TODO: log"
                            else:
                                # responder-stopping pipe broken?
                                raise (self.SW__manager
                                      ).RespStopping(self.SW__manager,
                                                     traceback.format_exc())
                        else:
                            # responder-stopping message (closed pipe)
                            raise (self.SW__manager
                                  ).RespStopping(self.SW__manager, None)
                    if self.SW__sock in sel:
                        try:
                            return self.SW__sock_reading_method(*args,
                                                                **kwargs)
                        except EnvironmentError as exc:
                            if exc.errno in (errno.EAGAIN,
                                             errno.EWOULDBLOCK):
                                # nothing really happened -> continue...
                                # (see: BUGS section of select(2) manpage
                                # about spurious readiness notifications)
                                "TODO: log"
                            else:
                                raise
            finally:
                self.SW__sock.settimeout(orig_timeout)


    class RespStopping(AMQPClientServiceThread.StoppingException):
        "Exception raised when stopping request from the responder received"
        def __init__(self, manager, *args, **kwargs):
            manager.unwrap_the_reader_transport()
            super(self.__class__, self).__init__(*args, **kwargs)


    #
    # Methods

    def init(self,
             amqp_params,           # dict (params for AMQP connection...)
             bindings,              # seq of lists of BindingProps instances  # !TODO! to idea stara, ale jara...
             exchange_types,        # dict of exchange: 'topic'|'direct' items
             client_id,             # string: globally unique client name    # !TODO! ...
             rpc_tree,              # methodtree.RPCTree instance
             responder,             # RPCResponder instance (not started)
             task_dict,             # empty dict
             result_fifo,           # Queue.Queue instance
             mutex,                 # threading.Lock instance
             final_callback=None):  # callable object or None

        """Manager specific initalization.

        Arguments (to be used for instance creation together with
        ServiceThread-specific arguments, see: ServiceThread.__init__()):

        * amqp_params -- dict of keyword arguments for amqp.Connection(),
          see docs of amqplib.client_0_8.Connection.__init__() for details;

        * bindings -- sequence (e.g. list) of BindingProps instances;

        * exchange_types -- dict that maps AMQP exchanges (str) to exchange
          types (str: 'topic' or 'direct'...);

        * client_id -- string: globally unique client id, goes into exchange   !TODO! ...
          and queue names;

        * rpc_tree -- methodtree.RPCTree instance;

        * responder -- RPCResponder instance (not started);

        * task_dict -- empty dict, must be the same that the responder has
          been created with -- see: RPCResponder.init();

        * result_fifo -- Queue.Queue instance, must be the same that
          the responder has been created with;

        * mutex -- threading.Lock instance, must be the same that
          the responder has been created with;

        * final_callback [optional argument, defaults to None] -- to be called
          from the manager thread, just before termination of it; the callback
          will be called with the thread object as (the only one) argument
          -- if it raises TypeError it will be called again, without any
          arguments (so it is allowed to take or not to take the argument).

        """

        # TODO:
        #* bindings -- sequence of lists of BindingProps instances; instances
        #  being items of the same list will be used with the same AMQP queue;


        AMQPClientServiceThread.init(self, amqp_params)

        self._queues = []   # queue names (in sorted order)
        self._queues2bindings = {}   # queue names and their binding props
        # !TODO!
        #for i, binding_props in enumerate(bindings):
        #    queue = '.'.join([self.queue_prefix, str(i)])
        #    self._queues.append(queue)
        #    self._queues2bindings[queue] = BindingProps._make(binding_props)
        for binding_props in bindings:
            unique_id = hashlib.sha1('{0.exchange}|{0.routing_key}'.format(
                binding_props)).hexdigest()[0:6]
            queue = '.'.join(['mtrpc_queue', client_id, unique_id])
            self._queues.append(queue)
            self._queues2bindings[queue] = BindingProps._make(binding_props)
        #
        #self._wakeup_queue = '.'.join([self.queue_prefix,
        #                               self.wakeup_queue_suffix])
        #self._queues.append(self._wakeup_queue)
        #(self._queues2bindings[self._wakeup_queue]
        #) = BindingProps(self.wakeup_exchange, self.wakeup_routing_key,
        #                 None, None)
        self.client_id = client_id
        #

        self._exchange_types = exchange_types
        self.rpc_tree = rpc_tree

        # responder and attributes shared with it
        self.responder = responder
        if not (responder.task_dict is task_dict
                and responder.result_fifo is result_fifo
                and responder.mutex is mutex):
            raise ValueError("Manager and responder *must* share"
                             " `task_dict', `result_fifo' and "
                             " `mutex' attributes")
        self.task_dict = task_dict  # (maps task ids to not completed tasks)
        self.result_fifo = result_fifo  # (<- shared also with task threads)
        self.mutex = mutex
        self.responder.manager = self
        (self.resp_stopping_fd_r,
         self.responder.stopping_fd_w) = os.pipe()
        self.responder.start()

        self.final_callback = final_callback
        self._task_id_gen = itertools.count(1)


    def amqp_init(self):

        "Init AMQP communication, bind queues/exchanges, declare consuming"

        AMQPClientServiceThread.amqp_init(self)

        self.log.info('Declaring and binding AMQP queues/exchanges...')
        try:
            for i, queue in enumerate(self._queues):
                props = self._queues2bindings[queue]  # binding properties
                self.amqp_channel.queue_declare(queue=queue,
                                                durable=True,
                                                auto_delete=True)
                (exchange_type
                ) = self._exchange_types.get(props.exchange,
                                             self.default_exchange_type)
                self.amqp_channel.exchange_declare(exchange=props.exchange,
                                                   type=exchange_type,
                                                   durable=True,
                                                   auto_delete=False)
                self.amqp_channel.queue_bind(queue=queue,
                                             exchange=props.exchange,
                                             routing_key=props.routing_key)
                self.amqp_channel.basic_consume(queue=queue,
                                                no_ack=False,
                                                callback=self.get_and_go,
                                                consumer_tag=queue)  # (<-yes)
            # !TODO!
            #(wakeup_q, _, _
            #) = self.amqp_channel.queue_declare(durable=False,
            #                                    auto_delete=True)
            #
            #self.wakeup_routing_key = '.'.join(['wakeup', self.client_id, wakeup_q])
            #
            #self.amqp_channel.exchange_declare(exchange=self.wakeup_exchange,
            #                                   type='direct',
            #                                   durable=False,
            #                                   auto_delete=True)
            #self.amqp_channel.queue_bind(queue=wakeup_q,
            #                             exchange=self.wakeup_exchange,
            #                             routing_key=self.wakeup_routing_key)
            #self.amqp_channel.basic_consume(queue=wakeup_q,
            #                                no_ack=False,
            #                                callback=self.get_and_go,
            #                                consumer_tag="_wakeup_queue")  # (<-yes)
            #
            #(self._queues2bindings["_wakeup_queue"]
            #) = BindingProps(self.wakeup_exchange, self.wakeup_routing_key,
            #                 None, None)
            #
        except Exception:
            raise self.AMQPError(traceback.format_exc())

        self.wrap_the_reader_transport()


    def wrap_the_reader_transport(self):
        # wrap the transport object used to receive AMQP messages from the broker
        method_reader = self.amqp_conn.method_reader
        transport = method_reader.source
        if isinstance(transport, amqp_transport.SSLTransport):
            method_reader.source = self.SSLTransportWrapper(self, transport)
        elif isinstance(transport, amqp_transport.TCPTransport):
            method_reader.source = self.TCPTransportWrapper(self, transport)
        else:
            raise TypeError('Only {0} and {1} transport'
                            ' classes are supported'
                            .format(amqp_transport.SSLTransport,
                                    amqp_transport.TCPTransport))
        self.log.debug('{0} transport object wrapped with {1}'
                       .format(transport, method_reader.source))


    def unwrap_the_reader_transport(self):
        # unwrap the transport object used to receive AMQP messages from the broker
        method_reader = self.amqp_conn.method_reader
        method_reader.source = method_reader.source.RTW__get_unwrapped()
        self.log.debug('{0} transport object unwrapped'
                       .format(method_reader.source))


    def main_loop(self):

        "Main activity loop: consume messages, spawn tasks"

        try:
            while not (self.stopping or self.responder.stopping):
                self.wait_for_msg()
        except self.RespStopping:
            "TODO: log"
            '''TODO: check for exception info [e.g. broken responder-stopping pipe?]
            => if exists, change stopping reason info'''

        if not self.stopping:
            # stopping originally caused by the responder
            reason = self.responder.stopping.reason
            self.stopping = self.responder.stopping._replace(
                    reason='requested by the responder {0} ({1})'
                    .format(self.responder, reason)
            )


    @AMQPClientServiceThread.retry
    def wait_for_msg(self):
        "Consume AMQP message when it arrives (calling get_and_go() callback)"
        return self.amqp_channel.wait()


    def get_and_go(self, msg):

        "AMQP consume callback: prepare a task and start a task thread"

        queue = msg.delivery_info['consumer_tag']  # (queue == consumer tag)
        #if queue == "_wakeup_queue":  # !TODO! ...
        #if queue == self._wakeup_queue:
        #    self.amqp_channel.basic_ack(msg.delivery_tag)
        #    return
        binding_props = self._queues2bindings[queue]
        reply_to = msg.properties['reply_to']
        access_dict = self.create_access_dict(queue,
                                              binding_props,
                                              msg.delivery_info,
                                              reply_to)
        task_id = next(self._task_id_gen)
        task = Task(task_id,
                    request_message=msg.body,
                    access_dict=access_dict,
                    access_key_patt=binding_props.access_key_patt,
                    access_keyhole_patt=binding_props.access_keyhole_patt,
                    reply_to=reply_to)

        task_recorded = False
        try:
            with self.mutex:  # see: RPCResponder's main_loop() + final_action()
                if self.responder.stopping:
                    return
                self.task_dict[task_id] = task
                #self.amqp_channel.basic_ack(msg.delivery_tag)
                task_recorded = True
                self.log.debug('Message received, task %s created', task)
                task_thread = RPCTaskThread(task,
                                            self.rpc_tree,
                                            self.result_fifo,
                                            self.log)
                task_thread.start()
                self.log.debug('%s created and started', task_thread)
        finally:
            if task_recorded:
                self.amqp_channel.basic_ack(msg.delivery_tag)

        return task


    @staticmethod
    def create_access_dict(queue, binding_props, delivery_info, reply_to):

        "Prepare the dict to be used to format actual key and keyhole strings"

        rk = binding_props.routing_key
        msg_rk = delivery_info['routing_key']

        # (Please note such formatting possibilites as:
        # "{rk_split[2]}", "{delivery_info[redelivered]" etc.)
        return dict(
            exchange = binding_props.exchange,      # exchange name
            queue = queue,                          # queue name
            rk = rk,                                # consumer's routing key
            rk_split = rk.split('.'),                      # ^ split using '.'
            rk_revsplit = list(reversed(rk.split('.'))),          # ^ reversed
            msg_rk = msg_rk,                        # original msg routing key
            msg_rk_split = msg_rk.split('.'),              # ^ split using '.'
            msg_rk_revsplit = list(reversed(msg_rk.split('.'))),  # ^ reversed
            delivery_info = delivery_info,          # msg delivery info dict
            reply_to = reply_to,                    # msg reply-to info
        )


    def final_action(self):

        "Close AMQP conn, request the responder to stop, run final callback"

        try:
            try:
                self.amqp_close()

                # (we don't need to use mutex, because possible
                # redundant stop request is harmless)
                if not self.responder.stopping:
                    # request the responder to stop
                    self._stop_the_responder(self.stopping)

                # wait until the responder terminates
                self.responder.join_stopping(None)

            finally:
                os.close(self.resp_stopping_fd_r)

        finally:
            if self.final_callback is not None:
                self.log.info('Final callback %r is set, calling it...',
                              self.final_callback)
                try:
                    try:
                        self.final_callback(self)
                    except TypeError:
                        self.final_callback()
                except:
                    self.log.exception('Error while tried to call '
                                       'the final callback:')


    def stop(self, reason='manual stop', loglevel='info', force=False,
             timeout=None):

        """Stop the service thread; to be called from another thread.

        force=True  => the responder will not wait for
                       remaining tasks to being completed

        timeout=None  => wait until the server terminates
        timeout=<i>   => wait, but no longer than <i> seconds
        timeout=0     => don't wait, return immediately

        """

        self.stopping = Stopping(reason, loglevel, force)
        self._stop_the_responder(self.stopping)
        return self.join_stopping(timeout)


    def _stop_the_responder(self, stopping):
        "Request the responder to stop"
        if threading.current_thread() is self:
            (responder_stopping
            ) = stopping._replace(reason='{0} {1} ({2})'
                                         .format(MGR_REASON_PREFIX,
                                                 self, stopping.reason))
        else:
            responder_stopping = stopping
        self.result_fifo.put(responder_stopping)



#
# Responder (publisher of results) class

class RPCResponder(AMQPClientServiceThread):

    """RPC responder (puts task results into appropriate AMQP queues).

    After creation of an instance (see init() method docstring) it should
    be passed to the manager constructor (the manager's init() starts the
    responder itself).

    """

    exchange = DEFAULT_RESP_EXCHANGE
    instance_counter = itertools.count(1)


    def init(self, amqp_params, task_dict, result_fifo, mutex):

        """Responder specific initalization.

        Arguments (to be used for instance creation together with
        ServiceThread-specific arguments, see: ServiceThread.__init__()):

        * amqp_params -- dict of keyword arguments for amqp.Connection(),
          see docs of amqplib.client_0_8.Connection.__init__() for details;

        * task_dict -- empty dict, must be the same that the manager will be
          created with -- see: RPCManager.init();

        * result_fifo -- Queue.Queue instance, must be the same that
          the manager will be created with;

        * mutex -- threading.Lock instance, must be the same that the manager
          will be created with.

        """

        AMQPClientServiceThread.init(self, amqp_params)

        # objects shared with the manager:
        self.task_dict = task_dict
        self.result_fifo = result_fifo  # (<- ...as well as with task threads)
        self.mutex = mutex


    def amqp_init(self):

        "Init AMQP communication and declare the exchange"

        AMQPClientServiceThread.amqp_init(self)
        self.log.info('Declaring responder exchange...')
        try:
            self.amqp_channel.exchange_declare(exchange=self.exchange,
                                               type='direct',
                                               durable=True,
                                               auto_delete=False)
        except Exception:
            raise self.AMQPError(traceback.format_exc())


    def main_loop(self):

        "Main activity loop: getting and sending responses with results"

        while not (self.stopping
                   and (self.stopping.force or not self.task_dict)):
            result = self.result_fifo.get()
            if isinstance(result, Stopping):
                with self.mutex:
                    self.stopping = result
                continue
            msg = amqp.Message(result.response_message, delivery_mode=2)
            self.reply(result.reply_to, msg)
            del self.task_dict[result.task_id]


    @AMQPClientServiceThread.retry
    def reply(self, reply_to, msg):
        "Send a response to RPC client (via AMQP broker)"
        self.amqp_channel.basic_publish(msg, exchange=self.exchange,
                                        routing_key=reply_to)


    def final_action(self):

        "Wake up the manager, close the connection, check state of tasks"

        try:
            os.close(self.stopping_fd_w)
            #if (self._is_connected
            #      and not self.stopping.reason.startswith(MGR_REASON_PREFIX)):
            #    self.manager_wakeup()

        finally:
            self.amqp_close()

            with self.mutex:
                # take a snapshot of the current state of tasks
                not_completed = self.task_dict.values()
                task_threads = [thread for thread in threading.enumerate()
                                if isinstance(thread, RPCTaskThread)]
            if not_completed:
                if task_threads:
                    self.log.warning('%d RPC tasks not completed. %d task '
                                     'threads are still working -- '
                                     'their results to be dropped '
                                     '(responses will not be sent)',
                                     len(not_completed), len(task_threads))
                    if len(not_completed) != len(task_threads):
                        self.log.warning('These numbers are not equal '
                                         '(some task threads probably '
                                         'have crashed -- so some responses '
                                         'has not been sent)')
                else:
                    self.log.warning('%d RPC tasks not completed. No working '
                                     'task threads (some probably have '
                                     'crashed -- so some responses has not '
                                     'been sent)', len(not_completed))
                self.log.debug('Not completed tasks: %s.\n'
                               'Remaining task threads: %s',
                               ', '.join(sorted(map(str, not_completed))),
                               ', '.join(sorted(map(str, task_threads))))


    #@AMQPClientServiceThread.retry
    #def manager_wakeup(self):
    #    "Send 'wakeup' message to the manager (then it'll be able to stop)"
    #    manager_wakeup_msg = amqp.Message('wakeup', delivery_mode=2)
    #    self.amqp_channel.basic_publish(manager_wakeup_msg,
    #                                    exchange=self.manager.wakeup_exchange,
    #                                    routing_key
    #                                    =self.manager.wakeup_routing_key)



#
# Worker class (its instances are created by manager)

class RPCTaskThread(threading.Thread):

    "RPC task thread (deserializes and executes requests, serializes results)"

    instance_counter = itertools.count(1)


    def __init__(self, task, rpc_tree, result_fifo, log):
        task_thread_id = next(self.instance_counter)
        threading.Thread.__init__(self, name='TaskThread-{0}/task-{1}'
                                             .format(task_thread_id, task.id))
        self.task = task
        self.rpc_tree = rpc_tree
        self.result_fifo = result_fifo
        self.log = log


    def __str__(self):
        return '<{0}>'.format(self.name)


    def run(self):

        "Thread activity"

        self.log.debug('Task thread started')
        try:
            task = self.task
            exc_type = None
            try:
                self.log.debug('Deserializing request message: %r...',
                               task.request_message)
                request = self._deserialize_request(task.request_message)
                (rpc_method
                ) = self.rpc_tree.try_to_obtain(request.method,
                                                task.access_dict,
                                                task.access_key_patt,
                                                task.access_keyhole_patt,
                                                required_type
                                                =methodtree.RPCMethod)
                (origin_method_pymod
                ) = self.rpc_tree.method_names2pymods[request.method]

            except RPCNotFoundError:
                self.log.debug('Exception related to access check:',
                               exc_info=True)
                exc_type, exc = sys.exc_info()[:2]

            except RPCError:
                self.log.debug('Exception related to malformed request:',
                               exc_info=True)
                exc_type, exc = sys.exc_info()[:2]

            except Exception:
                self.log.critical('Server misconfigured or other problem:',
                                  exc_info=True)
                exc_type = RPCInternalServerError
                exc = RPCInternalServerError('Internal server error')

            if exc_type is None:
                self.log.info('Calling %s%s', request.method,
                               rpc_method.format_args(request.params, request.kwparams))
                try:
                    result = rpc_method(# the actual arguments (params):
                                        args=request.params,
                                        kwargs=request.kwparams,
                                        # + access-related information:
                                        access_dict=task.access_dict,
                                        access_key_patt=task.access_key_patt,
                                        access_keyhole_patt
                                        =task.access_keyhole_patt)

                except RPCMethodArgError:
                    exc_type, orig_exc = sys.exc_info()[:2]
                    self.log.debug('Bad user arguments (params/kwparams) '
                                   'for %r RPC-method call. Exception info:',
                                   request.method, exc_info=True)
                    exc_args = list(orig_exc.args)
                    exc_args[0] = exc_args[0].format(name=request.method)
                    exc = exc_type(*exc_args)

                except methodtree.BadAccessPatternError:
                    self.log.error('Bad access key or access keyhole '
                                   'pattern stated when calling RPC-method '
                                   '%r. Exception info:', exc_info=True)
                    exc_type = RPCInternalServerError
                    exc = RPCInternalServerError('Internal server error')

                except MethodExcWrapper as meth_exc_wrapper:
                    log_entry = ('Exception raised during '
                                 'execution of RPC-method %r: %s',
                                 request.method, meth_exc_wrapper)
                    self.log.debug(*log_entry)
                    module_log = getattr(origin_method_pymod, RPC_LOG, None)
                    if module_log is not None:
                        module_log.error(*log_entry)
                    exc_type = meth_exc_wrapper.wrapped_exc_type
                    exc = meth_exc_wrapper.wrapped_exc

                except:
                    self.log.error('Unexpected exception during '
                                   'execution of RPC-method %r (to '
                                   'be sent as RPCInternalServerError):',
                                   request.method, exc_info=True)
                    # for security reasons: to be sent as a misterious
                    # RPCInternalServerError without exception details
                    exc_type = RPCInternalServerError
                    exc = RPCInternalServerError('Internal server error')

                else:
                    response_dict = dict(result=result,
                                         error=None,
                                         id=request.id)
                    self.log.info('%s call completed: %s',
                                   request.method, rpc_method.format_result(result))

            if exc_type is not None:
                try:
                    request_id = request.id
                except NameError:
                    self.log.warning("Could not get id from the request. "
                                     "Using task's `reply to' instead...")
                    request_id = task.reply_to  # should be equal to reqest.id
                error = dict(name=exc_type.__name__, message=str(exc), data=exc.__dict__)
                response_dict = dict(result=None,
                                     error=error,
                                     id=request_id)

            response_message = self._serialize_response(response_dict)
            result = Result(task.id, task.reply_to, response_message)
            self.result_fifo.put(result)
            self.log.debug('Result %r put into result fifo', result)

        except:
            self.log.critical('Uncommon error (no result sent, '
                              'task thread terminates):', exc_info=True)

        else:
            self.log.debug('Task completed, task thread terminates...')


    def _deserialize_request(self, request_message):
        try:
            message_data = encoding.loads(request_message)
        except ValueError:
            raise RPCDeserializationError(request_message)

        try:
            message_data.setdefault('kwparams', {})
            request = RPCRequest(**utils.kwargs_to_str(message_data))

            # for security reasons: strict type tests, not isinstance()-based
            if not (type(request.method) is str
                    or type(request.method) is unicode):
                raise TypeError
            if not type(request.params) is list:
                raise TypeError
            if not type(request.kwparams) is dict:
                raise TypeError
            request = request._replace(method=str(request.method))
        except (TypeError, AttributeError, UnicodeError):
            raise RPCInvalidRequestError(message_data)

        if request.id is None:
            raise RPCNotificationsNotImplementedError(message_data)

        return request


    def _serialize_response(self, response_dict):
        try:
            return encoding.dumps(response_dict)
        except TypeError:
            error = dict(name='RPCServerSerializationError',
                         message='Result not serializable')
            err_response_dict = dict(result=None, error=error,
                                     id=response_dict['id'])
            return encoding.dumps(err_response_dict)
