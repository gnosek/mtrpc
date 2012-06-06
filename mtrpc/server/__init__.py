#!/usr/bin/env python
#
# Author: Jan Kaliszewski (zuo)
# Copyright (c) 2010, MegiTeam

"""MegiTeam-RPC (MTRPC) framework -- the server part

================
A simple example
================

* Server script:

    from mtrpc.server import MTRPCServerInterface

    # configure and start the server -- then wait for KeyboardInterrupt
    # exception or OS signals specified in the config file...

    MTRPCServerInterface.configure_and_start(
            config_path='server_simple_example_conf.json',
            loop_mode=True,    # <- stay there and wait for OS signals
            final_callback=MTRPCServerInterface.restart_on,
    )

* Config file -- server_simple_example_conf.json:

    {
        "rpc_tree_init": {
            "imports": ["mtrpc.server.sysmethods as system"],
            "paths": ["my_module.py"]
        },
        "amqp_params": {
            "host": "localhost:5672",
            "userid": "guest",
            "password": "guest",
            "virtual_host": "/"
        },
        "bindings": [
            ["request_amqp_exchange", "request_amqp_routing_key", "", ""]
        ],
        "os_settings": {
            "daemon": false,
            "signal_actions": {
                "SIGHUP": "restart",
                "SIGTERM": "exit"
            },
            "sig_stopping_timeout": 45
        }
    }

* RPC-module definition -- my_module.py:

    import my_submodule

    __rpc_doc__ = u'Very sophisticated RPC module'
    __rpc_methods__ = '*'

    def add(x, y):
        u"Add one argument to the other"
        return x + y

    # one of the special _access_* arguments is used --
    # passed to the method on the server side, not seen by the client:
    def tell_the_rk(_access_dict):
        u"Tell the AMQP routing key what the client used"
        return ("You sent your request using the following "
                "routing key: '{0}'".format(_access_dict['msg_rk']))

* Another RPC-module definition -- my_submodule.py:

    from mtrpc.common.errors import raise_exc

    __rpc_doc__ = u'Another very sophisticated RPC module'
    __rpc_methods__ = 'mul', 'div'

    def mul(x, y):
        u"Multiply one argument by the other"
        return x * y

    def div(x, y):
        u"Divide one argument by the other"
        try:
            return float(x) / y
        except ZeroDivisionError as exc:
            # to indicate that the exception can be safely sent (is not
            # unexpected and its message does not compromise any secret)
            # we raise it explicitly using mtrpc.common.errors.raise_exc()
            raise_exc(exc)

Now --

* run an AMQP broker (that listens at localhost:5672 and accepts the
  'guest' user with the 'guest' password, for '/' virtual host),
* run the above server script,
* run the client example script from mtrpc.client module documentation

-- and enjoy! :-)

============
More details
============

To run your MTRPC server:

* write your server script, using MTRPCServerInterface class,
* prepare the configuration file,
* define your RPC-modules and methods,
* run an AMQP broker and then -- your server script.

There is also possibility to instantiate the classes from
mtrpc.server.methodtree and mtrpc.server.threads directly (without using
MTRPCServerInterface)-- but it would be a rather unnecessary effort
("reinventing the wheel") so we don't document this way of setup [if you
are interested in it anyway, look at the code and docstrings of
mtrpc.server.methodtree and mtrpc.server.threads classes/functions].

-----------------
Basic terminology
-----------------

* RPC-tree -- methodtree.RPCTree instance being a dict-like but also
  tree-like container of RPC-modules and RPC-methods;

* RPC-module -- methodtree.RPCModule instance being a dict-like container
  of other RPC-modules and RPC-methods; each RPC-module has been created
  from one of Python modules specified in the config (in the "paths" or
  "imports" list) or from one of their Python submodules, or frpm one of
  their consecutive Python submodules... and so on recursively;
  RPC-modules hierarchy (and the RPC-tree) reflects membership [importing]
  hierarchy of those Python modules;

* RPC-method -- methodtree.RPCMethod instance being a callable object
  wrapping the actual callable being a member (attribute) of one of Python
  modules specified in config (mentioned above);

* full name -- an absolute.dot.separated.name of an RPC-module or
  RPC-method e.g. 'my_module.my_submodule.div'; to obtain an RPC-module
  or RPC-method from the RPC-tree, you need to use the full name as a key;

* local name -- the rightmost part of a full name, e.g. 'div'; to obtain
  an RPC-module or RPC-method from its parent RPC-module, you need to use
  the local name as a key;

* manager -- threads.RPCManager instance, the service thread responsible
  for receiving RPC-requests and spawning task threads that make RPC-method
  calls (see: threads and threads.RPCManager documentation);

* responder -- threads.RPCResponder instance, the service thread responsible
  for obtaining RPC-responses from task threads and sending them to the
  client (see: threads and threads.RPCResponder documentation).

-------------------------------------------------
MTRPCServerInterface class methods and attributes
-------------------------------------------------

See: MTRPCServerInterface class documentation.

-----------------------------------------------
Server configuration file structure and content
-----------------------------------------------

MTRPCServerInterface.load_config() loads configuration from a JSON-formatted
file which content, after deserialization, would be a dict of the following
items:

* "rpc_tree_init": a dict (an obligatory item) -- containing:

  * "paths": a list (empty by default)

  * "imports": a list (empty by default)

  -- both are lists of strings specifying Python modules that define
     candidates for the highest level RPC-modules i.e. direct descentands
     of the RPC-tree root (see the next section: "RPC-modules and
     RPC-methods definitions and usage");

     each string included in the "paths" list can have either of the two
     forms:
        * "/path/to/python/module.py" or
        * "/path/to/python/module.py as rpc_module_name";

     each string included in the "imports" list can have either of the two
     forms:
        * "python_module_name" or
        * "python_module_name as rpc_module_name";

  * "postinit_kwargs": a dict containing keyword arguments to be passed
    to post-init callable objects (see: the next section); the default
    post-init callable, defined as mtrpc.common.utils.basic_postinit(),
    makes use of two arguments defined here (the default values mentioned
    below will be used for missed items/subitems):

    * "logging_settings": a dict containing logging settings for module
      loggers (that are something other than the server logger):

      * "mod_logger_pattern": a string (empty by default) being the name
        of the logger for a particular RPC-module (empty name means that
        the root logger will be used; see: Python stdlib `logging' module)
        -- that name can contain "{full_name}" substring which will be
        substituted with the actual full name of the RPC-module;

      * "level": a string (default: "info") specifying the logging level
        -- one of the following (starting with the most verbose option):
        "debug", "info", "warning", "error", "critical";

      * "handlers": a list of dicts (default: 1-element list containing
        a dict equal to mtrpc.common.consts.DEFAULT_LOG_HANDLER_SETTINGS)
        of the following items:

        * "cls" -- name of logging.Handler subclass being a member of the
          Python stdlib `logging' or `logging.handlers' module -- e.g.
          "StreamHandler", "RotatingFileHandler", "SocketHandler",
          "SMTPHandler" or any other...

        * "kwargs" -- keyword arguments for the class-specific constructor;

        * "level" -- logging level of the handler, one of: "debug",
          "info", "warning", "error", "critical" (default: "info");

        * "format" -- logging format, default:
          "%(asctime)s %(name)s:%(levelno)s @%(threadName)s: %(message)s"

        See Python stdlib `logging' module for the description of
        constructor arguments and other explanations.

      * "propagate": False (default) or True -- whether logged messages
        should be propagated up (again -- see Python stdlib `logging');

      * "custom_mod_loggers": a dict (empty by default) mapping full
        RPC-module names to dicts similar to the present "logging_settings"
        dict -- with "mod_logger" item instead of "mod_logger_pattern"
        (because it is not a pattern: no 'full_name' substitutions will
        be made) and obviously without "custom_mod_loggers";

    * "mod_globals": a dict (empty by default) mapping full RPC-module
      names to dicts mapping name of variables to their values (these
      variables will be set as global attributes of the particular Python
      module);

* amqp_params: a dict (an obligatory item), containing keyword arguments
  for AMQP Connection(), is to be used by the manager and the responder
  (see the amqplib.client_0_8.connection.Connection.__init__() signature
  for argument specification);

* exchange_types: a dict (empty by default) mapping AMQP exchange
  names to their types (in practice only two types are important:
  "direct" and "topic"); if a particular exchange is not included,
  RPCManager.default_exchange_type will be used (by default it is
  equal to "topic");

* bindings: a list (an obligatory item) -- contains 4-element lists (in
  load_config() turned into threads.BindingProps namedtuple instances)
  of strings:
  [0] exchange involved in a particular binding,
  [1] routing key used with it,
  [2] access-key pattern (see the next section: "RPC-modules and
     RPC-methods definitions and usage"),
  [3] access-keyhole pattern (see: the next section...);

* manager_attributes: a dict (empty by default) of additional manager
  object attributes (which, in particular, can override existing
  instance attributes or default RPCManager class attributes);

* responder_attributes: a dict (empty by default) of additional responder
  object attributes (which, in particular, can override existing
  instance attributes or default RPCResponder class attributes);

* logging_settings: a dict containing the server logger settings (the
  default values mentioned below will be used for missed items/subitems):

  * "server_logger": a string (empty by default) being the name of the
    logger (empty name means that the root logger will be used;
    see: Python stdlib `logging' module);

  * "level": a string (default: "info") specifying the logging level
    -- one of the following (starting with the most verbose option):
    "debug", "info", "warning", "error", "critical";

  * "handlers": a list of dicts (default: 1-element list containing
    a dict equal to mtrpc.common.consts.DEFAULT_LOG_HANDLER_SETTINGS)
    of the following items:

    * "cls" -- name of logging.Handler subclass being a member of the
      Python stdlib `logging' or `logging.handlers' module -- e.g.
      "StreamHandler", "RotatingFileHandler", "SocketHandler",
      "SMTPHandler" or another...

    * "kwargs" -- keyword arguments for the class-specific constructor,

    * "level" -- logging level of the handler, one of: "debug",
      "info", "warning", "error", "critical" (default: "info"),

    * "format" -- logging format, default:
      "%(asctime)s %(name)s:%(levelno)s @%(threadName)s: %(message)s"

    See Python stdlib `logging' module for the description of
    constructor arguments and other explanations.

  * "propagate": False (default) or True -- whether logged messages
    should be propagated up (again -- see Python stdlib `logging');

* "os_settings: a dict of various OS-related settings (the default
  values mentioned below will be used for missed items):

  * "umask": None (default) or an integer -- specifies umask setting
    (None means: no changes to the present state);

  * "working_dir": None (default) or a string -- working directory setting
    (None means: no changes to the present state);

  * "daemon": False (default) or True -- whether the whole server process
    should be daemonized;

  * "signal_actions": a dict mapping OS signal names (as defined in Python
    stdlib `signal' module) to the names of, so called, actions
    (see: MTRPCServerInterface documentation)
    -- default content: {"SIGTERM": "exit', "SIGHUP": "restart"}

  * "sig_stopping_timeout": an integer (default: 60) -- timeout for waiting
    for manager termination when it is being stopped/restarted by OS signals.

[The highest level items are sometimes called "config sections"].

----------------------------------------------
Defining and using RPC-modules and RPC-methods
----------------------------------------------

MTRPCServerInterface.load_rpc_tree() method takes four arguments:

* paths,
* imports,
* postinit_kwargs,
* default_postinit_callable.

The first three of them, if omitted (typical usage), are replaced with
appropriate config items from "rpc_tree_init" config section. The fourth
argument defaults to mtrpc.common.utils.basic_postinit function.

First, the Python modules specified in `paths' and `imports' are imported
(together with any submodules imported by them and their submodules...).

Then, the RPC-tree is being built on their basis (recursively, starting
with the paths/imports-specified modules) in the following way:

1. A Python module is examined for presence of special attributes whose
   names are equal to the constants defined in mtrpc.common.const
   as RPC_... variables. These special attributes are:

   * __rpc_methods__ -- a sequence (e.g. list) of names referring to
     member functions (or other callables) that define RPC-methods; the
     sequence can also include:

     * '*' string symbol -- meaning that all the module public functions
       (those whose names are included in __all__ sequence or -- if there
       is no __all__ attribute -- those whose names do not start with '_')
       define RPC-methods,

     * dot-separated names refering to submodule callable members (e.g.
       "submod.submod.my_function" or even "submod.submod.*");

     __rpc_methods__ attribute can also be a string -- then it will be
     treated as a sequence including that string as the only element;

   * __rpc_doc__ -- RPC-module documentation string (independent of the
     Python module documentation string);

   * __rpc_tags__ -- a dictionary with arbitrary items, which becomes
     the `tags' attribute of the RPC-module (a mapping containing any
     additional information useful for the framework code or your code);

   * __rpc_postinit__ -- the RPC-module post-init-callable (see Pt 3
     below).

   Only if the Python module has any of these four above-mentioned
   attributes -- is the corresponding RPC-module created and added to its
   parent RPC-module and to the RPC-tree.

2. __rpc_tags__ and __rpc_doc__ are turned into the `tags' and `doc'
   RPC-module attributes.

3. So called **post-init mechanism** is applied:

   __rpc_postinit__ -- or, in case of lack of it, the callable object
   referred by `default_postinit_callable' (see above) -- is being called
   with those of the following keyword arguments that its signature
   contains:

   * mod -- the Python module,
   * full_name -- the RPC-module full name,
   * rpc_tree -- the whole RPC-tree object,

   *plus* all the arguments defined as `postinit_kwargs' (see above).

   The basic implementation of post-init callable (the default for
   `default_postinit_callable' -- mtrpc.common.utils.basic_postinit)
   takes the arguments:

   * mod,
   * full_name,
   * logging_settings,
   * mod_globals,

   -- so `postinit_kwargs' should include `logging_settings' and
   `mod_globals' items (see: the previous section).

4. The RPC-module is being populated with RPC-methods created from the
   Python module callable members listed in the __rpc_methods__ sequence,
   and also from its callable members listed (with dot-separated notation,
   mentioned above) in __rpc_methods__ attributes of ancestor modules.

Any inter-module cyclic references are being broken (then suitable
warning is being logged).

RPC-method definition details and RPC-methods calls
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Define your RPC-methods as functions (or any other callables, however
only functions are taken into consederation when using '*' notation in
__rpc_methods__).

RPC-methods can take both positional and keyword arguments, declared as
their names and/or with *args and **kwargs notation (like any Python
callable objects).

* Protocol-related note: there is an MTRPC extension to the JSON-RPC 1.0
  protocol: keyword arguments can be transferred as a dict [JSON object]
  named "kwparams"; to get the context see the original protocol
  specification: http://json-rpc.org/wiki/specification).

* Protocol-related note: MTRPC doesn't implement JSON-RPC notifications
  (they are not very useful in that context) and responds to them with
  error messages.

The function (or another callable object) that defines an RPC-method can
have attributes that will be used by MTRPC:

* __doc__, i.e. the good old Python docstring -- will be turned into the
  RPC-method docstring (RPC-method `doc' attribute);

* __mtrpc_tags__, a dictionary with arbitrary items -- will become the
  `tags' attribute (a mapping containing any additional information useful
  for the framework code or your code) of the RPC-method.

**Attention:** Any strings that may be sent into the client side and that
may contain non-ascii characters *must* be unicode strings -- in particular
it applies to the RPC-module/method docstrings!

Access key/keyhole mechanism
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

MTRPC offers possibility of restricting of access to RPC-modules/methods,
called *key and keyhole mechanism*:

For each AMQP binding (see the section about configuration + `threads'
module docs) you can define *key pattern* and *keyhole pattern*; both are
template strings to be treated with .format() string method, and can
contain (in a "{key}" manner) any of the keys of so called *access dict*.

These keys -- and their corresponding values (set dynamically for each
request) -- are:

* "exchange" (string) -- a particular AMQP exchange name,

* "queue" (string) -- a particular AMQP queue name,

* "rk" (string) -- AMQP consumer's (server's) routing key,

* "rk_split" (list) -- that routing key split using ".",

* "rk_revsplit" (list) -- that routing key split using "." and reversed,

* "msg_rk" (string) -- original AMQP message (client's) routing key,

* "msg_rk_split" (list) -- that routing key split using ".",

* "msg_rk_revsplit" (list) -- that routing key split using "." and reversed,

* "delivery_info" (dict) -- a particular message's `delivery_info' dict
  (see: the amqplib.client_0_8.channel._basic_deliver() docstring),

* "reply_to" (string) -- a particular message's `reply_to' attribute
  (the reply-to routing key),

* "full_name" (string) -- a particular RPC-method/module full name,

* "local_name" (string) -- a particular RPC-method/module local name,

* "parentmod_name" (string) -- the `full_name' without the `local_name'
  (and of course without any trailing dot),

* "split_name" (list) -- the `full_name' split using ".",

* "doc" (string) -- RPC-method/module docstring (the `doc' attribute),

* "tags" (RPCObjectTags [dict subclass] instance)
  -- the RPC-method/module `tags' attribute,

* "help" (string)
  -- the RPC-method/module help-text formatted in the standard way,

* "type" (type) -- methodtree.RPCMethod or methodtree.RPCModule type.

Please note such formatting possibilites as:
"{rk_split[2]}", "{delivery_info[redelivered]}", "{type.__name__}" etc.

Before an RPC-method is being called (or an RPC-module/RPC-method is
being accessed "for a client", e.g. with system.* RPC-methods), the
*access key* and *access keyhole* are dynamically created from their
patterns, using .format() with keyword arguments from *access dict*.

Then, the *access keyhole* is being used as a regular expression pattern
to match (search) the *access key* [re.search(access_keyhole, access_key)]
-- if the result is negative, access is *denied* (errors.RPCNotFoundError
is sent to the client).

Normally all that mechanism works under the hood, without a need of your
additional work -- except defining appropriate access-key/keyhole patterns
in the config. But, if you need that, your RPC-methods can take *access key
pattern*, *access keyhole pattern* and *access dict* as keyword arguments:
they should be placed in method signatures as the righmost arguments --
and named as defined in mtrpc.common.const module.

Exceptions
^^^^^^^^^^

MTRPC defines a set of exceptions -- especially those that are being sent
to a client. There is also special way of raising exceptions to be sent.
See: mtrpc.common.errors module for details.

* Protocol-related note: errors are serialized to JSON message members
  as dicts (JSON objects): {"error": <an exception class name>,
                            "message": <str(<exception instance>)>}

Additionally `methodtree' and `threads' submodules define some internal
exceptions (raised and caught by the server framework code).

------------------
AMQP communication
------------------

For information about AMQP routing keys, exchange and queue names etc.
-- see: the `threads' submodule documentation.

"""



from future_builtins import filter, map, zip

import functools
import imp
import json
import logging
import logging.handlers
import os
import os.path
import pkg_resources
import Queue
import signal
import sys
import threading
import time
import traceback
import types
import warnings

from . import threads
from . import methodtree
from . import _daemon_recipe
from .config import loader
from ..common import utils
from ..common.const import (
    DEFAULT_LOG_HANDLER_SETTINGS,
    RPC_METHOD_LIST,
    )



__all__ = [
        'OBLIGATORY_CONFIG_SECTIONS',
        'CONFIG_SECTION_TYPES',
        'CONFIG_SECTION_FIELDS',
        'MTRPCServerInterface',
        'validate_and_complete_config',
        'make_config_stub',
        'write_config_skeleton',
]



#
# Config-file-related constants (see: mtrpc.server docstring)
#

OBLIGATORY_CONFIG_SECTIONS = 'rpc_tree_init', 'amqp_params', 'bindings'
CONFIG_SECTION_TYPES = dict(
        rpc_tree_init=dict,
        amqp_params=dict,
        exchange_types=dict,
        bindings=list,
        manager_settings=dict,  # !TODO! - inaczej...
        manager_attributes=dict,
        responder_attributes=dict,
        logging_settings=dict,
        os_settings=dict,
)
# allowed sections of a config file and their default content
CONFIG_SECTION_FIELDS = dict(
        rpc_tree_init = dict(
            paths=[],
            imports=['mtrpc.server.sysmethods as system'],
            postinit_kwargs=dict(
                logging_settings=dict(
                    mod_logger_pattern='mtrpc.server.rpc_log.{full_name}',
                    level='warning',
                    handlers=[DEFAULT_LOG_HANDLER_SETTINGS],
                    propagate=False,
                    custom_mod_loggers=dict(
                        # maps RPC-module full names to logging settings dicts
                        # with 'mod_logger' key pointing at a logger name;
                        # omitted items will be substituted with general ones
                    ),
                ),
                mod_globals=dict(
                    # maps RPC-module full names to dicts of attributes
                ),
            ),
        ),
        amqp_params = None,           # to be a dict with some keys...
        exchange_types = None,        # to be a dict: {exchange, its type}
        bindings = None,              # to be a list of binding props
        manager_settings = None,      # to be a dict with some keys... !TODO! - inaczej...
        manager_attributes = None,    # to be a dict with some keys...
        responder_attributes = None,  # to be a dict with some keys...
        logging_settings = dict(
            server_logger='mtrpc.server',
            level='info',
            handlers=[DEFAULT_LOG_HANDLER_SETTINGS],
            propagate=False
        ),
        os_settings = dict(
            umask=None,
            working_dir=None,
            daemon=False,
            signal_actions=dict(
                SIGTERM='exit',
                SIGHUP='restart',
            ),
            sig_stopping_timeout = 60,
        ),
)



#
# Server convenience interface class
#

class MTRPCServerInterface(object):

    """

    Instantiation
    ^^^^^^^^^^^^^

    MTRPCServerInterface is a singleton type, i.e. it can have at most one
    instance -- and it should not be instantiated directly but with one of
    three alternative constructors (being class methods):

    * get_instance() -- get (create if it does not exist) the
      MTRPCServerInterface instance; it doesn't do anything else, so
      after creating the instance your script is supposed to call
      load_config(), configure_logging(), do_os_settings(), start()...

    * configure() -- get the instance, read config file (see: above config
      file structure/content description), set up logging, OS-related stuff
      (signal handlers, optional daemonization and some other things...)
      and loads RPC-module/method definitions building the RPC-tree;
      the only thing left to do by your script to run the server is to
      call the start() method.

    * configure_and_start() -- do the same what configure() does *plus* start
      the server, and then:

      * either return immediately (if `loop_mode' argument is false),
      * or stay and wait for KeyboardInterrupt or OS signals (if `loop_mode'
        argument is true).

    See documentation of these methods for detailed info about arguments.

    Public instance methods
    ^^^^^^^^^^^^^^^^^^^^^^^

    * load_config() -- load, parse, validate, adjust the config;
    * configure_logging() -- set up the server logger;
    * do_os_settings() -- set OS signal handlers and do some other things,
    * load_rpc_tree() -- load RPC-module/methods;
    * start() -- create and start service threads (manager and responder),
    * stop() -- stop these service threads.

    By default, most of these methods base on the instance attributes
    (see below: "Public instance attributes"), but ignore them if gets
    adequate objects as arguments.

    See documentation and signatures of that methods for more details.

    OS signal handlers
    ^^^^^^^^^^^^^^^^^^

    * _exit_handler(),
    * _force_exit_handler(),
    * _restart_handler() and its alias _reload_handler()

    They define so called "actions" and their names are constructed
    in such a way: '_' + <action name> + '_handler' -- so they define:
    'exit', 'force_exit', 'restart' ('reload') actions. That action names
    can be used in the config to define OS signal handlers (which are set,
    basing on that config settings, by do_os_settings() method). It is
    possible to add custom action handlers (e.g. in a subclass of this
    class) and refer to them in the config by action names.

    See signatures (argument specs) of that methods for some details.

    Public static/class methods
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * restart_on() -- set the instance [sic] attribute `_restart' to True;
      in loop-mode (see: configure_and_start()) it causes break of waiting
      for KeyboardInterrupt/OS signals and is especially useful as
      manager's final callback (=> auto-restarting after a fatal failure).

    * validate_and_complete_config(),
    * make_config_stub(),
    * write_config_skeleton()
    -- config-manipulation-related static/class methods, accessible also
    as module-level functions.

    See documentation and signatures of that methods for more details.

    Public instance attributes
    ^^^^^^^^^^^^^^^^^^^^^^^^^^

    * manager (mtrpc.server.thread.RPCManager instance, set at server start,
      initially set to None) -- the service thread responsible for starting
      and stopping other threads as well as for receiving RPC requests;
      see also: mtrpc.server.thread.RPCManager documentation;

    * responder (mtrpc.server.thread.RPCResponder instance, set at server
      start, initially set to None) -- a service thread responsible for
      sending RPC-responses after getting them from task threads
      (mtrpc.server.thread.RPCTaskThread instances);
      see also: mtrpc.server.thread.RPCResponder documentation;

    * config -- a dict with content read from config file (see: the next
      section: "Server configuration file..."; initially set to None),
      complemented with default values and slightly adjusted (e.g. some
      sub-dicts transformed into named tuples);

    * daemonized: True or False (set when OS settings are made, initially set
      to False; definitely should not be set manually) -- informs whether
      the server process has been daemonized;

    * log (logging.Logger instance) -- the main server logger object (initially
      set to the root logger, with logging.getLogger(), and configured with
      logging.basicConfig; it may change after config is read, if other
      settings are specified in config);

    * rpc_tree (mtrpc.server.methodtree.RPCTree instance) -- populated with
      RPC-modules and RPC-methods defined in modules whose names or paths are
      specified in config + their submodules (set on tree load -- after config
      load; initially set to None);

    * task_dict (a dict),
    * result_fifo (Queue.Queue instance),
    * mutex (threading.Lock instance)
    -- objects (set on init) that are passed both into the manager and
    responder constructors when server is started.

    """

    _instance = None
    _server_iface_rlock = threading.RLock()


    def __init__(self):

        """Attention: MTRPCServerInterface is a singleton class.

        Use one of the alternavice constructor methods: get_instance(),
        configure() or configure_and_start() -- rather than instantiate
        the class directly.

        """

        with self.__class__._server_iface_rlock:
            if self.__class__._instance is not None:
                raise TypeError("{0} is a singleton class and its instance "
                                "has been already created; use "
                                "get_instance() class method to obtain it"
                                .format(self.__class__.__name__))

            self.__class__._instance = self

        self.manager = None
        self.responder = None
        self.config = None

        # to be set in do_os_settings()
        self.daemonized = False
        self._signal_handlers = {}

        # to be used in configure_and_start() and restart_on()
        self._restart = False

        # the actual logger to be configured and set in configure_logging()
        logging.basicConfig(format="%(asctime)s %(levelno)s "
                                   "@%(threadName)s: %(message)s")
        self.log = logging.getLogger()
        self._log_handlers = []

        # the RPC-tree -- to be set in load_rpc_tree()
        self.rpc_tree = None

        # to be used in start()
        self.task_dict = {}
        self.result_fifo = Queue.Queue()
        self.mutex = threading.Lock()


    @classmethod
    def get_instance(cls):
        "Get (the only) class instance; create it if it does not exist yet"
        with cls._server_iface_rlock:
            if cls._instance is None:
                return cls()
            else:
                return cls._instance


    @classmethod
    def configure(cls, config_path=None, config_dict=None,
                  force_daemon=False,
                  default_postinit_callable=utils.basic_postinit):

        """Get the instance, load config + configure (don't start) the server.

        Obligatory argument: config_path -- path of the config file OR
                             config_dict -- parsed config

        Optional arguments:

        * force_daemon (bool) -- if True => always daemonize the OS process
          (ignore the 'os_settings'->'daemon' field), default: False;

        * default_postinit_callable (callable object) -- to be passed to
          methodtree.RPCTree.build_new(); default: common.utils.basic_postinit.

        """

        if (config_path is None) == (config_dict is None):
            raise ValueError('Either config_path or config_dict is required; ({0!r}, {1!r})'.format(config_path, config_dict))

        try:
            self = cls.get_instance()
            if config_path is not None:
                self.load_config(config_path)
            else:
                self.config = self.validate_and_complete_config(config_dict)
            self.configure_logging()
            self.do_os_settings(force_daemon=force_daemon)
            self.load_rpc_tree(default_postinit_callable
                               =default_postinit_callable)
        except:
            self.log.critical('Error during server configuration. '
                              'Raising exception...', exc_info=True)
            raise
        else:
            return self


    @classmethod
    def configure_and_start(cls, config_path=None, config_dict=None,
                            force_daemon=False,
                            default_postinit_callable=utils.basic_postinit,
                            loop_mode=False,
                            final_callback=None):

        """The same what configure() does, then run the server.

        Obligatory argument: config_path -- path of a config file.

        Optional arguments:

        * loop_mode
        * force_daemon
        * default_postinit_callable
        -- see: configure();

        * loop_mode (bool) --
          if True => stay here waiting for KeyboardInterrupt, an OS signal
                     or restart request (setting _restart attribute to True),
          if False => return immediately after server start;

        * final_callback (callable object) -- to be called from the
          manager thread before it terminates.

        """

        while True:
            self = cls.configure(config_path, config_dict,
                                 force_daemon, default_postinit_callable)
            try:
                self.start(final_callback=final_callback)
            except:
                self.log.critical('Error during server start. '
                                  'Raising exception...', exc_info=True)
                raise

            if not loop_mode:
                # non-loop mode: return the instance object immediately
                return self

            else:
                # loop mode: wait for a restart/OS signals
                try:
                    signal.pause()
                except SystemExit:  # probably raised by a signal handler
                    self.log.debug('System exit...')
                    raise
                except KeyboardInterrupt:
                    self.log.debug('Keyboard interrupt...')
                    self.stop()  # it may be Ctrl+C -caused in non-deamon mode
                    sys.exit()   # => we must finalize the program here because
                                 # of strange effects on module namespaces
                                 # when control gets another module :-/
                except:
                    self.log.critical('Error while handling or waiting for a '
                                      'system signal. Raising exception...',
                                      exc_info=True)
                    raise
                else:
                    self._restart = False


    # it is usefuf as final_callback in loop mode
    @classmethod
    def restart_on(cls):
        cls._instance._restart = True


    def load_config(self, config_path):
        "Load the config from a JSON file; check, adjust, return as a dict"
        try:
            with open(config_path) as config_file:
                config = loader.load_props(config_file)
                config = self.validate_and_complete_config(config)

        except Exception:
            raise RuntimeError("Can't load configuration -- {0}"
                               .format(traceback.format_exc()))
        self.config = config
        return config


    @staticmethod
    def validate_and_complete_config(config):

        """Check and supplement a given config dict.

        Check item types (specified in CONFIG_SECTION_TYPES); check presence
        of obligatory items (specified in OBLIGATORY_CONFIG_SECTIONS)
        and complete the rest with default content (defined in
        CONFIG_SECTION_FIELDS and CONFIG_SECTION_TYPES).

        Adjust 'bindings' item -- transforming it from a list of lists into
        a list of threads.BindingProps (namedtuple) instances.

        Return the same -- but modified -- config dict.

        """

        # verify section content types
        for section, sect_content in config.iteritems():
            if not isinstance(sect_content, CONFIG_SECTION_TYPES[section]):
                raise TypeError('{0} section should be a {1.__name__}'
                                .format(section,
                                        CONFIG_SECTION_TYPES[section]))

        # verify completeness
        omitted = set(OBLIGATORY_CONFIG_SECTIONS).difference(config)
        if omitted:
            raise ValueError('Section(s): {0} -- should not be omitted'
                             .format(', '.join(sorted(omitted))))

        # complement omited non-obligatory sections
        for section in set(CONFIG_SECTION_TYPES
                          ).difference(OBLIGATORY_CONFIG_SECTIONS):
            config.setdefault(section, CONFIG_SECTION_TYPES[section]())

        # verify section fields and complement them with default values
        for section, sect_content in CONFIG_SECTION_FIELDS.iteritems():
            if sect_content is not None:
                # verify (check for illegal fields)
                used_fields = set(config[section])
                if not used_fields.issubset(sect_content):
                    bad = sorted(used_fields.difference(sect_content))
                    raise ValueError('Illegal fields in {0} section: {1}'
                                     .format(section, ', '.join(bad)))
                # complement omitted fields
                content = sect_content.copy()
                content.update(config[section])
                config[section] = content

        # replace unicode-keys with str-keys in kwargs-based sections
        (config['amqp_params']
        ) = utils.kwargs_to_str(config['amqp_params'])
        (config['manager_attributes']
        ) = utils.kwargs_to_str(config['manager_attributes'])
        (config['responder_attributes']
        ) = utils.kwargs_to_str(config['responder_attributes'])

        # verify RPC-tree-init-related settings
        for field, value in config['rpc_tree_init'].iteritems():
            if not (field in ('paths', 'imports') and isinstance(value, list)
                    or field == 'postinit_kwargs' and isinstance(value, dict)):
                raise ValueError("Illegal item in rpc_tree_init section:"
                                 " {0!r}: {1!r}".format(field, value))

        # verify and prepare exchange types
        for exchange, etype in config['exchange_types'].iteritems():
            if not (isinstance(exchange, basestring)
                    and isinstance(etype, basestring)):
                raise ValueError("Illegal item in exchange_types section:"
                                 " {0!r}: {1!r}".format(exchange, etype))

        # verify and prepare binding properties (turn it into a list
        # of threads.BindingProps namedtuple instances)
        bindings = []
        for binding_props in config['bindings']:
            try:
                if not all(isinstance(x, basestring)
                           for x in binding_props):
                    raise TypeError
                (binding_props
                ) = threads.BindingProps._make(binding_props)
            except (ValueError, TypeError):
                raise ValueError("Illegal item in bindings section: "
                                 "{0!r}".format(q_name_props))
            else:
                bindings.append(binding_props)
        config['bindings'] = bindings

        return config


    #
    # Environment-related preparations

    def configure_logging(self, log_config=None):

        "Configure server logger and its handlers"

        prev_log = self.log
        if log_config is None:
            log_config = self.config['logging_settings']

        # get the logger
        self.log = logging.getLogger(log_config.get('server_logger', ''))

        # configure it
        utils.configure_logging(self.log, prev_log, self._log_handlers,
                                log_config)
        return self.log


    def do_os_settings(self, os_settings=None, force_daemon=False):

        "Set umask and working dir; daemonize or not; set OS signal handlers"

        if os_settings is None:
            os_settings = self.config['os_settings']

        umask = os_settings.get('umask')
        if umask is None:
            umask = os.umask(0)   # (os.umask() sets new, returns previous)
        os.umask(umask)

        working_dir = os_settings.get('working_dir')
        if working_dir is not None:
            os.chdir(working_dir)

        if (os_settings.get('daemon') or force_daemon) and not self.daemonized:
            # daemonize:
            _daemon_recipe.UMASK = umask
            _daemon_recipe.WORKDIR = os.getcwd()
            _daemon_recipe.createDaemon()
            self.daemonized = True

        # unregister old signal handlers (when restarting)
        while self._signal_handlers:
            signal_num, handler = self._signal_handlers.popitem()
            signal.signal(signal_num, signal.SIG_DFL)

        # register signal handlers
        signal_actions = os_settings.get('signal_actions',
                                         dict(SIGTERM='exit',
                                              SIGHUP='restart'))
        sig_stopping_timeout = os_settings.get('sig_stopping_timeout', 60)
        if signal_actions is not None:
            for signal_name, action_name in signal_actions.iteritems():
                signal_num = getattr(signal, signal_name)
                handler_func = getattr(self, '_{0}_handler'.format(action_name))
                handler = functools.partial(handler_func,
                                            stopping_timeout
                                            =sig_stopping_timeout)
                signal.signal(signal_num, handler)
                self._signal_handlers[signal_num] = handler

    #
    # OS signal handlers:

    def _restart_handler(self, signal_num, stack_frame, stopping_timeout):
        '"restart" action'
        try:
            self.log.info('Signal #%s received by the process -- '
                          '"restart" action starts...', signal_num)
            if self.stop(reason='restart', timeout=stopping_timeout):
                self.restart_on()
        except:
            self.log.critical('Error while restarting. Raising exception...',
                              exc_info=True)
            raise

    _reload_handler = _restart_handler

    def _exit_handler(self, signal_num, stack_frame, stopping_timeout):
        '"exit" action'
        try:
            self.log.info('Signal #%s received by the process -- '
                          '"exit" action starts...', signal_num)
            if self.stop(reason='exit', timeout=stopping_timeout):
                sys.exit()
        except SystemExit:
            raise
        except:
            self.log.critical('Error while exiting. Raising exception...',
                              exc_info=True)
            raise

    def _force_exit_handler(self, signal_num, stack_frame, stopping_timeout):
        '"force_exit" action'
        try:
            self.log.info('Signal #%s received by the process -- '
                          '"force_exit" action starts...', signal_num)
            if self.stop(reason='force-exit', force=True,
                         timeout=stopping_timeout):
                sys.exit()
        except SystemExit:
            raise
        except:
            self.log.critical('Error while force-exiting. Raising '
                              'exception...', exc_info=True)
            raise


    #
    # RPC-methods tree loading

    def load_rpc_tree(self, paths=None, imports=None, postinit_kwargs=None,
                      default_postinit_callable=utils.basic_postinit):

        "Load RPC-methods from modules specified by names or filesystem paths"

        try:
            rpc_tree_init_conf = self.config.get('rpc_tree_init', {})

            if paths is None:
                paths = rpc_tree_init_conf.get('paths', [])

            if imports is None:
                imports = rpc_tree_init_conf.get('imports', [])

            if postinit_kwargs is None:
                postinit_kwargs = rpc_tree_init_conf.get('postinit_kwargs', {})

            root_mod = types.ModuleType('_MTRPC_ROOT_MODULE_')
            root_method_list = []
            setattr(root_mod, RPC_METHOD_LIST, root_method_list)

            # load modules using absolute filesystem paths
            for path_req in paths:
                tokens = [s.strip() for s in path_req.rsplit(None, 2)]
                if len(tokens) == 3 and tokens[1] == 'as':
                    file_path = tokens[0]
                    # e.g. '/home/zuo/foo.py as bar' => dst_name='bar'
                    dst_name = tokens[2]
                else:
                    file_path = path_req
                    # e.g. '/home/zuo/foo.py' => dst_name='foo'
                    dst_name = os.path.splitext(os.path.basename(file_path))[0]
                name_owner = getattr(root_mod, dst_name, None)
                if name_owner is None:
                    module_name = 'mtrpc_pathloaded_{0}'.format(dst_name)
                    module = imp.load_source(module_name, file_path)
                    setattr(root_mod, dst_name, module)
                    root_method_list.append(dst_name)
                else:
                    self.log.warning('Cannot load module from path "{0}" as '
                                     '"{1}" -- because "{1}" name is already '
                                     'used by module {2!r}'
                                     .format(file_path, dst_name, name_owner))

            # import modules using module names
            for import_req in imports:
                tokens = [s.strip() for s in import_req.split()]
                if len(tokens) == 3 and tokens[1] == 'as':
                    src_name = tokens[0]
                    # e.g. 'module1.modulo2.modula3 as foo' => dst_name='foo'
                    dst_name = tokens[2]
                elif len(tokens) == 1:
                    src_name = tokens[0]
                    # e.g. 'module1.modulo2.modula3' => dst_name='modula3'
                    dst_name = tokens[0].split('.')[-1]
                else:
                    raise ValueError('Malformed import request: "{0}"'
                                     .format(import_req))
                name_owner = getattr(root_mod, dst_name, None)
                if name_owner is None:
                    module = __import__(src_name,
                                        fromlist=['__dict__'],
                                        level=0)
                    setattr(root_mod, dst_name, module)
                    root_method_list.append(dst_name)
                else:
                    self.log.warning('Cannot import module "{0}" as "{1}" -- '
                                     'because "{1}" name is already used by '
                                     'module {2!r}'
                                     .format(src_name, dst_name, name_owner))

            # (use warnings framework to log any warnings with the logger)
            with warnings.catch_warnings():
                self._set_warnings_logging_func(self.log,
                                                warnings.showwarning)

                # (RPCTree.build_new() creates a new RPC-tree object,
                # walks recursively over submodules of the root module
                # to collect names and callables -- to create RPC-modules
                # and RPC-methods and populate the tree with them)
                (rpc_tree
                ) = methodtree.RPCTree.build_new(root_mod,
                                                 default_postinit_callable,
                                                 postinit_kwargs)

        except Exception:
            raise RuntimeError('Error when loading RPC-methods -- {0}'
                               .format(traceback.format_exc()))

        self.rpc_tree = rpc_tree
        return rpc_tree


    @staticmethod
    def _set_warnings_logging_func(log, orig_showwarning):

        def showwarning(message, category, filename, lineno,
                        file=None, line=None):
            if issubclass(category, methodtree.LogWarning):
                log.warning(message)
            else:
                orig_showwarning(message, category, filename, lineno,
                                 file=None, line=None)

        warnings.showwarning = showwarning


    #
    # The actual server management

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


    def stop(self, reason='manual stop', loglevel='info', force=False,
             timeout=30):

        """Request the manager to stop the responder and then to stop itself.

        Arguments:

        * reason (str) -- an arbitrary message (to be recorded in the log);

        * loglevel (str) -- one of: 'debug', 'info', 'warning', 'error',
          'critical';

        * force (bool) -- if true the server responder will not wait for
                          remaining tasks to be completed;

        * timeout (int or None)
          -- timeout=None  => wait until the manager thread terminates,
          -- timeout=<i>   => wait, but no longer than <i> seconds,
          -- timeout=0     => don't wait, return immediately.

        Return True if the manager thread has been stopped successfully
        (then set the `manager' attribute to None); False if it's still
        alive.

        """

        with self._server_iface_rlock:
            if self.manager is None or not self.manager.is_alive():
                self.log.warning("Futile attempt to stop the server "
                                 "while it's not started")
                return True

        self.log.info('Stopping the server (reason: "%s")...', reason)
        stopped = self.manager.stop(reason, loglevel, force, timeout)
        if stopped:
            self.manager = None
        else:
            self.log.warning('Server stop has been requested but the '
                             'server is not stopped (yet?)')

        return stopped


    #
    # Additional public static/class methods useful
    # when you prepare your own config file

    @staticmethod
    def make_config_stub():
        "Create a dict contaning (empty) obligatory config sections"
        return dict((section, CONFIG_SECTION_TYPES[section]())
                    for section in OBLIGATORY_CONFIG_SECTIONS)


    @classmethod
    def write_config_skeleton(dest_path, config_stub=None):
        "Write config skeleton into file (you'll adjust that file by hand)"
        if config_stub is None:
            config_stub = cls.make_config_stub()
        config = cls.validate_and_complete_config(config_stub)
        with open(dest_path, 'w') as dest_file:
            json.dump(config, dest_file, sort_keys=True, indent=4)



#
# MTRPCServerInterface config-manipulating-related static/class methods
# as standalone convenience functions

(validate_and_complete_config
) = MTRPCServerInterface.validate_and_complete_config

make_config_stub = MTRPCServerInterface.make_config_stub

write_config_skeleton = MTRPCServerInterface.write_config_skeleton

def config_file(config_path):
    if config_path.startswith('/'):
        return open(config_path)

    if '=' in config_path:
        key, value = config_path.split('=', 1)
        key = key.strip()
        value = value.strip()
        try:
            json.loads(value)
        except ValueError:
            value = json.dumps(value)
        # convert key=value to key: value
        # key:value syntax is already taken by package loader
        return ['{key}: {value}'.format(key=key, value=value)]

    if ':' in config_path:
        package, relative_path = config_path.split(':', 1)

        resource_manager = pkg_resources.ResourceManager()
        provider = pkg_resources.get_provider(package)

        return provider.get_resource_stream(resource_manager, relative_path)

    return open(config_path)

def run_server(config_paths, daemon=False, pidfile_path=None):
    restart_lock = threading.Lock()
    final_callback = restart_lock.release
    # (^ to restart the server when the service threads are stopped)
    try:
        # no inner server loop needed, we have the outer one here
        while True:
            if restart_lock.acquire(False):   # (<- non-blocking)
                config_dict = dict()
                for p in config_paths:
                    fp = config_file(p)
                    config_dict = loader.load_props(fp, config_dict)
                server = MTRPCServerInterface.configure_and_start(
                        config_dict=config_dict,
                        force_daemon=daemon,
                        loop_mode=False,  # <- return immediately
                        final_callback=final_callback,
                )
                if daemon and pidfile_path:
                    with open(pidfile_path, 'w') as f:
                        print >>f, os.getpid()
            signal.pause()
    except KeyboardInterrupt:
        server.stop()

def main():
    from optparse import OptionParser
    parser = OptionParser(usage='%prog [options] config_file...')
    parser.add_option('-d', '--daemon', dest='daemon', action='store_true', default=False, help='daemonize')
    parser.add_option('-p', '--pidfile', dest='pidfile', action='store', default=None, help='write pid to file')

    (o, a) = parser.parse_args()

    run_server(a, o.daemon, o.pidfile)

if __name__ == '__main__':
    main()
