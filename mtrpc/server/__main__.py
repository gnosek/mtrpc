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
    }

* RPC-module definition -- my_module.py:

    import my_submodule

    __rpc_doc__ = u'Very sophisticated RPC module'
    __rpc_methods__ = '*'

    def add(x, y):
        u"Add one argument to the other"
        return x + y


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

* bindings: a list (an obligatory item) -- contains 4-element lists
  (turned into threads.BindingProps namedtuple instances)
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

[The highest level items are sometimes called "config sections"].

----------------------------------------------
Defining and using RPC-modules and RPC-methods
----------------------------------------------

MTRPCServerInterface.load_rpc_tree() method takes one argument:

* rpc_mode

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

   * __rpc_postinit__ -- the RPC-module post-init-callable (see Pt 3
     below).

   Only if the Python module has any of these above-mentioned
   attributes -- is the corresponding RPC-module created and added to its
   parent RPC-module and to the RPC-tree.

2. __rpc_doc__ is turned into the `doc` RPC-module attribute.

3. So called **post-init mechanism** is applied:

   __rpc_postinit__ -- or, in case of lack of it, utils.basic_postinit --
   is being called with those of the following keyword arguments that its
   signature contains:

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

**Attention:** Any strings that may be sent into the client side and that
may contain non-ascii characters *must* be unicode strings -- in particular
it applies to the RPC-module/method docstrings!

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

from . import daemonize
from mtrpc.server.amqp import AmqpServer
from mtrpc.server.cli import MtrpcCli
from mtrpc.server.http import HttpServer
from mtrpc.server.server_config import ServerConfig


def main():
    from optparse import OptionParser
    parser = OptionParser(usage='%prog [options] config_file...')
    parser.add_option('-d', '--daemon', dest='daemon', action='store_true', default=False, help='daemonize')
    parser.add_option('-p', '--pidfile', dest='pidfile', action='store', default=None, help='write pid to file')
    parser.add_option('-c', '--cli', dest='server_class', action='store_const', const=MtrpcCli, default=AmqpServer,
                      help='run CLI')
    parser.add_option('-H', '--http', dest='server_class', action='store_const', const=HttpServer,
                      help='run HTTP backend')

    (o, a) = parser.parse_args()

    server = ServerConfig(a, o.server_class)

    if o.daemon and o.server_class is not MtrpcCli:
        daemonize.daemonize()

    server.run()

if __name__ == '__main__':
    main()
