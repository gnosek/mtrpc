# mtrpc/server/__init__.py

"""
================================================
MegiTeam Remote Procedule Call (MTRPC) framework
================================================

An RPC framework with hierarchical structure of RPC-modules and methods
(RPC-tree).

Author: Jan Kaliszewski (zuo)
Copyright (c) 2010, MegiTeam

Portions of mtrpc.server.threads module were inspired with:

* QAM 0.2.18 (a Python RPC framework using AMQP, based on Carrot),
  copyrighted by Christian Haintz, Karin Pichler (2009), BSD-licensed;

* jsonrpc 0.01 (a Python JSON-RPC framework using HTTP), copyrighted
  by Jan-Klaas Kollhof (2007), LGPL-licensed.

----------------------------
Requiremens and dependencies
----------------------------

MTRPC was written for and tested under GNU/Linux (however it is possible
that it works on other platforms) and requires Python 2.6 (not tested
with 2.7).

It uses:

* Advanced Message Queuing Protocol (AMQP -- see: http://www.amqp.org/)
in version 0.8 -- as the transport protocol;

* JSON-RPC protocol (see: http://json-rpc.org/) in version 1.0 with some
extensions and limitations (see server subpackage documentation) -- as
the actual RPC protocol.

It depends on py-amqplib library (tested with version 0.6.1)
-- see: http://code.google.com/p/py-amqplib/

Also an AMQP broker is necessary to use it (tested with RabbitMQ
-- see: http://www.rabbitmq.com/).

-----------------------
Public package contents
-----------------------

* mtrpc.server -- MTRPC server convenience-interface class
  (MTRPCServerInterface) and server-related auxiliary functions;

* mtrpc.server.methodtree -- RPC-method container structure classes
  (especially implementing RPC-method, RPC-module and RPC-tree);

* mtrpc.server.sysmethods -- definition of the standard ('system')
  RPC-module containing RPC-tree introspection (listing and help) methods;

* mtrpc.server.threads -- the actual service thread classes,
  implementing receiving RPC-requests from AMQP broker, executing
  RPC-methods and sending RPC-responses to AMQP broker;

* mtrpc.client -- simple MTRPC client (RPC-proxy) implementation;

* mtrpc.common.const -- common MTRPC constants;

* mtrpc.common.errors -- common MTRPC exception classes and an auxiliary
  exception-related function;

* mtrpc.common.utils -- common MTRPC utility classes and functions;

* mtrpc.common.test.test_* -- testing modules (being also scripts).

For details -- see documentation of particular modules, functions,
classes and their methods. [The main part of the server documentation
is placed in the module: mtrpc.server (file: mtrpc/server/__init__.py)].

"""
