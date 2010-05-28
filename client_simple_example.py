#!/usr/bin/env python

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
