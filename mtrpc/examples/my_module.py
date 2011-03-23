import os
import sys

sys.path.insert(0, os.getcwd())

import my_submodule

__rpc_doc__ = u'Very sophisticated RPC module'
__rpc_methods__ = '*'


def add(x, y):
    u"Add one argument to the other"
    return x + y

# one of the special _access_* arguments is used --
# passed on the server side, not seen by the client:
def tell_the_rk(_access_dict):
    u"Tell what AMQP routing key the client used"
    return ("You sent your request using the following "
            "routing key: '{0}'".format(_access_dict['msg_rk']))
