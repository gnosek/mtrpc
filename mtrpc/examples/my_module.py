import os
import sys

sys.path.insert(0, os.getcwd())

import my_submodule

__rpc_doc__ = u'Very sophisticated RPC module'
__rpc_methods__ = ['*', 'my_submodule']


def add(x, y):
    u"Add one argument to the other"
    return x + y
