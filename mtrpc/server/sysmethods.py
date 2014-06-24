# mtrpc/server/sysmethods.py
#
# Author: Jan Kaliszewski (zuo)
# Copyright (c) 2010, MegiTeam

"""Python module that defines standard RPC-module 'system' with its methods"""

import __builtin__

from ..common.utils import basic_postinit


__rpc_doc__ = u'Standard MTRPC introspection methods'
__rpc_methods__ = 'list', 'list_string', 'help', 'help_string'
rpc_tree = None  # set by __rpc_postinit__


def __rpc_postinit__(rpc_tree, mod, full_name, logging_settings, mod_globals):
    u"""Add rpc_tree to globals + do basic module post-init"""

    setattr(mod, 'rpc_tree', rpc_tree)  # set rpc_tree as global variable
    basic_postinit(mod, full_name, logging_settings, mod_globals)


#
# Functions that define 'system.*' RPC-methods
#

def list(module_name, deep=False):
    u"""List module names and method signatures (within a given module).

    Arguments:
    
    * module_name (string) -- full (absolute) name of a particular module,
      e.g. '', 'system', 'module.some.other';
    * deep (bool) -- if set to True, list all submodules/methods recursively
      (from all the subtree, not only direct children).

    Result: a list of names/signatures (strings).

    """

    return __builtin__.list(_iter_signatures(module_name, deep))
list.readonly = True


def list_string(module_name, deep=False):
    u"""List module names and method signatures -- as one string.

    Arguments:
    
    * module_name (string) -- full (absolute) name of a particular module,
      e.g. '', 'system', 'module.some.other';
    * deep (bool) -- if set to True, list all submodules/methods recursively
      (from all the subtree, not only direct children).

    Result: a string containing names/signatures.

    """

    return '\n'.join(_iter_signatures(module_name, deep))
list_string.readonly = True


def help(name, deep=False):
    u"""List module/method help-texts, i.e signatures + docstrings.

    Arguments:
    
    * name (string) -- full (absolute) name of a module or method,
      e.g. '', 'system', 'module.some.other' (if it is name of a method
      only the method's help-text is returned);
    * deep (bool) -- if set to True, walk through all submodules/methods
      recursively (from all the subtree, not only direct children).

    Result: a list of help-texts (strings).

    """

    return __builtin__.list(_iter_help_texts(name, deep))
help.readonly = True


def help_string(name, deep=False):
    u"""List module/method help-texts -- as one string.

    Arguments:
    
    * name (string) -- full (absolute) name of a module or method,
      e.g. '', 'system', 'module.some.other' (if it is name of a method
      only the method's help-text is returned);
    * deep (bool) -- if set to True, walk through all submodules/methods
      recursively (from all the subtree, not only direct children).

    Result: a string containing help-texts.

    """

    return u'\n'.join(_iter_help_texts(name, deep))
help_string.readonly = True


#
# Private functions (containing the actual implementation)
#

def _iter_signatures(module_name, deep):
    """Iterate over submodule names and method signatures"""

    yield module_name

    for name, item in _iter_mod_subitems(module_name, deep):
        yield name + getattr(item, 'formatted_arg_spec', '')


def _iter_help_texts(name, deep):
    """Iterate over module/method help-texts"""

    rpc_obj = rpc_tree.try_to_obtain(name, access_dict={})

    yield rpc_obj.__doc__

    if isinstance(rpc_obj, rpc_tree.RPCModule):
        for name, item in _iter_mod_subitems(name, deep):
            yield item.__doc__


def _iter_mod_subitems(module_name, deep):
    """Iter. over accessible pairs (<full name>, <rpc submodule or method>)"""

    for name, item in rpc_tree.all_items(module_name, deep=deep):
        yield name, item
