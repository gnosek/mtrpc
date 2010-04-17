"""Definitions of MTRPC standard 'system.*' method callables"""

# Author: Jan Kaliszewski (zuo)
# Copyright (c) 2010, MegiTeam


import __builtin__
import functools
import itertools

from ..common.const import RPC_METHOD_LIST, RPC_MODULE_DOC, RPC_INIT_CALLABLE
from ..common.utils import basic_mod_init



def _rpc_init(rpc_tree, mod, full_name, logging_settings, mod_globals):
    u"Add rpc_tree to globals + basic module init"
    
    setattr(mod, 'rpc_tree', rpc_tree)   # set rpc_tree as global variable
    basic_mod_init(mod, full_name, logging_settings, mod_globals)
    

mod_namespace = globals()
mod_namespace[RPC_METHOD_LIST] = 'list', 'help'
mod_namespace[RPC_MODULE_DOC] = u'Standard MTRPC system methods'
mod_namespace[RPC_INIT_CALLABLE] = _rpc_init



#
# Actual RPC-system-method callables
#

def list(module_name, deep=False, as_string=False, _access_dict=None,
         _access_key_patt=None, _access_keyhole_patt=None):
    u'''List module names and method signatures (within a given module).

    Arguments:
    * module_name (string) -- full (absolute) name of a parent module,
      e.g. '', 'system', 'module.some.other';
    * deep (bool) -- if set to True, list all submodules/methods
      subtree recursively (not only direct children);
    * as_string (bool) -- if set to True, one string (joined with new-line
      character) is returned instead of a list.

    Result: a list of names (strings) or a string (if as_string set).
    '''

    filter_key = functools.partial(
            rpc_tree.check_access,
            access_dict=_access_dict,
            access_key_patt=_access_key_patt,
            access_keyhole_patt=_access_keyhole_patt,
            required_type=None,
    )
    items = filter(filter_key, rpc_tree.all_items(module_name, deep=deep))
    signatures = (name + getattr(obj, 'formatted_arg_spec', '')
                  for name, obj in items)
    r = __builtin__.list(itertools.chain([module_name], signatures))
    if as_string:
        return '\n\n'.join(r)
    else:
        return r


def help(name, deep=False, as_string=False, _access_dict=None,
         _access_key_patt=None, _access_keyhole_patt=None):
    u'''List module/method help-texts, i.e signatures + docstrings.

    Arguments:
    * name (string) -- full (absolute) name of a module or method,
      e.g. '', 'system', 'module.some.other' (if it is name of a method
      only the method's help-text is returned);
    * deep (bool) -- if set to True, walk through all submodules/methods
      subtree recursively (not only direct children);
    * as_string (bool) -- if set to True, one string (joined with double
      new-line character) is returned instead of a list.

    Result: a list of help-texts (strings) or a string (if as_string set).
    '''

    rpc_obj = rpc_tree.try_to_obtain(
            name, _access_dict, _access_key_patt, _access_keyhole_patt
    )
    if isinstance(rpc_obj, rpc_tree.RPCMethod):
        r = [rpc_obj.help.format(name=name)]
        
    else:
        filter_key = functools.partial(
                rpc_tree.check_access,
                access_dict=_access_dict, access_key_patt=_access_key_patt,
                access_keyhole_patt=_access_keyhole_patt,
                required_type=None,
        )
        items = filter(filter_key, rpc_tree.all_items(name, deep=deep))
        r = __builtin__.list(obj.help.format(name=n)
                             for n, obj in itertools.chain([(name, rpc_obj)],
                                                           items))
    if as_string:
        return u'\n\n'.join(r)
    else:
        return r
