# mtrpc/server/sysmethods.py
#
# Author: Jan Kaliszewski (zuo)
# Copyright (c) 2010, MegiTeam

"""Python module that defines standard RPC-module 'system' with its methods"""

import __builtin__
import itertools

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

def list(module_name, deep=False, _access_dict=None,
         _access_key_patt=None, _access_keyhole_patt=None):
    u"""List module names and method signatures (within a given module).

    Arguments:
    
    * module_name (string) -- full (absolute) name of a particular module,
      e.g. '', 'system', 'module.some.other';
    * deep (bool) -- if set to True, list all submodules/methods recursively
      (from all the subtree, not only direct children).

    Result: a list of names/signatures (strings).

    """

    return __builtin__.list(_iter_signatures(module_name, deep,
                                             _access_dict,
                                             _access_key_patt,
                                             _access_keyhole_patt))
list.readonly = True


def list_string(module_name, deep=False, _access_dict=None,
                _access_key_patt=None, _access_keyhole_patt=None):
    u"""List module names and method signatures -- as one string.

    Arguments:
    
    * module_name (string) -- full (absolute) name of a particular module,
      e.g. '', 'system', 'module.some.other';
    * deep (bool) -- if set to True, list all submodules/methods recursively
      (from all the subtree, not only direct children).

    Result: a string containing names/signatures.

    """

    return '\n'.join(_iter_signatures(module_name, deep,
                                      _access_dict,
                                      _access_key_patt,
                                      _access_keyhole_patt))
list_string.readonly = True


def help(name, deep=False, _access_dict=None,
         _access_key_patt=None, _access_keyhole_patt=None):
    u"""List module/method help-texts, i.e signatures + docstrings.

    Arguments:
    
    * name (string) -- full (absolute) name of a module or method,
      e.g. '', 'system', 'module.some.other' (if it is name of a method
      only the method's help-text is returned);
    * deep (bool) -- if set to True, walk through all submodules/methods
      recursively (from all the subtree, not only direct children).

    Result: a list of help-texts (strings).

    """

    return __builtin__.list(_iter_help_texts(name, deep,
                                             _access_dict,
                                             _access_key_patt,
                                             _access_keyhole_patt))
help.readonly = True


def help_string(name, deep=False, _access_dict=None,
                _access_key_patt=None, _access_keyhole_patt=None):
    u"""List module/method help-texts -- as one string.

    Arguments:
    
    * name (string) -- full (absolute) name of a module or method,
      e.g. '', 'system', 'module.some.other' (if it is name of a method
      only the method's help-text is returned);
    * deep (bool) -- if set to True, walk through all submodules/methods
      recursively (from all the subtree, not only direct children).

    Result: a string containing help-texts.

    """

    return u'\n'.join(_iter_help_texts(name, deep,
                                       _access_dict,
                                       _access_key_patt,
                                       _access_keyhole_patt))
help_string.readonly = True


#
# Private functions (containing the actual implementation)
#

def _iter_signatures(module_name, deep,
                     _access_dict, _access_key_patt, _access_keyhole_patt):
    """Iterate over submodule names and method signatures"""

    subitems = _iter_mod_subitems(module_name, deep,
                                  _access_dict,
                                  _access_key_patt,
                                  _access_keyhole_patt)
    signatures = (name + getattr(obj, 'formatted_arg_spec', '')
                  for name, obj in subitems)
    return itertools.chain([module_name], signatures)


def _iter_help_texts(name, deep,
                     _access_dict, _access_key_patt, _access_keyhole_patt):
    """Iterate over module/method help-texts"""

    rpc_obj = rpc_tree.try_to_obtain(name,
                                     _access_dict,
                                     _access_key_patt,
                                     _access_keyhole_patt)

    if isinstance(rpc_obj, rpc_tree.RPCMethod):
        # 1-element iterator with the help-text of the given method
        return iter([rpc_obj.__doc__])
    else:
        subitems = _iter_mod_subitems(name, deep,
                                      _access_dict,
                                      _access_key_patt,
                                      _access_keyhole_patt)
        return (obj.__doc__
                for n, obj in itertools.chain([(name, rpc_obj)], subitems))


def _iter_mod_subitems(module_name, deep,
                       _access_dict, _access_key_patt, _access_keyhole_patt):
    """Iter. over accessible pairs (<full name>, <rpc submodule or method>)"""

    for name, item in rpc_tree.all_items(module_name, deep=deep):
        if rpc_tree.check_access((name, item),
                                 access_dict=_access_dict,
                                 access_key_patt=_access_key_patt,
                                 access_keyhole_patt=_access_keyhole_patt,
                                 required_type=None):
            yield name, item
