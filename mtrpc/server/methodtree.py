"""MTRPC-server-method-tree-related constants, types and functions

Some terminology:
* full name -- an absolute name of RPC-method of RPC-module, e.g.:
  'foo.bar.baz',
* local name -- a name of RPC-method of RPC-module relative to its parent,
  e.g.: 'baz'.
"""

# Author: Jan Kaliszewski (zuo)
# Copyright (c) 2010, MegiTeam


from future_builtins import filter, map, zip

import inspect
import itertools
import re
import string
import warnings

from collections import defaultdict, \
                        Callable, Hashable, Mapping, \
                        MutableSequence, MutableSet, MutableMapping

from ..common.errors import *
from ..common.const import *



#
# Auxiliary types
#

class BadAccessPatternError(Exception):
    "Internal exception: key or keyhole pattern contains illegal {field}"


class LogWarning(UserWarning):
    "Used to log messages using a proper logger without knowning that logger"


class RPCObjectTags(dict):
    "RPC-module's or method's tag dict"

    def __init__(self, tag_dict=None):
        if tag_dict is None:
            tag_dict = {}
        dict.__init__(self, tag_dict)

    def __getitem__(self, key):
        return dict.get(self, key, '')

    def copy(*args, **kwargs): raise NotImplementedError

    @classmethod
    def fromkeys(*args, **kwargs): raise NotImplementedError



#
# Container classes for RPC-methods, RPC-modules and RPC-modules/methods-tree
#

class RPCMethod(Callable):
    '''Callable object wrapper with some additional attributes.

    When called:
    1) there is a check whether the callable takes `<common.const.ACCESS_...>'
       arguments -- if not, they are not passed to it,
    2) there is a check whether given arguments match the argument
       specification of the callable -- if not, RPCMethodArgError is thrown;
    3) the callable is called, the result is returned.

    Public attributes:
    * doc -- RPC-method's docstring,
    * tags -- a mapping (RPCObjectTags instance) with arbitrary content
      (may be used to store some additional info about method).
    '''

    def __init__(self, callable_obj):
        '''Initialize with callable object as the argument.

        Use docstring of the callable object as the `doc' RPC-method attribute
        and its `<common.const.RPC_TAGS>' attribute as `tags' RPC-method
        attribute.

        If callable's argument default values include a mutable object
        a warning will be logged (probably -- that test is not 100% reliable),
        unless `tags' contains 'suppress_mutable_arg_warning' key.
        '''
        
        if not isinstance(callable_obj, Callable):
            raise TypeError('Method object must be callable')
        self.callable_obj = callable_obj
        self.doc = getattr(callable_obj, '__doc__', u'')
        self.tags = RPCObjectTags(getattr(callable_obj, RPC_TAGS, None))
        self._examine_and_prepare_arg_spec()
        self._gen_help()


    def _gen_help(self):
        "Generate method's help-text pattern"
        
        # "{{name}}" will not be substituted know, but kept as "{name}"
        self.help = (u'Method: {{name}}{0}\n    {1}'
                     .format(self.formatted_arg_spec, self.doc))


    def _examine_and_prepare_arg_spec(self):
        spec = inspect.getargspec(self.callable_obj)
        defaults = spec.defaults or ()
        
        # difference between number of args and number of defined default vals
        args_defs_diff = len(spec.args) - len(defaults)
        
        # warn about default arg values that are mutable
        # (such situation may be risky because default values are initialized
        # once, therefore they can be shared by different calls)
        # note: this check is not reliable (but still may appear to be useful)
        if not self.tags['suppress_mutable_arg_warning']:
            for arg_i, default in enumerate(defaults, args_defs_diff):
                if not (self._check_arg_default(default)
                        or spec.args[arg_i] in ACC_KWARGS):
                    warnings.warn("Default value {0!r} of the argument {1!r} "
                                  "of the RPC-method's callable object {2!r} "
                                  "is (probably) a mutable container"
                                  .format(default, arg_i, self.callable_obj),
                                  LogWarning)

        # format official argument specification
        # -- without special access-related arguments (ACC_KWARGS)
        official_args, official_defaults = [], []
        last_acc_kwarg = None
        for def_i, arg in enumerate(spec.args, -args_defs_diff):
            if arg in ACC_KWARGS:
                last_acc_kwarg = arg
            elif last_acc_kwarg:
                TypeError("Bad argument specification of {0!r}: special "
                          "access-related arguments (such as {1!r}) must be "
                          "placed *after* any other arguments (such as {2!r})"
                          .format(self.callable_obj, last_acc_kwarg, arg))
            else:
                official_args.append(arg)
                if def_i >= 0:
                    official_defaults.append(defaults[def_i])
        official_defaults = tuple(official_defaults) or None
        self.formatted_arg_spec = inspect.formatargspec(official_args,
                                                        spec.varargs,
                                                        spec.keywords,
                                                        official_defaults)

        # set attrs informing about special access-related arguments
        self._gets_access_dict = ACCESS_DICT_KWARG in spec.args
        self._gets_access_key = ACCESS_KEY_KWARG in spec.args
        self._gets_access_keyhole = ACCESS_KEYHOLE_KWARG in spec.args
        
        # create argument testing callable object:
        _arg_test_callable_str = ('def _arg_test_callable{0}: pass'
                                  .format(inspect.formatargspec(*spec)))
        _temp_namespace = {}
        exec _arg_test_callable_str in _temp_namespace
        self._arg_test_callable = _temp_namespace['_arg_test_callable']


    @staticmethod
    def _check_arg_default(arg):
        "Try to check if default val is immutable (test isn't 100%-reliable!)"

        return (isinstance(arg, Hashable)
                and not isinstance(arg, (MutableSequence,
                                         MutableSet,
                                         MutableMapping)))


    def __call__(self, *args, **kwargs):
        "Call the method"

        assert ACCESS_DICT_KWARG in kwargs
        assert ACCESS_KEY_KWARG in kwargs
        assert ACCESS_KEYHOLE_KWARG in kwargs

        if not self._gets_access_dict:
            del kwargs[ACCESS_DICT_KWARG]
            
        if not self._gets_access_key:
            del kwargs[ACCESS_KEY_KWARG]

        if not self._gets_access_keyhole:
            del kwargs[ACCESS_KEYHOLE_KWARG]

        try:
            # test given arguments (params)
            self._arg_test_callable(*args, **kwargs)
        except TypeError:
            kwargs.pop(ACCESS_DICT_KWARG, None)
            kwargs.pop(ACCESS_KEY_KWARG, None)
            kwargs.pop(ACCESS_KEYHOLE_KWARG, None)
            a = map(str, args)
            kw = ('{0}={1!r}'.format(name, val)
                  for name, val in sorted(kwargs.iteritems()))
            raise RPCMethodArgError("Given arguments: ({0}) don't match "
                                    "method's argument specification: {1}"
                                    .format(', '.join(itertools.chain(a, kw)),
                                            self.formatted_arg_spec))
        else:
            return self.callable_obj(*args, **kwargs)



class RPCModule(Mapping):
    '''RPC-module maps local names to RPC-methods and other RPC-modules

    Public attributes:
    * doc -- RPC-module's docstring,
    * tags -- a mapping (RPCObjectTags instance) with arbitrary content
      (may be used to store some additional info about module).
    '''
    
    def __init__(self, doc=u'', tags=None):
        'Initialize; optionally with doc and/or tags'
        
        self._method_dict = {}  # maps RPC-method local names to RPC-methods
        self._submod_dict = {}  # maps RPC-module local names to RPC-methods
        # caches:
        self._sorted_method_items = None  # sorted (locname, RPC-method) pairs
        self._sorted_submod_items = None  # sorted (locname, RPC-module) pairs
        
        self.doc = doc
        self.tags = RPCObjectTags(tags)
        self._gen_help()


    def _gen_help(self):
        "Generate module's help-text pattern"
        
        self.help = u'\n    '.join(filter(None, [u'Module: {name}',
                                                 u'{0}'.format(self.doc)]))


    def declare_doc_and_tags(self, doc, tag_dict):
        'Add doc and tags if needed, re-generate help text if needed'
        
        if doc != u'':
            if self.doc:
                assert self.doc == doc, (self.doc, doc)
            else:
                self.doc = doc
                self._gen_help()
        if tag_dict is not None:
            if self.tags:
                assert self.tags == tag_dict
            else:
                self.tags = RPCObjectTags(tag_dict)


    def add_method(self, local_name, rpc_method):
        'Add a new method'
        
        if not local_name:
            raise ValueError("Local RPC-name must not be empty")
        if local_name in self._method_dict:
            raise ValueError("Local RPC-name {0} already is use"
                             .format(local_name))
        if not isinstance(rpc_method, RPCMethod):
            raise TypeError("`rpc_method' argument must be "
                            "an RPCMethod instance")
        self._sorted_method_items = None  # forget the cache
        self._method_dict[local_name] = rpc_method


    def add_submod(self, local_name, rpc_module):
        'Add a new submodule'

        if not local_name:
            raise ValueError("Local RPC-name must not be empty")
        if local_name in self._submod_dict:
            raise ValueError("Local RPC-name {0} already is use"
                             .format(local_name))
        if not isinstance(rpc_module, RPCModule):
            raise TypeError("`rpc_module' argument must be "
                            "an RPCModule instance")
        self._sorted_submod_items = None  # forget the cache
        self._submod_dict[local_name] = rpc_module


    def loc_names2all(self):
        'Iter. sorted (local name, method) pairs + (local name, method) pairs'

        return itertools.chain(self.loc_names2methods(),
                               self.loc_names2submods())


    def loc_names2methods(self):
        'Iterator over sorted (local name, method) pairs'

        if self._sorted_method_items is None:
            # rebuild the cache
            self._sorted_method_items = sorted(self._method_dict.iteritems())
        return iter(self._sorted_method_items)


    def loc_names2submods(self):
        'Iterator over sorted (local name, submodule) pairs'

        if self._sorted_submod_items is None:
            # rebuild the cache
            self._sorted_submod_items = sorted(self._submod_dict.iteritems())
        return iter(self._sorted_submod_items)


    # implementation of Mapping's abstract methods:

    def __iter__(self):
        'Iterator over sorted method local names + sorted module local names'
        return (name for name, _ in self.loc_names2all())

    def __getitem__(self, local_name):
        'Get RPC-submodule or method (by local name)'
        try:
            return self._method_dict[local_name]
        except KeyError:
            return self._submod_dict[local_name]


    # ...and other Mapping's methods:

    def __contains__(self, local_name):
        'Check existence of (local) name'
        return (local_name in self._method_dict
                or local_name in self._submod_dict)

    def __len__(self):
        return len(self._method_dict) + len(self._submod_dict)


    # instances are hashable and will be compared by identity:

    __hash__ = object.__hash__

    def __eq__(self, other):
        return self is other

    def __ne__(self, other):
        return self is not other



class RPCTree(Mapping):
    'Maps full names (hierarchical keys) to RPC-modules and methods (values)'

    RPCMethod = RPCMethod
    RPCModule = RPCModule
    

    def __init__(self):
        'Initialize with root RPC-module only'
        
        self._root_module = RPCModule()
        self._root_module.help = (u"Root module ('')\n    "
                                  u"Contains all the RPC module/method-tree")
        self._item_dict = {'': self._root_module}


    def add_rpc_method(self, module_full_name, method_local_name,
                       callable_obj):
        'Add RPC-method'

        if not module_full_name:
            raise TypeError("Cannot add methods to root module")

        method_full_name = '{0}.{1}'.format(module_full_name,
                                            method_local_name)
        assert method_full_name not in self._item_dict

        rpc_module = self._get_rpc_mod(module_full_name,
                                       arg_name='module_full_name')
        rpc_method = RPCMethod(callable_obj)
        rpc_module.add_method(method_local_name, rpc_method)
        self._item_dict[method_full_name] = rpc_method


    def _get_rpc_mod(self, full_name, arg_name='full_name'):
        rpc_module = self._item_dict[full_name]
        if not isinstance(rpc_module, RPCModule):
            raise TypeError("`{0}' argument must not point to anything else "
                            "than RPCModule instance".format(arg_name))
        return rpc_module


    def get_rpc_module(self, full_name, doc=u'', tag_dict=None):
        'Get RPC-module; if needed, create it and any missing ancestors of it'

        rpc_module = self._item_dict.get(full_name)
        if rpc_module is None:
            if full_name.startswith('.'):
                raise ValueError("RPC-name must not start with '.'")
            split_name = full_name.split('.')
            parent_full_name = '.'.join(split_name[:-1])
            
            # *recursively*
            # get/create ancestor module(s):
            parent_rpc_module = self.get_rpc_module(parent_full_name)
            
            # create this module:
            rpc_module = RPCModule(doc, tag_dict)

            # add it to the parent module:
            local_name = split_name[-1]
            parent_rpc_module.add_submod(local_name, rpc_module)

            # add it to the tree:
            self._item_dict[full_name] = rpc_module
        elif not isinstance(rpc_module, RPCModule):
            raise TypeError("`full_name' argument must not point "
                            "to anything else than RPCModule instance")
        else:
            # update module doc and tags if needed:
            rpc_module.declare_doc_and_tags(doc, tag_dict)
            
        return rpc_module


    def try_to_obtain(self, full_name, access_dict, access_key_patt,
                      access_keyhole_patt, required_type=None):
        "Restricted access: get RPC-module/method only if key matches keyhole"
                            
        # RPC-object name must be a key in the RPC-tree
        try:
            rpc_object = self[full_name]
        except KeyError:
            raise RPCNotFoundError('RPC-name not found: {0}'
                                   .format(full_name))
        try:
            if self.check_access((full_name, rpc_object), access_dict,
                                 access_key_patt, access_keyhole_patt,
                                 required_type):
                return rpc_object
            else:
                raise RPCNotFoundError('RPC-name not found: {0}'
                                       .format(full_name))
        except TypeError as exc:
            try:
                if not exc.args[0].startswith('Bad RPC-object type'):
                    raise TypeError
            except (IndexError, AttributeError, TypeError):
                raise exc
            else:
                raise RPCNotFoundError(exc.args[0])


    @staticmethod
    def check_access(rpc_item, access_dict, access_key_patt,
                     access_keyhole_patt, required_type=None):

        full_name, rpc_object = rpc_item   # (name, RPC-method/module)
        rpc_object_type = type(rpc_object)  # RPCMethod or RPCModule
        
        # If type is specified, the RPC-object must be an instance of it
        if not (required_type is None
                or issubclass(rpc_object_type, required_type)):
            raise TypeError('Bad RPC-object type ({0} required)'
                            .format(required_type.__name__))
        
        split_name = full_name.split('.')
        
        # Creating the actual access dict...
        actual_access_dict = dict(
                full_name=full_name,
                local_name=split_name[-1],
                parentmod_name='.'.join(split_name[:-1]),
                split_name=split_name,
                doc=rpc_object.doc,
                tags=rpc_object.tags,
                help=rpc_object.help.format(name=full_name),
                type=rpc_object_type,
        )
        # ...also with fields set in RPCManager.create_access_dict()
        actual_access_dict.update(access_dict)

        # Formatting access_key (using actual_access_dict)...
        try:
            access_key = access_key_patt.format(**actual_access_dict)
        except KeyError:
            raise BadAccessPatternError('access_key_patt: {0!r}'
                                        .format(access_key_patt))
        # Formatting access_keyhole (using actual_access_dict)...
        try:
            access_keyhole = access_keyhole_patt.format(**actual_access_dict)
        except KeyError:
            raise BadAccessPatternError('access_keyhole_patt: {0!r}'
                                        .format(access_keyhole_patt))
            
        # Test: access_key must match access_keyhole (regular expression)
        if re.search(access_keyhole, access_key):
            return True
        else:
            return False
        

    #
    # Iterators

    # ...over RPC-method/module names:

    def all_names(self, full_name='', get_relative_names=False, deep=False):
        "Iterator over sorted RPC-method/submodule names"
        
        rpc_module = self._get_rpc_mod(full_name)
        if deep:
            # get recursively
            return self._iter_subtree(full_name, rpc_module,
                                      get_relative_names,
                                      get_names_only=True,
                                      include_methods=True,
                                      include_submods=True)
        else:
            prefix = ('' if get_relative_names else full_name)
            return self._iter_prefixed_names(prefix,
                                             rpc_module.loc_names2all())


    def method_names(self, full_name='', get_relative_names=False, deep=False):
        "Iterator over sorted RPC-method names"
        
        rpc_module = self._get_rpc_mod(full_name)
        if deep:
            # get recursively
            return self._iter_subtree(full_name, rpc_module,
                                      get_relative_names,
                                      get_names_only=True,
                                      include_methods=True,
                                      include_submods=False)
        else:
            prefix = ('' if get_relative_names else full_name)
            return self._iter_prefixed_names(prefix,
                                             rpc_module.loc_names2methods())

    
    def submod_names(self, full_name='', get_relative_names=False, deep=False):
        "Iterator over sorted RPC-submodule names"
        
        rpc_module = self._get_rpc_mod(full_name)
        if deep:
            # get recursively
            return self._iter_subtree(full_name, rpc_module,
                                      get_relative_names,
                                      get_names_only=True,
                                      include_methods=False,
                                      include_submods=True)
        else:
            prefix = ('' if get_relative_names else full_name)
            return self._iter_prefixed_names(prefix,
                                             rpc_module.loc_names2submods())


    # over (<name>, <RPC-method or module object>) pairs:
    
    def all_items(self, full_name='', get_relative_names=False, deep=False):
        "Iterator over (name, RPC-method/submodule) pairs (sorted by name)"

        rpc_module = self._get_rpc_mod(full_name)
        if deep:
            # get recursively
            return self._iter_subtree(full_name, rpc_module,
                                      get_relative_names,
                                      get_names_only=False,
                                      include_methods=True,
                                      include_submods=True)
        else:
            prefix = ('' if get_relative_names else full_name)
            return self._iter_prefixed_items(prefix,
                                             rpc_module.loc_names2all())


    def method_items(self, full_name='', get_relative_names=False, deep=False):
        "Iterator over (name, RPC-method) pairs (sorted by method name)"

        rpc_module = self._get_rpc_mod(full_name)
        if deep:
            # get recursively
            return self._iter_subtree(full_name, rpc_module,
                                      get_relative_names,
                                      get_names_only=False,
                                      include_methods=True,
                                      include_submods=False)
        else:
            prefix = ('' if get_relative_names else full_name)
            return self._iter_prefixed_items(prefix,
                                             rpc_module.loc_names2methods())


    def submod_items(self, full_name='', get_relative_names=False, deep=False):
        "Iterator over (name, RPC-submodule) pairs (sorted by submodule name)"

        rpc_module = self._get_rpc_mod(full_name)
        if deep:
            # get recursively
            return self._iter_subtree(full_name, rpc_module,
                                      get_relative_names,
                                      get_names_only=False,
                                      include_methods=False,
                                      include_submods=True)
        else:
            prefix = ('' if get_relative_names else full_name)
            return self._iter_prefixed_items(prefix,
                                             rpc_module.loc_names2submods())


    def _iter_subtree(self, base_full_name, rpc_module,
                      get_relative_names, get_names_only,
                      include_methods, include_submods,
                      include_this_module=False, sufix=''):

        if sufix:
            sufix += '.'
            if base_full_name:
                full_prefix = base_full_name + '.' + sufix
            else:
                full_prefix = sufix
        else:
            if base_full_name:
                full_prefix = base_full_name + '.'
            else:
                full_prefix = ''
            
        if get_relative_names:
            prefix = sufix
        else:
            prefix = full_prefix

        submod_items = ((sufix + local_name, submod)
                        for local_name, submod
                        in rpc_module.loc_names2submods())
        # *recursion*:
        subtree = (self._iter_subtree(base_full_name, submodule,
                                      get_relative_names, get_names_only,
                                      include_methods, include_submods,
                                      include_this_module=include_submods,
                                      sufix=subsufix)
                   for subsufix, submodule in submod_items)
        subiter = itertools.chain.from_iterable(subtree)

        with_this_module = ()
        with_methods = ()
        if get_names_only:
            if include_this_module:
                with_this_module = [prefix.rstrip('.')]   # (this module name)
            if include_methods:
                with_methods = (prefix + local_name
                                for local_name, _
                                in rpc_module.loc_names2methods())
        else:
            if include_this_module:
                with_this_module = [(prefix.rstrip('.'), rpc_module)]
            if include_methods:
                with_methods = ((prefix + local_name, method)
                                for local_name, method
                                in rpc_module.loc_names2methods())
            
        return itertools.chain(with_this_module, with_methods, subiter)


    @staticmethod
    def _iter_prefixed_names(prefix, items):
        if prefix:
            prefix += '.'
            return (prefix + name for name, _ in items)
        else:
            return (name for name, _ in items)


    @staticmethod
    def _iter_prefixed_items(prefix, items):
        if prefix:
            prefix += '.'
            return ((prefix + name, obj) for name, obj in items)
        else:
            return iter(items)


    # implementation of necessary Mapping's methods:

    def __iter__(self):
        return itertools.chain([''], self.all_names(deep=True))

    def __getitem__(self, full_name):
        'Get RPC-module or method (by full name)'
        return self._item_dict[full_name]

    def __contains__(self, full_name):
        'Check existence of (full) name'
        return full_name in self._item_dict

    def __len__(self):
        return len(self._item_dict)

    # instances are not hashable:
    
    __hash__ = None



#
# Buliding tree...
#

_NAME_CHARS = frozenset(string.ascii_letters + string.digits + '_.')

def build_rpc_tree(root_mod,
                   default_mod_init_callable=(lambda: None),
                   mod_init_kwargs=None):
                       
    'Walk thru py-modules, populating RPC-tree with RPC-modules and methods'

    if mod_init_kwargs is None: mod_init_kwargs = {}
    rpc_tree = RPCTree()
    _build_subtree(rpc_tree, root_mod, '',
                   default_mod_init_callable, mod_init_kwargs,
                   ancestor_mods=set(), initialized_mods={},
                   mods2anticipated_names=defaultdict(set))
    return rpc_tree


def _build_subtree(rpc_tree, cur_mod, cur_full_name,
                   default_mod_init_callable, mod_init_kwargs,
                   ancestor_mods, initialized_mods,
                   mods2anticipated_names):
    
    # get <cur_mod>.__rpc_methods__ attribute -- a list of RPC-method names
    names = getattr(cur_mod, RPC_METHOD_LIST, ())
    if isinstance(names, basestring):
        names = [names]  # (string is treated as single item)

    if not all(_NAME_CHARS.issuperset(name)
               or (_NAME_CHARS.issuperset(name[:-1])
                   and (name == '*' or name.endswith('.*')))
               for name in names):
        raise ValueError('Illegal characters in item(s) of {0}, in {1} module'
                         .format(RPC_METHOD_LIST, cur_full_name))

    mod_names2objs = dict(inspect.getmembers(cur_mod, inspect.ismodule))
    mod_objs = set(mod_names2objs.itervalues())

    # (the module (cur_mod) might be mentioned in __rpc_methods__
    # of some higher module, in "mod1.mod2.mod3.method"-way)
    ant_names = mods2anticipated_names[(cur_mod, cur_full_name)].union(names)
    
    # '*' symbol means: all *public functions* (not all callable objects)
    if '*' in ant_names:
        func_names = set(dict(inspect.getmembers(cur_mod, inspect.isfunction)))
        try:
            cur_mod_public = cur_mod.__all__
        except AttributeError:
            # no __all__ attribute
            public_func_names = (name for name in func_names
                                 if not name.startswith('_'))
        else:
            public_func_names = func_names.intersection(cur_mod_public)
        ant_names.update(public_func_names)
        ant_names.remove('*')

    doc = getattr(cur_mod, RPC_MODULE_DOC, u'')
    tag_dict = getattr(cur_mod, RPC_TAGS, None)
    if ant_names or doc or tag_dict:
        _full_name = initialized_mods.setdefault(cur_mod, cur_full_name)
        if _full_name != cur_full_name:
            warnings.warn('Cannot create RPC-module {0} based on Python-'
                               'module {1!r} -- that Python-module has been '
                               'already used as a base for {2} RPC-module.'
                               .format(cur_full_name, cur_mod,
                                       _full_name))
            return
        # declare (create if needed) RPC-module
        rpc_tree.get_rpc_module(cur_full_name, doc, tag_dict)
        # run module initialization callable (with Python module as 1st arg)
        mod_init_callable = getattr(cur_mod, RPC_INIT_CALLABLE,
                                    default_mod_init_callable)
        _mod_init(mod_init_callable, mod_init_kwargs,
                  cur_mod, cur_full_name, rpc_tree)
            
    for name in ant_names:
        split_name = name.split('.')
        if len(split_name) > 1:
            mod_name = split_name[0]
            mod = getattr(cur_mod, mod_name)
            if mod in mod_objs:
                rest_of_name = '.'.join(split_name[1:])
                if cur_full_name:
                    mod_full_name = '{0}.{1}'.format(cur_full_name, mod_name)
                else:
                    mod_full_name = mod_name
                mods2anticipated_names[(mod, mod_full_name)].add(rest_of_name)
            else:
                raise TypeError('{0}.{1} is not a module'
                                .format(cur_full_name, name))
        else:
            try:
                callable_obj = getattr(cur_mod, name)
            except AttributeError:
                warnings.warn('No such method: {0}.{1} (so we skip it)'
                              .format(cur_full_name, name), LogWarning)
            else:
                # create and put into tree RPC-method
                rpc_tree.add_rpc_method(cur_full_name, name, callable_obj)

    ancestor_mods.add(cur_mod)
    for mod_name, mod in mod_names2objs.iteritems():
        if mod in ancestor_mods:
            warnings.warn('Module {0} contains cyclic module reference: '
                          '{1} (we must break that cycle)'
                          .format(cur_full_name, mod_name), LogWarning)
        else:
            if cur_full_name:
                mod_full_name = '{0}.{1}'.format(cur_full_name, mod_name)
            else:
                mod_full_name = mod_name
            # *recursion*:
            _build_subtree(rpc_tree, mod, mod_full_name,
                           default_mod_init_callable, mod_init_kwargs,
                           ancestor_mods, initialized_mods,
                           mods2anticipated_names)
    ancestor_mods.remove(cur_mod)


def _mod_init(mod_init_callable, mod_init_kwargs, mod, full_name, rpc_tree):
    "Run module's (or the default) init callable"
                       
    # prepare kwargs for the particular init callable
    _init_kwargs = mod_init_kwargs.copy()
    _init_kwargs.update(
            mod=mod,
            full_name=full_name,
            rpc_tree=rpc_tree,
    )
    arg_names = inspect.getargspec(mod_init_callable).args
    try:
        (this_mod_init_kwargs
        ) = dict((name, _init_kwargs[name]) for name in arg_names)
    except KeyError as exc:
        raise KeyError("Init callable used for module {0} (based on Python "
                       "module: {1!r}) takes keyword argument '{2}' but "
                       "given keyword arg. dict {3!r} doesn't contain it"
                       .format(full_name, mod, exc.args[0], _init_kwargs))

    # run the init callable
    mod_init_callable(**this_mod_init_kwargs)
