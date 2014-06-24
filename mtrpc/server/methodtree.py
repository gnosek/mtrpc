# mtrpc/server/methodtree.py
#
# Author: Jan Kaliszewski (zuo)
# Copyright (c) 2010, MegiTeam

"""MTRPC-server-method-tree-related classes.

----------------
Terminology note
----------------

* full name -- an absolute dot-separated name of RPC-method of RPC-module,
  e.g.: 'foo.bar.baz',
* local name -- a name of RPC-method of RPC-module relative to its parent,
  e.g.: 'baz'.

"""

import inspect
import itertools
import os
import string
import textwrap
import traceback
import types
import warnings
import sys
import imp
from mtrpc.common import utils

from collections import defaultdict, Callable, Mapping

from mtrpc.common.const import RPC_METHOD_LIST, RPC_POSTINIT, RPC_MODULE_DOC, DEFAULT_LOG_HANDLER_SETTINGS
from mtrpc.common.errors import RPCMethodArgError, RPCNotFoundError
from mtrpc.server import schema


#
# Auxiliary types
#
class BadAccessPatternError(Exception):
    """Internal exception: key or keyhole pattern contains illegal {field}"""


class DocDecodeError(UnicodeError):
    """Internal exception: cannot convert RPC-module/method doc to unicode"""


def format_help(head, body, body_indent):
    body_lines = prepare_doc(body).splitlines()

    yield head
    if body_lines:
        yield body_indent
        for line in body_lines:
            yield body_indent + line
        yield body_indent
    yield ''


def format_method_help(full_name, callable_obj):
    doc = getattr(callable_obj, '__doc__', u'')
    argspec = get_effective_signature(callable_obj)
    head = u'    * {full_name}{argspec}\n'.format(full_name=full_name, argspec=argspec)
    return '\n'.join(format_help(head, doc, u'        '))


def format_module_help(full_name, module_doc):
    if not full_name:
        full_name = u'[root]'

    head = u'Module: {0}\n'.format(full_name)
    return '\n'.join(format_help(head, module_doc, u'    '))


def prepare_doc(doc):
    """Prepare RPC-object doc (assert that it's Unicode, trim it etc.)"""

    if not doc:
        return u''

    try:
        doc = unicode(doc)
    except UnicodeError:
        raise DocDecodeError('Cannot convert documentation string of '
                             '{{rpc_kind}} {{full_name}} to unicode. '
                             'To be on the safe side you should always '
                             'assure that all your RPC-module/method '
                             'docs containing any non-ASCII characters '
                             'are of unicode type (not of str). '
                             'Original exception info follows:\n{0}'
                             .format(traceback.format_exc()))

    doc = doc.strip()
    try:
        first_line, rest = doc.split('\n', 1)
    except ValueError:
        return doc
    else:
        return u'\n'.join((first_line.strip(), textwrap.dedent(rest)))


def get_effective_signature(obj):
    spec = inspect.getargspec(obj)
    defaults = spec.defaults or ()
    # difference between number of args and number of defined default vals
    args_defs_diff = len(spec.args) - len(defaults)

    # format official argument specification
    # -- without special access-related arguments (ACC_KWARGS)
    official_args, official_defaults = [], []
    for def_i, arg in enumerate(spec.args, -args_defs_diff):
        official_args.append(arg)
        if def_i >= 0:
            official_defaults.append(defaults[def_i])
    official_defaults = tuple(official_defaults) or None
    return inspect.formatargspec(official_args, spec.varargs, spec.keywords, official_defaults)


class RPCMethod(Callable):
    """Callable object wrapper with some additional attributes.

    When an instance is called:
    1) there is a check whether given arguments match the argument
       specification of the callable -- if not, RPCMethodArgError is thrown;
    2) the callable is called, the result is returned.
    """

    def __init__(self, callable_obj, full_name=''):

        """Initialize with a callable object as the argument.

        Use docstring of the callable object as a base for the `doc'
        RPC-method attribute.
        """

        if not isinstance(callable_obj, Callable):
            raise TypeError('Method object must be callable')
        self.callable_obj = callable_obj
        spec = inspect.getargspec(self.callable_obj)
        self.formatted_arg_spec = get_effective_signature(self.callable_obj)
        self._test_argspec(spec)
        self.full_name = full_name
        self.__doc__ = format_method_help(full_name, callable_obj)
        try:
            self.module = sys.modules[callable_obj.__module__]
        except (AttributeError, KeyError):
            self.module = None
        self.readonly = getattr(callable_obj, 'readonly', False)

    def _test_argspec(self, spec):
        # create argument testing callable object:
        _arg_test_callable_str = ('def _arg_test_callable{0}: pass'
                                  .format(inspect.formatargspec(*spec)))
        _temp_namespace = {}
        exec _arg_test_callable_str in _temp_namespace
        self._arg_test_callable = _temp_namespace['_arg_test_callable']

    def format_args(self, args, kw):
        """Format arguments in a way suitable for logging"""

        spec = inspect.getargspec(self.callable_obj)
        defaults = list(spec.defaults or ())
        reqd_count = len(spec.args) - len(defaults)
        real_args = [None] * reqd_count + defaults
        real_args[0:len(args)] = args
        for i, arg in enumerate(spec.args):
            if 'passw' in arg:
                real_args[i] = '***'
            elif arg in kw:
                real_args[i] = kw[arg]

        real_args[len(spec.args):] = []
        real_args = [utils.log_repr(a) for a in real_args]

        return inspect.formatargspec(spec.args, spec.varargs,
                                     spec.keywords, real_args, formatvalue=lambda v: '=' + v)

    def authorize(self, **kwargs):
        if hasattr(self.callable_obj, 'authorize'):
            self.callable_obj.authorize(**kwargs)  # raise RPCAccessDenied on auth error
        else:
            return NotImplemented

    def __call__(self, *args, **kw):
        """Call the method"""

        try:
            # test given arguments (params)
            self._arg_test_callable(*args, **kw)
        except TypeError:
            self._raise_arg_error(args, kw)
        else:
            return self.callable_obj(*args, **kw)

    def _raise_arg_error(self, args, kw):
        a = itertools.imap(repr, args)
        kw = ('{0}={1!r}'.format(name, val) for name, val in sorted(kw.iteritems()))
        raise RPCMethodArgError("Cannot call method {{name}} -- "
                                "given arguments: ({0}) don't match "
                                "method argument specification: {1}"
                                .format(', '.join(itertools.chain(a, kw)), self.formatted_arg_spec))


class RPCModule(Mapping):
    """RPC-module maps local names to RPC-methods and other RPC-modules"""

    def __init__(self, full_name, doc=u''):
        """Initialize; optionally with doc"""
        self._method_dict = {}  # maps RPC-method local names to RPC-methods
        self._submod_dict = {}  # maps RPC-module local names to RPC-methods
        # caches:
        self._sorted_method_items = None  # sorted (locname, RPC-method) pairs
        self._sorted_submod_items = None  # sorted (locname, RPC-module) pairs
        # public attributes:
        self.full_name = full_name
        self.__doc__ = format_module_help(full_name, doc)

    def authorize(self, **kwargs):
        pass

    def declare_attrs(self, doc):
        """Add doc if needed, re-generate help text if needed"""
        self.__doc__ = format_module_help(self.full_name, doc)

    def add_method(self, local_name, rpc_method):
        """Add a new method"""
        if not local_name:
            raise ValueError("Local RPC-name must not be empty")
        if local_name in self._method_dict:
            raise ValueError("Local RPC-name {0} already is use".format(local_name))
        if not isinstance(rpc_method, RPCMethod):
            raise TypeError("`rpc_method' argument must be an RPCMethod instance")
        self._sorted_method_items = None  # forget the cache
        self._method_dict[local_name] = rpc_method

    def add_submod(self, local_name, rpc_module):
        """Add a new submodule"""
        if not local_name:
            raise ValueError("Local RPC-name must not be empty")
        if local_name in self._submod_dict:
            raise ValueError("Local RPC-name {0} already is use".format(local_name))
        if not isinstance(rpc_module, RPCModule):
            raise TypeError("`rpc_module' argument must be an RPCModule instance")
        self._sorted_submod_items = None  # forget the cache
        self._submod_dict[local_name] = rpc_module

    def loc_names2all(self):
        """Iterate over sorted (local name, method) pairs and
        (local name, submodule) pairs"""
        return itertools.chain(self.loc_names2methods(),
                               self.loc_names2submods())

    def loc_names2methods(self):
        """Iterator over sorted (local name, method) pairs"""
        if self._sorted_method_items is None:
            # rebuild the cache
            self._sorted_method_items = sorted(self._method_dict.iteritems())
        return iter(self._sorted_method_items)

    def loc_names2submods(self):
        """Iterator over sorted (local name, submodule) pairs"""
        if self._sorted_submod_items is None:
            # rebuild the cache
            self._sorted_submod_items = sorted(self._submod_dict.iteritems())
        return iter(self._sorted_submod_items)

    def contains_methods(self):
        """Does it contain any methods?"""
        return bool(self._method_dict)

    def contains_submods(self):
        """Does it contain any submodules?"""
        return bool(self._submod_dict)

    # implementation of Mapping's abstract methods:

    def __iter__(self):
        """Iterator over sorted method local names + sorted module local names"""
        return (name for name, _ in self.loc_names2all())

    def __getitem__(self, local_name):
        """Get RPC-submodule or method (by local name)"""
        try:
            return self._method_dict[local_name]
        except KeyError:
            return self._submod_dict[local_name]

    # ...and other Mapping's methods:
    def __contains__(self, local_name):
        """Check existence of (local) name"""
        return local_name in self._method_dict or local_name in self._submod_dict

    def __len__(self):
        return len(self._method_dict) + len(self._submod_dict)

    # instances are hashable and will be compared by identity:
    __hash__ = object.__hash__

    def __eq__(self, other):
        return self is other

    def __ne__(self, other):
        return self is not other


class RPCTree(Mapping):
    """Maps full names (hierarchical keys) to RPC-modules and methods (values)"""

    NAME_CHARS = frozenset(string.ascii_letters + string.digits + '_.')

    RPCMethod = RPCMethod
    RPCModule = RPCModule

    CONFIG_DEFAULTS = {
        'rpc_tree_init': {
            'paths': [],
            'imports': ['mtrpc.server.sysmethods as system'],
            'postinit_kwargs': {
                'logging_settings': {
                    'mod_logger_pattern': 'mtrpc.server.rpc_log.{full_name}',
                    'level': 'warning',
                    'handlers': [DEFAULT_LOG_HANDLER_SETTINGS],
                    'propagate': False,
                    'custom_mod_loggers': {}
                },
                'mod_globals': {}
            }
        }
    }

    CONFIG_SCHEMAS = [schema.by_example(CONFIG_DEFAULTS)]

    @classmethod
    def load(cls, config, rpc_mode):
        paths = config['rpc_tree_init']['paths']
        imports = config['rpc_tree_init']['imports']
        postinit_kwargs = config['rpc_tree_init']['postinit_kwargs']
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
                raise ValueError('Cannot load module from path "{0}" as '
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
                raise ValueError('Cannot import module "{0}" as "{1}" -- '
                                 'because "{1}" name is already used by '
                                 'module {2!r}'
                                 .format(src_name, dst_name, name_owner))

        return cls(root_mod, utils.basic_postinit, postinit_kwargs, rpc_mode)

    def __init__(self,
                 root_pymod=None,
                 default_postinit_callable=(lambda: None),
                 postinit_kwargs=None,
                 rpc_mode='server'):

        """Build the tree (populate it with RPC-modules/methods)"""

        self.item_dict = {}  # maps full names to RPC-objects
        self.rpc_mode = rpc_mode
        if postinit_kwargs is None:
            postinit_kwargs = {}

        self._build_subtree(root_pymod, '',
                            default_postinit_callable, postinit_kwargs,
                            ancestor_pymods=set(), initialized_pymods={},
                            pymods2anticipated_names=defaultdict(set))

    def _build_subtree(self, cur_pymod, cur_full_name,
                       default_postinit_callable, postinit_kwargs,
                       ancestor_pymods, initialized_pymods,
                       pymods2anticipated_names):

        """Walk through py-modules populating the tree with RPC-modules/methods"""

        # from cur_pymod get __rpc_methods__ -- a list of RPC-method names
        names = getattr(cur_pymod, RPC_METHOD_LIST, ())
        if isinstance(names, basestring):
            names = [names]  # (string is treated as single item)

        if not all(self.NAME_CHARS.issuperset(name)
                   or (self.NAME_CHARS.issuperset(name[:-1])
                       and (name == '*' or name.endswith('.*')))
                   for name in names):
            raise ValueError('Illegal characters in item(s) of {0}, in {1} '
                             'module'.format(RPC_METHOD_LIST, cur_full_name))

        pymod_names2objs = dict(inspect.getmembers(cur_pymod, inspect.ismodule))
        pymod_objs = set(pymod_names2objs.itervalues())

        # (the module local name might be mentioned in __rpc_methods__
        # of some higher module, in "mod1.mod2.mod3.method"-way)
        ant_names = pymods2anticipated_names[(cur_pymod, cur_full_name)].union(names)
        scoped_ant_names = set(name.split('.')[0] for name in ant_names)

        # '*' symbol means: all *public functions* (not all callable objects)
        if '*' in ant_names:
            func_names = set(dict(inspect.getmembers(cur_pymod, inspect.isfunction)))
            try:
                cur_pymod_public = cur_pymod.__all__
            except AttributeError:
                # no __all__ attribute
                public_func_names = (name for name in func_names
                                     if not name.startswith('_'))
            else:
                public_func_names = func_names.intersection(cur_pymod_public)
            ant_names.update(public_func_names)
            ant_names.remove('*')

        try:
            doc = prepare_doc(getattr(cur_pymod, RPC_MODULE_DOC, None))
        except DocDecodeError as exc:
            raise UnicodeError(exc.args[0].format(rpc_kind='RPC-module',
                                                  full_name=cur_full_name))

        postinit_callable = getattr(cur_pymod, RPC_POSTINIT, None)

        if ant_names or doc or postinit_callable:
            _full_name = initialized_pymods.setdefault(cur_pymod, cur_full_name)
            if _full_name != cur_full_name:
                # !TODO! -- sprawdzic czy return tutaj jest ok...
                # raise RuntimeError('Cannot create RPC-module {0} based on '
                # 'Python-module {1!r} -- that Python-'
                # 'module has been already used as a base '
                # 'for {2} RPC-module.'.format(cur_full_name,
                # cur_pymod,
                # _full_name))
                warnings.warn('Cannot create RPC-module {0} based on '
                              'Python-module {1!r} -- that Python-'
                              'module has been already used as a base '
                              'for {2} RPC-module.'.format(cur_full_name,
                                                           cur_pymod,
                                                           _full_name))
                return
            # declare (create if needed) RPC-module
            self.get_rpc_module(cur_full_name, doc)
            # post-init on Python module
            if postinit_callable is None:
                postinit_callable = default_postinit_callable
            self._mod_postinit(postinit_callable, postinit_kwargs,
                               cur_pymod, cur_full_name)

        for name in ant_names:
            split_name = name.split('.')
            if len(split_name) > 1:
                mod_name = split_name[0]
                pymod = getattr(cur_pymod, mod_name)
                if pymod in pymod_objs:
                    rest_of_name = '.'.join(split_name[1:])
                    if cur_full_name:
                        mod_full_name = '{0}.{1}'.format(cur_full_name, mod_name)
                    else:
                        mod_full_name = mod_name
                    pymods2anticipated_names[(pymod, mod_full_name)].add(rest_of_name)
                else:
                    raise TypeError('{0}.{1} is not a module'
                                    .format(cur_full_name, name))
            else:
                meth_full_name = '{0}.{1}'.format(cur_full_name, name)
                callable_obj = getattr(cur_pymod, name)
                # create and put into tree an RPC-method
                try:
                    self.add_rpc_method(cur_full_name, name, callable_obj)
                except DocDecodeError as exc:
                    raise UnicodeError(exc.args[0].format(rpc_kind='RPC-method', full_name=meth_full_name))

        ancestor_pymods.add(cur_pymod)

        for mod_name, pymod in pymod_names2objs.iteritems():
            if mod_name not in scoped_ant_names:
                continue

            if pymod in ancestor_pymods:
                raise ValueError('Module reference cycle {0} -> {1}'.format(cur_full_name, mod_name))
            else:
                if cur_full_name:
                    mod_full_name = '{0}.{1}'.format(cur_full_name, mod_name)
                else:
                    mod_full_name = mod_name
                # *recursion*:
                self._build_subtree(pymod,
                                    mod_full_name,
                                    default_postinit_callable,
                                    postinit_kwargs,
                                    ancestor_pymods,
                                    initialized_pymods,
                                    pymods2anticipated_names)

        ancestor_pymods.remove(cur_pymod)

    def _mod_postinit(self, postinit_callable, postinit_kwargs,
                      pymod, full_name):

        """Run module post-init callable -- default or module's custom one"""

        # prepare kwargs for the particular post-init callable
        _kwargs = postinit_kwargs.copy()
        _kwargs.update(
            mod=pymod,
            full_name=full_name,
            rpc_tree=self,
        )
        arg_names = inspect.getargspec(postinit_callable).args
        try:
            this_postinit_kwargs = dict((name, _kwargs[name]) for name in arg_names)
        except KeyError as exc:
            raise KeyError("Post-init callable used for module {0} "
                           "(based on Python module {1!r}) takes "
                           "keyword argument '{2}' but given keyword "
                           "arg. dict {3!r} doesn't contain it"
                           .format(full_name, pymod, exc.args[0], _kwargs))

        # run the post-init callable
        postinit_callable(**this_postinit_kwargs)

    def add_rpc_method(self, module_full_name, method_local_name, callable_obj):
        """Add RPC-method"""

        if not callable(callable_obj):
            return

        if not module_full_name:
            raise TypeError("Cannot add methods to root module")

        method_full_name = '{0}.{1}'.format(module_full_name,
                                            method_local_name)
        assert method_full_name not in self.item_dict

        rpc_module = self.item_dict[module_full_name]
        rpc_method = RPCMethod(callable_obj, method_full_name)
        rpc_module.add_method(method_local_name, rpc_method)
        self.item_dict[method_full_name] = rpc_method

    def get_rpc_module(self, full_name, doc=u''):

        """Get RPC-module; if needed, create it and any missing ancestors of it"""

        rpc_module = self.item_dict.get(full_name)
        if rpc_module is None:
            if full_name.startswith('.'):
                raise ValueError("RPC-name must not start with '.'")

            elif full_name == '':
                # create the root module
                rpc_module = RPCModule(full_name, doc)

            else:
                split_name = full_name.split('.')
                parent_full_name = '.'.join(split_name[:-1])

                # *recursively*
                # get/create ancestor module(s):
                parent_rpc_module = self.get_rpc_module(parent_full_name)

                # create this module:
                rpc_module = RPCModule(full_name, doc)

                # add it to the parent module:
                local_name = split_name[-1]
                parent_rpc_module.add_submod(local_name, rpc_module)

            # add it to the tree:
            self.item_dict[full_name] = rpc_module
        elif not isinstance(rpc_module, RPCModule):
            raise TypeError("`full_name' argument must point to an RPCModule instance")
        else:
            # update module doc if needed:
            rpc_module.declare_attrs(doc)

        return rpc_module

    def try_to_obtain(self, full_name, access_dict, required_type=None):

        """Restricted access: get RPC-module/method only if key matches keyhole"""

        # RPC-object name must be a key in the RPC-tree
        try:
            rpc_object = self[full_name]
        except KeyError:
            raise RPCNotFoundError('RPC-name not found: {0}'.format(full_name))
        rpc_object.authorize(**access_dict)
        return rpc_object

    #
    # Iterators
    # ...over RPC-method/module names:
    def all_names(self, full_name='', get_relative_names=False, deep=False):

        """Iterator over sorted RPC-method/submodule names"""

        rpc_module = self.item_dict[full_name]
        if deep:
            # get recursively
            return self._iter_subtree(full_name, rpc_module,
                                      get_relative_names,
                                      get_names_only=True,
                                      include_methods=True,
                                      include_submods=True)
        else:
            prefix = ('' if get_relative_names else full_name)
            return self._iter_prefixed_names(prefix, rpc_module.loc_names2all())

    # over (<name>, <RPC-method or module object>) pairs:
    def all_items(self, full_name='', get_relative_names=False, deep=False):

        """Iterator over (name, RPC-method/submodule) pairs (sorted by name)"""

        rpc_module = self.item_dict[full_name]
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

    def _iter_subtree(self, base_full_name, rpc_module,
                      get_relative_names, get_names_only,
                      include_methods, include_submods,
                      include_this_module=False, suffix=''):

        if suffix:
            suffix += '.'
            if base_full_name:
                full_prefix = base_full_name + '.' + suffix
            else:
                full_prefix = suffix
        else:
            if base_full_name:
                full_prefix = base_full_name + '.'
            else:
                full_prefix = ''

        if get_relative_names:
            prefix = suffix
        else:
            prefix = full_prefix

        submod_items = ((suffix + local_name, submod)
                        for local_name, submod
                        in rpc_module.loc_names2submods())
        # *recursion*:
        subtree = (self._iter_subtree(base_full_name, submodule,
                                      get_relative_names, get_names_only,
                                      include_methods, include_submods,
                                      include_this_module=include_submods,
                                      suffix=subsuffix)
                   for subsuffix, submodule in submod_items)
        subiter = itertools.chain.from_iterable(subtree)

        with_this_module = ()
        with_methods = ()
        if get_names_only:
            if include_this_module:
                with_this_module = [prefix.rstrip('.')]  # (this module name)
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
        """Get RPC-module or method (by full name)"""
        return self.item_dict[full_name]

    def __contains__(self, full_name):
        """Check existence of (full) name"""
        return full_name in self.item_dict

    def __len__(self):
        return len(self.item_dict)

    # instances are not hashable:

    __hash__ = None


class RPCSubTree(object):
    def build_attrs(self):
        if self.prefix:
            prefix = self.prefix + '.'
        else:
            prefix = ''
        for key, method in self.rpc_tree.item_dict.iteritems():
            if not callable(method):
                continue
            if not key.startswith(prefix):
                continue
            k = key[len(prefix):]
            if '.' in k:
                k, tail = k.split('.', 1)
                setattr(self, k, RPCSubTree(self.rpc_tree, k))
            else:
                setattr(self, k, method)

    def __init__(self, rpc_tree, prefix=''):
        self.prefix = prefix
        self.rpc_tree = rpc_tree
        self.build_attrs()
