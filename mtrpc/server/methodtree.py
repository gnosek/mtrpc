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



import abc
import inspect
import itertools
import re
import string
import textwrap
import traceback
import warnings
import sys

from collections import defaultdict, \
                        Callable, Hashable, Mapping, \
                        MutableSequence, MutableSet, MutableMapping
from repr import Repr

from ..common.errors import *
from ..common.const import *
from ..common import utils



#
# Auxiliary types
#

class BadAccessPatternError(Exception):
    "Internal exception: key or keyhole pattern contains illegal {field}"


class DocDecodeError(UnicodeError):
    "Internal exception: cannot convert RPC-module/method doc to unicode"


class LogWarning(UserWarning):
    "Used to log messages using a proper logger without knowning that logger"



class RPCObjectTags(dict):

    "Tag dict of RPC-module or method"

    def __init__(self, tag_dict=None):
        if tag_dict is None:
            tag_dict = {}
        dict.__init__(self, tag_dict)

    def __getitem__(self, key):
        "A missing item is returned as ''; KeyError is not raised"
        return dict.get(self, key, '')

    def copy(*args, **kwargs): raise NotImplementedError

    @classmethod
    def fromkeys(*args, **kwargs): raise NotImplementedError



class RPCObjectHelp(object):

    "Abstract class: help-text generator for RPC-method/RPC-module instance"

    __metaclass__ = abc.ABCMeta

    def __init__(self, rpc_object):
        self.rpc_object = rpc_object
        self.head = self._format_head()
        self.rest_lines = rpc_object.doc.splitlines(True)  # (True => keep \n)

    @abc.abstractmethod
    def _format_head(self):
        "Format the head line"

    @abc.abstractmethod
    def format(self):
        "Format as a (unicode) string"

    def _prepare_parts(self, name, head_indent, rest_indent):
        if self.rest_lines:
            return [head_indent, self.head.format(name=name), u'\n',
                    rest_indent, u'\n', rest_indent,
                    rest_indent.join(self.rest_lines), u'\n']
        else:
            return [head_indent, self.head.format(name=name), u'\n']

    def __iter__(self):
        "Iterate over all help lines"
        return itertools.chain((self.help_head,), self.rest_lines)



class RPCMethodHelp(RPCObjectHelp):

    "RPCMethod help-text generator"

    def _format_head(self):
        argspec = self.rpc_object.formatted_arg_spec
        argspec = argspec.replace('{', '{{').replace('}', '}}')
        return u'{{name}}{0}'.format(argspec)

    def format(self, name, with_meth='[blah]',
               meth_head_indent=(4 * u' ' + u'* '), mod_head_indent='[blah]',
               meth_rest_indent=(8 * u' '), mod_rest_indent='[blah]'):
        "Format as a (unicode) string"
        parts = self._prepare_parts(name, meth_head_indent, meth_rest_indent)
        return u''.join(parts)



class RPCModuleHelp(RPCObjectHelp):

    "RPCModule help-text generator"

    ROOT_NAME_SUBSTITTUTE_TAG = "root_name_substitute"
    DEFAULT_ROOT_NAME_SUBSTITUTE = "'' [the root]"

    def _format_head(self):
        return u'{name}'  # [sic]

    def format(self, name, with_meth=True,
               meth_head_indent='[blah]', mod_head_indent=(u'Module: '),
               meth_rest_indent='[blah]', mod_rest_indent=(4 * u' ')):
        "Format as a (unicode) string"
        if name == '':
            # the root RPC-module
            name = (self.rpc_object.tags[self.ROOT_NAME_SUBSTITTUTE_TAG]
                    or self.DEFAULT_ROOT_NAME_SUBSTITUTE)
        parts = self._prepare_parts(name, mod_head_indent, mod_rest_indent)
        parts.insert(0, u'\n')
        if with_meth and self.rpc_object.contains_methods():
            parts.extend([u'\n', mod_rest_indent, u'Methods:', u'\n'])
        return u''.join(parts)



#
# Container classes for RPC-methods, RPC-modules and RPC-modules/methods-tree
#

class RPCObject(object):

    "Abstract class: RPC-method or RPC-module"

    @staticmethod
    def _prepare_doc(doc):
        "Prepare RPC-object doc (assert that it's Unicode, trim it etc.)"

        if not doc:
            return u''

        try:
            doc = unicode(doc)
        except UnicodeError:
            raise DocDecodeError('Cannot convert documentation string of '
                                 '{{rpc_kind}} {{full_name}} to unicode. '
                                 'To be on the safe side you should always '
                                 'assure that all your RPC-module/method '
                                 'docs contaning any non-ASCII characters '
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



class RPCMethod(RPCObject, Callable):

    """Callable object wrapper with some additional attributes.

    When an instance is called:
    1) there is a check whether the callable takes `<common.const.ACCESS_...>'
       arguments -- if not, they are not passed to it,
    2) there is a check whether given arguments match the argument
       specification of the callable -- if not, RPCMethodArgError is thrown;
    3) the callable is called, the result is returned.

    Public attributes:
    * doc -- RPC-method's docstring,
    * tags -- a mapping (RPCObjectTags instance) with arbitrary content
      (may be used to store some additional info about the method).

    """

    SUPPRESS_MUTABLE_ARG_WARNING_TAG = 'suppress_mutable_arg_warning'


    def __init__(self, callable_obj, full_name=''):

        """Initialize with a callable object as the argument.

        Use docstring of the callable object as a base for the `doc'
        RPC-method attribute and the callable's `<common.const.RPC_TAGS>'
        attribute as a base for `tags' RPC-method attribute.

        If default values of callable's arguments include a mutable object
        a warning will be logged (probably; that test is not 100% reliable),
        unless `tags' contains a key == self.SUPPRESS_MUTABLE_ARG_WARNING_TAG.
        """

        if not isinstance(callable_obj, Callable):
            raise TypeError('Method object must be callable')
        self.callable_obj = callable_obj
        self.doc = RPCObject._prepare_doc(getattr(callable_obj,
                                                  '__doc__', None))
        self.tags = RPCObjectTags(getattr(callable_obj, RPC_TAGS, None))
        self._examine_and_prepare_arg_spec()
        self.help = RPCMethodHelp(self)
        self.full_name = full_name
        self.__doc__ = self.help.format(full_name)
        try:
            self.module = sys.modules[callable_obj.__module__]
        except (AttributeError, KeyError):
            self.module = None

    def _examine_and_prepare_arg_spec(self):
        spec = inspect.getargspec(self.callable_obj)
        defaults = spec.defaults or ()

        # difference between number of args and number of defined default vals
        args_defs_diff = len(spec.args) - len(defaults)

        # warn about default arg values that are mutable
        # (such situation may be risky because default values are initialized
        # once, therefore they can be shared by different calls)
        # note: this check is not reliable (but still may appear to be useful)
        if not self.tags[self.SUPPRESS_MUTABLE_ARG_WARNING_TAG]:
            for arg_i, default in enumerate(defaults, args_defs_diff):
                if not (self._check_arg_default(default)
                        or spec.args[arg_i] in ACC_KWARGS):
                    warnings.warn("Default value {0!r} of the argument {1!r} "
                                  "of the RPC-method's callable object {2!r} "
                                  "seens to be a mutable container"
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

    def format_args(self, args, kwargs):
        "Format arguments in a way suitable for logging"

        spec = inspect.getargspec(self.callable_obj)
        defaults = list(spec.defaults or ())
        reqd_count = len(spec.args) - len(defaults)
        real_args = [None] * reqd_count + defaults
        real_args[0:len(args)] = args
        for i, arg in enumerate(spec.args):
            if 'passw' in arg:
                real_args[i] = '***'
            elif arg in kwargs:
                real_args[i] = kwargs[arg]

        spec_args = [a for a in spec.args if a not in ACC_KWARGS]
        real_args[len(spec_args):] = []
        r = Repr()
        r.maxstring = 60
        r.maxother = 60
        real_args = [r.repr(a) for a in real_args]

        return inspect.formatargspec(spec_args, spec.varargs,
            spec.keywords, real_args, formatvalue=lambda v: '='+v)

    def format_result(self, result):
        r = Repr()
        r.maxstring = 60
        r.maxother = 60
        return r.repr(result)

    def __call__(self, *args, **kwargs):
        "Call the method"

        kwargs = utils.kwargs_to_str(kwargs)

        if not self._gets_access_dict:
            kwargs.pop(ACCESS_DICT_KWARG, None)

        if not self._gets_access_key:
            kwargs.pop(ACCESS_KEY_KWARG, None)

        if not self._gets_access_keyhole:
            kwargs.pop(ACCESS_KEYHOLE_KWARG, None)

        try:
            # test given arguments (params)
            self._arg_test_callable(*args, **kwargs)
        except TypeError:
            kwargs.pop(ACCESS_DICT_KWARG, None)
            kwargs.pop(ACCESS_KEY_KWARG, None)
            kwargs.pop(ACCESS_KEYHOLE_KWARG, None)
            self._raise_arg_error(args, kwargs)
        else:
            return self.callable_obj(*args, **kwargs)


    def _raise_arg_error(self, args, kwargs):
        a = itertools.imap(repr, args)
        kw = ('{0}={1!r}'.format(name, val)
              for name, val in sorted(kwargs.iteritems()))
        raise RPCMethodArgError("Cannot call method {{name}} -- "
                                "given arguments: ({0}) don't match "
                                "method argument specification: {1}"
                                .format(', '.join(itertools.chain(a, kw)),
                                        self.formatted_arg_spec))



class RPCModule(RPCObject, Mapping):

    """RPC-module maps local names to RPC-methods and other RPC-modules

    Public attributes:
    * doc -- RPC-module's docstring,
    * tags -- a mapping (RPCObjectTags instance) with arbitrary content
      (may be used to store some additional info about module).
    """

    def __init__(self, doc=u'', tags=None):
        "Initialize; optionally with doc and/or tags"
        self._method_dict = {}  # maps RPC-method local names to RPC-methods
        self._submod_dict = {}  # maps RPC-module local names to RPC-methods
        # caches:
        self._sorted_method_items = None  # sorted (locname, RPC-method) pairs
        self._sorted_submod_items = None  # sorted (locname, RPC-module) pairs
        # public attributes:
        self.doc = doc
        self.tags = RPCObjectTags(tags)
        self.help = RPCModuleHelp(self)


    def declare_attrs(self, doc, tag_dict):
        "Add doc and tags if needed, re-generate help text if needed"
        if doc != u'':
            if self.doc:
                assert self.doc == doc, (self.doc, doc)
            else:
                self.doc = doc
                self.help = RPCModuleHelp(self)
        if tag_dict is not None:
            if self.tags:
                assert self.tags == tag_dict
            else:
                self.tags = RPCObjectTags(tag_dict)


    def add_method(self, local_name, rpc_method):
        "Add a new method"
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
        "Add a new submodule"
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
        """Iterate over sorted (local name, method) pairs and
        (local name, submodule) pairs"""
        return itertools.chain(self.loc_names2methods(),
                               self.loc_names2submods())


    def loc_names2methods(self):
        "Iterator over sorted (local name, method) pairs"
        if self._sorted_method_items is None:
            # rebuild the cache
            self._sorted_method_items = sorted(self._method_dict.iteritems())
        return iter(self._sorted_method_items)


    def loc_names2submods(self):
        "Iterator over sorted (local name, submodule) pairs"
        if self._sorted_submod_items is None:
            # rebuild the cache
            self._sorted_submod_items = sorted(self._submod_dict.iteritems())
        return iter(self._sorted_submod_items)


    def contains_methods(self):
        "Does it contain any methods?"
        return bool(self._method_dict)

    def contains_submods(self):
        "Does it contain any submodules?"
        return bool(self._submod_dict)


    # implementation of Mapping's abstract methods:

    def __iter__(self):
        "Iterator over sorted method local names + sorted module local names"
        return (name for name, _ in self.loc_names2all())

    def __getitem__(self, local_name):
        "Get RPC-submodule or method (by local name)"
        try:
            return self._method_dict[local_name]
        except KeyError:
            return self._submod_dict[local_name]


    # ...and other Mapping's methods:

    def __contains__(self, local_name):
        "Check existence of (local) name"
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

    "Maps full names (hierarchical keys) to RPC-modules and methods (values)"

    NAME_CHARS = frozenset(string.ascii_letters + string.digits + '_.')

    RPCObject = RPCObject
    RPCMethod = RPCMethod
    RPCModule = RPCModule


    def __init__(self):

        "Basic initialization"

        self._item_dict = {}  # maps full names to RPC-objects
        self.is_built = False
        self.method_names2pymods = {}  # maps method full names to the
                                       # Python modules that the method
                                       # callables were taken from


    def build(self,
              root_pymod=None,
              default_postinit_callable=(lambda: None),
              postinit_kwargs=None):

        "Build the tree (populate it with RPC-modules/methods)"

        if self.is_built:
            raise RuntimeError("Cannot run build() method of RPCTree instance"
                               " more than once -- it has been already done")
        if postinit_kwargs is None:
            postinit_kwargs = {}
        self._build_subtree(root_pymod, '',
                            default_postinit_callable, postinit_kwargs,
                            ancestor_pymods=set(), initialized_pymods={},
                            pymods2anticipated_names=defaultdict(set))
        self.is_built = True


    def _build_subtree(self, cur_pymod, cur_full_name,
                       default_postinit_callable, postinit_kwargs,
                       ancestor_pymods, initialized_pymods,
                       pymods2anticipated_names):

        "Walk through py-modules populating the tree with RPC-modules/methods"

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

        (pymod_names2objs
        ) = dict(inspect.getmembers(cur_pymod, inspect.ismodule))
        pymod_objs = set(pymod_names2objs.itervalues())

        # (the module local name might be mentioned in __rpc_methods__
        # of some higher module, in "mod1.mod2.mod3.method"-way)
        (ant_names
        ) = pymods2anticipated_names[(cur_pymod, cur_full_name)].union(names)
        scoped_ant_names = set(name.split('.')[0] for name in ant_names)

        # '*' symbol means: all *public functions* (not all callable objects)
        if '*' in ant_names:
            (func_names
            ) = set(dict(inspect.getmembers(cur_pymod, inspect.isfunction)))
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
            doc = RPCObject._prepare_doc(getattr(cur_pymod,
                                                 RPC_MODULE_DOC, None))
        except DocDecodeError as exc:
            raise UnicodeError(exc.args[0].format(rpc_kind='RPC-module',
                                                  full_name=cur_full_name))

        tag_dict = getattr(cur_pymod, RPC_TAGS, None)
        postinit_callable = getattr(cur_pymod, RPC_POSTINIT, None)

        if ant_names or doc or tag_dict or postinit_callable:
            (_full_name
            ) = initialized_pymods.setdefault(cur_pymod, cur_full_name)
            if _full_name != cur_full_name:
                # !TODO! -- sprawdzic czy return tutaj jest ok...
                #raise RuntimeError('Cannot create RPC-module {0} based on '
                #                   'Python-module {1!r} -- that Python-'
                #                   'module has been already used as a base '
                #                   'for {2} RPC-module.'.format(cur_full_name,
                #                                                cur_pymod,
                #                                                _full_name))
                warnings.warn('Cannot create RPC-module {0} based on '
                              'Python-module {1!r} -- that Python-'
                              'module has been already used as a base '
                              'for {2} RPC-module.'.format(cur_full_name,
                                                           cur_pymod,
                                                           _full_name))
                return
            # declare (create if needed) RPC-module
            self.get_rpc_module(cur_full_name, doc, tag_dict)
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
                        (mod_full_name
                        ) = '{0}.{1}'.format(cur_full_name, mod_name)
                    else:
                        mod_full_name = mod_name
                    (pymods2anticipated_names[(pymod, mod_full_name)]
                    ).add(rest_of_name)
                else:
                    raise TypeError('{0}.{1} is not a module'
                                    .format(cur_full_name, name))
            else:
                meth_full_name = '{0}.{1}'.format(cur_full_name, name)
                try:
                    callable_obj = getattr(cur_pymod, name)
                except AttributeError:
                    warnings.warn('No such method: {0} (so we skip it)'
                                  .format(meth_full_name), LogWarning)
                else:
                    # create and put into tree an RPC-method
                    try:
                        self.add_rpc_method(cur_full_name, name,
                                            callable_obj, cur_pymod)
                    except DocDecodeError as exc:
                        raise UnicodeError(exc.args[0]
                                           .format(rpc_kind='RPC-method',
                                                   full_name=meth_full_name))

        ancestor_pymods.add(cur_pymod)

        for mod_name, pymod in pymod_names2objs.iteritems():
            if mod_name not in scoped_ant_names:
                continue

            if pymod in ancestor_pymods:
                warnings.warn('Module {0} contains cyclic module reference: '
                              '{1} (we must break that cycle)'
                              .format(cur_full_name, mod_name), LogWarning)
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

        "Run module post-init callable -- default or module's custom one"

        # prepare kwargs for the particular post-init callable
        _kwargs = postinit_kwargs.copy()
        _kwargs.update(
                mod=pymod,
                full_name=full_name,
                rpc_tree=self,
        )
        arg_names = inspect.getargspec(postinit_callable).args
        try:
            (this_postinit_kwargs
            ) = dict((name, _kwargs[name]) for name in arg_names)
        except KeyError as exc:
            raise KeyError("Post-init callable used for module {0} "
                           "(based on Python module {1!r}) takes "
                           "keyword argument '{2}' but given keyword "
                           "arg. dict {3!r} doesn't contain it"
                           .format(full_name, pymod, exc.args[0], _kwargs))

        # run the post-init callable
        postinit_callable(**this_postinit_kwargs)


    def add_rpc_method(self, module_full_name, method_local_name,
                       callable_obj, python_module):

        "Add RPC-method"

        if not callable(callable_obj):
            return

        if not module_full_name:
            raise TypeError("Cannot add methods to root module")

        method_full_name = '{0}.{1}'.format(module_full_name,
                                            method_local_name)
        assert method_full_name not in self._item_dict

        rpc_module = self._get_rpc_mod(module_full_name,
                                       arg_name='module_full_name')
        rpc_method = RPCMethod(callable_obj, method_full_name)
        self.method_names2pymods[method_full_name] = python_module
        rpc_module.add_method(method_local_name, rpc_method)
        self._item_dict[method_full_name] = rpc_method


    def _get_rpc_mod(self, full_name, arg_name='full_name'):

        rpc_module = self._item_dict[full_name]
        if not isinstance(rpc_module, RPCModule):
            raise TypeError("`{0}' argument must not point to anything else "
                            "than RPCModule instance".format(arg_name))
        return rpc_module


    def get_rpc_module(self, full_name, doc=u'', tag_dict=None):

        "Get RPC-module; if needed, create it and any missing ancestors of it"

        rpc_module = self._item_dict.get(full_name)
        if rpc_module is None:
            if full_name.startswith('.'):
                raise ValueError("RPC-name must not start with '.'")

            elif full_name == '':
                # create the root module
                rpc_module = RPCModule(doc, tag_dict)

            else:
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
            rpc_module.declare_attrs(doc, tag_dict)

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

        "Check: * rpc_item type (if specified); * whether key matches keyhole"

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
        "Get RPC-module or method (by full name)"
        return self._item_dict[full_name]

    def __contains__(self, full_name):
        "Check existence of (full) name"
        return full_name in self._item_dict

    def __len__(self):
        return len(self._item_dict)

    # instances are not hashable:

    __hash__ = None


class RPCSubTree(object):
    def build_attrs(self):
        keys = set()
        if self.prefix:
            prefix = self.prefix + '.'
        else:
            prefix = ''
        for key, method in self.rpc_tree._item_dict.iteritems():
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
