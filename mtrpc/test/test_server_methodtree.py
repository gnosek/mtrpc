#!/usr/bin/env python
#
# Author: Jan Kaliszewski (zuo)
# Copyright (c) 2010, MegiTeam

"""Unit tests for mtrpc.server.methodtree module"""



from future_builtins import filter, map, zip

import itertools
import os.path
import random
import sys
import types
import unittest
import warnings

from inspect import getmembers, getargspec, isfunction, isclass, ismodule
from collections import namedtuple

if __name__ == '__main__':
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    (mtrpc_package_path
    ) = os.path.join(script_dir, os.path.pardir, os.path.pardir)
    if mtrpc_package_path not in sys.path:
        sys.path.insert(0, mtrpc_package_path)

from mtrpc.server import methodtree
from mtrpc.common import errors



class Test_RPCObjectTags_class(unittest.TestCase):

    def setUp(self):
        self.tag_dict = {
                'foo': 'bar',
                'bar': (1,2,3),
        }
        self.no_tags = methodtree.RPCObjectTags(None)
        self.some_tags = methodtree.RPCObjectTags(self.tag_dict)


    def test_init_eq(self):
        self.assertEqual(self.no_tags, {})
        self.assertEqual(self.some_tags, self.tag_dict)
        self.assertEqual(methodtree.RPCObjectTags(self.some_tags), self.tag_dict)


    def test_getitem(self):
        self.assertEqual(self.no_tags['boo'], '')
        self.assertEqual(self.some_tags['boo'], '')
        self.assertEqual(self.some_tags['foo'], 'bar')
        self.assertEqual(self.some_tags['bar'], (1, 2, 3))


    def test_not_implemented(self):
        self.assertRaises(NotImplementedError,
                          methodtree.RPCObjectTags.fromkeys, self.tag_dict)
        self.assertRaises(NotImplementedError,
                          self.no_tags.fromkeys, self.tag_dict)
        self.assertRaises(NotImplementedError, self.some_tags.copy)


class Test_RPCMethod_class(unittest.TestCase):

    def setUp(self):

        def callable1(a, b, c=None):
            "A docstring1"
            return a, b, c

        def callable2(a, b, _access_dict,
                      _access_key_patt, _access_keyhole_patt):
            "A docstring2"
            return a, b, _access_dict, _access_key_patt, _access_keyhole_patt

        def callable3(a, b={}, c=[], _access_dict={}, _access_keyhole_patt=[]):
            "A docstring3"
            return a, b, c

        def callable4(a, b, c=[], _access_key_patt='aaa'):
            "A docstring4"
            return a, b, c

        setattr(callable1, methodtree.RPC_TAGS, dict(x=1, y='Y', z=[1,2,3]))
        setattr(callable4, methodtree.RPC_TAGS, {
                'suppress_mutable_arg_warning': True,
                33: [],
        })

        self.callable1 = callable1
        self.callable2 = callable2
        self.callable3 = callable3
        self.callable4 = callable4

        self.not_callable = (1, 2, 3)

        with warnings.catch_warnings(True) as self.warnings1:
            self.rpc_method1 = methodtree.RPCMethod(callable1)
        with warnings.catch_warnings(True) as self.warnings2:
            self.rpc_method2 = methodtree.RPCMethod(callable2)
        with warnings.catch_warnings(True) as self.warnings3:
            self.rpc_method3 = methodtree.RPCMethod(callable3)
        with warnings.catch_warnings(True) as self.warnings4:
            self.rpc_method4 = methodtree.RPCMethod(callable4)


    def test_attrs_etc(self):
        self.assertTrue(self.rpc_method1.callable_obj is self.callable1)
        self.assertTrue(self.rpc_method2.callable_obj is self.callable2)
        self.assertTrue(self.rpc_method3.callable_obj is self.callable3)
        self.assertTrue(self.rpc_method4.callable_obj is self.callable4)

        self.assertEqual(self.rpc_method1.doc, "A docstring1")
        self.assertEqual(self.rpc_method2.doc, "A docstring2")
        self.assertEqual(self.rpc_method3.doc, "A docstring3")
        self.assertEqual(self.rpc_method4.doc, "A docstring4")

        self.assertEqual(self.rpc_method1.tags, dict(x=1, y='Y', z=[1,2,3]))
        self.assertEqual(self.rpc_method2.tags, {})
        self.assertEqual(self.rpc_method3.tags, {})
        self.assertEqual(self.rpc_method4.tags,
                         {'suppress_mutable_arg_warning': True,
                          33: []})

        help1 = "Method: {name}(a, b, c=None)\n    A docstring1"
        help2 = "Method: {name}(a, b)\n    A docstring2"
        help3 = "Method: {name}(a, b={}, c=[])\n    A docstring3"
        help4 = "Method: {name}(a, b, c=[])\n    A docstring4"

        self.assertEqual(self.rpc_method1.help, help1)
        self.assertEqual(self.rpc_method2.help, help2)
        self.assertEqual(self.rpc_method3.help, help3)
        self.assertEqual(self.rpc_method4.help, help4)

        self.assertFalse(self.rpc_method1._gets_access_dict)
        self.assertTrue(self.rpc_method2._gets_access_dict)
        self.assertTrue(self.rpc_method3._gets_access_dict)
        self.assertFalse(self.rpc_method4._gets_access_dict)

        self.assertFalse(self.rpc_method1._gets_access_key)
        self.assertTrue(self.rpc_method2._gets_access_key)
        self.assertFalse(self.rpc_method3._gets_access_key)
        self.assertTrue(self.rpc_method4._gets_access_key)

        self.assertFalse(self.rpc_method1._gets_access_keyhole)
        self.assertTrue(self.rpc_method2._gets_access_keyhole)
        self.assertTrue(self.rpc_method3._gets_access_keyhole)
        self.assertFalse(self.rpc_method4._gets_access_keyhole)

        # hashability and equality
        hash_test = {
                self.rpc_method1: 1,
                self.rpc_method2: 2,
                self.rpc_method3: 3,
                self.rpc_method4: 4,
        }
        self.assertEqual(hash_test[self.rpc_method1], 1)
        self.assertEqual(hash_test[self.rpc_method2], 2)
        self.assertEqual(hash_test[self.rpc_method3], 3)
        self.assertEqual(hash_test[self.rpc_method4], 4)
        self.assertEqual(self.rpc_method1, self.rpc_method1)
        self.assertNotEqual(self.rpc_method1, self.rpc_method2)
        self.assertNotEqual(self.rpc_method1, self.rpc_method3)
        self.assertNotEqual(self.rpc_method1, self.rpc_method4)


    def test_err_warn(self):
        self.assertRaises(TypeError, methodtree.RPCMethod, self.not_callable)

        self.assertEqual(len(self.warnings1), 0)
        self.assertEqual(len(self.warnings2), 0)
        self.assertEqual(len(self.warnings4), 0)

        self.assertEqual(len(self.warnings3), 2)
        self.assertTrue(type(self.warnings3[0].category
                        is methodtree.LogWarning))
        self.assertTrue(type(self.warnings3[1].category
                        is methodtree.LogWarning))


    def test_call(self):
        self.assertEqual(self.rpc_method1(6, 7, 8,
                                          _access_dict={'x': 33},
                                          _access_key_patt='{x}',
                                          _access_keyhole_patt='[0-9]{{2}}'),
                         (6, 7, 8))
        self.assertEqual(self.rpc_method2(6, b=7,
                                          _access_dict={'x': 33},
                                          _access_key_patt='{x}',
                                          _access_keyhole_patt='[0-9]{{2}}'),
                         (6, 7, {'x': 33}, '{x}', '[0-9]{{2}}'))
        self.assertEqual(self.rpc_method3(6, 7, c=8,
                                          _access_dict={'x': 33},
                                          _access_key_patt='',
                                          _access_keyhole_patt=''),
                         (6, 7, 8))
        self.assertEqual(self.rpc_method4(6, 7, c=8,
                                          _access_dict={'x': 33},
                                          _access_key_patt='aaa',
                                          _access_keyhole_patt='aaa'),
                         (6, 7, 8))

        # all three _rpc_access* arguments must be given, as keyword args
        self.assertRaises(AssertionError, self.rpc_method1, 6, 7, 8)
        self.assertRaises(AssertionError, self.rpc_method2, 6, 7,
                          {'x': 33}, '{x}', '[0-9]{{2}}')
        self.assertRaises(AssertionError, self.rpc_method3, 6, 7, 8,
                          _access_dict={})
        self.assertRaises(AssertionError, self.rpc_method4, 6, 7, 8,
                          _access_key_patt='aa', _access_keyhole_patt='aa')

        # arg. spec. check
        try:
            self.rpc_method1(1, _access_dict={}, _access_key_patt='b',
                             _access_keyhole_patt='b')
        except errors.RPCMethodArgError as exc:
            self.assertEqual(exc.args[0],
                    "Given arguments: (1) don't match "
                    "method's argument specification: (a, b, c=None)"
            )
        try:
            self.rpc_method2(1, 2, 3, 4, _access_dict={'b': 'b'},
                             _access_key_patt='b', _access_keyhole_patt='b')
        except errors.RPCMethodArgError as exc:
            self.assertEqual(exc.args[0],
                    "Given arguments: (1, 2, 3, 4) don't match "
                    "method's argument specification: (a, b)"
            )
        try:
            self.rpc_method3(x=1, _access_dict={'b': 'b'}, _access_key_patt='b',
                             _access_keyhole_patt='b')
        except errors.RPCMethodArgError as exc:
            self.assertEqual(exc.args[0],
                    "Given arguments: (x=1) don't match "
                    "method's argument specification: (a, b={}, c=[])"
            )
        try:
            self.rpc_method4(1, 2, w=3, z=4, _access_dict={},
                             _access_key_patt='b', _access_keyhole_patt='b')
        except errors.RPCMethodArgError as exc:
            self.assertEqual(exc.args[0],
                    "Given arguments: (1, 2, w=3, z=4) don't match "
                    "method's argument specification: (a, b, c=[])"
            )


class Test_RPCModule_class(unittest.TestCase):

    def setUp(self):
        self.rpc_module1 = methodtree.RPCModule()
        self.rpc_module2 = methodtree.RPCModule('A docstring2',
                                                {'a': 'b', 3: 'x'})


    def test_attrs_and_declaring(self):
        self.assertTrue(type(self.rpc_module1.tags)
                        is methodtree.RPCObjectTags)
        self.assertTrue(type(self.rpc_module2.tags)
                        is methodtree.RPCObjectTags)

        self.assertEqual(self.rpc_module1.doc, '')
        self.assertEqual(self.rpc_module1.tags, {})
        self.assertEqual(self.rpc_module1.help, 'Module: {name}')

        self.rpc_module1.declare_doc_and_tags('', None)
        self.assertEqual(self.rpc_module1.doc, '')
        self.assertEqual(self.rpc_module1.help, 'Module: {name}')
        self.assertEqual(self.rpc_module1.tags, {})

        self.rpc_module1.declare_doc_and_tags('A docstring1', None)
        self.assertEqual(self.rpc_module1.doc, 'A docstring1')
        self.assertEqual(self.rpc_module1.help, 'Module: {name}'
                                                '\n    A docstring1')
        self.assertEqual(self.rpc_module1.tags, {})

        self.rpc_module1.declare_doc_and_tags('A docstring1', {'x': 'y'})
        self.assertEqual(self.rpc_module1.doc, 'A docstring1')
        self.assertEqual(self.rpc_module1.help, 'Module: {name}'
                                                '\n    A docstring1')
        self.assertEqual(self.rpc_module1.tags, {'x': 'y'})

        self.rpc_module1.declare_doc_and_tags('A docstring1', {'x': 'y'})
        self.assertEqual(self.rpc_module1.doc, 'A docstring1')
        self.assertEqual(self.rpc_module1.help, 'Module: {name}'
                                                '\n    A docstring1')
        self.assertEqual(self.rpc_module1.tags, {'x': 'y'})

        self.rpc_module1.declare_doc_and_tags('A docstring1', None)
        self.assertEqual(self.rpc_module1.doc, 'A docstring1')
        self.assertEqual(self.rpc_module1.help, 'Module: {name}'
                                                '\n    A docstring1')
        self.assertEqual(self.rpc_module1.tags, {'x': 'y'})

        self.rpc_module1.declare_doc_and_tags('', {'x': 'y'})
        self.assertEqual(self.rpc_module1.doc, 'A docstring1')
        self.assertEqual(self.rpc_module1.help, 'Module: {name}'
                                                '\n    A docstring1')
        self.assertEqual(self.rpc_module1.tags, {'x': 'y'})

        self.rpc_module1.declare_doc_and_tags('', None)
        self.assertEqual(self.rpc_module1.doc, 'A docstring1')
        self.assertEqual(self.rpc_module1.help, 'Module: {name}'
                                                '\n    A docstring1')
        self.assertEqual(self.rpc_module1.tags, {'x': 'y'})

        self.assertRaises(AssertionError,
                          self.rpc_module1.declare_doc_and_tags, 'zzz', None)
        self.assertRaises(AssertionError,
                          self.rpc_module1.declare_doc_and_tags, '', {})

        self.assertEqual(self.rpc_module2.doc, 'A docstring2')
        self.assertEqual(self.rpc_module2.tags, {'a': 'b', 3: 'x'})
        self.assertEqual(self.rpc_module2.help, 'Module: {name}'
                                                '\n    A docstring2')

        self.rpc_module2.declare_doc_and_tags('', None)
        self.assertEqual(self.rpc_module2.doc, 'A docstring2')
        self.assertEqual(self.rpc_module2.tags, {'a': 'b', 3: 'x'})
        self.assertEqual(self.rpc_module2.help, 'Module: {name}'
                                                '\n    A docstring2')

        self.rpc_module2.declare_doc_and_tags('A docstring2',
                                              {'a': 'b', 3: 'x'})
        self.assertEqual(self.rpc_module2.doc, 'A docstring2')
        self.assertEqual(self.rpc_module2.tags, {'a': 'b', 3: 'x'})
        self.assertEqual(self.rpc_module2.help, 'Module: {name}'
                                                '\n    A docstring2')

        self.assertRaises(AssertionError,
                          self.rpc_module2.declare_doc_and_tags, 'zzz', None)
        self.assertRaises(AssertionError,
                          self.rpc_module2.declare_doc_and_tags, '', {2: 3})

        self.assertTrue(type(self.rpc_module1.tags)
                        is methodtree.RPCObjectTags)
        self.assertTrue(type(self.rpc_module2.tags)
                        is methodtree.RPCObjectTags)


    def test_methods_submods_etc(self):
        method_dict = dict((name, methodtree.RPCMethod(callable_obj=func))
                           for name, func in getmembers(os.path, isfunction)
                           if not name.startswith('_'))
        sorted_method_names = sorted(method_dict)
        sorted_method_items = sorted(method_dict.iteritems())
        a_method_name = sorted_method_names[0]

        submod_dict = dict((name, methodtree.RPCModule(doc=name))
                           for name in dir(sys))
        sorted_submod_names = sorted(submod_dict)
        sorted_submod_items = sorted(submod_dict.iteritems())
        a_submod_name = sorted_submod_names[0]

        all_sorted_names = sorted_method_names + sorted_submod_names
        all_sorted_items = sorted_method_items + sorted_submod_items

        # (euqality-is-identity test 1)
        self.assertNotEqual(self.rpc_module1, self.rpc_module2)

        # (hashability test 1)
        hash_test = {self.rpc_module1: 1, self.rpc_module2: 2}
        self.assertEqual(hash_test[self.rpc_module1], 1)
        self.assertEqual(hash_test[self.rpc_module2], 2)

        # add_method(), loc_names2methods(), loc_names2all(), __iter__()
        self.assertTrue(self.rpc_module1._sorted_method_items is None)
        for name, method in method_dict.iteritems():
            self.rpc_module1.add_method(name, method)
        self.assertTrue(self.rpc_module1._sorted_method_items is None)
        self.assertRaises(ValueError, self.rpc_module1.add_method,  # repeated name
                          a_method_name, method_dict[a_method_name])
        self.assertRaises(ValueError, self.rpc_module1.add_method,
                          '', method_dict[a_method_name])            # empty name
        self.assertRaises(TypeError, self.rpc_module1.add_method,
                          'xxxxx', 'zzzzz')           # not an RPCMethod instance
        self.assertTrue(self.rpc_module1._sorted_submod_items is None)
        self.assertEqual(list(self.rpc_module1.loc_names2all()),
                         sorted_method_items)
        self.assertEqual(list(self.rpc_module1),  # __iter__()
                         sorted_method_names)
        self.assertEqual(self.rpc_module1._sorted_submod_items, [])
        self.assertEqual(self.rpc_module1._sorted_method_items,
                         sorted_method_items)
        self.assertEqual(list(self.rpc_module1.loc_names2methods()),
                         sorted_method_items)

        # add_submod(), loc_names2submods()
        for name, submod in submod_dict.iteritems():
            self.rpc_module1.add_submod(name, submod)
        self.assertTrue(self.rpc_module1._sorted_submod_items is None)
        self.assertRaises(ValueError, self.rpc_module1.add_submod,  # repeated name
                          a_submod_name, submod_dict[a_submod_name])
        self.assertRaises(ValueError, self.rpc_module1.add_submod,
                          '', submod_dict[a_submod_name])            # empty name
        self.assertRaises(TypeError, self.rpc_module1.add_submod,
                          'xxxxx', 'zzzzz')           # not an RPCModule instance
        self.assertEqual(list(self.rpc_module1.loc_names2submods()),
                         sorted_submod_items)
        self.assertEqual(self.rpc_module1._sorted_submod_items,
                         sorted_submod_items)

        # loc_names2all(), __iter__()
        self.assertEqual(list(self.rpc_module1.loc_names2all()),
                         all_sorted_items)
        self.assertEqual(list(self.rpc_module1),  # __iter__()
                         all_sorted_names)

        # add_submod(), loc_names2submods(), loc_names2all(), __iter__()
        self.assertTrue(self.rpc_module2._sorted_submod_items is None)
        for name, submod in submod_dict.iteritems():
            self.rpc_module2.add_submod(name, submod)
        self.assertTrue(self.rpc_module2._sorted_submod_items is None)
        self.assertRaises(ValueError, self.rpc_module2.add_submod,  # repeated name
                          a_submod_name, submod_dict[a_submod_name])
        self.assertRaises(ValueError, self.rpc_module2.add_submod,
                          '', submod_dict[a_submod_name])            # empty name
        self.assertRaises(TypeError, self.rpc_module2.add_submod,
                          'xxxxx', 'zzzzz')           # not an RPCModule instance
        self.assertTrue(self.rpc_module2._sorted_method_items is None)
        self.assertEqual(list(self.rpc_module2.loc_names2all()),
                         sorted_submod_items)
        self.assertEqual(list(self.rpc_module2),  # __iter__()
                         sorted_submod_names)
        self.assertEqual(self.rpc_module2._sorted_method_items, [])
        self.assertEqual(self.rpc_module2._sorted_submod_items,
                         sorted_submod_items)
        self.assertEqual(list(self.rpc_module2.loc_names2submods()),
                         sorted_submod_items)

        # add_method(), loc_names2methods()
        for name, method in method_dict.iteritems():
            self.rpc_module2.add_method(name, method)
        self.assertTrue(self.rpc_module2._sorted_method_items is None)
        self.assertRaises(ValueError, self.rpc_module2.add_method,  # repeated name
                          a_method_name, method_dict[a_method_name])
        self.assertRaises(ValueError, self.rpc_module2.add_method,
                          '', method_dict[a_method_name])            # empty name
        self.assertRaises(TypeError, self.rpc_module2.add_method,
                          'xxxxx', 'zzzzz')           # not an RPCMethod instance
        self.assertEqual(list(self.rpc_module2.loc_names2methods()),
                         sorted_method_items)
        self.assertEqual(self.rpc_module2._sorted_method_items,
                         sorted_method_items)

        # loc_names2all(), __iter__()
        self.assertEqual(list(self.rpc_module2.loc_names2all()),
                         all_sorted_items)
        self.assertEqual(list(self.rpc_module2),  # __iter__()
                         all_sorted_names)

        # __getitem__(), __contains__(), __len__()
        self.assertTrue(self.rpc_module1[a_method_name]
                        is method_dict[a_method_name])
        self.assertTrue(self.rpc_module1[a_submod_name]
                        is submod_dict[a_submod_name])
        self.assertTrue(self.rpc_module2[a_method_name]
                        is method_dict[a_method_name])
        self.assertTrue(self.rpc_module2[a_submod_name]
                        is submod_dict[a_submod_name])
        self.assertRaises(KeyError, self.rpc_module1.__getitem__, 'aaaaa')
        self.assertRaises(KeyError, self.rpc_module2.__getitem__, 'aaaaa')
        self.assertTrue(a_method_name in self.rpc_module1)
        self.assertTrue(a_submod_name in self.rpc_module1)
        self.assertTrue(a_method_name in self.rpc_module2)
        self.assertTrue(a_submod_name in self.rpc_module2)
        self.assertEqual(len(self.rpc_module1), len(sorted_submod_names)
                                                + len(sorted_method_names))
        self.assertEqual(len(self.rpc_module2), len(sorted_submod_names)
                                                + len(sorted_method_names))
        self.assertEqual(len(sorted_method_names), len(sorted_method_items))
        self.assertEqual(len(sorted_submod_names), len(sorted_submod_items))

        # (euqality-is-identity test 2)
        self.assertNotEqual(self.rpc_module1, self.rpc_module2)

        # (hashability test 2)
        self.assertEqual(hash_test[self.rpc_module1], 1)
        self.assertEqual(hash_test[self.rpc_module2], 2)


class Test_RPCTree_class(unittest.TestCase):

    def setUp(self):

        self.rpc_tree = methodtree.RPCTree()

        def cal(counter=[0]):
            "Get new callable"
            counter[0] += 1
            nr = str(counter[0])
            def method(x):
                return x
            method.__name__ += nr
            method.__doc__ = 'Docstring ' + nr
            setattr(method, methodtree.RPC_TAGS, {'nr': nr})
            return method

        self.tree_fixture = (
                ('a.meth2', cal()),
                ('a.meth1', cal()),
                ('a.a.meth5', cal()),
                ('b.a.a.a.meth4', cal()),
                ('a.a.meth2', cal()),
                ('a.a.meth3', cal()),
                ('b.b.meth6', cal()),
                ('b.a.a.meth7', cal()),
                ('b.a.a.a.meth8', cal()),
                ('b.meth9', cal()),
                ('b.a.a.meth11', cal()),
                ('a.a.meth11', cal()),
                ('a.meth12', cal()),
                ('b.a.a.meth15', cal()),
                ('c.a.meth16', cal()),
                ('c.a.a.meth16', cal()),
                ('c.a.a.meth13', cal()),
                ('c.meth1', cal()),
                ('b.b.a.meth5', cal()),
                ('b.a.meth19', cal()),
        )
        self.tree_dict = dict(self.tree_fixture)

        self.exceed_additions = (
                ('a.meth1', cal()),
                ('b.a.a', cal()),
                ('a.a', cal()),
                ('b.b', cal()),
        )


    def test_getting_mods(self):
        rpc_submods = {}
        for name, _ in self.tree_fixture + self.exceed_additions + (('z', 'x'),):
            rpc_mod_name = '.'.join(name.split('.')[:-1])
            if rpc_mod_name and random.randint(0, 1):
                rpc_mod = self.rpc_tree.get_rpc_module(rpc_mod_name,
                                                       doc='AAA',
                                                       tag_dict={1: 2})
            else:
                rpc_mod = self.rpc_tree.get_rpc_module(rpc_mod_name)
            self.assertEqual(rpc_mod,
                             rpc_submods.setdefault(rpc_mod_name, rpc_mod))
            self.assertEqual(rpc_mod, self.rpc_tree[rpc_mod_name])

        self.assertEqual(self.rpc_tree.get_rpc_module(''),
                         self.rpc_tree._root_module)


    def test_add_get_iter(self):

        # adding and getting...

        rpc_submods = {}
        rpc_methods = {}
        for name, callable_obj in self.tree_fixture:
            split_name = name.split('.')
            rpc_mod_name = '.'.join(split_name[:-1])
            rpc_mod = self.rpc_tree.get_rpc_module(rpc_mod_name, doc='AAA',
                                                   tag_dict={1: 2})
            local_meth_name = split_name[-1]
            self.rpc_tree.add_rpc_method(rpc_mod_name, local_meth_name,
                                         callable_obj)
            rpc_meth = self.rpc_tree[name]
            self.assertEqual(rpc_mod[local_meth_name], rpc_meth)
            self.assertEqual(rpc_mod, self.rpc_tree[rpc_mod_name])
            self.assertTrue(rpc_mod[local_meth_name].callable_obj
                            is self.tree_dict[name])
            self.assertEqual(rpc_mod,
                             rpc_submods.setdefault(rpc_mod_name, rpc_mod))
            rpc_methods[name] = rpc_meth

        (_all_names_tree
        ) = (itertools.chain((rpc_mod_name,),
                             ('{0}.{1}'.format(rpc_mod_name, rpc_meth_name)
                              for rpc_meth_name, _
                              in rpc_mod.loc_names2methods()))
             for rpc_mod_name, rpc_mod
             in sorted(rpc_submods.iteritems()))

        all_names = list(itertools.chain.from_iterable(_all_names_tree))
        method_names = list(filter(lambda s: ('meth' in s), all_names))
        submod_names = list(sorted(rpc_submods))

        self.assertEqual(sorted(method_names + submod_names),
                         sorted(all_names))

        rpc_all = rpc_submods.copy()
        rpc_all.update(rpc_methods)
        self.assertEqual(len(rpc_all), len(rpc_submods) + len(rpc_methods))
        self.assertEqual(sorted(rpc_all), sorted(all_names))
        self.assertEqual(sorted(rpc_methods), sorted(method_names))
        self.assertEqual(sorted(rpc_submods), submod_names)

        all_items = [(name, rpc_all[name]) for name in all_names]
        method_items = [(name, rpc_methods[name]) for name in method_names]
        submod_items = [(name, rpc_submods[name]) for name in submod_names]

        # get with access check
        self.assertRaises(
                errors.RPCNotFoundError,
                self.rpc_tree.try_to_obtain, 'sorry.no.bonus', {}, '', '',
        )
        for name, rpc_obj in all_items:
            if random.randint(0, 1):
                self.assertEqual(
                        rpc_obj,
                        self.rpc_tree.try_to_obtain(name, {}, '', '')
                )
                self.assertTrue(
                        self.rpc_tree.check_access((name, rpc_obj), {}, '', '')
                )
                self.assertRaises(
                        errors.RPCNotFoundError,
                        self.rpc_tree.try_to_obtain,
                        name, {}, 'yes', 'no!',
                )
                self.assertFalse(
                        self.rpc_tree.check_access(
                                (name, rpc_obj), {}, 'yes', 'no!',
                        )
                )
                self.assertRaises(
                        methodtree.BadAccessPatternError,
                        self.rpc_tree.try_to_obtain,
                        name, {}, '{yes}', 'no!',
                )
                self.assertRaises(
                        methodtree.BadAccessPatternError,
                        self.rpc_tree.check_access,
                        (name, rpc_obj), {}, 'yes', '{no}',
                )
            elif isinstance(rpc_obj, methodtree.RPCModule):
                self.assertEqual(
                        rpc_obj,
                        self.rpc_tree.try_to_obtain(
                                name, {}, '', '',
                                required_type=methodtree.RPCModule,
                        )
                )
                self.assertTrue(
                        self.rpc_tree.check_access(
                                (name, rpc_obj), {}, '', '',
                                required_type=methodtree.RPCModule,
                        )
                )
                self.assertRaises(
                        errors.RPCNotFoundError,
                        self.rpc_tree.try_to_obtain,
                        name, {}, '', '',
                        required_type=methodtree.RPCMethod,
                )
                self.assertRaises(
                        TypeError,
                        self.rpc_tree.check_access,
                        (name, rpc_obj), {}, '', '',
                        required_type=methodtree.RPCMethod,
                )
                self.assertRaises(
                        errors.RPCNotFoundError,
                        self.rpc_tree.try_to_obtain,
                        name, {}, 'yes', 'no!',
                        required_type=methodtree.RPCModule,
                )
                self.assertFalse(
                        self.rpc_tree.check_access(
                            (name, rpc_obj), {}, 'yes', 'no!',
                            required_type=methodtree.RPCModule,
                        )
                )
                self.assertRaises(
                        methodtree.BadAccessPatternError,
                        self.rpc_tree.try_to_obtain,
                        name, {}, '{yes}', 'no!',
                        required_type=methodtree.RPCModule,
                )
                self.assertRaises(
                        methodtree.BadAccessPatternError,
                        self.rpc_tree.check_access,
                        (name, rpc_obj), {}, 'yes', '{no}',
                        required_type=methodtree.RPCModule,
                )
            elif isinstance(rpc_obj, methodtree.RPCMethod):
                self.assertEqual(
                        rpc_obj,
                        self.rpc_tree.try_to_obtain(
                                name, {}, '', '',
                                required_type=methodtree.RPCMethod,
                        )
                )
                self.assertTrue(
                        self.rpc_tree.check_access(
                                (name, rpc_obj), {}, '', '',
                                required_type=methodtree.RPCMethod,
                        )
                )
                self.assertRaises(
                        errors.RPCNotFoundError,
                        self.rpc_tree.try_to_obtain, name, {}, '', '',
                        required_type=methodtree.RPCModule,
                )
                self.assertRaises(
                        TypeError,
                        self.rpc_tree.check_access,
                        (name, rpc_obj), {}, '', '',
                        required_type=methodtree.RPCModule,
                )
                self.assertRaises(
                        errors.RPCNotFoundError,
                        self.rpc_tree.try_to_obtain,
                        name, {}, 'yes', 'no!',
                        required_type=methodtree.RPCMethod,
                )
                self.assertFalse(
                        self.rpc_tree.check_access(
                                (name, rpc_obj), {}, 'yes', 'no!',
                                required_type=methodtree.RPCMethod,
                        )
                )
                self.assertRaises(
                        methodtree.BadAccessPatternError,
                        self.rpc_tree.try_to_obtain,
                        name, {}, '{yes}', 'no!',
                        required_type=methodtree.RPCMethod,
                )
                self.assertRaises(
                        methodtree.BadAccessPatternError,
                        self.rpc_tree.check_access,
                        (name, rpc_obj), {}, 'yes', '{no}',
                        required_type=methodtree.RPCMethod,
                )

        # iterations...

        # whole tree
        self.assertEqual(list(self.rpc_tree.all_names(deep=True)),
                         all_names)
        self.assertEqual(list(self.rpc_tree.method_names(deep=True)),
                         method_names)
        self.assertEqual(list(self.rpc_tree.submod_names(deep=True)),
                         submod_names)
        self.assertEqual(list(self.rpc_tree.all_items(deep=True)),
                         all_items)
        self.assertEqual(list(self.rpc_tree.method_items(deep=True)),
                         method_items)
        self.assertEqual(list(self.rpc_tree.submod_items(deep=True)),
                         submod_items)
        # +relative names (the same because `relative to root' means absolute)
        self.assertEqual(list(self.rpc_tree.all_names(deep=True,
                                                      get_relative_names=True)),
                         all_names)
        self.assertEqual(list(self.rpc_tree.method_names(deep=True,
                                                         get_relative_names=True)),
                         method_names)
        self.assertEqual(list(self.rpc_tree.submod_names(deep=True,
                                                         get_relative_names=True)),
                         submod_names)
        self.assertEqual(list(self.rpc_tree.all_items(deep=True,
                                                      get_relative_names=True)),
                         all_items)
        self.assertEqual(list(self.rpc_tree.method_items(deep=True,
                                                         get_relative_names=True)),
                         method_items)
        self.assertEqual(list(self.rpc_tree.submod_items(deep=True,
                                                         get_relative_names=True)),
                         submod_items)

        # root level only
        root_submod_names = list(filter((lambda s: '.' not in s), submod_names))
        self.assertEqual(list(filter((lambda s: '.' not in s), all_names)),
                         root_submod_names)
        root_submod_items = list(filter((lambda s: '.' not in s[0]), submod_items))
        self.assertEqual(list(filter((lambda s: '.' not in s[0]), all_items)),
                         root_submod_items)

        self.assertEqual(list(self.rpc_tree.all_names(deep=False)),
                         root_submod_names)
        self.assertEqual(list(self.rpc_tree.method_names(deep=False)),
                         [])
        self.assertEqual(list(self.rpc_tree.submod_names(deep=False)),
                         root_submod_names)
        self.assertEqual(list(self.rpc_tree.all_items(deep=False)),
                         root_submod_items)
        self.assertEqual(list(self.rpc_tree.method_items(deep=False)),
                         [])
        self.assertEqual(list(self.rpc_tree.submod_items(deep=False)),
                         root_submod_items)
        # +relative names (the same because `relative to root' means absolute)
        self.assertEqual(list(self.rpc_tree.all_names(deep=False,
                                                      get_relative_names=True)),
                         root_submod_names)
        self.assertEqual(list(self.rpc_tree.method_names(deep=False,
                                                         get_relative_names=True)),
                         [])
        self.assertEqual(list(self.rpc_tree.submod_names(deep=False,
                                                         get_relative_names=True)),
                         root_submod_names)
        self.assertEqual(list(self.rpc_tree.all_items(deep=False,
                                                      get_relative_names=True)),
                         root_submod_items)
        self.assertEqual(list(self.rpc_tree.method_items(deep=False,
                                                         get_relative_names=True)),
                         [])
        self.assertEqual(list(self.rpc_tree.submod_items(deep=False,
                                                         get_relative_names=True)),
                         root_submod_items)

        for super_name in ('a', 'b', 'b.a', 'b.b', 'b.a.a', 'c'):
            # subtree...
            # [prepare]
            cut = len(super_name + '.')  # (for relative names)

            s_all_names = list(name for name in all_names
                               if name.startswith(super_name + '.'))
            s_method_names = list(name for name in method_names
                                   if name.startswith(super_name + '.'))
            s_submod_names = list(name for name in submod_names
                                   if name.startswith(super_name + '.'))
            self.assertEqual(sorted(s_method_names + s_submod_names),
                             sorted(s_all_names))

            s_all_items = list((name, obj) for name, obj in all_items
                               if name.startswith(super_name + '.'))
            s_method_items = list((name, obj) for name, obj in method_items
                                   if name.startswith(super_name + '.'))
            s_submod_items = list((name, obj) for name, obj in submod_items
                                   if name.startswith(super_name + '.'))
            self.assertEqual(sorted(s_method_items + s_submod_items),
                             sorted(s_all_items))

            # ...absolute names
            self.assertEqual(list(self.rpc_tree.all_names(super_name, deep=True)),
                             s_all_names)
            self.assertEqual(list(self.rpc_tree.method_names(super_name, deep=True)),
                             s_method_names)
            self.assertEqual(list(self.rpc_tree.submod_names(super_name, deep=True)),
                             s_submod_names)
            self.assertEqual(list(self.rpc_tree.all_items(super_name, deep=True)),
                             s_all_items)
            self.assertEqual(list(self.rpc_tree.method_items(super_name, deep=True)),
                             s_method_items)
            self.assertEqual(list(self.rpc_tree.submod_items(super_name, deep=True)),
                             s_submod_items)

            # ...relative names
            self.assertEqual(list(self.rpc_tree.all_names(super_name, deep=True,
                                                          get_relative_names=True)),
                             [s[cut:] for s in s_all_names])
            self.assertEqual(list(self.rpc_tree.method_names(super_name, deep=True,
                                                             get_relative_names=True)),
                             [s[cut:] for s in s_method_names])
            self.assertEqual(list(self.rpc_tree.submod_names(super_name, deep=True,
                                                             get_relative_names=True)),
                             [s[cut:] for s in s_submod_names])
            self.assertEqual(list(self.rpc_tree.all_items(super_name, deep=True,
                                                          get_relative_names=True)),
                             [(s[cut:], obj) for s, obj in s_all_items])
            self.assertEqual(list(self.rpc_tree.method_items(super_name, deep=True,
                                                             get_relative_names=True)),
                             [(s[cut:], obj) for s, obj in s_method_items])
            self.assertEqual(list(self.rpc_tree.submod_items(super_name, deep=True,
                                                             get_relative_names=True)),
                             [(s[cut:], obj) for s, obj in s_submod_items])

            # one level only...
            # [prepare]
            dot_number = super_name.count('.') + 1
            filter_func = (lambda s: s.count('.') == dot_number)
            lev_all_names = list(filter(filter_func, s_all_names))
            lev_method_names = list(filter(filter_func, s_method_names))
            lev_submod_names = list(filter(filter_func, s_submod_names))
            self.assertEqual(lev_all_names, lev_method_names + lev_submod_names)

            filter_func = (lambda s: s[0].count('.') == dot_number)
            lev_all_items = list(filter(filter_func, s_all_items))
            lev_method_items = list(filter(filter_func, s_method_items))
            lev_submod_items = list(filter(filter_func, s_submod_items))
            self.assertEqual(lev_all_items, lev_method_items + lev_submod_items)

            # ...absolute names
            self.assertEqual(list(self.rpc_tree.all_names(super_name, deep=False)),
                             lev_all_names)
            self.assertEqual(list(self.rpc_tree.method_names(super_name, deep=False)),
                             lev_method_names)
            self.assertEqual(list(self.rpc_tree.submod_names(super_name, deep=False)),
                             lev_submod_names)
            self.assertEqual(list(self.rpc_tree.all_items(super_name, deep=False)),
                             lev_all_items)
            self.assertEqual(list(self.rpc_tree.method_items(super_name, deep=False)),
                             lev_method_items)
            self.assertEqual(list(self.rpc_tree.submod_items(super_name, deep=False)),
                             lev_submod_items)

            # ...relative names
            self.assertEqual(list(self.rpc_tree.all_names(super_name, deep=False,
                                                          get_relative_names=True)),
                             [s[cut:] for s in lev_all_names])
            self.assertEqual(list(self.rpc_tree.method_names(super_name, deep=False,
                                                             get_relative_names=True)),
                             [s[cut:] for s in lev_method_names])
            self.assertEqual(list(self.rpc_tree.submod_names(super_name, deep=False,
                                                             get_relative_names=True)),
                             [s[cut:] for s in lev_submod_names])
            self.assertEqual(list(self.rpc_tree.all_items(super_name, deep=False,
                                                          get_relative_names=True)),
                             [(s[cut:], obj) for s, obj in lev_all_items])
            self.assertEqual(list(self.rpc_tree.method_items(super_name, deep=False,
                                                             get_relative_names=True)),
                             [(s[cut:], obj) for s, obj in lev_method_items])
            self.assertEqual(list(self.rpc_tree.submod_items(super_name, deep=False,
                                                             get_relative_names=True)),
                             [(s[cut:], obj) for s, obj in lev_submod_items])

        # __iter__()
        self.assertEqual(list(self.rpc_tree), [''] + all_names)   # (with root module)

        # __len__()
        self.assertEqual(len(self.rpc_tree), 1 + len(all_names))

        # __contains__()
        self.assertTrue(all((name in self.rpc_tree) for name in [''] + all_names))

        # not hashable:
        self.assertRaises(TypeError, set, [self.rpc_tree])

        # adding and getting (2) -- errors:
        # module-or-method name (key) not found
        self.assertRaises(KeyError, self.rpc_tree.__getitem__, 'xyz')
        # (there is no 'xyz' module)
        self.assertRaises(KeyError, self.rpc_tree.add_rpc_method,
                          'a.b.c.xyz', 'meth999', lambda x: x)
        # (a.a.meth3 is a method name, not a module name)
        self.assertRaises(TypeError, self.rpc_tree.add_rpc_method,
                          'a.a.meth3', 'meth999', lambda x: x)
        # (cannot add a method to the root module)
        self.assertRaises(TypeError, self.rpc_tree.add_rpc_method,
                          '', 'meth999', lambda x: x)
        # (a.a.meth3 is already a method name)
        self.assertRaises(TypeError, self.rpc_tree.get_rpc_module,
                          'a.a.meth3', doc='', tag_dict=None)
        # (module name cannot start with '.')
        self.assertRaises(ValueError, self.rpc_tree.get_rpc_module,
                          '.a.module', doc='', tag_dict=None)
        # (repeated method names)
        for name, callable_obj in self.exceed_additions:
            split_name = name.split('.')
            rpc_mod_name = '.'.join(split_name[:-1])
            local_meth_name = split_name[-1]
            self.assertRaises(AssertionError, self.rpc_tree.add_rpc_method,
                              rpc_mod_name, local_meth_name, callable_obj)


class Test_build_rpc_tree_function(unittest.TestCase):

    @classmethod
    def classes2modules(cls, obj):
        "Transform classes info Python modules (recursively)"
        if isclass(obj):
            mod = types.ModuleType('RPCModule_{0}'.format(obj.__name__))
            mod.__dict__.update((name, cls.classes2modules(subobj))
                                for name, subobj in obj.__dict__.iteritems()
                                if name.startswith('__rpc_')
                                   or not name.startswith('_'))
            return mod
        else:
            return obj


    def setUp(self):
        class root:
            def nothing(): "not an RPC-method"
            class a:
                nothing = 3
                class b:
                    not_anything_special = "Tralala"
                    class c:
                        __rpc_doc__ = 'module a.b.c'
                        __rpc_methods__ = 'meth1', 'meth2'
                        def meth1(i): "method a.b.c.1"
                        def meth2(i, j): "method a.b.c.2"
                        def nmeth(): "not an RPC-method"
                class g:
                    class gg:
                        class ggg: "not an RPC-module"
                class h:
                    class gg: "not an RPC-module"
                class i: "not an RPC-module"
            class c:
                __rpc_doc__ = 'module c'
                __rpc_methods__ = 'x.y.meth1', 'x.y.meth2'
                class x:
                    class y:
                        __rpc_doc__ = 'module c.x.y'
                        __rpc_methods__ = 'meth3'
                        __rpc_tags__ = {'x': 1}
                        def meth1(): "method c.x.y.1"
                        def meth2(i, j, k): "method c.x.y.2"
                        def meth3(i, j): "method c.x.y.3"
                        def nmeth(x, y): "not an RPC-method"
                class z:
                    __rpc_methods__ = '*'
                    def meth1(i, j, k): "method c.z.1"
                    def meth2(i, j): "method c.z.2"
                    def meth3(i): "method c.z.3"
            class b:
                __rpc_methods__ = '*', 'q.w.e.r.t.y.*', 'q.w.e.s.*', 'q.w.e.a.meth1'
                def meth1(i, j): "method b.1"
                def meth2(i, j, k): "method b.2"
                def meth3(i, j, k, l): "method b.3"
                class v:
                    class b:
                        class n:
                            __rpc_tags__ = {1: 2, 3: 4}
                class q:
                    class w:
                        __rpc_methods__ = 'meth0'
                        def meth0(): "method ...0"
                        class e:
                            class r:
                                class t:
                                    class y:
                                        def meth1(): "method ...1"
                                        def meth2(): "method ...2"
                                        def meth3(): "method ...3"
                                        def meth4(): "method ...4"
                                        def meth5(): "method ...5"
                                        meth1.__rpc_tags__ = {'a': 'b'}
                                        class u:
                                            class i: "not an RPC-module"
                            class d:
                                __rpc_doc__ = "module c...d"
                            class f: "nothing"
                            class s:
                                __rpc_methods__ = 'meth1', 'z.y.x.*'
                                def meth1(): "method ...1"
                                def meth2(): "method ...2"
                                class z:
                                    __rpc_doc__ = 'ZZZ'
                                    def nmeth(): "not an RPC-method"
                                    class y:
                                        __rpc_methods__ = '*'
                                        def meth0(): "method ...0"
                                        class x:
                                            def methX(): "method ...X"
                                            class y:
                                                def nmeth(): "nothing"
                            class a:
                                __rpc_methods__ = '*'
                                def meth1(): "method ...1"
                                def meth2(): "method ...2"
                        class h:
                            hh = "nothing special"

        self.root = root
        self.Mod = Mod = namedtuple('Mod', 'doc tags')
        self.Method = Method = namedtuple('Method', 'doc tags number_of_args')
        self.expected_items = [
                ('a', Mod('', {})),
                ('a.b', Mod('', {})),
                ('a.b.c', Mod('module a.b.c', {})),
                ('a.b.c.meth1', Method('method a.b.c.1', {}, 1)),
                ('a.b.c.meth2', Method('method a.b.c.2', {}, 2)),

                ('b', Mod('', {})),
                ('b.meth1', Method("method b.1", {}, 2)),
                ('b.meth2', Method("method b.2", {}, 3)),
                ('b.meth3', Method("method b.3", {}, 4)),
                ('b.q', Mod('', {})),
                ('b.q.w', Mod('', {})),
                ('b.q.w.meth0', Method("method ...0", {}, 0)),
                ('b.q.w.e', Mod('', {})),
                ('b.q.w.e.a', Mod('', {})),
                ('b.q.w.e.a.meth1', Method("method ...1", {}, 0)),
                ('b.q.w.e.a.meth2', Method("method ...2", {}, 0)),
                ('b.q.w.e.d', Mod("module c...d", {})),
                ('b.q.w.e.r', Mod('', {})),
                ('b.q.w.e.r.t', Mod('', {})),
                ('b.q.w.e.r.t.y', Mod('', {})),
                ('b.q.w.e.r.t.y.meth1', Method("method ...1", {'a': 'b'}, 0)),
                ('b.q.w.e.r.t.y.meth2', Method("method ...2", {}, 0)),
                ('b.q.w.e.r.t.y.meth3', Method("method ...3", {}, 0)),
                ('b.q.w.e.r.t.y.meth4', Method("method ...4", {}, 0)),
                ('b.q.w.e.r.t.y.meth5', Method("method ...5", {}, 0)),
                ('b.q.w.e.s', Mod('', {})),
                ('b.q.w.e.s.meth1', Method("method ...1", {}, 0)),
                ('b.q.w.e.s.meth2', Method("method ...2", {}, 0)),
                ('b.q.w.e.s.z', Mod('ZZZ', {})),
                ('b.q.w.e.s.z.y', Mod('', {})),
                ('b.q.w.e.s.z.y.meth0', Method("method ...0", {}, 0)),
                ('b.q.w.e.s.z.y.x', Mod('', {})),
                ('b.q.w.e.s.z.y.x.methX', Method("method ...X", {}, 0)),
                ('b.v', Mod('', {})),
                ('b.v.b', Mod('', {})),
                ('b.v.b.n', Mod('', {1: 2, 3: 4})),

                ('c', Mod('module c', {})),
                ('c.x', Mod('', {})),
                ('c.x.y', Mod('module c.x.y', {'x': 1})),
                ('c.x.y.meth1', Method("method c.x.y.1", {}, 0)),
                ('c.x.y.meth2', Method("method c.x.y.2", {}, 3)),
                ('c.x.y.meth3', Method("method c.x.y.3", {}, 2)),
                ('c.z', Mod('', {})),
                ('c.z.meth1', Method("method c.z.1", {}, 3)),
                ('c.z.meth2', Method("method c.z.2", {}, 2)),
                ('c.z.meth3', Method("method c.z.3", {}, 1)),
        ]


    def test_normal_build(self):
        py_root = self.classes2modules(self.root)
        rpc_tree = methodtree.build_rpc_tree(py_root)

        expected_items = iter(self.expected_items)
        for i, rpc_item in enumerate(rpc_tree.all_items(deep=True), 1):
            name, obj = rpc_item
            if isinstance(obj, methodtree.RPCModule):
                item = (name, self.Mod(obj.doc, obj.tags))
            else:
                self.assertTrue(isinstance(obj, methodtree.RPCMethod))
                item = (name, self.Method(obj.doc, obj.tags,
                                          len(getargspec(obj.callable_obj).args)))
            expected = next(expected_items)
            self.assertEqual(item, expected)

        self.assertEqual(i, len(self.expected_items))

    def test_err__illegal_chars_in_meth_declaration(self):
        self.root.a.b.__rpc_methods__ = 'n-'  # illegal character '-'
        py_root = self.classes2modules(self.root)
        self.assertRaises(ValueError, methodtree.build_rpc_tree, py_root)
        self.root.a.b.__rpc_methods__ = 'bb*'  # '*' could be only at the end
        py_root = self.classes2modules(self.root)
        self.assertRaises(ValueError, methodtree.build_rpc_tree, py_root)

    def test_err__not_a_mod_declared_as_mod(self):
        self.root.a.__rpc_methods__ = 'nothing.*'
        py_root = self.classes2modules(self.root)
        self.assertRaises(TypeError, methodtree.build_rpc_tree, py_root)

    def test_err__no_such_method_warning(self):
        self.root.a.b.c.__rpc_methods__ += ('ghost',)
        py_root = self.classes2modules(self.root)
        with warnings.catch_warnings():
            warnings.filterwarnings('error', category=methodtree.LogWarning)
            self.assertRaises(methodtree.LogWarning,
                              methodtree.build_rpc_tree, py_root)

    def test_err__cyclic_ref_warning(self):
        py_root = self.classes2modules(self.root)
        py_root.c.x.y.x = py_root.c.x
        with warnings.catch_warnings():
            warnings.filterwarnings('error', category=methodtree.LogWarning)
            self.assertRaises(methodtree.LogWarning,
                              methodtree.build_rpc_tree, py_root)

    def test_normal_build_with_ignored_warnings(self):
        # no such methods:
        self.root.a.b.c.__rpc_methods__ += ('ghost', 'x')
        py_root = self.classes2modules(self.root)
        # cyclic ref:
        py_root.c.x.y.x = py_root.c.x

        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=methodtree.LogWarning)
            rpc_tree = methodtree.build_rpc_tree(py_root)

        expected_items = iter(self.expected_items)
        for i, rpc_item in enumerate(rpc_tree.all_items(deep=True), 1):
            name, obj = rpc_item
            if isinstance(obj, methodtree.RPCModule):
                item = (name, self.Mod(obj.doc, obj.tags))
            else:
                self.assertTrue(isinstance(obj, methodtree.RPCMethod))
                item = (name, self.Method(obj.doc, obj.tags,
                                          len(getargspec(obj.callable_obj).args)))
            expected = next(expected_items)
            self.assertEqual(item, expected)

        self.assertEqual(i, len(self.expected_items))


if __name__ == '__main__':
    unittest.main(testRunner=unittest.TextTestRunner(verbosity=2))
