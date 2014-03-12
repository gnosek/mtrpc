#!/usr/bin/python

import json
import string
import doctest

STATE_INIT = 1
STATE_ESCAPED = 2
STATE_OPEN_INDEX = 3
STATE_INDEX_DIGIT = 4
STATE_CLOSE_INDEX = 5
STATE_IN_KEY = 6
STATE_AFTER_ESCAPE = 7
STATE_ESCAPED_BACKSLASH = 8

identifier_chars = set(string.ascii_letters + string.digits + '_')
digits = set(string.digits)

def get_path_element(path):
    """Split dotted path into head and tail
    >>> get_path_element('foo.bar.baz')
    ('foo', None, 'bar.baz')
    >>> get_path_element('foo')
    ('foo', None, '')
    >>> get_path_element('foo[3].bar.baz')
    ('foo', 3, 'bar.baz')
    >>> get_path_element('{foo}[3]')
    ('foo', 3, '')
    >>> get_path_element('foo[3]')
    ('foo', 3, '')
    >>> get_path_element('{foo.bar.baz}.quux.quuux')
    ('foo.bar.baz', None, 'quux.quuux')
    >>> get_path_element('{foo\{bar\}baz}.quux.quuux')
    ('foo{bar}baz', None, 'quux.quuux')
    >>> get_path_element('qx{foo.bar.baz}.quux.quuux')
    ('qx', None, '{foo.bar.baz}.quux.quuux')
    """

    i = None
    c = None

    def _unexpected():
        raise ValueError('Parse error at or near {0}: unexpected {1}'.format(path[i:], c))

    key = []
    index = None
    state = STATE_INIT
    for i, c in enumerate(path):
        if state == STATE_INIT:
            if c == '{':
                state = STATE_ESCAPED
            elif c in identifier_chars:
                key.append(c)
                state = STATE_IN_KEY
            else:
                _unexpected()

        elif state == STATE_IN_KEY:
            if c == '.':
                break
            elif c == '[':
                state = STATE_OPEN_INDEX
                index = 0
            elif c == '{':
                i = i-1
                break
            elif c in identifier_chars:
                key.append(c)
            else:
                _unexpected()

        elif state == STATE_ESCAPED:
            if c == '}':
                state = STATE_AFTER_ESCAPE
            elif c == '{':
                # only for symmetry with forced escaping of }
                _unexpected()
            elif c == '\\':
                state = STATE_ESCAPED_BACKSLASH
            else:
                key.append(c)

        elif state == STATE_ESCAPED_BACKSLASH:
            key.append(c)
            state = STATE_ESCAPED

        elif state == STATE_AFTER_ESCAPE:
            if c == '.':
                break
            elif c == '[':
                state = STATE_OPEN_INDEX
                index = 0
            elif c == '{':
                i = i-1
                break
            else:
                _unexpected()

        elif state == STATE_OPEN_INDEX:
            if c in digits:
                index = int(c)
                state = STATE_INDEX_DIGIT
            else:
                _unexpected()

        elif state == STATE_INDEX_DIGIT:
            if c in digits:
                index = 10 * index + int(c)
            elif c == ']':
                state = STATE_CLOSE_INDEX
            else:
                _unexpected()

        elif state == STATE_CLOSE_INDEX:
            if c == '.':
                break
            elif c == '{':
                i = i-1
                break
            else:
                _unexpected()

    key = ''.join(key)
    remainder = path[i+1:]
    return key, index, remainder

def extend_array(array, length):
    """Pad array to @length
    >>> extend_array(['a', 'b', 'c'], 6)
    ['a', 'b', 'c', None, None, None]
    """
    return [
        array[x] if x < len(array) else None
        for x in range(0, length)
    ]

def merge(parent, index, key, value):
    """Merge a value into a list or dict, returning (possibly) a new object
    >>> merge(dict(), 1, 'foo', 'bar')
    {'foo': [None, 'bar']}
    >>> merge(dict(), None, 'foo', 'bar')
    {'foo': 'bar'}
    >>> merge(None, None, 'foo', 'bar')
    {'foo': 'bar'}
    >>> merge(None, 1, 'foo', 'bar')
    {'foo': [None, 'bar']}
    >>> merge(None, 0, 'foo', 'bar')
    {'foo': ['bar']}
    """
    if parent is None:
        parent = dict()
    if index is not None:
        if key not in parent:
            parent[key] = []
        if index >= len(parent[key]):
            parent[key] = extend_array(parent[key], index+1)
        parent[key][index] = value
        return parent
    else:
        parent[key] = value
        return parent

def walk(path, value):
    """Walk down @path and set @value at the bottom

    Returns value suitable for merging
    >>> walk('foo', 3)
    {'foo': 3}
    >>> walk('foo[1]', 3)
    {'foo': [None, 3]}
    >>> walk('foo.bar.baz', 3)
    {'foo': {'bar': {'baz': 3}}}
    >>> walk('foo.bar[0]', 3)
    {'foo': {'bar': [3]}}
    """
    key, index, remainder = get_path_element(path)
    if not remainder:
        return merge(None, index, key, value)
    else:
        rem = walk(remainder, value)
        return merge(None, index, key, rem)

def zip_arrays(a, b):
    """Recursively zip contents of two lists together
    >>> arr1 = ['a', None]
    >>> arr2 = [None, 'b', None, 'c']
    >>> zip_arrays(arr1, arr2)
    ['a', 'b', None, 'c']
    """
    l = max(len(a), len(b))
    ret = []
    for i in range(0, l):
        try:
            val_a = a[i]
        except IndexError:
            val_a = None
        try:
            val_b = b[i]
        except IndexError:
            val_b = None
        if val_a is None:
            ret.append(val_b)
        elif val_b is None:
            ret.append(val_a)
        else:
            ret.append(zip_objects(val_a, val_b))
    return ret

def zip_dicts(a, b):
    """Recursively zip contents of two dicts together
    >>> d1 = {'a': 'aa', 'b': None}
    >>> d2 = {'a': None, 'b': 'bb', 'c': 'cc'}
    >>> zd = zip_dicts(d1, d2)
    >>> set(zd.keys()) == set('abc')
    True
    >>> zd['a'] == 'aa'
    True
    >>> zd['b'] == 'bb'
    True
    >>> zd['c'] == 'cc'
    True
    """
    k = set(a.keys() + b.keys())
    ret = {}
    for key in k:
        try:
            val_a = a[key]
        except KeyError:
            val_a = None
        try:
            val_b = b[key]
        except KeyError:
            val_b = None
        if val_a is None:
            ret[key] = val_b
        elif val_b is None:
            ret[key] = val_a
        else:
            ret[key] = zip_objects(val_a, val_b)
    return ret

def zip_objects(a, b):
    """Recursively zip contents of two objects together
    >>> d1 = {'a': 'aa', 'b': None, 'c': [None, 'cc']}
    >>> d2 = {'a': None, 'b': 'bb', 'c': ['c2', None]}
    >>> d = zip_objects(d1, d2)
    >>> d['c']
    ['c2', 'cc']
    >>> d1 = {'a': 'aa', 'b': 'xx', 'c': [None, 'cc']}
    >>> d2 = {'a': None, 'b': 'bb', 'c': ['c2', None]}
    >>> d = zip_objects(d1, d2)
    >>> d['b']
    'bb'
    """
    if a is None:
        return b
    if b is None:
        return a
    ta = type(a)
    tb = type(b)
    if ta != tb:
        raise TypeError('Cannot zip objects of different types')
    if ta == list:
        return zip_arrays(a, b)
    elif ta == dict:
        return zip_dicts(a, b)
    else:
        return b


def process_logical_line(props, ll):
    if ll:
        key, value = ll.split(':', 1)
        try:
            _value = json.loads(value)
        except ValueError as exc:
            try:
                _value = json.loads(ll)
            except ValueError as exc:
                raise ValueError("malformed JSON/Prop: {0}\n{1}".format(exc, ll))
            else:
                new_props = _value
        else:
            new_props = walk(key, _value)
        return zip_objects(props, new_props)
    else:
        return props

def load_props(f, props=None):
    logical_line = ''
    if props is None:
        props = {}
    for line in f:
        if len(line) == 0 or line.startswith('#'):
            continue
        elif line[0] in string.whitespace + '}]':
            logical_line = logical_line + line
        else:
            props = process_logical_line(props, logical_line)
            logical_line = line
    return process_logical_line(props, logical_line)

if __name__ == '__main__':
    import sys
    if len(sys.argv) == 1 or '--test' in sys.argv:
        doctest.testmod()
    else:
        with open(sys.argv[1]) as f:
            props = load_props(f)
            print repr(props)
