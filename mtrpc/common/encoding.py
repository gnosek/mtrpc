#!/usr/bin/env python

import json
import datetime

import sys

try:
    pattern = json.decoder.pattern

    # json module as seen in python 2.6
    json_v26 = True
except AttributeError:
    json_v26 = False

ISO8601_FORMAT_V26 = '%Y%m%dT%H:%M:%S.%f'
ISO8601_FORMAT_V25 = '%Y%m%dT%H:%M:%S'

if sys.version_info >= (2, 6):
    ISO8601_FORMATS = (ISO8601_FORMAT_V26, ISO8601_FORMAT_V25)
else:
    ISO8601_FORMATS = (ISO8601_FORMAT_V25,)

class MtrpcJsonEncoder(json.JSONEncoder):
    """Serialize datetime instances as iso8601 timestamps"""

    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.strftime(ISO8601_FORMATS[0])

        return super(MtrpcJsonEncoder, self).default(o)

if json_v26:
    # custom iterscan instead of dumb hardcoded default,
    # identical otherwise
    @json.scanner.pattern(r'{')
    def mtrpc_object(match, context, _w=json.decoder.WHITESPACE.match):
        pairs = {}
        s = match.string
        end = _w(s, match.end()).end()
        nextchar = s[end:end + 1]
        # Trivial empty object
        if nextchar == '}':
            return pairs, end + 1
        if nextchar != '"':
            raise ValueError(errmsg("Expecting property name", s, end))
        end += 1
        encoding = getattr(context, 'encoding', None)
        strict = getattr(context, 'strict', True)
        iterscan = context._scanner.iterscan
        while True:
            key, end = json.decoder.scanstring(s, end, encoding, strict)
            end = _w(s, end).end()
            if s[end:end + 1] != ':':
                raise ValueError(errmsg("Expecting : delimiter", s, end))
            end = _w(s, end + 1).end()
            try:
                value, end = iterscan(s, idx=end, context=context).next()
            except StopIteration:
                raise ValueError(errmsg("Expecting object", s, end))
            pairs[key] = value
            end = _w(s, end).end()
            nextchar = s[end:end + 1]
            end += 1
            if nextchar == '}':
                break
            if nextchar != ',':
                raise ValueError(errmsg("Expecting , delimiter", s, end - 1))
            end = _w(s, end).end()
            nextchar = s[end:end + 1]
            end += 1
            if nextchar != '"':
                raise ValueError(errmsg("Expecting property name", s, end - 1))
        object_hook = getattr(context, 'object_hook', None)
        if object_hook is not None:
            pairs = object_hook(pairs)
        return pairs, end

    # custom iterscan instead of dumb hardcoded default,
    # identical otherwise
    @json.decoder.pattern(r'\[')
    def mtrpc_json_array(match, context, _w=json.decoder.WHITESPACE.match):
        values = []
        s = match.string
        end = _w(s, match.end()).end()
        # Look-ahead for trivial empty array
        nextchar = s[end:end + 1]
        if nextchar == ']':
            return values, end + 1
        iterscan = context._scanner.iterscan
        while True:
            try:
                value, end = iterscan(s, idx=end, context=context).next()
            except StopIteration:
                raise ValueError(errmsg("Expecting object", s, end))
            values.append(value)
            end = _w(s, end).end()
            nextchar = s[end:end + 1]
            end += 1
            if nextchar == ']':
                break
            if nextchar != ',':
                raise ValueError(errmsg("Expecting , delimiter", s, end))
            end = _w(s, end).end()
        return values, end

    @json.scanner.pattern(r'"')
    def mtrpc_string(match, context):
        s, end = json.decoder.JSONString(match, context)
        for fmt in ISO8601_FORMATS:
            try:
                s = datetime.datetime.strptime(s, fmt)
                return s, end
            except ValueError:
                continue

        return s, end

    MTRPC_JSON_DECODERS = [
        mtrpc_object,
        mtrpc_json_array,
        mtrpc_string,
        json.decoder.JSONConstant,
        json.decoder.JSONNumber,
    ]

    class MtrpcJsonDecoder(json.decoder.JSONDecoder):
        _scanner = json.scanner.Scanner(MTRPC_JSON_DECODERS)

else:

    def mtrpc_scanstring(s, end, *args, **kwargs):
        s, end = json.decoder.scanstring(s, end, *args, **kwargs)
        for fmt in ISO8601_FORMATS:
            try:
                s = datetime.datetime.strptime(s, fmt)
                return s, end
            except ValueError:
                continue

        return s, end

    class MtrpcJsonDecoder(json.decoder.JSONDecoder):

        def __init__(self, *args, **kwargs):
            super(MtrpcJsonDecoder, self).__init__(*args, **kwargs)
            self.parse_string = mtrpc_scanstring

            ## sadly, we cannot use the C version as it apparently has
            ## a hardcoded parse_string hook. sigh.
            self.scan_once = json.scanner.py_make_scanner(self)

def dumps(obj, *args, **kwargs):
    '''Serialize an object tree

    >>> dumps(dict(a=1, b=2))
    '{"a": 1, "b": 2}'
    >>> dumps(dict(a=1, b=datetime.datetime(2011, 1, 2, 15, 30, 15, 30101)))
    '{"a": 1, "b": "20110102T15:30:15.030101"}'
    '''
    kwargs['cls'] = MtrpcJsonEncoder
    return json.dumps(obj, *args, **kwargs)

def loads(s, *args, **kwargs):
    '''Deserialize an object tree

    >>> loads('"20110102T15:30:15.030101"')
    datetime.datetime(2011, 1, 2, 15, 30, 15, 30101)
    >>> j = '{"a": 1, "b": "20110102T15:30:15.030101", "c": "2011"}'
    >>> d = loads(j)
    >>> d['a']
    1
    >>> d['b']
    datetime.datetime(2011, 1, 2, 15, 30, 15, 30101)
    >>> unicode(d['c'])
    u'2011'
    >>> loads('{}')
    {}
    '''
    kwargs['cls'] = MtrpcJsonDecoder
    return json.loads(s, *args, **kwargs)

if __name__ == '__main__':
    import doctest
    doctest.testmod()
