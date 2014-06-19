def by_example(obj):
    if isinstance(obj, basestring):
        return {'type': 'string', 'default': obj}
    if obj is None:
        return {}
    if isinstance(obj, (list, tuple)):
        ret = {'type': 'array', 'default': obj}
        if obj and obj[0] is not None:
            item = by_example(obj[0])
            item.pop('default')
            ret['items'] = item
        return ret
    if isinstance(obj, dict):
        ret = {'type': 'object', 'default': obj, 'properties': {}}
        for k, v in obj.items():
            ret['properties'][k] = by_example(v)
        return ret
    if isinstance(obj, bool):
        return {'type': 'boolean', 'default': obj}
    raise TypeError('Invalid item {0!r} of class {1}'.format(obj, type(obj)))

if __name__ == '__main__':
    import sys
    import json
    import pprint
    json_file = open(sys.argv[1])
    pprint.pprint(by_example(json.load(json_file)))
