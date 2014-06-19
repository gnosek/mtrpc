import json

import pkg_resources
from jsonschema import validators, Draft4Validator

from mtrpc.server.config import loader
from mtrpc.server.methodtree import RPCTree


def extend_with_default(validator_class):
    validate_properties = validator_class.VALIDATORS["properties"]

    def set_defaults(validator, properties, instance, schema):
        for error in validate_properties(validator, properties, instance, schema):
            yield error

        for prop, subschema in properties.iteritems():
            if "default" in subschema:
                instance.setdefault(prop, subschema["default"])

    return validators.extend(validator_class, {"properties": set_defaults})


DefaultValidatingDraft4Validator = extend_with_default(Draft4Validator)


def load_config(config_path):
    if config_path.startswith('/'):
        return open(config_path)

    if '=' in config_path:
        key, value = config_path.split('=', 1)
        key = key.strip()
        value = value.strip()
        try:
            json.loads(value)
        except ValueError:
            value = json.dumps(value)
        # convert key=value to key: value
        # key:value syntax is already taken by package loader
        return ['{key}: {value}'.format(key=key, value=value)]

    if ':' in config_path:
        package, relative_path = config_path.split(':', 1)

        resource_manager = pkg_resources.ResourceManager()
        provider = pkg_resources.get_provider(package)

        return provider.get_resource_stream(resource_manager, relative_path)

    return open(config_path)


class ServerConfig(object):

    def __init__(self, config_paths, server_class, rpc_tree_class=RPCTree):
        config_dict = {}
        for p in config_paths:
            fp = load_config(p)
            config_dict = loader.load_props(fp, config_dict)
        self.config_dict = config_dict
        self.server_class = server_class
        self.rpc_tree_class = rpc_tree_class
        self.server = None
        self.rpc_tree = None

    def validate_config(self, cls):
        if hasattr(cls, 'CONFIG_SCHEMAS'):
            for schema in cls.CONFIG_SCHEMAS:
                validator = DefaultValidatingDraft4Validator(schema)
                validator.validate(self.config_dict)

    def validate(self):
        self.validate_config(self.rpc_tree_class)
        self.validate_config(self.server_class)

    def run(self, final_callback=None):
        self.validate()
        self.rpc_tree = self.rpc_tree_class.load(self.config_dict, rpc_mode=self.server_class.RPC_MODE)
        server = self.server_class.configure(self.config_dict, self.rpc_tree)
        server.start(final_callback=final_callback)

    def stop(self):
        if hasattr(self.server, 'stop'):
            self.server.stop()


if __name__ == '__main__':
    import sys
    from mtrpc.server.amqp import AmqpServer
    conf = ServerConfig(sys.argv[1:], AmqpServer)
    conf.validate()