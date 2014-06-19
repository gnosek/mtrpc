import json
import pkg_resources
from mtrpc.server.config import loader


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

    def __init__(self, config_paths, server_class):
        config_dict = {}
        for p in config_paths:
            fp = load_config(p)
            config_dict = loader.load_props(fp, config_dict)
        self.config_dict = config_dict
        self.server_class = server_class

    def run(self, final_callback=None):
        self.server_class.configure_and_start(self.config_dict, final_callback)