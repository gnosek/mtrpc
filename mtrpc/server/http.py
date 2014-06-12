import os
from itertools import chain

from flask import Flask, Response, abort, jsonify, request
from gunicorn.config import Config
from gunicorn.app.base import Application

from mtrpc.common.const import ACCESS_DICT_KWARG, ACCESS_KEY_KWARG, ACCESS_KEYHOLE_KWARG
from mtrpc.common.errors import RPCMethodArgError
from mtrpc.server.core import MTRPCServerInterface


flask_app = Flask(__name__)


def find_rpc_object(url):
    full_path = url.strip('/').replace('/', '.')
    server = HttpServer.get_instance()
    try:
        return server.rpc_tree[full_path]
    except KeyError:
        abort(404, 'RPC endpoint not found')


def build_rpc_args(args):
    out = {}
    for k, v in args.items():
        if len(v) == 1:
            out[k] = v[0]
        else:
            out[k] = v

    out[ACCESS_DICT_KWARG] = {}
    out[ACCESS_KEY_KWARG] = ''
    out[ACCESS_KEYHOLE_KWARG] = ''

    return out


def call_rpc_object(rpc_object, args):
    try:
        return jsonify(response=rpc_object(**build_rpc_args(args)))
    except RPCMethodArgError as exc:
        abort(400, str(exc).replace('{name}', rpc_object.full_name))


@flask_app.route('/help/<path:rpc_object_url>', methods=['GET'])
def get_help(rpc_object_url):
    rpc_object = find_rpc_object(rpc_object_url)
    return Response(rpc_object.__doc__, content_type='text/plain')


@flask_app.route('/call/<path:rpc_object_url>', methods=['GET'])
def call_view(rpc_object_url):
    rpc_object = find_rpc_object(rpc_object_url)
    if not callable(rpc_object):
        abort(403, 'RPC object is not callable')
    if not getattr(rpc_object, 'readonly', False):
        abort(405, 'Method not allowed')
    return call_rpc_object(rpc_object, request.args)


@flask_app.route('/call/<path:rpc_object_url>', methods=['POST'])
def call(rpc_object_url):
    rpc_object = find_rpc_object(rpc_object_url)
    if not callable(rpc_object):
        abort(403, 'RPC object is not callable')
    return call_rpc_object(rpc_object, request.form)


class ConfigurableApplication(Application):
    @staticmethod
    def default_cfg():
        return {
            'worker_class': 'sync',
            'workers': 2,
            'secure_scheme_headers': {
                'X-FORWARDED-PROTO': 'https'
            },
            'preload_app': True,
            'user': os.getuid(),
        }

    def __init__(self, app, **config):
        self.app = app
        self._config = config
        super(ConfigurableApplication, self).__init__()

    def load(self):
        return self.app

    def init(self, parser, opts, args):
        self.cfg.set('default_proc_name', 'mtrpc')

    def load_config(self):
        self.cfg = Config()
        for k, v in chain(self.default_cfg().items(), self._config.items()):
            self.cfg.set(k.lower(), v)


class HttpServer(MTRPCServerInterface):
    RPC_MODE = 'server'

    CONFIG_SECTION_TYPES = dict(
        MTRPCServerInterface.CONFIG_SECTION_TYPES,
        http=dict
    )
    CONFIG_SECTION_FIELDS = dict(
        MTRPCServerInterface.CONFIG_SECTION_FIELDS,
        http={
            'bind': '127.0.0.1:5000',
            'debug': False,
        }
    )

    def start(self, final_callback=None):
        http_debug = self.config['http'].get('debug', self.CONFIG_SECTION_FIELDS['http']['debug'])
        http_bind = self.config['http'].get('bind', self.CONFIG_SECTION_FIELDS['http']['bind'])
        if http_debug:
            host, port = http_bind.rsplit(':', 1)
            flask_app.run(host=host, port=int(port), debug=True)
        else:
            app = ConfigurableApplication(flask_app, bind=http_bind)
            app.run()

    def stop(self, reason='manual stop', loglevel='info', force=False, timeout=30):
        pass


if __name__ == '__main__':
    HttpServer.configure_and_start({})
