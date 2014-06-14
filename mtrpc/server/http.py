import multiprocessing
import os
from itertools import chain

from flask import Flask, Response, abort, jsonify, request
from gunicorn.config import Config
from gunicorn.app.base import Application

from mtrpc.common.const import ACCESS_DICT_KWARG, ACCESS_KEY_KWARG, ACCESS_KEYHOLE_KWARG
from mtrpc.common.errors import RPCMethodArgError
from mtrpc.server.core import MTRPCServerInterface


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

    def __init__(self):
        super(HttpServer, self).__init__()
        self.writer_lock = multiprocessing.Lock()

    def find_rpc_object(self, url):
        full_path = url.strip('/').replace('/', '.')
        try:
            return self.rpc_tree[full_path]
        except KeyError:
            abort(404, 'RPC endpoint not found')

    @classmethod
    def build_rpc_args(cls, args):
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

    @classmethod
    def call_rpc_object(cls, rpc_object, args):
        try:
            return jsonify(response=rpc_object(**cls.build_rpc_args(args)))
        except RPCMethodArgError as exc:
            abort(400, str(exc).replace('{name}', rpc_object.full_name))

    def get_help(self, rpc_object_url):
        rpc_object = self.find_rpc_object(rpc_object_url)
        return Response(rpc_object.__doc__, content_type='text/plain')

    def call_view(self, rpc_object_url):
        rpc_object = self.find_rpc_object(rpc_object_url)
        if not callable(rpc_object):
            abort(403, 'RPC object is not callable')
        if not getattr(rpc_object, 'readonly', False):
            abort(405, 'Method not allowed')
        return self.call_rpc_object(rpc_object, request.args)

    def call(self, rpc_object_url):
        rpc_object = self.find_rpc_object(rpc_object_url)
        if not callable(rpc_object):
            abort(403, 'RPC object is not callable')
        if getattr(rpc_object, 'readonly', False):
            return self.call_rpc_object(rpc_object, request.form)
        if self.writer_lock.acquire(False):
            try:
                return self.call_rpc_object(rpc_object, request.form)
            finally:
                self.writer_lock.release()
        else:
                abort(503, 'A writer method is already running')

    def build_wsgi_app(self):
        flask_app = Flask(__name__)
        flask_app.route('/help/<path:rpc_object_url>', methods=['GET'])(self.get_help)
        flask_app.route('/call/<path:rpc_object_url>', methods=['GET'])(self.call_view)
        flask_app.route('/call/<path:rpc_object_url>', methods=['POST'])(self.call)
        return flask_app

    def start(self, final_callback=None):
        http_debug = self.config['http'].get('debug', self.CONFIG_SECTION_FIELDS['http']['debug'])
        http_bind = self.config['http'].get('bind', self.CONFIG_SECTION_FIELDS['http']['bind'])
        flask_app = self.build_wsgi_app()
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
