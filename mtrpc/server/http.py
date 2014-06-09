from mtrpc.common.const import ACCESS_DICT_KWARG, ACCESS_KEY_KWARG, ACCESS_KEYHOLE_KWARG
from mtrpc.server.core import MTRPCServerInterface
from flask import Flask, Response, abort, jsonify, request


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
    return jsonify(response=rpc_object(**build_rpc_args(request.args)))


@flask_app.route('/call/<path:rpc_object_url>', methods=['POST'])
def call(rpc_object_url):
    rpc_object = find_rpc_object(rpc_object_url)
    if not callable(rpc_object):
        abort(403, 'RPC object is not callable')
    return jsonify(response=rpc_object(**build_rpc_args(request.form)))


class HttpServer(MTRPCServerInterface):

    RPC_MODE = 'server'

    def start(self, final_callback=None):
        flask_app.run(debug=True)

    def stop(self, reason='manual stop', loglevel='info', force=False, timeout=30):
        pass


if __name__ == '__main__':
    HttpServer.configure_and_start({})
