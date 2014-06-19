import readline
import rlcompleter
import code

from mtrpc.server.core import MTRPCServerInterface
from mtrpc.server import methodtree


class MtrpcCli(MTRPCServerInterface):

    RPC_MODE = 'cli'

    def start(self, final_callback=None):
        params = dict(rpc=methodtree.RPCSubTree(self.rpc_tree))
        readline.set_completer(rlcompleter.Completer(params).complete)
        readline.parse_and_bind("tab:complete")

        code.interact(local=params)


if __name__ == '__main__':
    cli = MtrpcCli()
    cli.configure_and_start({})