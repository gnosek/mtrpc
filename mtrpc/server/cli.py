import readline
import rlcompleter
import code

from mtrpc.server import MTRPCServerInterface, methodtree


class MtrpcCli(MTRPCServerInterface):

    RPC_MODE = 'cli'

    def start(self, final_callback=None):
        params = dict(rpc=methodtree.RPCSubTree(self.rpc_tree))
        readline.set_completer(rlcompleter.Completer(params).complete)
        readline.parse_and_bind("tab:complete")

        code.interact(local=params)
