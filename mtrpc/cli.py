import inspect
import sys
from optparse import OptionParser

def print_func_sig(name, func):
    argspec = inspect.getargspec(func)
    print "\t%s%s" % (name, inspect.formatargspec(*argspec))

def run_cli(module, opt=None):
    if opt is None:
        opt=sys.argv

    parser=OptionParser()
    parser.add_option("-i", "--info", action="store_true", dest="info")
    (options, args) = parser.parse_args(opt[1:])
    j = len(args)

    if options.info:
        for arg in args:
            print_func_sig(arg, getattr(module, arg))

    else:
        if j == 0:
            for name, func in inspect.getmembers(module, inspect.isfunction):
                print_func_sig(name, func)

        elif j == 1:
            f = getattr(module, args[0])
            print f()

        else:
            slownik = dict()
            for i in args[1:]:
                k, v = i.split('=', 1)
                slownik[k] = v
            f = getattr(module, args[0])
            print f(**slownik)

if __name__ == '__main__':
    run_cli(sys.modules[__name__])

