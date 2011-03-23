from mtrpc.common.errors import raise_exc

__rpc_doc__ = u'Another very sophisticated RPC module'
__rpc_methods__ = 'mul', 'div'


def mul(x, y):
    u"Multiply one argument by the other"
    return x * y

def div(x, y):
    u"Divide one argument by the other"
    try:
        return float(x) / y
    except ZeroDivisionError as exc:
        # to indicate that the exception can be safely sent (is not
        # unexpected and its message doesn't compromise any secret)
        # we raise it explicitly using mtrpc.common.errors.raise_exc()
        raise_exc(exc)
