# -*- encoding: utf-8 -*-

import re  # <- tak dla hecy i pokazania, że da się wykorzystywać
           # istniejące pythonowe moduły bez ingerowania w nie
           # (z tym, że nie zadziała to dla funkcji nie poddających
           # się inspect.getargspec() -- a więc np. funkcji wbudowanych)



__rpc_doc__ = u'Próbno-przykładowy moduł RPC'
__rpc_methods__ = '*', 're.*'



def proba(s):
    u"Próbna metoda... Zażóć gęślą jaźń!"
    return u"{0} {0} {0} Zażóć gęślą jaźń!".format(s)


def proba2(x):
    u"Druga próbna metoda... Również zażóć gęślą jaźń!"
    return [x, u"Również zażóć gęślą jaźń", {'po co?': 'bo tak'}]
