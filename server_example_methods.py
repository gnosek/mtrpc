# -*- encoding: utf-8 -*-

import itertools
import re  # <- tak dla hecy i pokazania, że da się wykorzystywać
           # istniejące pythonowe moduły bez ingerowania w nie
           # (z zastrzeżeniem, że nie zadziała to dla funkcji nie poddających
           # się inspect.getargspec() a więc funkcji wbudowanych)


__rpc_doc__ = u'Próbno-przykładowy moduł RPC'
__rpc_methods__ = '*', 're.*'



def proba(s):
    u"Próbna metoda... Zażóć gęślą jaźń!"

    return u" ".join([s, u"Zażóć gęślą jaźń"])
