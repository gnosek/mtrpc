#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from mtrpc.client import MTRPCProxy


# FIXME: raczej do wyrzucenia stad:
# przykład użycia pluginu do konfigurowania forwardów maildropa
#
#with MTRPCProxy(
#        'rpc.friendly.exchange',
#        'rk.usr.{full_name}',
#        loglevel='debug',
#        host="localhost:5672",
#        userid="guest",
#        password="guest"
#) as rpc:
#                    
#    print rpc.netservices.conf_maildrop_forwards(
#            # (to warto dostosować do zawartości swojego katalogu domowego...)
#            account='zuo',
#            domain='',
#            localpart='',
#            forwards=['zuo', 'zuo@chopin', 'bububu@pl'],
#            keep=False,
#    )
#
#
#print '\n' * 7


# trochę przykładów z liberalną polityką dostępu
# (vide ustawienia w pliku konf. dot. exchange 'rpc.friendly.exchange')

with MTRPCProxy('rpc.friendly.exchange',
                'rk.usr.{full_name}',
                loglevel='debug',
                host="localhost:5672",
                userid="guest",
                password="guest") as rpc:

    # wywołanie próbnych metod
    print rpc.example.proba('No i?')
    print rpc.example.proba2('No i?')
    print '----------------------------'

    # próba wywołania nieistniejącej metody
    try:
        print rpc.example.nic.takiego.nie_istnieje('b')
    except Exception as exc:
        print exc
    print '----------------------------'

    # listujemy moduł najwyższego poziomu (root), wynik w postaci listy
    print rpc.system.list('')
    print '----------------------------'

    # listujemy moduł system, wynik w postaci napisu
    print rpc.system.list_string('system')
    print '----------------------------'

    # wypisujemy help dla modułu system, wynik w postaci listy napisów
    print rpc.system.help('system')
    print '----------------------------'

    # wypisujemy help dla metody system.help, wynik w postaci napisu
    print rpc.system.help_string('system.help')
    print '----------------------------'

    # wypisujemy help dla wszystkiego, wynik w postaci napisu
    print rpc.system.help_string('', deep=True)


print '\n' * 7


# trochę przykładów z nieco mniej liberalną polityką dostępu
# (vide ustawienia w pliku konf. dot. exchange 'rpc.systemonly.exchange')

with MTRPCProxy('rpc.systemonly.exchange',
                'rk.usr.{full_name}',
                loglevel='info',
                host="localhost:5672",
                userid="guest",
                password="guest") as rpc:

    # tutaj moduł 'example' jest niedostępny
    try: print rpc.example.proba('No i?')
    except Exception as exc:
        print exc
    try: print rpc.example.proba2('No i?')
    except Exception as exc:
        print exc
    print '----------------------------'

    # próba wywołania nieistniejącej metody
    try:
        print rpc.example.nic.takiego.nie_istnieje('b')
    except Exception as exc:
        print exc
    print '----------------------------'

    # listujemy moduł najwyższego poziomu (root), wynik w postaci listy
    # (zauważmy: tym razem nie widać 'example')
    print rpc.system.list('')
    print '----------------------------'

    # listujemy moduł system, wynik w postaci napisu
    print rpc.system.list_string('system')
    print '----------------------------'

    # wypisujemy help dla modułu system, wynik w postaci listy napisów
    print rpc.system.help('system')
    print '----------------------------'

    # wypisujemy help dla metody system.help, wynik w postaci napisu
    print rpc.system.help_string('system.help')
    print '----------------------------'

    # wypisujemy help dla wszystkiego, wynik w postaci napisu
    # (zauważmy: tym razem nie widać 'example.*')
    print rpc.system.help_string('', deep=True)
