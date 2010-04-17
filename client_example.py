#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import logging
import traceback

from mtrpc.client import MTRPCProxy


with MTRPCProxy('rpc.friendly.exchange',
                'rk.usr.{full_name}',
                loglevel='debug',
                host="localhost:5672",
                userid="guest",
                password="guest") as rpc:
                    
    print rpc.netservices.conf_maildrop_forwards(
            account='zuo',
            domain='',
            localpart='',
            forwards=['zuo', 'zuo@chopin', 'bububu@pl'],
            keep=False,
    )

#~ with MTRPCProxy('rpc.friendly.exchange',
                #~ 'rk.usr.{full_name}',
                #~ loglevel='debug',
                #~ host="localhost:5672",
                #~ userid="guest",
                #~ password="guest") as rpc:
#~ 
    #~ # wywołanie próbnej metody
    #~ print rpc.example.proba('No i?')
    #~ print '----------------------------'
#~ 
    #~ # próba wywołania nieistniejącej metody
    #~ try:
        #~ print rpc.example.nic.takiego.nie_istnieje('b')
    #~ except Exception as exc:
        #~ print exc
    #~ print '----------------------------'
#~ 
    #~ # listujemy moduły najwyższego poziomu
    #~ print rpc.system.list('')
    #~ print '----------------------------'
#~ 
    #~ # listujemy metody system.*
    #~ print rpc.system.list('system', as_string=True)
    #~ print '----------------------------'
#~ 
    #~ # wypisujemy help dla wszystkiego
    #~ print rpc.system.help('', deep=True, as_string=True)
#~ 
#~ print '\n' * 7
#~ 
#~ with MTRPCProxy('rpc.systemonly.exchange',
                #~ 'rk.usr.{full_name}',
                #~ loglevel='info',
                #~ host="localhost:5672",
                #~ userid="guest",
                #~ password="guest") as rpc:
#~ 
    #~ # tutaj metoda jest niedostępna
    #~ try:
        #~ print rpc.example.proba('No i?')
    #~ except Exception as exc:
        #~ print exc
    #~ print '----------------------------'
#~ 
    #~ # próba wywołania nieistniejącej metody
    #~ try:
        #~ print rpc.example.nic.takiego.nie_istnieje('b')
    #~ except Exception as exc:
        #~ print exc
    #~ print '----------------------------'
#~ 
    #~ # listujemy moduły najwyższego poziomu (nie widać 'example')
    #~ print rpc.system.list('')
    #~ print '----------------------------'
#~ 
    #~ # listujemy metody system.*
    #~ print rpc.system.list('system', as_string=True)
    #~ print '----------------------------'
#~ 
    #~ # wypisujemy help dla wszystkiego (nie widać 'example.*')
    #~ print rpc.system.help('', deep=True, as_string=True)
