#!/usr/local/python2.6/bin/python2.6
# -*- encoding: utf-8 -*-

import logging
import traceback
import sys
import json
from optparse import OptionParser

sys.path.insert(0, '/usr/local/megiteam/python2.6')

from mtrpc.client import MTRPCProxy

def decode_arg(arg):
    try:
        return json.loads(arg)
    except ValueError:
        return arg

parser = OptionParser(usage="usage: %prog [options] method args...")
parser.add_option('-x', '--exchange', dest='req_exchange', default='rpc.friendly.exchange', help='AMQP exchange name', metavar='EXCHANGE')
parser.add_option('-r', '--routing-key', dest='req_rk_pattern', default='rk.usr.{full_name}', help='AMQP routing key pattern', metavar='RK')
parser.add_option('-l', '--loglevel', dest='loglevel', default='WARNING', help='Log level', metavar='LEVEL')
parser.add_option('-H', '--host', dest='host', default='localhost:5672', help='AMQP broker')
parser.add_option('-u', '--user', dest='userid', default='guest', help='AMQP user login')
parser.add_option('-p', '--password', dest='password', default='guest', help='AMQP user password')
parser.add_option('-R', '--raw', dest='raw', action='store_true', help="Don't decode JSON in command line params")
parser.add_option('-j', '--json', dest='json', action='store_true', help="Dump response in JSON format")

(o, a) = parser.parse_args(sys.argv[1:])

if len(a) == 0:
    parser.print_help()
    sys.exit(1)

meth = a[0]

try:
    args = a[1:]
except:
    args = []

if not o.raw:
    args = [decode_arg(arg) for arg in args]

retcode = 0
with MTRPCProxy(**o.__dict__) as rpc:
    try:
        ret = getattr(rpc, meth)(*args)
    except:
        logging.getLogger().error('RPC call failed', exc_info=True)
        retcode = 1
    else:
        if o.json:
            print json.dumps(ret),
        else:
            print ret,

sys.exit(retcode)
