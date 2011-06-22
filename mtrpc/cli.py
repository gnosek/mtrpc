import inspect
import sys
from optparse import OptionParser


def run_cli(module, opt=None):
	if opt is None:
		opt=sys.argv

	parser=OptionParser()
	parser.add_option("-i", "--info", action="store_true", dest="info")
	(options, args) = parser.parse_args(opt[1:])
	j = len(args)

	if options.info and j == 1:
		A = inspect.getargspec(getattr(module, args[0]))
		print 'lista argumentow funkcji:'
		for i in A[0]:
			print i					#wyswietlanie argumentow funkcji

	else:
		if j == 0:
			print "Modul %s posiada funkcje: " % module.__name__
			for name, func in inspect.getmembers(module, inspect.isfunction):
				A = inspect.getargspec(func)
				print "\t%s(%s)" % (name, ', '.join(A[0]))

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
