# http://code.activestate.com/recipes/278731-creating-a-daemon-the-python-way/

__author__ = "Chad J. Schroeder"
__copyright__ = "Copyright (C) 2005 Chad J. Schroeder"

__revision__ = "$Id$"
__version__ = "0.2"

import os
import sys


def daemonize(umask=0, workdir='/'):
    pid = os.fork()

    if pid == 0:
        # noinspection PyArgumentList
        os.setsid()

        pid = os.fork()
        if pid == 0:
            os.chdir(workdir)
            os.umask(umask)
        else:
            os._exit(0)
    else:
        os._exit(0)

    os.close(0)
    os.open(os.devnull, os.O_RDWR)

    os.dup2(0, 1)
    os.dup2(0, 2)

    return 0


if __name__ == "__main__":
    retCode = daemonize()

    procParams = """
   return code = %s
   process ID = %s
   parent process ID = %s
   process group ID = %s
   session ID = %s
   user ID = %s
   effective user ID = %s
   real group ID = %s
   effective group ID = %s
   """ % (retCode, os.getpid(), os.getppid(), os.getpgrp(), os.getsid(0),
          os.getuid(), os.geteuid(), os.getgid(), os.getegid())

    open("createDaemon.log", "w").write(procParams + "\n")

    sys.exit(retCode)
