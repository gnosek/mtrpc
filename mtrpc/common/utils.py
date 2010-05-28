# mtrpc/common/utils.py
#
# Author: Jan Kaliszewski (zuo)
# Copyright (c) 2010, MegiTeam

"""MTRPC common utility classes and functions"""



from future_builtins import filter, map, zip

import errno
import fcntl
import grp
import hashlib
import logging
import os
import os.path
import pwd
import shutil
import string
import tempfile
import time
from cStringIO import StringIO

from .const import *
from .errors import raise_exc



#
# Config file manager class
#

class ConfigFileManager(object):

    """A tool for possibly atomic + md5-digest-controlled file modifying"""


    # filename-related constants
    FLOCK_PREFIX = '.flock--'
    FLOCK_SUFFIX = '--flock'
    TMP_SUBDIR_PREFIX = '.tmp-mtrpc-cfmanager-'

    # legal characters in path parts (important for security!)
    DEFAULT_PATH_FIELD_CHARS = string.ascii_letters + string.digits + '._-'


    class ModifiedManually(Exception):
        "Cannot write -- file has been modified manually (digest test failed)"


    def __init__(self, path_pattern, path_fields, user_name, group_name=None,
                 mode=0o600, flocking=30, comment_indicator='#',
                 digest_line_start_pattern='{comment_indicator} md5:',
                 path_field_chars=DEFAULT_PATH_FIELD_CHARS,
                 override_manual=False):

        """Initialize manager: acquire flock, read content, check digest etc.

        Arguments:

        * path_pattern (str) -- file path pattern (see: path_fields);

        * path_fields (dict) -- keyword args for path_pattern.format() to
          make the actual file path;

        * user_name (str) -- name of the unix user owning the config file;

        * group_name (str) -- name of the unix group owning the config file;
          if None (default) the main group of the user specified with
          user_name will be used;

        * mode (int) -- desired config file access mode (default: 0o600);

        * flocking (int | float | NoneType) -- timeout of acquiring the
          config file's advisory lock (in seconds, default: 30); if None,
          no flock will be aquired and the flock file will not be created
          (if it does not exist yet);

          the flock file name is based on the managed file name -- enclosed
          with the FLOCK_PREFIX and FLOCK_SUFFIX class attributes;

          the flock file is not being removed when the flock is released;

          please note that flock is an advisory lock -- other applications
          and scripts are not forced to be concerned about it; to use it,
          they must acquire/release it explicitly (e.g. with 'flock' system
          call or 'flock' linux command; see 'flock' linux manpages 1 and 2);

        * comment_indicator (str) -- keyword argument value for
          digest_line_start_pattern.format() (default: '#');
          may be None to disable digest support;  [!TODO! moze lepiej w tym celu digest_line_start_pattern=None???]

        * digest_line_start_pattern (str) -- pattern of the beginning of
          the (last) line including file's md5-hexdigest (default:
          '{comment_indicator} md5:');

        * path_field_chars (str) -- legal characters in path parts, important
          for security (default: ConfigFileManager.DEFAULT_PATH_FIELD_CHARS);

        * override_manual (bool) -- override manual modifications (don't raise
          ModifiedManually exception (default: False).

        """

        # modifying-related attributes
        self._writer_io = StringIO()
        self._to_clear = False

        # security check: whether all characters in path fields are legal
        _path_field_chars = set(path_field_chars)
        illegal_path_fields = ['{0}={1!r}'.format(name, val)
                               for name, val in path_fields.iteritems()
                               if not _path_field_chars.issuperset(val)]
        if illegal_path_fields:
            raise_exc(ValueError, "Illegal path fields: {0}"
                                  .format(', '.join(illegal_path_fields)))

        # path checks
        self.file_path = path_pattern.format(**path_fields)
        self.dir_path, self.file_name = os.path.split(self.file_path)
        if not os.path.isdir(self.dir_path):
            exc_msg = "Config directory does not exist"
            exc = IOError(exc_msg)
            exc.strerror = exc_msg
            exc.errno = errno.ENOENT   # ("no such file or directory")
            raise_exc(exc)

        # user/group/mode settings
        user_info = pwd.getpwnam(user_name)
        self.uid = user_info.pw_uid
        if group_name is not None:
            self.gid = grp.getgrnam(group_name).gr_gid
        else:
            self.gid = user_info.pw_gid
        self.mode = mode

        # to flock or not to flock...
        if flocking is None:
            self._flock_file = None
        else:
            self._flock_file = self.acquire_flock(self.dir_path,
                                                  self.file_name,
                                                  timeout=flocking)
        # digest line start pattern...
        if comment_indicator is not None:
            self.digest_line_start = digest_line_start_pattern.format(
                                          comment_indicator=comment_indicator)
            self.use_digest = True
        else:
            self.use_digest = False

        # ignore manual modifications?
        self._override_manual = override_manual

        # determine existing content
        # + whether it has been modified manually (md5 digest test)
        try:
            with open(self.file_path) as existing_file:
                self._existing_lines = [line.rstrip('\n')
                                        for line in existing_file]
        except IOError as exc:
            if exc.errno == errno.ENOENT:   # ("no such file or directory")
                self._existing_lines = []
                self._existing_content = ''
                self._modified_manually = False
            else:
                raise
        else:
            if comment_indicator is None:
                self._existing_content = '\n'.join(self._existing_lines + [''])
                self._modified_manually = False
            elif (self._existing_lines and self._existing_lines[-1].startswith(
                                                     self.digest_line_start)):
                (declared_digest
                ) = self._existing_lines.pop()[len(self.digest_line_start):]
                self._existing_content = '\n'.join(self._existing_lines + [''])
                hash_obj = hashlib.md5()
                hash_obj.update(self._existing_content)
                if declared_digest == hash_obj.hexdigest():
                    self._modified_manually = False
                else:
                    self._modified_manually = True
            else:
                self._existing_content = '\n'.join(self._existing_lines + [''])
                self._modified_manually = True


    @staticmethod
    def acquire_flock(dir_path, locked_file_name, timeout):
        "Acquire the file lock"
        flock_file_name = ''.join([ConfigFileManager.FLOCK_PREFIX,
                                   locked_file_name,
                                   ConfigFileManager.FLOCK_SUFFIX])
        flock_file_path = os.path.join(dir_path, flock_file_name)
        lock_file = open(flock_file_path, 'w')
        for i in xrange(int(timeout * 10) + 1):
            try:
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except IOError as exc:
                if exc.errno not in (errno.EACCES, errno.EAGAIN):
                    raise
            else:
                break
            time.sleep(0.1)
        else:
            exc_msg = ("Timeout {0}s exceeded while trying to "
                       "acquire the file lock".format(timeout))
            exc = IOError(exc_msg)
            exc.strerror = exc_msg
            exc.errno = errno.EAGAIN   # ("try again")
            raise_exc(exc)
        return lock_file


    @property
    def existing_content(self):
        "File content as a string -- *without* md5-hexdigest line"
        return self._existing_content


    @property
    def existing_lines(self):
        "File content as a list -- *including* md5-hexdigest line if present"
        return self._existing_lines


    @property
    def modified_manually(self):
        "Has the file been modified manually? (digest-test failed?)"
        return self._modified_manually


    @property
    def writer(self):
        '''A file-like object -- use it to append data to the file
        (only if not self.modified_manually)'''
        if self._modified_manually and not self._override_manual:
            raise self.ModifiedManually("Config file has been modified "
                                        "manually, cannot change it")
        else:
            return self._writer_io


    def clear(self):
        "Clear the file content (only if not self.modified_manually)"
        if self._modified_manually and not self._override_manual:
            raise self.ModifiedManually("Config file has been modified "
                                        "manually, cannot change it")
        else:
            self._to_clear = True
            self._writer_io.close()
            self._writer_io = StringIO()


    def finalize(self):
        "Release the file lock etc."
        try:
            self._writer_io.close()
        finally:
            if self._flock_file is not None:
                self._flock_file.close()


    def commit(self):

        "Confirm all changes (write them actually into the file)"

        to_write = self._writer_io.getvalue()
        if not (to_write or self._to_clear):
            return   # without any changes

        content = (to_write if self._to_clear
                   else ''.join([self._existing_content, to_write]))
        if not content.endswith('\n'):
            content = ''.join([content, '\n'])

        if self.use_digest:
            hash_obj = hashlib.md5()
            hash_obj.update(content)
            new_digest = hash_obj.hexdigest()

        tmp_subdir = tempfile.mkdtemp(dir=self.dir_path,
                                      prefix=self.TMP_SUBDIR_PREFIX)
        try:
            tmp_file_path = os.path.join(tmp_subdir, self.file_name)
            with open(tmp_file_path, 'w') as tmp_file:
                tmp_file.write(content)
                if self.use_digest:
                    tmp_file.write(self.digest_line_start)
                    tmp_file.write(new_digest)
            os.chown(tmp_file_path, self.uid, self.gid)
            os.chmod(tmp_file_path, self.mode)
            os.rename(tmp_file_path, self.file_path)
        finally:
            shutil.rmtree(tmp_subdir)


    def __enter__(self):
        "Context-manager implementation ('with' statement...)"
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        "Context-manager implementation ('with' statement...)"
        if exc_tb is None:
            # no error
            try:
                self.commit()
            finally:
                self.finalize()
        else:
            # error
            self.finalize()



#
# Standard module post-init callable
#

def basic_postinit(mod, full_name, logging_settings, mod_globals):

    """Initialize the module logger and add custom globals

    Arguments:
    
    * mod -- the Python module object;
    
    * full_name -- an absolute dot.separated.name;
    
    * logging_settings -- a dict with all or some of the keys:
      'mod_logger_pattern', 'level', 'handlers', 'propagate',
      'custom_mod_loggers' (see: the fragment of mtrpc.server
      documentation about configuration file structure and content);
      
    * mod_globals -- a dict of variables to be set as module globals.

    """

    # configure the module logger
    log_config = logging_settings.copy()
    log_config.update(log_config.get('custom_mod_loggers', {})
                      .get(full_name, {}))
    log_name = log_config.get('mod_logger')
    if log_name is None:
        log_name = (log_config.get('mod_logger_pattern', '')
                                   .format(full_name=full_name))
    new_log = logging.getLogger(log_name)
    prev_log = getattr(mod, RPC_LOG, None)
    if prev_log is None:
        log_handlers = []
        setattr(mod, RPC_LOG_HANDLERS, log_handlers)
    else:
        log_handlers = getattr(mod, RPC_LOG_HANDLERS)
    configure_logging(new_log, prev_log, log_handlers, log_config)
    setattr(mod, RPC_LOG, new_log)

    # set custom module globals
    mod.__dict__.update(mod_globals.get(full_name, {}))



#
# Other functions
#

def configure_logging(log, prev_log, log_handlers, log_config):

    """Configure logging for a particular logger, using given settings.

    Arguments:
    
    * log (logging.Logger instance) -- a new logger (to configure);
    
    * prev_log (logging.Logger instance) -- the previous logger;
    
    * log_handlers -- auxiliary list of logger handlers being in use;
    
    * log_config -- a dict with all or some of the keys: 'level' (str),
      'handlers' (dict), 'propagate' (bool); see: the fragment of
      mtrpc.server documentation about configuration file structure
      and content.

    """

    while log_handlers:  # when restarting -- disable old handlers
        prev_log.removeHandler(log_handlers.pop())

    # set some attributes of the new logger
    log.propagate = log_config.get('propagate', False)
    level = log_config.get('level', 'info').upper()
    log.setLevel(getattr(logging, level))

    # configure logger handlers
    default_hprops = DEFAULT_LOG_HANDLER_SETTINGS
    for handler_props in log_config['handlers']:
        class_name = handler_props['cls']
        try:
            HandlerClass = getattr(logging, class_name)
        except AttributeError:
            HandlerClass = getattr(logging.handlers, class_name)

        kwargs = handler_props.get('kwargs', default_hprops['kwargs'])
        level = handler_props.get('level', default_hprops['level']).upper()
        format = handler_props.get('format', default_hprops['format'])

        handler = HandlerClass(**kwargs_to_str(kwargs))
        handler.setLevel(getattr(logging, level))
        handler.setFormatter(logging.Formatter(format))

        log_handlers.append(handler)
        log.addHandler(handler)

    log.debug('Logger %s configured', log.name)


# to overcome a problem fixed in Python 2.6.5; see Python issue tracker: #4978
def kwargs_to_str(kwargs):
    """Create a new dict from a dict, replacung unicode-keys with str-keys"""
    return dict((str(key), value) for key, value in kwargs.iteritems())
