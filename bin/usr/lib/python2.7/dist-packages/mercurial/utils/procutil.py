# procutil.py - utility for managing processes and executable environment
#
#  Copyright 2005 K. Thananchayan <thananck@yahoo.com>
#  Copyright 2005-2007 Matt Mackall <mpm@selenic.com>
#  Copyright 2006 Vadim Gelfer <vadim.gelfer@gmail.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import contextlib
import imp
import io
import os
import signal
import subprocess
import sys
import time

from ..i18n import _

from .. import (
    encoding,
    error,
    policy,
    pycompat,
)

osutil = policy.importmod(r'osutil')

stderr = pycompat.stderr
stdin = pycompat.stdin
stdout = pycompat.stdout

def isatty(fp):
    try:
        return fp.isatty()
    except AttributeError:
        return False

# glibc determines buffering on first write to stdout - if we replace a TTY
# destined stdout with a pipe destined stdout (e.g. pager), we want line
# buffering (or unbuffered, on Windows)
if isatty(stdout):
    if pycompat.iswindows:
        # Windows doesn't support line buffering
        stdout = os.fdopen(stdout.fileno(), r'wb', 0)
    else:
        stdout = os.fdopen(stdout.fileno(), r'wb', 1)

if pycompat.iswindows:
    from .. import windows as platform
    stdout = platform.winstdout(stdout)
else:
    from .. import posix as platform

findexe = platform.findexe
_gethgcmd = platform.gethgcmd
getuser = platform.getuser
getpid = os.getpid
hidewindow = platform.hidewindow
quotecommand = platform.quotecommand
readpipe = platform.readpipe
setbinary = platform.setbinary
setsignalhandler = platform.setsignalhandler
shellquote = platform.shellquote
shellsplit = platform.shellsplit
spawndetached = platform.spawndetached
sshargs = platform.sshargs
testpid = platform.testpid

try:
    setprocname = osutil.setprocname
except AttributeError:
    pass
try:
    unblocksignal = osutil.unblocksignal
except AttributeError:
    pass

closefds = pycompat.isposix

def explainexit(code):
    """return a message describing a subprocess status
    (codes from kill are negative - not os.system/wait encoding)"""
    if code >= 0:
        return _("exited with status %d") % code
    return _("killed by signal %d") % -code

class _pfile(object):
    """File-like wrapper for a stream opened by subprocess.Popen()"""

    def __init__(self, proc, fp):
        self._proc = proc
        self._fp = fp

    def close(self):
        # unlike os.popen(), this returns an integer in subprocess coding
        self._fp.close()
        return self._proc.wait()

    def __iter__(self):
        return iter(self._fp)

    def __getattr__(self, attr):
        return getattr(self._fp, attr)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.close()

def popen(cmd, mode='rb', bufsize=-1):
    if mode == 'rb':
        return _popenreader(cmd, bufsize)
    elif mode == 'wb':
        return _popenwriter(cmd, bufsize)
    raise error.ProgrammingError('unsupported mode: %r' % mode)

def _popenreader(cmd, bufsize):
    p = subprocess.Popen(tonativestr(quotecommand(cmd)),
                         shell=True, bufsize=bufsize,
                         close_fds=closefds,
                         stdout=subprocess.PIPE)
    return _pfile(p, p.stdout)

def _popenwriter(cmd, bufsize):
    p = subprocess.Popen(tonativestr(quotecommand(cmd)),
                         shell=True, bufsize=bufsize,
                         close_fds=closefds,
                         stdin=subprocess.PIPE)
    return _pfile(p, p.stdin)

def popen2(cmd, env=None):
    # Setting bufsize to -1 lets the system decide the buffer size.
    # The default for bufsize is 0, meaning unbuffered. This leads to
    # poor performance on Mac OS X: http://bugs.python.org/issue4194
    p = subprocess.Popen(tonativestr(cmd),
                         shell=True, bufsize=-1,
                         close_fds=closefds,
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         env=tonativeenv(env))
    return p.stdin, p.stdout

def popen3(cmd, env=None):
    stdin, stdout, stderr, p = popen4(cmd, env)
    return stdin, stdout, stderr

def popen4(cmd, env=None, bufsize=-1):
    p = subprocess.Popen(tonativestr(cmd),
                         shell=True, bufsize=bufsize,
                         close_fds=closefds,
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         env=tonativeenv(env))
    return p.stdin, p.stdout, p.stderr, p

def pipefilter(s, cmd):
    '''filter string S through command CMD, returning its output'''
    p = subprocess.Popen(tonativestr(cmd),
                         shell=True, close_fds=closefds,
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    pout, perr = p.communicate(s)
    return pout

def tempfilter(s, cmd):
    '''filter string S through a pair of temporary files with CMD.
    CMD is used as a template to create the real command to be run,
    with the strings INFILE and OUTFILE replaced by the real names of
    the temporary files generated.'''
    inname, outname = None, None
    try:
        infd, inname = pycompat.mkstemp(prefix='hg-filter-in-')
        fp = os.fdopen(infd, r'wb')
        fp.write(s)
        fp.close()
        outfd, outname = pycompat.mkstemp(prefix='hg-filter-out-')
        os.close(outfd)
        cmd = cmd.replace('INFILE', inname)
        cmd = cmd.replace('OUTFILE', outname)
        code = system(cmd)
        if pycompat.sysplatform == 'OpenVMS' and code & 1:
            code = 0
        if code:
            raise error.Abort(_("command '%s' failed: %s") %
                              (cmd, explainexit(code)))
        with open(outname, 'rb') as fp:
            return fp.read()
    finally:
        try:
            if inname:
                os.unlink(inname)
        except OSError:
            pass
        try:
            if outname:
                os.unlink(outname)
        except OSError:
            pass

_filtertable = {
    'tempfile:': tempfilter,
    'pipe:': pipefilter,
}

def filter(s, cmd):
    "filter a string through a command that transforms its input to its output"
    for name, fn in _filtertable.iteritems():
        if cmd.startswith(name):
            return fn(s, cmd[len(name):].lstrip())
    return pipefilter(s, cmd)

def mainfrozen():
    """return True if we are a frozen executable.

    The code supports py2exe (most common, Windows only) and tools/freeze
    (portable, not much used).
    """
    return (pycompat.safehasattr(sys, "frozen") or # new py2exe
            pycompat.safehasattr(sys, "importers") or # old py2exe
            imp.is_frozen(u"__main__")) # tools/freeze

_hgexecutable = None

def hgexecutable():
    """return location of the 'hg' executable.

    Defaults to $HG or 'hg' in the search path.
    """
    if _hgexecutable is None:
        hg = encoding.environ.get('HG')
        mainmod = sys.modules[r'__main__']
        if hg:
            _sethgexecutable(hg)
        elif mainfrozen():
            if getattr(sys, 'frozen', None) == 'macosx_app':
                # Env variable set by py2app
                _sethgexecutable(encoding.environ['EXECUTABLEPATH'])
            else:
                _sethgexecutable(pycompat.sysexecutable)
        elif (os.path.basename(
            pycompat.fsencode(getattr(mainmod, '__file__', ''))) == 'hg'):
            _sethgexecutable(pycompat.fsencode(mainmod.__file__))
        else:
            exe = findexe('hg') or os.path.basename(sys.argv[0])
            _sethgexecutable(exe)
    return _hgexecutable

def _sethgexecutable(path):
    """set location of the 'hg' executable"""
    global _hgexecutable
    _hgexecutable = path

def _testfileno(f, stdf):
    fileno = getattr(f, 'fileno', None)
    try:
        return fileno and fileno() == stdf.fileno()
    except io.UnsupportedOperation:
        return False # fileno() raised UnsupportedOperation

def isstdin(f):
    return _testfileno(f, sys.__stdin__)

def isstdout(f):
    return _testfileno(f, sys.__stdout__)

def protectstdio(uin, uout):
    """Duplicate streams and redirect original if (uin, uout) are stdio

    If uin is stdin, it's redirected to /dev/null. If uout is stdout, it's
    redirected to stderr so the output is still readable.

    Returns (fin, fout) which point to the original (uin, uout) fds, but
    may be copy of (uin, uout). The returned streams can be considered
    "owned" in that print(), exec(), etc. never reach to them.
    """
    uout.flush()
    fin, fout = uin, uout
    if _testfileno(uin, stdin):
        newfd = os.dup(uin.fileno())
        nullfd = os.open(os.devnull, os.O_RDONLY)
        os.dup2(nullfd, uin.fileno())
        os.close(nullfd)
        fin = os.fdopen(newfd, r'rb')
    if _testfileno(uout, stdout):
        newfd = os.dup(uout.fileno())
        os.dup2(stderr.fileno(), uout.fileno())
        fout = os.fdopen(newfd, r'wb')
    return fin, fout

def restorestdio(uin, uout, fin, fout):
    """Restore (uin, uout) streams from possibly duplicated (fin, fout)"""
    uout.flush()
    for f, uif in [(fin, uin), (fout, uout)]:
        if f is not uif:
            os.dup2(f.fileno(), uif.fileno())
            f.close()

@contextlib.contextmanager
def protectedstdio(uin, uout):
    """Run code block with protected standard streams"""
    fin, fout = protectstdio(uin, uout)
    try:
        yield fin, fout
    finally:
        restorestdio(uin, uout, fin, fout)

def shellenviron(environ=None):
    """return environ with optional override, useful for shelling out"""
    def py2shell(val):
        'convert python object into string that is useful to shell'
        if val is None or val is False:
            return '0'
        if val is True:
            return '1'
        return pycompat.bytestr(val)
    env = dict(encoding.environ)
    if environ:
        env.update((k, py2shell(v)) for k, v in environ.iteritems())
    env['HG'] = hgexecutable()
    return env

if pycompat.iswindows:
    def shelltonative(cmd, env):
        return platform.shelltocmdexe(cmd, shellenviron(env))

    tonativestr = encoding.strfromlocal
else:
    def shelltonative(cmd, env):
        return cmd

    tonativestr = pycompat.identity

def tonativeenv(env):
    '''convert the environment from bytes to strings suitable for Popen(), etc.
    '''
    return pycompat.rapply(tonativestr, env)

def system(cmd, environ=None, cwd=None, out=None):
    '''enhanced shell command execution.
    run with environment maybe modified, maybe in different dir.

    if out is specified, it is assumed to be a file-like object that has a
    write() method. stdout and stderr will be redirected to out.'''
    try:
        stdout.flush()
    except Exception:
        pass
    cmd = quotecommand(cmd)
    env = shellenviron(environ)
    if out is None or isstdout(out):
        rc = subprocess.call(tonativestr(cmd),
                             shell=True, close_fds=closefds,
                             env=tonativeenv(env),
                             cwd=pycompat.rapply(tonativestr, cwd))
    else:
        proc = subprocess.Popen(tonativestr(cmd),
                                shell=True, close_fds=closefds,
                                env=tonativeenv(env),
                                cwd=pycompat.rapply(tonativestr, cwd),
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)
        for line in iter(proc.stdout.readline, ''):
            out.write(line)
        proc.wait()
        rc = proc.returncode
    if pycompat.sysplatform == 'OpenVMS' and rc & 1:
        rc = 0
    return rc

def gui():
    '''Are we running in a GUI?'''
    if pycompat.isdarwin:
        if 'SSH_CONNECTION' in encoding.environ:
            # handle SSH access to a box where the user is logged in
            return False
        elif getattr(osutil, 'isgui', None):
            # check if a CoreGraphics session is available
            return osutil.isgui()
        else:
            # pure build; use a safe default
            return True
    else:
        return pycompat.iswindows or encoding.environ.get("DISPLAY")

def hgcmd():
    """Return the command used to execute current hg

    This is different from hgexecutable() because on Windows we want
    to avoid things opening new shell windows like batch files, so we
    get either the python call or current executable.
    """
    if mainfrozen():
        if getattr(sys, 'frozen', None) == 'macosx_app':
            # Env variable set by py2app
            return [encoding.environ['EXECUTABLEPATH']]
        else:
            return [pycompat.sysexecutable]
    return _gethgcmd()

def rundetached(args, condfn):
    """Execute the argument list in a detached process.

    condfn is a callable which is called repeatedly and should return
    True once the child process is known to have started successfully.
    At this point, the child process PID is returned. If the child
    process fails to start or finishes before condfn() evaluates to
    True, return -1.
    """
    # Windows case is easier because the child process is either
    # successfully starting and validating the condition or exiting
    # on failure. We just poll on its PID. On Unix, if the child
    # process fails to start, it will be left in a zombie state until
    # the parent wait on it, which we cannot do since we expect a long
    # running process on success. Instead we listen for SIGCHLD telling
    # us our child process terminated.
    terminated = set()
    def handler(signum, frame):
        terminated.add(os.wait())
    prevhandler = None
    SIGCHLD = getattr(signal, 'SIGCHLD', None)
    if SIGCHLD is not None:
        prevhandler = signal.signal(SIGCHLD, handler)
    try:
        pid = spawndetached(args)
        while not condfn():
            if ((pid in terminated or not testpid(pid))
                and not condfn()):
                return -1
            time.sleep(0.1)
        return pid
    finally:
        if prevhandler is not None:
            signal.signal(signal.SIGCHLD, prevhandler)

@contextlib.contextmanager
def uninterruptable(warn):
    """Inhibit SIGINT handling on a region of code.

    Note that if this is called in a non-main thread, it turns into a no-op.

    Args:
      warn: A callable which takes no arguments, and returns True if the
            previous signal handling should be restored.
    """

    oldsiginthandler = [signal.getsignal(signal.SIGINT)]
    shouldbail = []

    def disabledsiginthandler(*args):
        if warn():
            signal.signal(signal.SIGINT, oldsiginthandler[0])
            del oldsiginthandler[0]
        shouldbail.append(True)

    try:
        try:
            signal.signal(signal.SIGINT, disabledsiginthandler)
        except ValueError:
            # wrong thread, oh well, we tried
            del oldsiginthandler[0]
        yield
    finally:
        if oldsiginthandler:
            signal.signal(signal.SIGINT, oldsiginthandler[0])
        if shouldbail:
            raise KeyboardInterrupt
