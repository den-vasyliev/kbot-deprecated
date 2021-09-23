# commandserver.py - communicate with Mercurial's API over a pipe
#
#  Copyright Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import errno
import gc
import os
import random
import signal
import socket
import struct
import traceback

try:
    import selectors
    selectors.BaseSelector
except ImportError:
    from .thirdparty import selectors2 as selectors

from .i18n import _
from . import (
    encoding,
    error,
    util,
)
from .utils import (
    procutil,
)

logfile = None

def log(*args):
    if not logfile:
        return

    for a in args:
        logfile.write(str(a))

    logfile.flush()

class channeledoutput(object):
    """
    Write data to out in the following format:

    data length (unsigned int),
    data
    """
    def __init__(self, out, channel):
        self.out = out
        self.channel = channel

    @property
    def name(self):
        return '<%c-channel>' % self.channel

    def write(self, data):
        if not data:
            return
        # single write() to guarantee the same atomicity as the underlying file
        self.out.write(struct.pack('>cI', self.channel, len(data)) + data)
        self.out.flush()

    def __getattr__(self, attr):
        if attr in (r'isatty', r'fileno', r'tell', r'seek'):
            raise AttributeError(attr)
        return getattr(self.out, attr)

class channeledinput(object):
    """
    Read data from in_.

    Requests for input are written to out in the following format:
    channel identifier - 'I' for plain input, 'L' line based (1 byte)
    how many bytes to send at most (unsigned int),

    The client replies with:
    data length (unsigned int), 0 meaning EOF
    data
    """

    maxchunksize = 4 * 1024

    def __init__(self, in_, out, channel):
        self.in_ = in_
        self.out = out
        self.channel = channel

    @property
    def name(self):
        return '<%c-channel>' % self.channel

    def read(self, size=-1):
        if size < 0:
            # if we need to consume all the clients input, ask for 4k chunks
            # so the pipe doesn't fill up risking a deadlock
            size = self.maxchunksize
            s = self._read(size, self.channel)
            buf = s
            while s:
                s = self._read(size, self.channel)
                buf += s

            return buf
        else:
            return self._read(size, self.channel)

    def _read(self, size, channel):
        if not size:
            return ''
        assert size > 0

        # tell the client we need at most size bytes
        self.out.write(struct.pack('>cI', channel, size))
        self.out.flush()

        length = self.in_.read(4)
        length = struct.unpack('>I', length)[0]
        if not length:
            return ''
        else:
            return self.in_.read(length)

    def readline(self, size=-1):
        if size < 0:
            size = self.maxchunksize
            s = self._read(size, 'L')
            buf = s
            # keep asking for more until there's either no more or
            # we got a full line
            while s and s[-1] != '\n':
                s = self._read(size, 'L')
                buf += s

            return buf
        else:
            return self._read(size, 'L')

    def __iter__(self):
        return self

    def next(self):
        l = self.readline()
        if not l:
            raise StopIteration
        return l

    __next__ = next

    def __getattr__(self, attr):
        if attr in (r'isatty', r'fileno', r'tell', r'seek'):
            raise AttributeError(attr)
        return getattr(self.in_, attr)

class server(object):
    """
    Listens for commands on fin, runs them and writes the output on a channel
    based stream to fout.
    """
    def __init__(self, ui, repo, fin, fout):
        self.cwd = encoding.getcwd()

        # developer config: cmdserver.log
        logpath = ui.config("cmdserver", "log")
        if logpath:
            global logfile
            if logpath == '-':
                # write log on a special 'd' (debug) channel
                logfile = channeledoutput(fout, 'd')
            else:
                logfile = open(logpath, 'a')

        if repo:
            # the ui here is really the repo ui so take its baseui so we don't
            # end up with its local configuration
            self.ui = repo.baseui
            self.repo = repo
            self.repoui = repo.ui
        else:
            self.ui = ui
            self.repo = self.repoui = None

        self.cerr = channeledoutput(fout, 'e')
        self.cout = channeledoutput(fout, 'o')
        self.cin = channeledinput(fin, fout, 'I')
        self.cresult = channeledoutput(fout, 'r')

        self.client = fin

    def cleanup(self):
        """release and restore resources taken during server session"""

    def _read(self, size):
        if not size:
            return ''

        data = self.client.read(size)

        # is the other end closed?
        if not data:
            raise EOFError

        return data

    def _readstr(self):
        """read a string from the channel

        format:
        data length (uint32), data
        """
        length = struct.unpack('>I', self._read(4))[0]
        if not length:
            return ''
        return self._read(length)

    def _readlist(self):
        """read a list of NULL separated strings from the channel"""
        s = self._readstr()
        if s:
            return s.split('\0')
        else:
            return []

    def runcommand(self):
        """ reads a list of \0 terminated arguments, executes
        and writes the return code to the result channel """
        from . import dispatch  # avoid cycle

        args = self._readlist()

        # copy the uis so changes (e.g. --config or --verbose) don't
        # persist between requests
        copiedui = self.ui.copy()
        uis = [copiedui]
        if self.repo:
            self.repo.baseui = copiedui
            # clone ui without using ui.copy because this is protected
            repoui = self.repoui.__class__(self.repoui)
            repoui.copy = copiedui.copy # redo copy protection
            uis.append(repoui)
            self.repo.ui = self.repo.dirstate._ui = repoui
            self.repo.invalidateall()

        for ui in uis:
            ui.resetstate()
            # any kind of interaction must use server channels, but chg may
            # replace channels by fully functional tty files. so nontty is
            # enforced only if cin is a channel.
            if not util.safehasattr(self.cin, 'fileno'):
                ui.setconfig('ui', 'nontty', 'true', 'commandserver')

        req = dispatch.request(args[:], copiedui, self.repo, self.cin,
                               self.cout, self.cerr)

        try:
            ret = dispatch.dispatch(req) & 255
            self.cresult.write(struct.pack('>i', int(ret)))
        finally:
            # restore old cwd
            if '--cwd' in args:
                os.chdir(self.cwd)

    def getencoding(self):
        """ writes the current encoding to the result channel """
        self.cresult.write(encoding.encoding)

    def serveone(self):
        cmd = self.client.readline()[:-1]
        if cmd:
            handler = self.capabilities.get(cmd)
            if handler:
                handler(self)
            else:
                # clients are expected to check what commands are supported by
                # looking at the servers capabilities
                raise error.Abort(_('unknown command %s') % cmd)

        return cmd != ''

    capabilities = {'runcommand': runcommand,
                    'getencoding': getencoding}

    def serve(self):
        hellomsg = 'capabilities: ' + ' '.join(sorted(self.capabilities))
        hellomsg += '\n'
        hellomsg += 'encoding: ' + encoding.encoding
        hellomsg += '\n'
        hellomsg += 'pid: %d' % procutil.getpid()
        if util.safehasattr(os, 'getpgid'):
            hellomsg += '\n'
            hellomsg += 'pgid: %d' % os.getpgid(0)

        # write the hello msg in -one- chunk
        self.cout.write(hellomsg)

        try:
            while self.serveone():
                pass
        except EOFError:
            # we'll get here if the client disconnected while we were reading
            # its request
            return 1

        return 0

class pipeservice(object):
    def __init__(self, ui, repo, opts):
        self.ui = ui
        self.repo = repo

    def init(self):
        pass

    def run(self):
        ui = self.ui
        # redirect stdio to null device so that broken extensions or in-process
        # hooks will never cause corruption of channel protocol.
        with procutil.protectedstdio(ui.fin, ui.fout) as (fin, fout):
            try:
                sv = server(ui, self.repo, fin, fout)
                return sv.serve()
            finally:
                sv.cleanup()

def _initworkerprocess():
    # use a different process group from the master process, in order to:
    # 1. make the current process group no longer "orphaned" (because the
    #    parent of this process is in a different process group while
    #    remains in a same session)
    #    according to POSIX 2.2.2.52, orphaned process group will ignore
    #    terminal-generated stop signals like SIGTSTP (Ctrl+Z), which will
    #    cause trouble for things like ncurses.
    # 2. the client can use kill(-pgid, sig) to simulate terminal-generated
    #    SIGINT (Ctrl+C) and process-exit-generated SIGHUP. our child
    #    processes like ssh will be killed properly, without affecting
    #    unrelated processes.
    os.setpgid(0, 0)
    # change random state otherwise forked request handlers would have a
    # same state inherited from parent.
    random.seed()

def _serverequest(ui, repo, conn, createcmdserver):
    fin = conn.makefile(r'rb')
    fout = conn.makefile(r'wb')
    sv = None
    try:
        sv = createcmdserver(repo, conn, fin, fout)
        try:
            sv.serve()
        # handle exceptions that may be raised by command server. most of
        # known exceptions are caught by dispatch.
        except error.Abort as inst:
            ui.error(_('abort: %s\n') % inst)
        except IOError as inst:
            if inst.errno != errno.EPIPE:
                raise
        except KeyboardInterrupt:
            pass
        finally:
            sv.cleanup()
    except: # re-raises
        # also write traceback to error channel. otherwise client cannot
        # see it because it is written to server's stderr by default.
        if sv:
            cerr = sv.cerr
        else:
            cerr = channeledoutput(fout, 'e')
        cerr.write(encoding.strtolocal(traceback.format_exc()))
        raise
    finally:
        fin.close()
        try:
            fout.close()  # implicit flush() may cause another EPIPE
        except IOError as inst:
            if inst.errno != errno.EPIPE:
                raise

class unixservicehandler(object):
    """Set of pluggable operations for unix-mode services

    Almost all methods except for createcmdserver() are called in the main
    process. You can't pass mutable resource back from createcmdserver().
    """

    pollinterval = None

    def __init__(self, ui):
        self.ui = ui

    def bindsocket(self, sock, address):
        util.bindunixsocket(sock, address)
        sock.listen(socket.SOMAXCONN)
        self.ui.status(_('listening at %s\n') % address)
        self.ui.flush()  # avoid buffering of status message

    def unlinksocket(self, address):
        os.unlink(address)

    def shouldexit(self):
        """True if server should shut down; checked per pollinterval"""
        return False

    def newconnection(self):
        """Called when main process notices new connection"""

    def createcmdserver(self, repo, conn, fin, fout):
        """Create new command server instance; called in the process that
        serves for the current connection"""
        return server(self.ui, repo, fin, fout)

class unixforkingservice(object):
    """
    Listens on unix domain socket and forks server per connection
    """

    def __init__(self, ui, repo, opts, handler=None):
        self.ui = ui
        self.repo = repo
        self.address = opts['address']
        if not util.safehasattr(socket, 'AF_UNIX'):
            raise error.Abort(_('unsupported platform'))
        if not self.address:
            raise error.Abort(_('no socket path specified with --address'))
        self._servicehandler = handler or unixservicehandler(ui)
        self._sock = None
        self._oldsigchldhandler = None
        self._workerpids = set()  # updated by signal handler; do not iterate
        self._socketunlinked = None

    def init(self):
        self._sock = socket.socket(socket.AF_UNIX)
        self._servicehandler.bindsocket(self._sock, self.address)
        if util.safehasattr(procutil, 'unblocksignal'):
            procutil.unblocksignal(signal.SIGCHLD)
        o = signal.signal(signal.SIGCHLD, self._sigchldhandler)
        self._oldsigchldhandler = o
        self._socketunlinked = False

    def _unlinksocket(self):
        if not self._socketunlinked:
            self._servicehandler.unlinksocket(self.address)
            self._socketunlinked = True

    def _cleanup(self):
        signal.signal(signal.SIGCHLD, self._oldsigchldhandler)
        self._sock.close()
        self._unlinksocket()
        # don't kill child processes as they have active clients, just wait
        self._reapworkers(0)

    def run(self):
        try:
            self._mainloop()
        finally:
            self._cleanup()

    def _mainloop(self):
        exiting = False
        h = self._servicehandler
        selector = selectors.DefaultSelector()
        selector.register(self._sock, selectors.EVENT_READ)
        while True:
            if not exiting and h.shouldexit():
                # clients can no longer connect() to the domain socket, so
                # we stop queuing new requests.
                # for requests that are queued (connect()-ed, but haven't been
                # accept()-ed), handle them before exit. otherwise, clients
                # waiting for recv() will receive ECONNRESET.
                self._unlinksocket()
                exiting = True
            try:
                ready = selector.select(timeout=h.pollinterval)
            except OSError as inst:
                # selectors2 raises ETIMEDOUT if timeout exceeded while
                # handling signal interrupt. That's probably wrong, but
                # we can easily get around it.
                if inst.errno != errno.ETIMEDOUT:
                    raise
                ready = []
            if not ready:
                # only exit if we completed all queued requests
                if exiting:
                    break
                continue
            try:
                conn, _addr = self._sock.accept()
            except socket.error as inst:
                if inst.args[0] == errno.EINTR:
                    continue
                raise

            pid = os.fork()
            if pid:
                try:
                    self.ui.debug('forked worker process (pid=%d)\n' % pid)
                    self._workerpids.add(pid)
                    h.newconnection()
                finally:
                    conn.close()  # release handle in parent process
            else:
                try:
                    selector.close()
                    self._sock.close()
                    self._runworker(conn)
                    conn.close()
                    os._exit(0)
                except:  # never return, hence no re-raises
                    try:
                        self.ui.traceback(force=True)
                    finally:
                        os._exit(255)
        selector.close()

    def _sigchldhandler(self, signal, frame):
        self._reapworkers(os.WNOHANG)

    def _reapworkers(self, options):
        while self._workerpids:
            try:
                pid, _status = os.waitpid(-1, options)
            except OSError as inst:
                if inst.errno == errno.EINTR:
                    continue
                if inst.errno != errno.ECHILD:
                    raise
                # no child processes at all (reaped by other waitpid()?)
                self._workerpids.clear()
                return
            if pid == 0:
                # no waitable child processes
                return
            self.ui.debug('worker process exited (pid=%d)\n' % pid)
            self._workerpids.discard(pid)

    def _runworker(self, conn):
        signal.signal(signal.SIGCHLD, self._oldsigchldhandler)
        _initworkerprocess()
        h = self._servicehandler
        try:
            _serverequest(self.ui, self.repo, conn, h.createcmdserver)
        finally:
            gc.collect()  # trigger __del__ since worker process uses os._exit
