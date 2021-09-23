# vfs.py - Mercurial 'vfs' classes
#
#  Copyright Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.
from __future__ import absolute_import

import contextlib
import errno
import os
import shutil
import stat
import threading

from .i18n import _
from . import (
    encoding,
    error,
    pathutil,
    pycompat,
    util,
)

def _avoidambig(path, oldstat):
    """Avoid file stat ambiguity forcibly

    This function causes copying ``path`` file, if it is owned by
    another (see issue5418 and issue5584 for detail).
    """
    def checkandavoid():
        newstat = util.filestat.frompath(path)
        # return whether file stat ambiguity is (already) avoided
        return (not newstat.isambig(oldstat) or
                newstat.avoidambig(path, oldstat))
    if not checkandavoid():
        # simply copy to change owner of path to get privilege to
        # advance mtime (see issue5418)
        util.rename(util.mktempcopy(path), path)
        checkandavoid()

class abstractvfs(object):
    """Abstract base class; cannot be instantiated"""

    def __init__(self, *args, **kwargs):
        '''Prevent instantiation; don't call this from subclasses.'''
        raise NotImplementedError('attempted instantiating ' + str(type(self)))

    def tryread(self, path):
        '''gracefully return an empty string for missing files'''
        try:
            return self.read(path)
        except IOError as inst:
            if inst.errno != errno.ENOENT:
                raise
        return ""

    def tryreadlines(self, path, mode='rb'):
        '''gracefully return an empty array for missing files'''
        try:
            return self.readlines(path, mode=mode)
        except IOError as inst:
            if inst.errno != errno.ENOENT:
                raise
        return []

    @util.propertycache
    def open(self):
        '''Open ``path`` file, which is relative to vfs root.

        Newly created directories are marked as "not to be indexed by
        the content indexing service", if ``notindexed`` is specified
        for "write" mode access.
        '''
        return self.__call__

    def read(self, path):
        with self(path, 'rb') as fp:
            return fp.read()

    def readlines(self, path, mode='rb'):
        with self(path, mode=mode) as fp:
            return fp.readlines()

    def write(self, path, data, backgroundclose=False, **kwargs):
        with self(path, 'wb', backgroundclose=backgroundclose, **kwargs) as fp:
            return fp.write(data)

    def writelines(self, path, data, mode='wb', notindexed=False):
        with self(path, mode=mode, notindexed=notindexed) as fp:
            return fp.writelines(data)

    def append(self, path, data):
        with self(path, 'ab') as fp:
            return fp.write(data)

    def basename(self, path):
        """return base element of a path (as os.path.basename would do)

        This exists to allow handling of strange encoding if needed."""
        return os.path.basename(path)

    def chmod(self, path, mode):
        return os.chmod(self.join(path), mode)

    def dirname(self, path):
        """return dirname element of a path (as os.path.dirname would do)

        This exists to allow handling of strange encoding if needed."""
        return os.path.dirname(path)

    def exists(self, path=None):
        return os.path.exists(self.join(path))

    def fstat(self, fp):
        return util.fstat(fp)

    def isdir(self, path=None):
        return os.path.isdir(self.join(path))

    def isfile(self, path=None):
        return os.path.isfile(self.join(path))

    def islink(self, path=None):
        return os.path.islink(self.join(path))

    def isfileorlink(self, path=None):
        '''return whether path is a regular file or a symlink

        Unlike isfile, this doesn't follow symlinks.'''
        try:
            st = self.lstat(path)
        except OSError:
            return False
        mode = st.st_mode
        return stat.S_ISREG(mode) or stat.S_ISLNK(mode)

    def reljoin(self, *paths):
        """join various elements of a path together (as os.path.join would do)

        The vfs base is not injected so that path stay relative. This exists
        to allow handling of strange encoding if needed."""
        return os.path.join(*paths)

    def split(self, path):
        """split top-most element of a path (as os.path.split would do)

        This exists to allow handling of strange encoding if needed."""
        return os.path.split(path)

    def lexists(self, path=None):
        return os.path.lexists(self.join(path))

    def lstat(self, path=None):
        return os.lstat(self.join(path))

    def listdir(self, path=None):
        return os.listdir(self.join(path))

    def makedir(self, path=None, notindexed=True):
        return util.makedir(self.join(path), notindexed)

    def makedirs(self, path=None, mode=None):
        return util.makedirs(self.join(path), mode)

    def makelock(self, info, path):
        return util.makelock(info, self.join(path))

    def mkdir(self, path=None):
        return os.mkdir(self.join(path))

    def mkstemp(self, suffix='', prefix='tmp', dir=None):
        fd, name = pycompat.mkstemp(suffix=suffix, prefix=prefix,
                                    dir=self.join(dir))
        dname, fname = util.split(name)
        if dir:
            return fd, os.path.join(dir, fname)
        else:
            return fd, fname

    def readdir(self, path=None, stat=None, skip=None):
        return util.listdir(self.join(path), stat, skip)

    def readlock(self, path):
        return util.readlock(self.join(path))

    def rename(self, src, dst, checkambig=False):
        """Rename from src to dst

        checkambig argument is used with util.filestat, and is useful
        only if destination file is guarded by any lock
        (e.g. repo.lock or repo.wlock).

        To avoid file stat ambiguity forcibly, checkambig=True involves
        copying ``src`` file, if it is owned by another. Therefore, use
        checkambig=True only in limited cases (see also issue5418 and
        issue5584 for detail).
        """
        srcpath = self.join(src)
        dstpath = self.join(dst)
        oldstat = checkambig and util.filestat.frompath(dstpath)
        if oldstat and oldstat.stat:
            ret = util.rename(srcpath, dstpath)
            _avoidambig(dstpath, oldstat)
            return ret
        return util.rename(srcpath, dstpath)

    def readlink(self, path):
        return util.readlink(self.join(path))

    def removedirs(self, path=None):
        """Remove a leaf directory and all empty intermediate ones
        """
        return util.removedirs(self.join(path))

    def rmdir(self, path=None):
        """Remove an empty directory."""
        return os.rmdir(self.join(path))

    def rmtree(self, path=None, ignore_errors=False, forcibly=False):
        """Remove a directory tree recursively

        If ``forcibly``, this tries to remove READ-ONLY files, too.
        """
        if forcibly:
            def onerror(function, path, excinfo):
                if function is not os.remove:
                    raise
                # read-only files cannot be unlinked under Windows
                s = os.stat(path)
                if (s.st_mode & stat.S_IWRITE) != 0:
                    raise
                os.chmod(path, stat.S_IMODE(s.st_mode) | stat.S_IWRITE)
                os.remove(path)
        else:
            onerror = None
        return shutil.rmtree(self.join(path),
                             ignore_errors=ignore_errors, onerror=onerror)

    def setflags(self, path, l, x):
        return util.setflags(self.join(path), l, x)

    def stat(self, path=None):
        return os.stat(self.join(path))

    def unlink(self, path=None):
        return util.unlink(self.join(path))

    def tryunlink(self, path=None):
        """Attempt to remove a file, ignoring missing file errors."""
        util.tryunlink(self.join(path))

    def unlinkpath(self, path=None, ignoremissing=False, rmdir=True):
        return util.unlinkpath(self.join(path), ignoremissing=ignoremissing,
                               rmdir=rmdir)

    def utime(self, path=None, t=None):
        return os.utime(self.join(path), t)

    def walk(self, path=None, onerror=None):
        """Yield (dirpath, dirs, files) tuple for each directories under path

        ``dirpath`` is relative one from the root of this vfs. This
        uses ``os.sep`` as path separator, even you specify POSIX
        style ``path``.

        "The root of this vfs" is represented as empty ``dirpath``.
        """
        root = os.path.normpath(self.join(None))
        # when dirpath == root, dirpath[prefixlen:] becomes empty
        # because len(dirpath) < prefixlen.
        prefixlen = len(pathutil.normasprefix(root))
        for dirpath, dirs, files in os.walk(self.join(path), onerror=onerror):
            yield (dirpath[prefixlen:], dirs, files)

    @contextlib.contextmanager
    def backgroundclosing(self, ui, expectedcount=-1):
        """Allow files to be closed asynchronously.

        When this context manager is active, ``backgroundclose`` can be passed
        to ``__call__``/``open`` to result in the file possibly being closed
        asynchronously, on a background thread.
        """
        # Sharing backgroundfilecloser between threads is complex and using
        # multiple instances puts us at risk of running out of file descriptors
        # only allow to use backgroundfilecloser when in main thread.
        if not isinstance(threading.currentThread(), threading._MainThread):
            yield
            return
        vfs = getattr(self, 'vfs', self)
        if getattr(vfs, '_backgroundfilecloser', None):
            raise error.Abort(
                _('can only have 1 active background file closer'))

        with backgroundfilecloser(ui, expectedcount=expectedcount) as bfc:
            try:
                vfs._backgroundfilecloser = bfc
                yield bfc
            finally:
                vfs._backgroundfilecloser = None

class vfs(abstractvfs):
    '''Operate files relative to a base directory

    This class is used to hide the details of COW semantics and
    remote file access from higher level code.

    'cacheaudited' should be enabled only if (a) vfs object is short-lived, or
    (b) the base directory is managed by hg and considered sort-of append-only.
    See pathutil.pathauditor() for details.
    '''
    def __init__(self, base, audit=True, cacheaudited=False, expandpath=False,
                 realpath=False):
        if expandpath:
            base = util.expandpath(base)
        if realpath:
            base = os.path.realpath(base)
        self.base = base
        self._audit = audit
        if audit:
            self.audit = pathutil.pathauditor(self.base, cached=cacheaudited)
        else:
            self.audit = (lambda path, mode=None: True)
        self.createmode = None
        self._trustnlink = None

    @util.propertycache
    def _cansymlink(self):
        return util.checklink(self.base)

    @util.propertycache
    def _chmod(self):
        return util.checkexec(self.base)

    def _fixfilemode(self, name):
        if self.createmode is None or not self._chmod:
            return
        os.chmod(name, self.createmode & 0o666)

    def __call__(self, path, mode="r", atomictemp=False, notindexed=False,
                 backgroundclose=False, checkambig=False, auditpath=True):
        '''Open ``path`` file, which is relative to vfs root.

        Newly created directories are marked as "not to be indexed by
        the content indexing service", if ``notindexed`` is specified
        for "write" mode access.

        If ``backgroundclose`` is passed, the file may be closed asynchronously.
        It can only be used if the ``self.backgroundclosing()`` context manager
        is active. This should only be specified if the following criteria hold:

        1. There is a potential for writing thousands of files. Unless you
           are writing thousands of files, the performance benefits of
           asynchronously closing files is not realized.
        2. Files are opened exactly once for the ``backgroundclosing``
           active duration and are therefore free of race conditions between
           closing a file on a background thread and reopening it. (If the
           file were opened multiple times, there could be unflushed data
           because the original file handle hasn't been flushed/closed yet.)

        ``checkambig`` argument is passed to atomictemplfile (valid
        only for writing), and is useful only if target file is
        guarded by any lock (e.g. repo.lock or repo.wlock).

        To avoid file stat ambiguity forcibly, checkambig=True involves
        copying ``path`` file opened in "append" mode (e.g. for
        truncation), if it is owned by another. Therefore, use
        combination of append mode and checkambig=True only in limited
        cases (see also issue5418 and issue5584 for detail).
        '''
        if auditpath:
            if self._audit:
                r = util.checkosfilename(path)
                if r:
                    raise error.Abort("%s: %r" % (r, path))
            self.audit(path, mode=mode)
        f = self.join(path)

        if "b" not in mode:
            mode += "b" # for that other OS

        nlink = -1
        if mode not in ('r', 'rb'):
            dirname, basename = util.split(f)
            # If basename is empty, then the path is malformed because it points
            # to a directory. Let the posixfile() call below raise IOError.
            if basename:
                if atomictemp:
                    util.makedirs(dirname, self.createmode, notindexed)
                    return util.atomictempfile(f, mode, self.createmode,
                                               checkambig=checkambig)
                try:
                    if 'w' in mode:
                        util.unlink(f)
                        nlink = 0
                    else:
                        # nlinks() may behave differently for files on Windows
                        # shares if the file is open.
                        with util.posixfile(f):
                            nlink = util.nlinks(f)
                            if nlink < 1:
                                nlink = 2 # force mktempcopy (issue1922)
                except (OSError, IOError) as e:
                    if e.errno != errno.ENOENT:
                        raise
                    nlink = 0
                    util.makedirs(dirname, self.createmode, notindexed)
                if nlink > 0:
                    if self._trustnlink is None:
                        self._trustnlink = nlink > 1 or util.checknlink(f)
                    if nlink > 1 or not self._trustnlink:
                        util.rename(util.mktempcopy(f), f)
        fp = util.posixfile(f, mode)
        if nlink == 0:
            self._fixfilemode(f)

        if checkambig:
            if mode in ('r', 'rb'):
                raise error.Abort(_('implementation error: mode %s is not'
                                    ' valid for checkambig=True') % mode)
            fp = checkambigatclosing(fp)

        if (backgroundclose and
                isinstance(threading.currentThread(), threading._MainThread)):
            if not self._backgroundfilecloser:
                raise error.Abort(_('backgroundclose can only be used when a '
                                  'backgroundclosing context manager is active')
                                  )

            fp = delayclosedfile(fp, self._backgroundfilecloser)

        return fp

    def symlink(self, src, dst):
        self.audit(dst)
        linkname = self.join(dst)
        util.tryunlink(linkname)

        util.makedirs(os.path.dirname(linkname), self.createmode)

        if self._cansymlink:
            try:
                os.symlink(src, linkname)
            except OSError as err:
                raise OSError(err.errno, _('could not symlink to %r: %s') %
                              (src, encoding.strtolocal(err.strerror)),
                              linkname)
        else:
            self.write(dst, src)

    def join(self, path, *insidef):
        if path:
            return os.path.join(self.base, path, *insidef)
        else:
            return self.base

opener = vfs

class proxyvfs(object):
    def __init__(self, vfs):
        self.vfs = vfs

    @property
    def options(self):
        return self.vfs.options

    @options.setter
    def options(self, value):
        self.vfs.options = value

class filtervfs(abstractvfs, proxyvfs):
    '''Wrapper vfs for filtering filenames with a function.'''

    def __init__(self, vfs, filter):
        proxyvfs.__init__(self, vfs)
        self._filter = filter

    def __call__(self, path, *args, **kwargs):
        return self.vfs(self._filter(path), *args, **kwargs)

    def join(self, path, *insidef):
        if path:
            return self.vfs.join(self._filter(self.vfs.reljoin(path, *insidef)))
        else:
            return self.vfs.join(path)

filteropener = filtervfs

class readonlyvfs(abstractvfs, proxyvfs):
    '''Wrapper vfs preventing any writing.'''

    def __init__(self, vfs):
        proxyvfs.__init__(self, vfs)

    def __call__(self, path, mode='r', *args, **kw):
        if mode not in ('r', 'rb'):
            raise error.Abort(_('this vfs is read only'))
        return self.vfs(path, mode, *args, **kw)

    def join(self, path, *insidef):
        return self.vfs.join(path, *insidef)

class closewrapbase(object):
    """Base class of wrapper, which hooks closing

    Do not instantiate outside of the vfs layer.
    """
    def __init__(self, fh):
        object.__setattr__(self, r'_origfh', fh)

    def __getattr__(self, attr):
        return getattr(self._origfh, attr)

    def __setattr__(self, attr, value):
        return setattr(self._origfh, attr, value)

    def __delattr__(self, attr):
        return delattr(self._origfh, attr)

    def __enter__(self):
        self._origfh.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        raise NotImplementedError('attempted instantiating ' + str(type(self)))

    def close(self):
        raise NotImplementedError('attempted instantiating ' + str(type(self)))

class delayclosedfile(closewrapbase):
    """Proxy for a file object whose close is delayed.

    Do not instantiate outside of the vfs layer.
    """
    def __init__(self, fh, closer):
        super(delayclosedfile, self).__init__(fh)
        object.__setattr__(self, r'_closer', closer)

    def __exit__(self, exc_type, exc_value, exc_tb):
        self._closer.close(self._origfh)

    def close(self):
        self._closer.close(self._origfh)

class backgroundfilecloser(object):
    """Coordinates background closing of file handles on multiple threads."""
    def __init__(self, ui, expectedcount=-1):
        self._running = False
        self._entered = False
        self._threads = []
        self._threadexception = None

        # Only Windows/NTFS has slow file closing. So only enable by default
        # on that platform. But allow to be enabled elsewhere for testing.
        defaultenabled = pycompat.iswindows
        enabled = ui.configbool('worker', 'backgroundclose', defaultenabled)

        if not enabled:
            return

        # There is overhead to starting and stopping the background threads.
        # Don't do background processing unless the file count is large enough
        # to justify it.
        minfilecount = ui.configint('worker', 'backgroundcloseminfilecount')
        # FUTURE dynamically start background threads after minfilecount closes.
        # (We don't currently have any callers that don't know their file count)
        if expectedcount > 0 and expectedcount < minfilecount:
            return

        maxqueue = ui.configint('worker', 'backgroundclosemaxqueue')
        threadcount = ui.configint('worker', 'backgroundclosethreadcount')

        ui.debug('starting %d threads for background file closing\n' %
                 threadcount)

        self._queue = pycompat.queue.Queue(maxsize=maxqueue)
        self._running = True

        for i in range(threadcount):
            t = threading.Thread(target=self._worker, name='backgroundcloser')
            self._threads.append(t)
            t.start()

    def __enter__(self):
        self._entered = True
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self._running = False

        # Wait for threads to finish closing so open files don't linger for
        # longer than lifetime of context manager.
        for t in self._threads:
            t.join()

    def _worker(self):
        """Main routine for worker thread."""
        while True:
            try:
                fh = self._queue.get(block=True, timeout=0.100)
                # Need to catch or the thread will terminate and
                # we could orphan file descriptors.
                try:
                    fh.close()
                except Exception as e:
                    # Stash so can re-raise from main thread later.
                    self._threadexception = e
            except pycompat.queue.Empty:
                if not self._running:
                    break

    def close(self, fh):
        """Schedule a file for closing."""
        if not self._entered:
            raise error.Abort(_('can only call close() when context manager '
                              'active'))

        # If a background thread encountered an exception, raise now so we fail
        # fast. Otherwise we may potentially go on for minutes until the error
        # is acted on.
        if self._threadexception:
            e = self._threadexception
            self._threadexception = None
            raise e

        # If we're not actively running, close synchronously.
        if not self._running:
            fh.close()
            return

        self._queue.put(fh, block=True, timeout=None)

class checkambigatclosing(closewrapbase):
    """Proxy for a file object, to avoid ambiguity of file stat

    See also util.filestat for detail about "ambiguity of file stat".

    This proxy is useful only if the target file is guarded by any
    lock (e.g. repo.lock or repo.wlock)

    Do not instantiate outside of the vfs layer.
    """
    def __init__(self, fh):
        super(checkambigatclosing, self).__init__(fh)
        object.__setattr__(self, r'_oldstat', util.filestat.frompath(fh.name))

    def _checkambig(self):
        oldstat = self._oldstat
        if oldstat.stat:
            _avoidambig(self._origfh.name, oldstat)

    def __exit__(self, exc_type, exc_value, exc_tb):
        self._origfh.__exit__(exc_type, exc_value, exc_tb)
        self._checkambig()

    def close(self):
        self._origfh.close()
        self._checkambig()
