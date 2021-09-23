# streamclone.py - producing and consuming streaming repository data
#
# Copyright 2015 Gregory Szorc <gregory.szorc@gmail.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import contextlib
import os
import struct

from .i18n import _
from . import (
    branchmap,
    cacheutil,
    error,
    narrowspec,
    phases,
    pycompat,
    repository,
    store,
    util,
)

def canperformstreamclone(pullop, bundle2=False):
    """Whether it is possible to perform a streaming clone as part of pull.

    ``bundle2`` will cause the function to consider stream clone through
    bundle2 and only through bundle2.

    Returns a tuple of (supported, requirements). ``supported`` is True if
    streaming clone is supported and False otherwise. ``requirements`` is
    a set of repo requirements from the remote, or ``None`` if stream clone
    isn't supported.
    """
    repo = pullop.repo
    remote = pullop.remote

    bundle2supported = False
    if pullop.canusebundle2:
        if 'v2' in pullop.remotebundle2caps.get('stream', []):
            bundle2supported = True
        # else
            # Server doesn't support bundle2 stream clone or doesn't support
            # the versions we support. Fall back and possibly allow legacy.

    # Ensures legacy code path uses available bundle2.
    if bundle2supported and not bundle2:
        return False, None
    # Ensures bundle2 doesn't try to do a stream clone if it isn't supported.
    elif bundle2 and not bundle2supported:
        return False, None

    # Streaming clone only works on empty repositories.
    if len(repo):
        return False, None

    # Streaming clone only works if all data is being requested.
    if pullop.heads:
        return False, None

    streamrequested = pullop.streamclonerequested

    # If we don't have a preference, let the server decide for us. This
    # likely only comes into play in LANs.
    if streamrequested is None:
        # The server can advertise whether to prefer streaming clone.
        streamrequested = remote.capable('stream-preferred')

    if not streamrequested:
        return False, None

    # In order for stream clone to work, the client has to support all the
    # requirements advertised by the server.
    #
    # The server advertises its requirements via the "stream" and "streamreqs"
    # capability. "stream" (a value-less capability) is advertised if and only
    # if the only requirement is "revlogv1." Else, the "streamreqs" capability
    # is advertised and contains a comma-delimited list of requirements.
    requirements = set()
    if remote.capable('stream'):
        requirements.add('revlogv1')
    else:
        streamreqs = remote.capable('streamreqs')
        # This is weird and shouldn't happen with modern servers.
        if not streamreqs:
            pullop.repo.ui.warn(_(
                'warning: stream clone requested but server has them '
                'disabled\n'))
            return False, None

        streamreqs = set(streamreqs.split(','))
        # Server requires something we don't support. Bail.
        missingreqs = streamreqs - repo.supportedformats
        if missingreqs:
            pullop.repo.ui.warn(_(
                'warning: stream clone requested but client is missing '
                'requirements: %s\n') % ', '.join(sorted(missingreqs)))
            pullop.repo.ui.warn(
                _('(see https://www.mercurial-scm.org/wiki/MissingRequirement '
                  'for more information)\n'))
            return False, None
        requirements = streamreqs

    return True, requirements

def maybeperformlegacystreamclone(pullop):
    """Possibly perform a legacy stream clone operation.

    Legacy stream clones are performed as part of pull but before all other
    operations.

    A legacy stream clone will not be performed if a bundle2 stream clone is
    supported.
    """
    from . import localrepo

    supported, requirements = canperformstreamclone(pullop)

    if not supported:
        return

    repo = pullop.repo
    remote = pullop.remote

    # Save remote branchmap. We will use it later to speed up branchcache
    # creation.
    rbranchmap = None
    if remote.capable('branchmap'):
        with remote.commandexecutor() as e:
            rbranchmap = e.callcommand('branchmap', {}).result()

    repo.ui.status(_('streaming all changes\n'))

    with remote.commandexecutor() as e:
        fp = e.callcommand('stream_out', {}).result()

    # TODO strictly speaking, this code should all be inside the context
    # manager because the context manager is supposed to ensure all wire state
    # is flushed when exiting. But the legacy peers don't do this, so it
    # doesn't matter.
    l = fp.readline()
    try:
        resp = int(l)
    except ValueError:
        raise error.ResponseError(
            _('unexpected response from remote server:'), l)
    if resp == 1:
        raise error.Abort(_('operation forbidden by server'))
    elif resp == 2:
        raise error.Abort(_('locking the remote repository failed'))
    elif resp != 0:
        raise error.Abort(_('the server sent an unknown error code'))

    l = fp.readline()
    try:
        filecount, bytecount = map(int, l.split(' ', 1))
    except (ValueError, TypeError):
        raise error.ResponseError(
            _('unexpected response from remote server:'), l)

    with repo.lock():
        consumev1(repo, fp, filecount, bytecount)

        # new requirements = old non-format requirements +
        #                    new format-related remote requirements
        # requirements from the streamed-in repository
        repo.requirements = requirements | (
                repo.requirements - repo.supportedformats)
        repo.svfs.options = localrepo.resolvestorevfsoptions(
            repo.ui, repo.requirements, repo.features)
        repo._writerequirements()

        if rbranchmap:
            branchmap.replacecache(repo, rbranchmap)

        repo.invalidate()

def allowservergeneration(repo):
    """Whether streaming clones are allowed from the server."""
    if repository.REPO_FEATURE_STREAM_CLONE not in repo.features:
        return False

    if not repo.ui.configbool('server', 'uncompressed', untrusted=True):
        return False

    # The way stream clone works makes it impossible to hide secret changesets.
    # So don't allow this by default.
    secret = phases.hassecret(repo)
    if secret:
        return repo.ui.configbool('server', 'uncompressedallowsecret')

    return True

# This is it's own function so extensions can override it.
def _walkstreamfiles(repo, matcher=None):
    return repo.store.walk(matcher)

def generatev1(repo):
    """Emit content for version 1 of a streaming clone.

    This returns a 3-tuple of (file count, byte size, data iterator).

    The data iterator consists of N entries for each file being transferred.
    Each file entry starts as a line with the file name and integer size
    delimited by a null byte.

    The raw file data follows. Following the raw file data is the next file
    entry, or EOF.

    When used on the wire protocol, an additional line indicating protocol
    success will be prepended to the stream. This function is not responsible
    for adding it.

    This function will obtain a repository lock to ensure a consistent view of
    the store is captured. It therefore may raise LockError.
    """
    entries = []
    total_bytes = 0
    # Get consistent snapshot of repo, lock during scan.
    with repo.lock():
        repo.ui.debug('scanning\n')
        for name, ename, size in _walkstreamfiles(repo):
            if size:
                entries.append((name, size))
                total_bytes += size

    repo.ui.debug('%d files, %d bytes to transfer\n' %
                  (len(entries), total_bytes))

    svfs = repo.svfs
    debugflag = repo.ui.debugflag

    def emitrevlogdata():
        for name, size in entries:
            if debugflag:
                repo.ui.debug('sending %s (%d bytes)\n' % (name, size))
            # partially encode name over the wire for backwards compat
            yield '%s\0%d\n' % (store.encodedir(name), size)
            # auditing at this stage is both pointless (paths are already
            # trusted by the local repo) and expensive
            with svfs(name, 'rb', auditpath=False) as fp:
                if size <= 65536:
                    yield fp.read(size)
                else:
                    for chunk in util.filechunkiter(fp, limit=size):
                        yield chunk

    return len(entries), total_bytes, emitrevlogdata()

def generatev1wireproto(repo):
    """Emit content for version 1 of streaming clone suitable for the wire.

    This is the data output from ``generatev1()`` with 2 header lines. The
    first line indicates overall success. The 2nd contains the file count and
    byte size of payload.

    The success line contains "0" for success, "1" for stream generation not
    allowed, and "2" for error locking the repository (possibly indicating
    a permissions error for the server process).
    """
    if not allowservergeneration(repo):
        yield '1\n'
        return

    try:
        filecount, bytecount, it = generatev1(repo)
    except error.LockError:
        yield '2\n'
        return

    # Indicates successful response.
    yield '0\n'
    yield '%d %d\n' % (filecount, bytecount)
    for chunk in it:
        yield chunk

def generatebundlev1(repo, compression='UN'):
    """Emit content for version 1 of a stream clone bundle.

    The first 4 bytes of the output ("HGS1") denote this as stream clone
    bundle version 1.

    The next 2 bytes indicate the compression type. Only "UN" is currently
    supported.

    The next 16 bytes are two 64-bit big endian unsigned integers indicating
    file count and byte count, respectively.

    The next 2 bytes is a 16-bit big endian unsigned short declaring the length
    of the requirements string, including a trailing \0. The following N bytes
    are the requirements string, which is ASCII containing a comma-delimited
    list of repo requirements that are needed to support the data.

    The remaining content is the output of ``generatev1()`` (which may be
    compressed in the future).

    Returns a tuple of (requirements, data generator).
    """
    if compression != 'UN':
        raise ValueError('we do not support the compression argument yet')

    requirements = repo.requirements & repo.supportedformats
    requires = ','.join(sorted(requirements))

    def gen():
        yield 'HGS1'
        yield compression

        filecount, bytecount, it = generatev1(repo)
        repo.ui.status(_('writing %d bytes for %d files\n') %
                         (bytecount, filecount))

        yield struct.pack('>QQ', filecount, bytecount)
        yield struct.pack('>H', len(requires) + 1)
        yield requires + '\0'

        # This is where we'll add compression in the future.
        assert compression == 'UN'

        progress = repo.ui.makeprogress(_('bundle'), total=bytecount,
                                        unit=_('bytes'))
        progress.update(0)

        for chunk in it:
            progress.increment(step=len(chunk))
            yield chunk

        progress.complete()

    return requirements, gen()

def consumev1(repo, fp, filecount, bytecount):
    """Apply the contents from version 1 of a streaming clone file handle.

    This takes the output from "stream_out" and applies it to the specified
    repository.

    Like "stream_out," the status line added by the wire protocol is not
    handled by this function.
    """
    with repo.lock():
        repo.ui.status(_('%d files to transfer, %s of data\n') %
                       (filecount, util.bytecount(bytecount)))
        progress = repo.ui.makeprogress(_('clone'), total=bytecount,
                                        unit=_('bytes'))
        progress.update(0)
        start = util.timer()

        # TODO: get rid of (potential) inconsistency
        #
        # If transaction is started and any @filecache property is
        # changed at this point, it causes inconsistency between
        # in-memory cached property and streamclone-ed file on the
        # disk. Nested transaction prevents transaction scope "clone"
        # below from writing in-memory changes out at the end of it,
        # even though in-memory changes are discarded at the end of it
        # regardless of transaction nesting.
        #
        # But transaction nesting can't be simply prohibited, because
        # nesting occurs also in ordinary case (e.g. enabling
        # clonebundles).

        with repo.transaction('clone'):
            with repo.svfs.backgroundclosing(repo.ui, expectedcount=filecount):
                for i in pycompat.xrange(filecount):
                    # XXX doesn't support '\n' or '\r' in filenames
                    l = fp.readline()
                    try:
                        name, size = l.split('\0', 1)
                        size = int(size)
                    except (ValueError, TypeError):
                        raise error.ResponseError(
                            _('unexpected response from remote server:'), l)
                    if repo.ui.debugflag:
                        repo.ui.debug('adding %s (%s)\n' %
                                      (name, util.bytecount(size)))
                    # for backwards compat, name was partially encoded
                    path = store.decodedir(name)
                    with repo.svfs(path, 'w', backgroundclose=True) as ofp:
                        for chunk in util.filechunkiter(fp, limit=size):
                            progress.increment(step=len(chunk))
                            ofp.write(chunk)

            # force @filecache properties to be reloaded from
            # streamclone-ed file at next access
            repo.invalidate(clearfilecache=True)

        elapsed = util.timer() - start
        if elapsed <= 0:
            elapsed = 0.001
        progress.complete()
        repo.ui.status(_('transferred %s in %.1f seconds (%s/sec)\n') %
                       (util.bytecount(bytecount), elapsed,
                        util.bytecount(bytecount / elapsed)))

def readbundle1header(fp):
    compression = fp.read(2)
    if compression != 'UN':
        raise error.Abort(_('only uncompressed stream clone bundles are '
            'supported; got %s') % compression)

    filecount, bytecount = struct.unpack('>QQ', fp.read(16))
    requireslen = struct.unpack('>H', fp.read(2))[0]
    requires = fp.read(requireslen)

    if not requires.endswith('\0'):
        raise error.Abort(_('malformed stream clone bundle: '
                            'requirements not properly encoded'))

    requirements = set(requires.rstrip('\0').split(','))

    return filecount, bytecount, requirements

def applybundlev1(repo, fp):
    """Apply the content from a stream clone bundle version 1.

    We assume the 4 byte header has been read and validated and the file handle
    is at the 2 byte compression identifier.
    """
    if len(repo):
        raise error.Abort(_('cannot apply stream clone bundle on non-empty '
                            'repo'))

    filecount, bytecount, requirements = readbundle1header(fp)
    missingreqs = requirements - repo.supportedformats
    if missingreqs:
        raise error.Abort(_('unable to apply stream clone: '
                            'unsupported format: %s') %
                            ', '.join(sorted(missingreqs)))

    consumev1(repo, fp, filecount, bytecount)

class streamcloneapplier(object):
    """Class to manage applying streaming clone bundles.

    We need to wrap ``applybundlev1()`` in a dedicated type to enable bundle
    readers to perform bundle type-specific functionality.
    """
    def __init__(self, fh):
        self._fh = fh

    def apply(self, repo):
        return applybundlev1(repo, self._fh)

# type of file to stream
_fileappend = 0 # append only file
_filefull = 1   # full snapshot file

# Source of the file
_srcstore = 's' # store (svfs)
_srccache = 'c' # cache (cache)

# This is it's own function so extensions can override it.
def _walkstreamfullstorefiles(repo):
    """list snapshot file from the store"""
    fnames = []
    if not repo.publishing():
        fnames.append('phaseroots')
    return fnames

def _filterfull(entry, copy, vfsmap):
    """actually copy the snapshot files"""
    src, name, ftype, data = entry
    if ftype != _filefull:
        return entry
    return (src, name, ftype, copy(vfsmap[src].join(name)))

@contextlib.contextmanager
def maketempcopies():
    """return a function to temporary copy file"""
    files = []
    try:
        def copy(src):
            fd, dst = pycompat.mkstemp()
            os.close(fd)
            files.append(dst)
            util.copyfiles(src, dst, hardlink=True)
            return dst
        yield copy
    finally:
        for tmp in files:
            util.tryunlink(tmp)

def _makemap(repo):
    """make a (src -> vfs) map for the repo"""
    vfsmap = {
        _srcstore: repo.svfs,
        _srccache: repo.cachevfs,
    }
    # we keep repo.vfs out of the on purpose, ther are too many danger there
    # (eg: .hg/hgrc)
    assert repo.vfs not in vfsmap.values()

    return vfsmap

def _emit2(repo, entries, totalfilesize):
    """actually emit the stream bundle"""
    vfsmap = _makemap(repo)
    progress = repo.ui.makeprogress(_('bundle'), total=totalfilesize,
                                    unit=_('bytes'))
    progress.update(0)
    with maketempcopies() as copy, progress:
        # copy is delayed until we are in the try
        entries = [_filterfull(e, copy, vfsmap) for e in entries]
        yield None # this release the lock on the repository
        seen = 0

        for src, name, ftype, data in entries:
            vfs = vfsmap[src]
            yield src
            yield util.uvarintencode(len(name))
            if ftype == _fileappend:
                fp = vfs(name)
                size = data
            elif ftype == _filefull:
                fp = open(data, 'rb')
                size = util.fstat(fp).st_size
            try:
                yield util.uvarintencode(size)
                yield name
                if size <= 65536:
                    chunks = (fp.read(size),)
                else:
                    chunks = util.filechunkiter(fp, limit=size)
                for chunk in chunks:
                    seen += len(chunk)
                    progress.update(seen)
                    yield chunk
            finally:
                fp.close()

def generatev2(repo, includes, excludes, includeobsmarkers):
    """Emit content for version 2 of a streaming clone.

    the data stream consists the following entries:
    1) A char representing the file destination (eg: store or cache)
    2) A varint containing the length of the filename
    3) A varint containing the length of file data
    4) N bytes containing the filename (the internal, store-agnostic form)
    5) N bytes containing the file data

    Returns a 3-tuple of (file count, file size, data iterator).
    """

    # temporarily raise error until we add storage level logic
    if includes or excludes:
        raise error.Abort(_("server does not support narrow stream clones"))

    with repo.lock():

        entries = []
        totalfilesize = 0

        matcher = None
        if includes or excludes:
            matcher = narrowspec.match(repo.root, includes, excludes)

        repo.ui.debug('scanning\n')
        for name, ename, size in _walkstreamfiles(repo, matcher):
            if size:
                entries.append((_srcstore, name, _fileappend, size))
                totalfilesize += size
        for name in _walkstreamfullstorefiles(repo):
            if repo.svfs.exists(name):
                totalfilesize += repo.svfs.lstat(name).st_size
                entries.append((_srcstore, name, _filefull, None))
        if includeobsmarkers and repo.svfs.exists('obsstore'):
            totalfilesize += repo.svfs.lstat('obsstore').st_size
            entries.append((_srcstore, 'obsstore', _filefull, None))
        for name in cacheutil.cachetocopy(repo):
            if repo.cachevfs.exists(name):
                totalfilesize += repo.cachevfs.lstat(name).st_size
                entries.append((_srccache, name, _filefull, None))

        chunks = _emit2(repo, entries, totalfilesize)
        first = next(chunks)
        assert first is None

    return len(entries), totalfilesize, chunks

@contextlib.contextmanager
def nested(*ctxs):
    this = ctxs[0]
    rest = ctxs[1:]
    with this:
        if rest:
            with nested(*rest):
                yield
        else:
            yield

def consumev2(repo, fp, filecount, filesize):
    """Apply the contents from a version 2 streaming clone.

    Data is read from an object that only needs to provide a ``read(size)``
    method.
    """
    with repo.lock():
        repo.ui.status(_('%d files to transfer, %s of data\n') %
                       (filecount, util.bytecount(filesize)))

        start = util.timer()
        progress = repo.ui.makeprogress(_('clone'), total=filesize,
                                        unit=_('bytes'))
        progress.update(0)

        vfsmap = _makemap(repo)

        with repo.transaction('clone'):
            ctxs = (vfs.backgroundclosing(repo.ui)
                    for vfs in vfsmap.values())
            with nested(*ctxs):
                for i in range(filecount):
                    src = util.readexactly(fp, 1)
                    vfs = vfsmap[src]
                    namelen = util.uvarintdecodestream(fp)
                    datalen = util.uvarintdecodestream(fp)

                    name = util.readexactly(fp, namelen)

                    if repo.ui.debugflag:
                        repo.ui.debug('adding [%s] %s (%s)\n' %
                                      (src, name, util.bytecount(datalen)))

                    with vfs(name, 'w') as ofp:
                        for chunk in util.filechunkiter(fp, limit=datalen):
                            progress.increment(step=len(chunk))
                            ofp.write(chunk)

            # force @filecache properties to be reloaded from
            # streamclone-ed file at next access
            repo.invalidate(clearfilecache=True)

        elapsed = util.timer() - start
        if elapsed <= 0:
            elapsed = 0.001
        repo.ui.status(_('transferred %s in %.1f seconds (%s/sec)\n') %
                       (util.bytecount(progress.pos), elapsed,
                        util.bytecount(progress.pos / elapsed)))
        progress.complete()

def applybundlev2(repo, fp, filecount, filesize, requirements):
    from . import localrepo

    missingreqs = [r for r in requirements if r not in repo.supported]
    if missingreqs:
        raise error.Abort(_('unable to apply stream clone: '
                            'unsupported format: %s') %
                          ', '.join(sorted(missingreqs)))

    consumev2(repo, fp, filecount, filesize)

    # new requirements = old non-format requirements +
    #                    new format-related remote requirements
    # requirements from the streamed-in repository
    repo.requirements = set(requirements) | (
            repo.requirements - repo.supportedformats)
    repo.svfs.options = localrepo.resolvestorevfsoptions(
        repo.ui, repo.requirements, repo.features)
    repo._writerequirements()
