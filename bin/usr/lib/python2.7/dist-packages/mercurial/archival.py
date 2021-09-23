# archival.py - revision archival for mercurial
#
# Copyright 2006 Vadim Gelfer <vadim.gelfer@gmail.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import gzip
import os
import struct
import tarfile
import time
import zipfile
import zlib

from .i18n import _
from .node import (
    nullrev,
)

from . import (
    error,
    formatter,
    match as matchmod,
    pycompat,
    scmutil,
    util,
    vfs as vfsmod,
)
stringio = util.stringio

# from unzip source code:
_UNX_IFREG = 0x8000
_UNX_IFLNK = 0xa000

def tidyprefix(dest, kind, prefix):
    '''choose prefix to use for names in archive.  make sure prefix is
    safe for consumers.'''

    if prefix:
        prefix = util.normpath(prefix)
    else:
        if not isinstance(dest, bytes):
            raise ValueError('dest must be string if no prefix')
        prefix = os.path.basename(dest)
        lower = prefix.lower()
        for sfx in exts.get(kind, []):
            if lower.endswith(sfx):
                prefix = prefix[:-len(sfx)]
                break
    lpfx = os.path.normpath(util.localpath(prefix))
    prefix = util.pconvert(lpfx)
    if not prefix.endswith('/'):
        prefix += '/'
    # Drop the leading '.' path component if present, so Windows can read the
    # zip files (issue4634)
    if prefix.startswith('./'):
        prefix = prefix[2:]
    if prefix.startswith('../') or os.path.isabs(lpfx) or '/../' in prefix:
        raise error.Abort(_('archive prefix contains illegal components'))
    return prefix

exts = {
    'tar': ['.tar'],
    'tbz2': ['.tbz2', '.tar.bz2'],
    'tgz': ['.tgz', '.tar.gz'],
    'zip': ['.zip'],
    }

def guesskind(dest):
    for kind, extensions in exts.iteritems():
        if any(dest.endswith(ext) for ext in extensions):
            return kind
    return None

def _rootctx(repo):
    # repo[0] may be hidden
    for rev in repo:
        return repo[rev]
    return repo[nullrev]

# {tags} on ctx includes local tags and 'tip', with no current way to limit
# that to global tags.  Therefore, use {latesttag} as a substitute when
# the distance is 0, since that will be the list of global tags on ctx.
_defaultmetatemplate = br'''
repo: {root}
node: {ifcontains(rev, revset("wdir()"), "{p1node}{dirty}", "{node}")}
branch: {branch|utf8}
{ifeq(latesttagdistance, 0, join(latesttag % "tag: {tag}", "\n"),
      separate("\n",
               join(latesttag % "latesttag: {tag}", "\n"),
               "latesttagdistance: {latesttagdistance}",
               "changessincelatesttag: {changessincelatesttag}"))}
'''[1:]  # drop leading '\n'

def buildmetadata(ctx):
    '''build content of .hg_archival.txt'''
    repo = ctx.repo()

    opts = {
        'template': repo.ui.config('experimental', 'archivemetatemplate',
                                   _defaultmetatemplate)
    }

    out = util.stringio()

    fm = formatter.formatter(repo.ui, out, 'archive', opts)
    fm.startitem()
    fm.context(ctx=ctx)
    fm.data(root=_rootctx(repo).hex())

    if ctx.rev() is None:
        dirty = ''
        if ctx.dirty(missing=True):
            dirty = '+'
        fm.data(dirty=dirty)
    fm.end()

    return out.getvalue()

class tarit(object):
    '''write archive to tar file or stream.  can write uncompressed,
    or compress with gzip or bzip2.'''

    class GzipFileWithTime(gzip.GzipFile):

        def __init__(self, *args, **kw):
            timestamp = None
            if r'timestamp' in kw:
                timestamp = kw.pop(r'timestamp')
            if timestamp is None:
                self.timestamp = time.time()
            else:
                self.timestamp = timestamp
            gzip.GzipFile.__init__(self, *args, **kw)

        def _write_gzip_header(self):
            self.fileobj.write('\037\213')             # magic header
            self.fileobj.write('\010')                 # compression method
            fname = self.name
            if fname and fname.endswith('.gz'):
                fname = fname[:-3]
            flags = 0
            if fname:
                flags = gzip.FNAME
            self.fileobj.write(pycompat.bytechr(flags))
            gzip.write32u(self.fileobj, int(self.timestamp))
            self.fileobj.write('\002')
            self.fileobj.write('\377')
            if fname:
                self.fileobj.write(fname + '\000')

    def __init__(self, dest, mtime, kind=''):
        self.mtime = mtime
        self.fileobj = None

        def taropen(mode, name='', fileobj=None):
            if kind == 'gz':
                mode = mode[0:1]
                if not fileobj:
                    fileobj = open(name, mode + 'b')
                gzfileobj = self.GzipFileWithTime(name,
                                                  pycompat.sysstr(mode + 'b'),
                                                  zlib.Z_BEST_COMPRESSION,
                                                  fileobj, timestamp=mtime)
                self.fileobj = gzfileobj
                return tarfile.TarFile.taropen(
                    name, pycompat.sysstr(mode), gzfileobj)
            else:
                return tarfile.open(
                    name, pycompat.sysstr(mode + kind), fileobj)

        if isinstance(dest, bytes):
            self.z = taropen('w:', name=dest)
        else:
            self.z = taropen('w|', fileobj=dest)

    def addfile(self, name, mode, islink, data):
        name = pycompat.fsdecode(name)
        i = tarfile.TarInfo(name)
        i.mtime = self.mtime
        i.size = len(data)
        if islink:
            i.type = tarfile.SYMTYPE
            i.mode = 0o777
            i.linkname = pycompat.fsdecode(data)
            data = None
            i.size = 0
        else:
            i.mode = mode
            data = stringio(data)
        self.z.addfile(i, data)

    def done(self):
        self.z.close()
        if self.fileobj:
            self.fileobj.close()

class zipit(object):
    '''write archive to zip file or stream.  can write uncompressed,
    or compressed with deflate.'''

    def __init__(self, dest, mtime, compress=True):
        if isinstance(dest, bytes):
            dest = pycompat.fsdecode(dest)
        self.z = zipfile.ZipFile(dest, r'w',
                                 compress and zipfile.ZIP_DEFLATED or
                                 zipfile.ZIP_STORED)

        # Python's zipfile module emits deprecation warnings if we try
        # to store files with a date before 1980.
        epoch = 315532800 # calendar.timegm((1980, 1, 1, 0, 0, 0, 1, 1, 0))
        if mtime < epoch:
            mtime = epoch

        self.mtime = mtime
        self.date_time = time.gmtime(mtime)[:6]

    def addfile(self, name, mode, islink, data):
        i = zipfile.ZipInfo(pycompat.fsdecode(name), self.date_time)
        i.compress_type = self.z.compression
        # unzip will not honor unix file modes unless file creator is
        # set to unix (id 3).
        i.create_system = 3
        ftype = _UNX_IFREG
        if islink:
            mode = 0o777
            ftype = _UNX_IFLNK
        i.external_attr = (mode | ftype) << 16
        # add "extended-timestamp" extra block, because zip archives
        # without this will be extracted with unexpected timestamp,
        # if TZ is not configured as GMT
        i.extra += struct.pack('<hhBl',
                               0x5455,     # block type: "extended-timestamp"
                               1 + 4,      # size of this block
                               1,          # "modification time is present"
                               int(self.mtime)) # last modification (UTC)
        self.z.writestr(i, data)

    def done(self):
        self.z.close()

class fileit(object):
    '''write archive as files in directory.'''

    def __init__(self, name, mtime):
        self.basedir = name
        self.opener = vfsmod.vfs(self.basedir)
        self.mtime = mtime

    def addfile(self, name, mode, islink, data):
        if islink:
            self.opener.symlink(data, name)
            return
        f = self.opener(name, "w", atomictemp=False)
        f.write(data)
        f.close()
        destfile = os.path.join(self.basedir, name)
        os.chmod(destfile, mode)
        if self.mtime is not None:
            os.utime(destfile, (self.mtime, self.mtime))

    def done(self):
        pass

archivers = {
    'files': fileit,
    'tar': tarit,
    'tbz2': lambda name, mtime: tarit(name, mtime, 'bz2'),
    'tgz': lambda name, mtime: tarit(name, mtime, 'gz'),
    'uzip': lambda name, mtime: zipit(name, mtime, False),
    'zip': zipit,
    }

def archive(repo, dest, node, kind, decode=True, matchfn=None,
            prefix='', mtime=None, subrepos=False):
    '''create archive of repo as it was at node.

    dest can be name of directory, name of archive file, or file
    object to write archive to.

    kind is type of archive to create.

    decode tells whether to put files through decode filters from
    hgrc.

    matchfn is function to filter names of files to write to archive.

    prefix is name of path to put before every archive member.

    mtime is the modified time, in seconds, or None to use the changeset time.

    subrepos tells whether to include subrepos.
    '''

    if kind == 'files':
        if prefix:
            raise error.Abort(_('cannot give prefix when archiving to files'))
    else:
        prefix = tidyprefix(dest, kind, prefix)

    def write(name, mode, islink, getdata):
        data = getdata()
        if decode:
            data = repo.wwritedata(name, data)
        archiver.addfile(prefix + name, mode, islink, data)

    if kind not in archivers:
        raise error.Abort(_("unknown archive type '%s'") % kind)

    ctx = repo[node]
    archiver = archivers[kind](dest, mtime or ctx.date()[0])

    if repo.ui.configbool("ui", "archivemeta"):
        name = '.hg_archival.txt'
        if not matchfn or matchfn(name):
            write(name, 0o644, False, lambda: buildmetadata(ctx))

    if matchfn:
        files = [f for f in ctx.manifest().keys() if matchfn(f)]
    else:
        files = ctx.manifest().keys()
    total = len(files)
    if total:
        files.sort()
        scmutil.prefetchfiles(repo, [ctx.rev()],
                              scmutil.matchfiles(repo, files))
        progress = scmutil.progress(repo.ui, _('archiving'), unit=_('files'),
                                    total=total)
        progress.update(0)
        for f in files:
            ff = ctx.flags(f)
            write(f, 'x' in ff and 0o755 or 0o644, 'l' in ff, ctx[f].data)
            progress.increment(item=f)
        progress.complete()

    if subrepos:
        for subpath in sorted(ctx.substate):
            sub = ctx.workingsub(subpath)
            submatch = matchmod.subdirmatcher(subpath, matchfn)
            total += sub.archive(archiver, prefix, submatch, decode)

    if total == 0:
        raise error.Abort(_('no files match the archive pattern'))

    archiver.done()
    return total
