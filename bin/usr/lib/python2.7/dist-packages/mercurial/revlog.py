# revlog.py - storage back-end for mercurial
#
# Copyright 2005-2007 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

"""Storage back-end for Mercurial.

This provides efficient delta storage with O(1) retrieve and append
and O(changes) merge between branches.
"""

from __future__ import absolute_import

import collections
import contextlib
import errno
import os
import struct
import zlib

# import stuff from node for others to import from revlog
from .node import (
    bin,
    hex,
    nullhex,
    nullid,
    nullrev,
    short,
    wdirfilenodeids,
    wdirhex,
    wdirid,
    wdirrev,
)
from .i18n import _
from .revlogutils.constants import (
    FLAG_GENERALDELTA,
    FLAG_INLINE_DATA,
    REVIDX_DEFAULT_FLAGS,
    REVIDX_ELLIPSIS,
    REVIDX_EXTSTORED,
    REVIDX_FLAGS_ORDER,
    REVIDX_ISCENSORED,
    REVIDX_KNOWN_FLAGS,
    REVIDX_RAWTEXT_CHANGING_FLAGS,
    REVLOGV0,
    REVLOGV1,
    REVLOGV1_FLAGS,
    REVLOGV2,
    REVLOGV2_FLAGS,
    REVLOG_DEFAULT_FLAGS,
    REVLOG_DEFAULT_FORMAT,
    REVLOG_DEFAULT_VERSION,
)
from .thirdparty import (
    attr,
)
from . import (
    ancestor,
    dagop,
    error,
    mdiff,
    policy,
    pycompat,
    repository,
    templatefilters,
    util,
)
from .revlogutils import (
    deltas as deltautil,
)
from .utils import (
    interfaceutil,
    storageutil,
    stringutil,
)

# blanked usage of all the name to prevent pyflakes constraints
# We need these name available in the module for extensions.
REVLOGV0
REVLOGV1
REVLOGV2
FLAG_INLINE_DATA
FLAG_GENERALDELTA
REVLOG_DEFAULT_FLAGS
REVLOG_DEFAULT_FORMAT
REVLOG_DEFAULT_VERSION
REVLOGV1_FLAGS
REVLOGV2_FLAGS
REVIDX_ISCENSORED
REVIDX_ELLIPSIS
REVIDX_EXTSTORED
REVIDX_DEFAULT_FLAGS
REVIDX_FLAGS_ORDER
REVIDX_KNOWN_FLAGS
REVIDX_RAWTEXT_CHANGING_FLAGS

parsers = policy.importmod(r'parsers')

# Aliased for performance.
_zlibdecompress = zlib.decompress

# max size of revlog with inline data
_maxinline = 131072
_chunksize = 1048576

# Store flag processors (cf. 'addflagprocessor()' to register)
_flagprocessors = {
    REVIDX_ISCENSORED: None,
}

# Flag processors for REVIDX_ELLIPSIS.
def ellipsisreadprocessor(rl, text):
    return text, False

def ellipsiswriteprocessor(rl, text):
    return text, False

def ellipsisrawprocessor(rl, text):
    return False

ellipsisprocessor = (
    ellipsisreadprocessor,
    ellipsiswriteprocessor,
    ellipsisrawprocessor,
)

def addflagprocessor(flag, processor):
    """Register a flag processor on a revision data flag.

    Invariant:
    - Flags need to be defined in REVIDX_KNOWN_FLAGS and REVIDX_FLAGS_ORDER,
      and REVIDX_RAWTEXT_CHANGING_FLAGS if they can alter rawtext.
    - Only one flag processor can be registered on a specific flag.
    - flagprocessors must be 3-tuples of functions (read, write, raw) with the
      following signatures:
          - (read)  f(self, rawtext) -> text, bool
          - (write) f(self, text) -> rawtext, bool
          - (raw)   f(self, rawtext) -> bool
      "text" is presented to the user. "rawtext" is stored in revlog data, not
      directly visible to the user.
      The boolean returned by these transforms is used to determine whether
      the returned text can be used for hash integrity checking. For example,
      if "write" returns False, then "text" is used to generate hash. If
      "write" returns True, that basically means "rawtext" returned by "write"
      should be used to generate hash. Usually, "write" and "read" return
      different booleans. And "raw" returns a same boolean as "write".

      Note: The 'raw' transform is used for changegroup generation and in some
      debug commands. In this case the transform only indicates whether the
      contents can be used for hash integrity checks.
    """
    _insertflagprocessor(flag, processor, _flagprocessors)

def _insertflagprocessor(flag, processor, flagprocessors):
    if not flag & REVIDX_KNOWN_FLAGS:
        msg = _("cannot register processor on unknown flag '%#x'.") % (flag)
        raise error.ProgrammingError(msg)
    if flag not in REVIDX_FLAGS_ORDER:
        msg = _("flag '%#x' undefined in REVIDX_FLAGS_ORDER.") % (flag)
        raise error.ProgrammingError(msg)
    if flag in flagprocessors:
        msg = _("cannot register multiple processors on flag '%#x'.") % (flag)
        raise error.Abort(msg)
    flagprocessors[flag] = processor

def getoffset(q):
    return int(q >> 16)

def gettype(q):
    return int(q & 0xFFFF)

def offset_type(offset, type):
    if (type & ~REVIDX_KNOWN_FLAGS) != 0:
        raise ValueError('unknown revlog index flags')
    return int(int(offset) << 16 | type)

@attr.s(slots=True, frozen=True)
class _revisioninfo(object):
    """Information about a revision that allows building its fulltext
    node:       expected hash of the revision
    p1, p2:     parent revs of the revision
    btext:      built text cache consisting of a one-element list
    cachedelta: (baserev, uncompressed_delta) or None
    flags:      flags associated to the revision storage

    One of btext[0] or cachedelta must be set.
    """
    node = attr.ib()
    p1 = attr.ib()
    p2 = attr.ib()
    btext = attr.ib()
    textlen = attr.ib()
    cachedelta = attr.ib()
    flags = attr.ib()

@interfaceutil.implementer(repository.irevisiondelta)
@attr.s(slots=True)
class revlogrevisiondelta(object):
    node = attr.ib()
    p1node = attr.ib()
    p2node = attr.ib()
    basenode = attr.ib()
    flags = attr.ib()
    baserevisionsize = attr.ib()
    revision = attr.ib()
    delta = attr.ib()
    linknode = attr.ib(default=None)

@interfaceutil.implementer(repository.iverifyproblem)
@attr.s(frozen=True)
class revlogproblem(object):
    warning = attr.ib(default=None)
    error = attr.ib(default=None)
    node = attr.ib(default=None)

# index v0:
#  4 bytes: offset
#  4 bytes: compressed length
#  4 bytes: base rev
#  4 bytes: link rev
# 20 bytes: parent 1 nodeid
# 20 bytes: parent 2 nodeid
# 20 bytes: nodeid
indexformatv0 = struct.Struct(">4l20s20s20s")
indexformatv0_pack = indexformatv0.pack
indexformatv0_unpack = indexformatv0.unpack

class revlogoldindex(list):
    def __getitem__(self, i):
        if i == -1:
            return (0, 0, 0, -1, -1, -1, -1, nullid)
        return list.__getitem__(self, i)

class revlogoldio(object):
    def __init__(self):
        self.size = indexformatv0.size

    def parseindex(self, data, inline):
        s = self.size
        index = []
        nodemap = {nullid: nullrev}
        n = off = 0
        l = len(data)
        while off + s <= l:
            cur = data[off:off + s]
            off += s
            e = indexformatv0_unpack(cur)
            # transform to revlogv1 format
            e2 = (offset_type(e[0], 0), e[1], -1, e[2], e[3],
                  nodemap.get(e[4], nullrev), nodemap.get(e[5], nullrev), e[6])
            index.append(e2)
            nodemap[e[6]] = n
            n += 1

        return revlogoldindex(index), nodemap, None

    def packentry(self, entry, node, version, rev):
        if gettype(entry[0]):
            raise error.RevlogError(_('index entry flags need revlog '
                                      'version 1'))
        e2 = (getoffset(entry[0]), entry[1], entry[3], entry[4],
              node(entry[5]), node(entry[6]), entry[7])
        return indexformatv0_pack(*e2)

# index ng:
#  6 bytes: offset
#  2 bytes: flags
#  4 bytes: compressed length
#  4 bytes: uncompressed length
#  4 bytes: base rev
#  4 bytes: link rev
#  4 bytes: parent 1 rev
#  4 bytes: parent 2 rev
# 32 bytes: nodeid
indexformatng = struct.Struct(">Qiiiiii20s12x")
indexformatng_pack = indexformatng.pack
versionformat = struct.Struct(">I")
versionformat_pack = versionformat.pack
versionformat_unpack = versionformat.unpack

# corresponds to uncompressed length of indexformatng (2 gigs, 4-byte
# signed integer)
_maxentrysize = 0x7fffffff

class revlogio(object):
    def __init__(self):
        self.size = indexformatng.size

    def parseindex(self, data, inline):
        # call the C implementation to parse the index data
        index, cache = parsers.parse_index2(data, inline)
        return index, getattr(index, 'nodemap', None), cache

    def packentry(self, entry, node, version, rev):
        p = indexformatng_pack(*entry)
        if rev == 0:
            p = versionformat_pack(version) + p[4:]
        return p

class revlog(object):
    """
    the underlying revision storage object

    A revlog consists of two parts, an index and the revision data.

    The index is a file with a fixed record size containing
    information on each revision, including its nodeid (hash), the
    nodeids of its parents, the position and offset of its data within
    the data file, and the revision it's based on. Finally, each entry
    contains a linkrev entry that can serve as a pointer to external
    data.

    The revision data itself is a linear collection of data chunks.
    Each chunk represents a revision and is usually represented as a
    delta against the previous chunk. To bound lookup time, runs of
    deltas are limited to about 2 times the length of the original
    version data. This makes retrieval of a version proportional to
    its size, or O(1) relative to the number of revisions.

    Both pieces of the revlog are written to in an append-only
    fashion, which means we never need to rewrite a file to insert or
    remove data, and can use some simple techniques to avoid the need
    for locking while reading.

    If checkambig, indexfile is opened with checkambig=True at
    writing, to avoid file stat ambiguity.

    If mmaplargeindex is True, and an mmapindexthreshold is set, the
    index will be mmapped rather than read if it is larger than the
    configured threshold.

    If censorable is True, the revlog can have censored revisions.
    """
    def __init__(self, opener, indexfile, datafile=None, checkambig=False,
                 mmaplargeindex=False, censorable=False):
        """
        create a revlog object

        opener is a function that abstracts the file opening operation
        and can be used to implement COW semantics or the like.
        """
        self.indexfile = indexfile
        self.datafile = datafile or (indexfile[:-2] + ".d")
        self.opener = opener
        #  When True, indexfile is opened with checkambig=True at writing, to
        #  avoid file stat ambiguity.
        self._checkambig = checkambig
        self._censorable = censorable
        # 3-tuple of (node, rev, text) for a raw revision.
        self._revisioncache = None
        # Maps rev to chain base rev.
        self._chainbasecache = util.lrucachedict(100)
        # 2-tuple of (offset, data) of raw data from the revlog at an offset.
        self._chunkcache = (0, '')
        # How much data to read and cache into the raw revlog data cache.
        self._chunkcachesize = 65536
        self._maxchainlen = None
        self._deltabothparents = True
        self.index = []
        # Mapping of partial identifiers to full nodes.
        self._pcache = {}
        # Mapping of revision integer to full node.
        self._nodecache = {nullid: nullrev}
        self._nodepos = None
        self._compengine = 'zlib'
        self._maxdeltachainspan = -1
        self._withsparseread = False
        self._sparserevlog = False
        self._srdensitythreshold = 0.50
        self._srmingapsize = 262144

        # Make copy of flag processors so each revlog instance can support
        # custom flags.
        self._flagprocessors = dict(_flagprocessors)

        mmapindexthreshold = None
        v = REVLOG_DEFAULT_VERSION
        opts = getattr(opener, 'options', None)
        if opts is not None:
            if 'revlogv2' in opts:
                # version 2 revlogs always use generaldelta.
                v = REVLOGV2 | FLAG_GENERALDELTA | FLAG_INLINE_DATA
            elif 'revlogv1' in opts:
                if 'generaldelta' in opts:
                    v |= FLAG_GENERALDELTA
            else:
                v = 0
            if 'chunkcachesize' in opts:
                self._chunkcachesize = opts['chunkcachesize']
            if 'maxchainlen' in opts:
                self._maxchainlen = opts['maxchainlen']
            if 'deltabothparents' in opts:
                self._deltabothparents = opts['deltabothparents']
            self._lazydeltabase = bool(opts.get('lazydeltabase', False))
            if 'compengine' in opts:
                self._compengine = opts['compengine']
            if 'maxdeltachainspan' in opts:
                self._maxdeltachainspan = opts['maxdeltachainspan']
            if mmaplargeindex and 'mmapindexthreshold' in opts:
                mmapindexthreshold = opts['mmapindexthreshold']
            self._sparserevlog = bool(opts.get('sparse-revlog', False))
            withsparseread = bool(opts.get('with-sparse-read', False))
            # sparse-revlog forces sparse-read
            self._withsparseread = self._sparserevlog or withsparseread
            if 'sparse-read-density-threshold' in opts:
                self._srdensitythreshold = opts['sparse-read-density-threshold']
            if 'sparse-read-min-gap-size' in opts:
                self._srmingapsize = opts['sparse-read-min-gap-size']
            if opts.get('enableellipsis'):
                self._flagprocessors[REVIDX_ELLIPSIS] = ellipsisprocessor

            # revlog v0 doesn't have flag processors
            for flag, processor in opts.get(b'flagprocessors', {}).iteritems():
                _insertflagprocessor(flag, processor, self._flagprocessors)

        if self._chunkcachesize <= 0:
            raise error.RevlogError(_('revlog chunk cache size %r is not '
                                      'greater than 0') % self._chunkcachesize)
        elif self._chunkcachesize & (self._chunkcachesize - 1):
            raise error.RevlogError(_('revlog chunk cache size %r is not a '
                                      'power of 2') % self._chunkcachesize)

        self._loadindex(v, mmapindexthreshold)

    def _loadindex(self, v, mmapindexthreshold):
        indexdata = ''
        self._initempty = True
        try:
            with self._indexfp() as f:
                if (mmapindexthreshold is not None and
                    self.opener.fstat(f).st_size >= mmapindexthreshold):
                    indexdata = util.buffer(util.mmapread(f))
                else:
                    indexdata = f.read()
            if len(indexdata) > 0:
                v = versionformat_unpack(indexdata[:4])[0]
                self._initempty = False
        except IOError as inst:
            if inst.errno != errno.ENOENT:
                raise

        self.version = v
        self._inline = v & FLAG_INLINE_DATA
        self._generaldelta = v & FLAG_GENERALDELTA
        flags = v & ~0xFFFF
        fmt = v & 0xFFFF
        if fmt == REVLOGV0:
            if flags:
                raise error.RevlogError(_('unknown flags (%#04x) in version %d '
                                          'revlog %s') %
                                        (flags >> 16, fmt, self.indexfile))
        elif fmt == REVLOGV1:
            if flags & ~REVLOGV1_FLAGS:
                raise error.RevlogError(_('unknown flags (%#04x) in version %d '
                                          'revlog %s') %
                                        (flags >> 16, fmt, self.indexfile))
        elif fmt == REVLOGV2:
            if flags & ~REVLOGV2_FLAGS:
                raise error.RevlogError(_('unknown flags (%#04x) in version %d '
                                          'revlog %s') %
                                        (flags >> 16, fmt, self.indexfile))
        else:
            raise error.RevlogError(_('unknown version (%d) in revlog %s') %
                                    (fmt, self.indexfile))

        self._storedeltachains = True

        self._io = revlogio()
        if self.version == REVLOGV0:
            self._io = revlogoldio()
        try:
            d = self._io.parseindex(indexdata, self._inline)
        except (ValueError, IndexError):
            raise error.RevlogError(_("index %s is corrupted") %
                                    self.indexfile)
        self.index, nodemap, self._chunkcache = d
        if nodemap is not None:
            self.nodemap = self._nodecache = nodemap
        if not self._chunkcache:
            self._chunkclear()
        # revnum -> (chain-length, sum-delta-length)
        self._chaininfocache = {}
        # revlog header -> revlog compressor
        self._decompressors = {}

    @util.propertycache
    def _compressor(self):
        return util.compengines[self._compengine].revlogcompressor()

    def _indexfp(self, mode='r'):
        """file object for the revlog's index file"""
        args = {r'mode': mode}
        if mode != 'r':
            args[r'checkambig'] = self._checkambig
        if mode == 'w':
            args[r'atomictemp'] = True
        return self.opener(self.indexfile, **args)

    def _datafp(self, mode='r'):
        """file object for the revlog's data file"""
        return self.opener(self.datafile, mode=mode)

    @contextlib.contextmanager
    def _datareadfp(self, existingfp=None):
        """file object suitable to read data"""
        if existingfp is not None:
            yield existingfp
        else:
            if self._inline:
                func = self._indexfp
            else:
                func = self._datafp
            with func() as fp:
                yield fp

    def tip(self):
        return self.node(len(self.index) - 1)
    def __contains__(self, rev):
        return 0 <= rev < len(self)
    def __len__(self):
        return len(self.index)
    def __iter__(self):
        return iter(pycompat.xrange(len(self)))
    def revs(self, start=0, stop=None):
        """iterate over all rev in this revlog (from start to stop)"""
        return storageutil.iterrevs(len(self), start=start, stop=stop)

    @util.propertycache
    def nodemap(self):
        if self.index:
            # populate mapping down to the initial node
            node0 = self.index[0][7]  # get around changelog filtering
            self.rev(node0)
        return self._nodecache

    def hasnode(self, node):
        try:
            self.rev(node)
            return True
        except KeyError:
            return False

    def candelta(self, baserev, rev):
        """whether two revisions (baserev, rev) can be delta-ed or not"""
        # Disable delta if either rev requires a content-changing flag
        # processor (ex. LFS). This is because such flag processor can alter
        # the rawtext content that the delta will be based on, and two clients
        # could have a same revlog node with different flags (i.e. different
        # rawtext contents) and the delta could be incompatible.
        if ((self.flags(baserev) & REVIDX_RAWTEXT_CHANGING_FLAGS)
            or (self.flags(rev) & REVIDX_RAWTEXT_CHANGING_FLAGS)):
            return False
        return True

    def clearcaches(self):
        self._revisioncache = None
        self._chainbasecache.clear()
        self._chunkcache = (0, '')
        self._pcache = {}

        try:
            self._nodecache.clearcaches()
        except AttributeError:
            self._nodecache = {nullid: nullrev}
            self._nodepos = None

    def rev(self, node):
        try:
            return self._nodecache[node]
        except TypeError:
            raise
        except error.RevlogError:
            # parsers.c radix tree lookup failed
            if node == wdirid or node in wdirfilenodeids:
                raise error.WdirUnsupported
            raise error.LookupError(node, self.indexfile, _('no node'))
        except KeyError:
            # pure python cache lookup failed
            n = self._nodecache
            i = self.index
            p = self._nodepos
            if p is None:
                p = len(i) - 1
            else:
                assert p < len(i)
            for r in pycompat.xrange(p, -1, -1):
                v = i[r][7]
                n[v] = r
                if v == node:
                    self._nodepos = r - 1
                    return r
            if node == wdirid or node in wdirfilenodeids:
                raise error.WdirUnsupported
            raise error.LookupError(node, self.indexfile, _('no node'))

    # Accessors for index entries.

    # First tuple entry is 8 bytes. First 6 bytes are offset. Last 2 bytes
    # are flags.
    def start(self, rev):
        return int(self.index[rev][0] >> 16)

    def flags(self, rev):
        return self.index[rev][0] & 0xFFFF

    def length(self, rev):
        return self.index[rev][1]

    def rawsize(self, rev):
        """return the length of the uncompressed text for a given revision"""
        l = self.index[rev][2]
        if l >= 0:
            return l

        t = self.revision(rev, raw=True)
        return len(t)

    def size(self, rev):
        """length of non-raw text (processed by a "read" flag processor)"""
        # fast path: if no "read" flag processor could change the content,
        # size is rawsize. note: ELLIPSIS is known to not change the content.
        flags = self.flags(rev)
        if flags & (REVIDX_KNOWN_FLAGS ^ REVIDX_ELLIPSIS) == 0:
            return self.rawsize(rev)

        return len(self.revision(rev, raw=False))

    def chainbase(self, rev):
        base = self._chainbasecache.get(rev)
        if base is not None:
            return base

        index = self.index
        iterrev = rev
        base = index[iterrev][3]
        while base != iterrev:
            iterrev = base
            base = index[iterrev][3]

        self._chainbasecache[rev] = base
        return base

    def linkrev(self, rev):
        return self.index[rev][4]

    def parentrevs(self, rev):
        try:
            entry = self.index[rev]
        except IndexError:
            if rev == wdirrev:
                raise error.WdirUnsupported
            raise

        return entry[5], entry[6]

    # fast parentrevs(rev) where rev isn't filtered
    _uncheckedparentrevs = parentrevs

    def node(self, rev):
        try:
            return self.index[rev][7]
        except IndexError:
            if rev == wdirrev:
                raise error.WdirUnsupported
            raise

    # Derived from index values.

    def end(self, rev):
        return self.start(rev) + self.length(rev)

    def parents(self, node):
        i = self.index
        d = i[self.rev(node)]
        return i[d[5]][7], i[d[6]][7] # map revisions to nodes inline

    def chainlen(self, rev):
        return self._chaininfo(rev)[0]

    def _chaininfo(self, rev):
        chaininfocache = self._chaininfocache
        if rev in chaininfocache:
            return chaininfocache[rev]
        index = self.index
        generaldelta = self._generaldelta
        iterrev = rev
        e = index[iterrev]
        clen = 0
        compresseddeltalen = 0
        while iterrev != e[3]:
            clen += 1
            compresseddeltalen += e[1]
            if generaldelta:
                iterrev = e[3]
            else:
                iterrev -= 1
            if iterrev in chaininfocache:
                t = chaininfocache[iterrev]
                clen += t[0]
                compresseddeltalen += t[1]
                break
            e = index[iterrev]
        else:
            # Add text length of base since decompressing that also takes
            # work. For cache hits the length is already included.
            compresseddeltalen += e[1]
        r = (clen, compresseddeltalen)
        chaininfocache[rev] = r
        return r

    def _deltachain(self, rev, stoprev=None):
        """Obtain the delta chain for a revision.

        ``stoprev`` specifies a revision to stop at. If not specified, we
        stop at the base of the chain.

        Returns a 2-tuple of (chain, stopped) where ``chain`` is a list of
        revs in ascending order and ``stopped`` is a bool indicating whether
        ``stoprev`` was hit.
        """
        # Try C implementation.
        try:
            return self.index.deltachain(rev, stoprev, self._generaldelta)
        except AttributeError:
            pass

        chain = []

        # Alias to prevent attribute lookup in tight loop.
        index = self.index
        generaldelta = self._generaldelta

        iterrev = rev
        e = index[iterrev]
        while iterrev != e[3] and iterrev != stoprev:
            chain.append(iterrev)
            if generaldelta:
                iterrev = e[3]
            else:
                iterrev -= 1
            e = index[iterrev]

        if iterrev == stoprev:
            stopped = True
        else:
            chain.append(iterrev)
            stopped = False

        chain.reverse()
        return chain, stopped

    def ancestors(self, revs, stoprev=0, inclusive=False):
        """Generate the ancestors of 'revs' in reverse topological order.
        Does not generate revs lower than stoprev.

        See the documentation for ancestor.lazyancestors for more details."""

        # first, make sure start revisions aren't filtered
        revs = list(revs)
        checkrev = self.node
        for r in revs:
            checkrev(r)
        # and we're sure ancestors aren't filtered as well
        if util.safehasattr(parsers, 'rustlazyancestors'):
            return ancestor.rustlazyancestors(
                self.index, revs,
                stoprev=stoprev, inclusive=inclusive)
        return ancestor.lazyancestors(self._uncheckedparentrevs, revs,
                                      stoprev=stoprev, inclusive=inclusive)

    def descendants(self, revs):
        return dagop.descendantrevs(revs, self.revs, self.parentrevs)

    def findcommonmissing(self, common=None, heads=None):
        """Return a tuple of the ancestors of common and the ancestors of heads
        that are not ancestors of common. In revset terminology, we return the
        tuple:

          ::common, (::heads) - (::common)

        The list is sorted by revision number, meaning it is
        topologically sorted.

        'heads' and 'common' are both lists of node IDs.  If heads is
        not supplied, uses all of the revlog's heads.  If common is not
        supplied, uses nullid."""
        if common is None:
            common = [nullid]
        if heads is None:
            heads = self.heads()

        common = [self.rev(n) for n in common]
        heads = [self.rev(n) for n in heads]

        # we want the ancestors, but inclusive
        class lazyset(object):
            def __init__(self, lazyvalues):
                self.addedvalues = set()
                self.lazyvalues = lazyvalues

            def __contains__(self, value):
                return value in self.addedvalues or value in self.lazyvalues

            def __iter__(self):
                added = self.addedvalues
                for r in added:
                    yield r
                for r in self.lazyvalues:
                    if not r in added:
                        yield r

            def add(self, value):
                self.addedvalues.add(value)

            def update(self, values):
                self.addedvalues.update(values)

        has = lazyset(self.ancestors(common))
        has.add(nullrev)
        has.update(common)

        # take all ancestors from heads that aren't in has
        missing = set()
        visit = collections.deque(r for r in heads if r not in has)
        while visit:
            r = visit.popleft()
            if r in missing:
                continue
            else:
                missing.add(r)
                for p in self.parentrevs(r):
                    if p not in has:
                        visit.append(p)
        missing = list(missing)
        missing.sort()
        return has, [self.node(miss) for miss in missing]

    def incrementalmissingrevs(self, common=None):
        """Return an object that can be used to incrementally compute the
        revision numbers of the ancestors of arbitrary sets that are not
        ancestors of common. This is an ancestor.incrementalmissingancestors
        object.

        'common' is a list of revision numbers. If common is not supplied, uses
        nullrev.
        """
        if common is None:
            common = [nullrev]

        return ancestor.incrementalmissingancestors(self.parentrevs, common)

    def findmissingrevs(self, common=None, heads=None):
        """Return the revision numbers of the ancestors of heads that
        are not ancestors of common.

        More specifically, return a list of revision numbers corresponding to
        nodes N such that every N satisfies the following constraints:

          1. N is an ancestor of some node in 'heads'
          2. N is not an ancestor of any node in 'common'

        The list is sorted by revision number, meaning it is
        topologically sorted.

        'heads' and 'common' are both lists of revision numbers.  If heads is
        not supplied, uses all of the revlog's heads.  If common is not
        supplied, uses nullid."""
        if common is None:
            common = [nullrev]
        if heads is None:
            heads = self.headrevs()

        inc = self.incrementalmissingrevs(common=common)
        return inc.missingancestors(heads)

    def findmissing(self, common=None, heads=None):
        """Return the ancestors of heads that are not ancestors of common.

        More specifically, return a list of nodes N such that every N
        satisfies the following constraints:

          1. N is an ancestor of some node in 'heads'
          2. N is not an ancestor of any node in 'common'

        The list is sorted by revision number, meaning it is
        topologically sorted.

        'heads' and 'common' are both lists of node IDs.  If heads is
        not supplied, uses all of the revlog's heads.  If common is not
        supplied, uses nullid."""
        if common is None:
            common = [nullid]
        if heads is None:
            heads = self.heads()

        common = [self.rev(n) for n in common]
        heads = [self.rev(n) for n in heads]

        inc = self.incrementalmissingrevs(common=common)
        return [self.node(r) for r in inc.missingancestors(heads)]

    def nodesbetween(self, roots=None, heads=None):
        """Return a topological path from 'roots' to 'heads'.

        Return a tuple (nodes, outroots, outheads) where 'nodes' is a
        topologically sorted list of all nodes N that satisfy both of
        these constraints:

          1. N is a descendant of some node in 'roots'
          2. N is an ancestor of some node in 'heads'

        Every node is considered to be both a descendant and an ancestor
        of itself, so every reachable node in 'roots' and 'heads' will be
        included in 'nodes'.

        'outroots' is the list of reachable nodes in 'roots', i.e., the
        subset of 'roots' that is returned in 'nodes'.  Likewise,
        'outheads' is the subset of 'heads' that is also in 'nodes'.

        'roots' and 'heads' are both lists of node IDs.  If 'roots' is
        unspecified, uses nullid as the only root.  If 'heads' is
        unspecified, uses list of all of the revlog's heads."""
        nonodes = ([], [], [])
        if roots is not None:
            roots = list(roots)
            if not roots:
                return nonodes
            lowestrev = min([self.rev(n) for n in roots])
        else:
            roots = [nullid] # Everybody's a descendant of nullid
            lowestrev = nullrev
        if (lowestrev == nullrev) and (heads is None):
            # We want _all_ the nodes!
            return ([self.node(r) for r in self], [nullid], list(self.heads()))
        if heads is None:
            # All nodes are ancestors, so the latest ancestor is the last
            # node.
            highestrev = len(self) - 1
            # Set ancestors to None to signal that every node is an ancestor.
            ancestors = None
            # Set heads to an empty dictionary for later discovery of heads
            heads = {}
        else:
            heads = list(heads)
            if not heads:
                return nonodes
            ancestors = set()
            # Turn heads into a dictionary so we can remove 'fake' heads.
            # Also, later we will be using it to filter out the heads we can't
            # find from roots.
            heads = dict.fromkeys(heads, False)
            # Start at the top and keep marking parents until we're done.
            nodestotag = set(heads)
            # Remember where the top was so we can use it as a limit later.
            highestrev = max([self.rev(n) for n in nodestotag])
            while nodestotag:
                # grab a node to tag
                n = nodestotag.pop()
                # Never tag nullid
                if n == nullid:
                    continue
                # A node's revision number represents its place in a
                # topologically sorted list of nodes.
                r = self.rev(n)
                if r >= lowestrev:
                    if n not in ancestors:
                        # If we are possibly a descendant of one of the roots
                        # and we haven't already been marked as an ancestor
                        ancestors.add(n) # Mark as ancestor
                        # Add non-nullid parents to list of nodes to tag.
                        nodestotag.update([p for p in self.parents(n) if
                                           p != nullid])
                    elif n in heads: # We've seen it before, is it a fake head?
                        # So it is, real heads should not be the ancestors of
                        # any other heads.
                        heads.pop(n)
            if not ancestors:
                return nonodes
            # Now that we have our set of ancestors, we want to remove any
            # roots that are not ancestors.

            # If one of the roots was nullid, everything is included anyway.
            if lowestrev > nullrev:
                # But, since we weren't, let's recompute the lowest rev to not
                # include roots that aren't ancestors.

                # Filter out roots that aren't ancestors of heads
                roots = [root for root in roots if root in ancestors]
                # Recompute the lowest revision
                if roots:
                    lowestrev = min([self.rev(root) for root in roots])
                else:
                    # No more roots?  Return empty list
                    return nonodes
            else:
                # We are descending from nullid, and don't need to care about
                # any other roots.
                lowestrev = nullrev
                roots = [nullid]
        # Transform our roots list into a set.
        descendants = set(roots)
        # Also, keep the original roots so we can filter out roots that aren't
        # 'real' roots (i.e. are descended from other roots).
        roots = descendants.copy()
        # Our topologically sorted list of output nodes.
        orderedout = []
        # Don't start at nullid since we don't want nullid in our output list,
        # and if nullid shows up in descendants, empty parents will look like
        # they're descendants.
        for r in self.revs(start=max(lowestrev, 0), stop=highestrev + 1):
            n = self.node(r)
            isdescendant = False
            if lowestrev == nullrev:  # Everybody is a descendant of nullid
                isdescendant = True
            elif n in descendants:
                # n is already a descendant
                isdescendant = True
                # This check only needs to be done here because all the roots
                # will start being marked is descendants before the loop.
                if n in roots:
                    # If n was a root, check if it's a 'real' root.
                    p = tuple(self.parents(n))
                    # If any of its parents are descendants, it's not a root.
                    if (p[0] in descendants) or (p[1] in descendants):
                        roots.remove(n)
            else:
                p = tuple(self.parents(n))
                # A node is a descendant if either of its parents are
                # descendants.  (We seeded the dependents list with the roots
                # up there, remember?)
                if (p[0] in descendants) or (p[1] in descendants):
                    descendants.add(n)
                    isdescendant = True
            if isdescendant and ((ancestors is None) or (n in ancestors)):
                # Only include nodes that are both descendants and ancestors.
                orderedout.append(n)
                if (ancestors is not None) and (n in heads):
                    # We're trying to figure out which heads are reachable
                    # from roots.
                    # Mark this head as having been reached
                    heads[n] = True
                elif ancestors is None:
                    # Otherwise, we're trying to discover the heads.
                    # Assume this is a head because if it isn't, the next step
                    # will eventually remove it.
                    heads[n] = True
                    # But, obviously its parents aren't.
                    for p in self.parents(n):
                        heads.pop(p, None)
        heads = [head for head, flag in heads.iteritems() if flag]
        roots = list(roots)
        assert orderedout
        assert roots
        assert heads
        return (orderedout, roots, heads)

    def headrevs(self):
        try:
            return self.index.headrevs()
        except AttributeError:
            return self._headrevs()

    def computephases(self, roots):
        return self.index.computephasesmapsets(roots)

    def _headrevs(self):
        count = len(self)
        if not count:
            return [nullrev]
        # we won't iter over filtered rev so nobody is a head at start
        ishead = [0] * (count + 1)
        index = self.index
        for r in self:
            ishead[r] = 1  # I may be an head
            e = index[r]
            ishead[e[5]] = ishead[e[6]] = 0  # my parent are not
        return [r for r, val in enumerate(ishead) if val]

    def heads(self, start=None, stop=None):
        """return the list of all nodes that have no children

        if start is specified, only heads that are descendants of
        start will be returned
        if stop is specified, it will consider all the revs from stop
        as if they had no children
        """
        if start is None and stop is None:
            if not len(self):
                return [nullid]
            return [self.node(r) for r in self.headrevs()]

        if start is None:
            start = nullrev
        else:
            start = self.rev(start)

        stoprevs = set(self.rev(n) for n in stop or [])

        revs = dagop.headrevssubset(self.revs, self.parentrevs, startrev=start,
                                    stoprevs=stoprevs)

        return [self.node(rev) for rev in revs]

    def children(self, node):
        """find the children of a given node"""
        c = []
        p = self.rev(node)
        for r in self.revs(start=p + 1):
            prevs = [pr for pr in self.parentrevs(r) if pr != nullrev]
            if prevs:
                for pr in prevs:
                    if pr == p:
                        c.append(self.node(r))
            elif p == nullrev:
                c.append(self.node(r))
        return c

    def commonancestorsheads(self, a, b):
        """calculate all the heads of the common ancestors of nodes a and b"""
        a, b = self.rev(a), self.rev(b)
        ancs = self._commonancestorsheads(a, b)
        return pycompat.maplist(self.node, ancs)

    def _commonancestorsheads(self, *revs):
        """calculate all the heads of the common ancestors of revs"""
        try:
            ancs = self.index.commonancestorsheads(*revs)
        except (AttributeError, OverflowError): # C implementation failed
            ancs = ancestor.commonancestorsheads(self.parentrevs, *revs)
        return ancs

    def isancestor(self, a, b):
        """return True if node a is an ancestor of node b

        A revision is considered an ancestor of itself."""
        a, b = self.rev(a), self.rev(b)
        return self.isancestorrev(a, b)

    def isancestorrev(self, a, b):
        """return True if revision a is an ancestor of revision b

        A revision is considered an ancestor of itself.

        The implementation of this is trivial but the use of
        commonancestorsheads is not."""
        if a == nullrev:
            return True
        elif a == b:
            return True
        elif a > b:
            return False
        return a in self._commonancestorsheads(a, b)

    def ancestor(self, a, b):
        """calculate the "best" common ancestor of nodes a and b"""

        a, b = self.rev(a), self.rev(b)
        try:
            ancs = self.index.ancestors(a, b)
        except (AttributeError, OverflowError):
            ancs = ancestor.ancestors(self.parentrevs, a, b)
        if ancs:
            # choose a consistent winner when there's a tie
            return min(map(self.node, ancs))
        return nullid

    def _match(self, id):
        if isinstance(id, int):
            # rev
            return self.node(id)
        if len(id) == 20:
            # possibly a binary node
            # odds of a binary node being all hex in ASCII are 1 in 10**25
            try:
                node = id
                self.rev(node) # quick search the index
                return node
            except error.LookupError:
                pass # may be partial hex id
        try:
            # str(rev)
            rev = int(id)
            if "%d" % rev != id:
                raise ValueError
            if rev < 0:
                rev = len(self) + rev
            if rev < 0 or rev >= len(self):
                raise ValueError
            return self.node(rev)
        except (ValueError, OverflowError):
            pass
        if len(id) == 40:
            try:
                # a full hex nodeid?
                node = bin(id)
                self.rev(node)
                return node
            except (TypeError, error.LookupError):
                pass

    def _partialmatch(self, id):
        # we don't care wdirfilenodeids as they should be always full hash
        maybewdir = wdirhex.startswith(id)
        try:
            partial = self.index.partialmatch(id)
            if partial and self.hasnode(partial):
                if maybewdir:
                    # single 'ff...' match in radix tree, ambiguous with wdir
                    raise error.RevlogError
                return partial
            if maybewdir:
                # no 'ff...' match in radix tree, wdir identified
                raise error.WdirUnsupported
            return None
        except error.RevlogError:
            # parsers.c radix tree lookup gave multiple matches
            # fast path: for unfiltered changelog, radix tree is accurate
            if not getattr(self, 'filteredrevs', None):
                raise error.AmbiguousPrefixLookupError(
                    id, self.indexfile, _('ambiguous identifier'))
            # fall through to slow path that filters hidden revisions
        except (AttributeError, ValueError):
            # we are pure python, or key was too short to search radix tree
            pass

        if id in self._pcache:
            return self._pcache[id]

        if len(id) <= 40:
            try:
                # hex(node)[:...]
                l = len(id) // 2  # grab an even number of digits
                prefix = bin(id[:l * 2])
                nl = [e[7] for e in self.index if e[7].startswith(prefix)]
                nl = [n for n in nl if hex(n).startswith(id) and
                      self.hasnode(n)]
                if nullhex.startswith(id):
                    nl.append(nullid)
                if len(nl) > 0:
                    if len(nl) == 1 and not maybewdir:
                        self._pcache[id] = nl[0]
                        return nl[0]
                    raise error.AmbiguousPrefixLookupError(
                        id, self.indexfile, _('ambiguous identifier'))
                if maybewdir:
                    raise error.WdirUnsupported
                return None
            except TypeError:
                pass

    def lookup(self, id):
        """locate a node based on:
            - revision number or str(revision number)
            - nodeid or subset of hex nodeid
        """
        n = self._match(id)
        if n is not None:
            return n
        n = self._partialmatch(id)
        if n:
            return n

        raise error.LookupError(id, self.indexfile, _('no match found'))

    def shortest(self, node, minlength=1):
        """Find the shortest unambiguous prefix that matches node."""
        def isvalid(prefix):
            try:
                node = self._partialmatch(prefix)
            except error.AmbiguousPrefixLookupError:
                return False
            except error.WdirUnsupported:
                # single 'ff...' match
                return True
            if node is None:
                raise error.LookupError(node, self.indexfile, _('no node'))
            return True

        def maybewdir(prefix):
            return all(c == 'f' for c in prefix)

        hexnode = hex(node)

        def disambiguate(hexnode, minlength):
            """Disambiguate against wdirid."""
            for length in range(minlength, 41):
                prefix = hexnode[:length]
                if not maybewdir(prefix):
                    return prefix

        if not getattr(self, 'filteredrevs', None):
            try:
                length = max(self.index.shortest(node), minlength)
                return disambiguate(hexnode, length)
            except error.RevlogError:
                if node != wdirid:
                    raise error.LookupError(node, self.indexfile, _('no node'))
            except AttributeError:
                # Fall through to pure code
                pass

        if node == wdirid:
            for length in range(minlength, 41):
                prefix = hexnode[:length]
                if isvalid(prefix):
                    return prefix

        for length in range(minlength, 41):
            prefix = hexnode[:length]
            if isvalid(prefix):
                return disambiguate(hexnode, length)

    def cmp(self, node, text):
        """compare text with a given file revision

        returns True if text is different than what is stored.
        """
        p1, p2 = self.parents(node)
        return storageutil.hashrevisionsha1(text, p1, p2) != node

    def _cachesegment(self, offset, data):
        """Add a segment to the revlog cache.

        Accepts an absolute offset and the data that is at that location.
        """
        o, d = self._chunkcache
        # try to add to existing cache
        if o + len(d) == offset and len(d) + len(data) < _chunksize:
            self._chunkcache = o, d + data
        else:
            self._chunkcache = offset, data

    def _readsegment(self, offset, length, df=None):
        """Load a segment of raw data from the revlog.

        Accepts an absolute offset, length to read, and an optional existing
        file handle to read from.

        If an existing file handle is passed, it will be seeked and the
        original seek position will NOT be restored.

        Returns a str or buffer of raw byte data.
        """
        # Cache data both forward and backward around the requested
        # data, in a fixed size window. This helps speed up operations
        # involving reading the revlog backwards.
        cachesize = self._chunkcachesize
        realoffset = offset & ~(cachesize - 1)
        reallength = (((offset + length + cachesize) & ~(cachesize - 1))
                      - realoffset)
        with self._datareadfp(df) as df:
            df.seek(realoffset)
            d = df.read(reallength)
        self._cachesegment(realoffset, d)
        if offset != realoffset or reallength != length:
            return util.buffer(d, offset - realoffset, length)
        return d

    def _getsegment(self, offset, length, df=None):
        """Obtain a segment of raw data from the revlog.

        Accepts an absolute offset, length of bytes to obtain, and an
        optional file handle to the already-opened revlog. If the file
        handle is used, it's original seek position will not be preserved.

        Requests for data may be returned from a cache.

        Returns a str or a buffer instance of raw byte data.
        """
        o, d = self._chunkcache
        l = len(d)

        # is it in the cache?
        cachestart = offset - o
        cacheend = cachestart + length
        if cachestart >= 0 and cacheend <= l:
            if cachestart == 0 and cacheend == l:
                return d # avoid a copy
            return util.buffer(d, cachestart, cacheend - cachestart)

        return self._readsegment(offset, length, df=df)

    def _getsegmentforrevs(self, startrev, endrev, df=None):
        """Obtain a segment of raw data corresponding to a range of revisions.

        Accepts the start and end revisions and an optional already-open
        file handle to be used for reading. If the file handle is read, its
        seek position will not be preserved.

        Requests for data may be satisfied by a cache.

        Returns a 2-tuple of (offset, data) for the requested range of
        revisions. Offset is the integer offset from the beginning of the
        revlog and data is a str or buffer of the raw byte data.

        Callers will need to call ``self.start(rev)`` and ``self.length(rev)``
        to determine where each revision's data begins and ends.
        """
        # Inlined self.start(startrev) & self.end(endrev) for perf reasons
        # (functions are expensive).
        index = self.index
        istart = index[startrev]
        start = int(istart[0] >> 16)
        if startrev == endrev:
            end = start + istart[1]
        else:
            iend = index[endrev]
            end = int(iend[0] >> 16) + iend[1]

        if self._inline:
            start += (startrev + 1) * self._io.size
            end += (endrev + 1) * self._io.size
        length = end - start

        return start, self._getsegment(start, length, df=df)

    def _chunk(self, rev, df=None):
        """Obtain a single decompressed chunk for a revision.

        Accepts an integer revision and an optional already-open file handle
        to be used for reading. If used, the seek position of the file will not
        be preserved.

        Returns a str holding uncompressed data for the requested revision.
        """
        return self.decompress(self._getsegmentforrevs(rev, rev, df=df)[1])

    def _chunks(self, revs, df=None, targetsize=None):
        """Obtain decompressed chunks for the specified revisions.

        Accepts an iterable of numeric revisions that are assumed to be in
        ascending order. Also accepts an optional already-open file handle
        to be used for reading. If used, the seek position of the file will
        not be preserved.

        This function is similar to calling ``self._chunk()`` multiple times,
        but is faster.

        Returns a list with decompressed data for each requested revision.
        """
        if not revs:
            return []
        start = self.start
        length = self.length
        inline = self._inline
        iosize = self._io.size
        buffer = util.buffer

        l = []
        ladd = l.append

        if not self._withsparseread:
            slicedchunks = (revs,)
        else:
            slicedchunks = deltautil.slicechunk(self, revs,
                                                targetsize=targetsize)

        for revschunk in slicedchunks:
            firstrev = revschunk[0]
            # Skip trailing revisions with empty diff
            for lastrev in revschunk[::-1]:
                if length(lastrev) != 0:
                    break

            try:
                offset, data = self._getsegmentforrevs(firstrev, lastrev, df=df)
            except OverflowError:
                # issue4215 - we can't cache a run of chunks greater than
                # 2G on Windows
                return [self._chunk(rev, df=df) for rev in revschunk]

            decomp = self.decompress
            for rev in revschunk:
                chunkstart = start(rev)
                if inline:
                    chunkstart += (rev + 1) * iosize
                chunklength = length(rev)
                ladd(decomp(buffer(data, chunkstart - offset, chunklength)))

        return l

    def _chunkclear(self):
        """Clear the raw chunk cache."""
        self._chunkcache = (0, '')

    def deltaparent(self, rev):
        """return deltaparent of the given revision"""
        base = self.index[rev][3]
        if base == rev:
            return nullrev
        elif self._generaldelta:
            return base
        else:
            return rev - 1

    def issnapshot(self, rev):
        """tells whether rev is a snapshot
        """
        if rev == nullrev:
            return True
        deltap = self.deltaparent(rev)
        if deltap == nullrev:
            return True
        p1, p2 = self.parentrevs(rev)
        if deltap in (p1, p2):
            return False
        return self.issnapshot(deltap)

    def snapshotdepth(self, rev):
        """number of snapshot in the chain before this one"""
        if not self.issnapshot(rev):
            raise error.ProgrammingError('revision %d not a snapshot')
        return len(self._deltachain(rev)[0]) - 1

    def revdiff(self, rev1, rev2):
        """return or calculate a delta between two revisions

        The delta calculated is in binary form and is intended to be written to
        revlog data directly. So this function needs raw revision data.
        """
        if rev1 != nullrev and self.deltaparent(rev2) == rev1:
            return bytes(self._chunk(rev2))

        return mdiff.textdiff(self.revision(rev1, raw=True),
                              self.revision(rev2, raw=True))

    def revision(self, nodeorrev, _df=None, raw=False):
        """return an uncompressed revision of a given node or revision
        number.

        _df - an existing file handle to read from. (internal-only)
        raw - an optional argument specifying if the revision data is to be
        treated as raw data when applying flag transforms. 'raw' should be set
        to True when generating changegroups or in debug commands.
        """
        if isinstance(nodeorrev, int):
            rev = nodeorrev
            node = self.node(rev)
        else:
            node = nodeorrev
            rev = None

        cachedrev = None
        flags = None
        rawtext = None
        if node == nullid:
            return ""
        if self._revisioncache:
            if self._revisioncache[0] == node:
                # _cache only stores rawtext
                if raw:
                    return self._revisioncache[2]
                # duplicated, but good for perf
                if rev is None:
                    rev = self.rev(node)
                if flags is None:
                    flags = self.flags(rev)
                # no extra flags set, no flag processor runs, text = rawtext
                if flags == REVIDX_DEFAULT_FLAGS:
                    return self._revisioncache[2]
                # rawtext is reusable. need to run flag processor
                rawtext = self._revisioncache[2]

            cachedrev = self._revisioncache[1]

        # look up what we need to read
        if rawtext is None:
            if rev is None:
                rev = self.rev(node)

            chain, stopped = self._deltachain(rev, stoprev=cachedrev)
            if stopped:
                rawtext = self._revisioncache[2]

            # drop cache to save memory
            self._revisioncache = None

            targetsize = None
            rawsize = self.index[rev][2]
            if 0 <= rawsize:
                targetsize = 4 * rawsize

            bins = self._chunks(chain, df=_df, targetsize=targetsize)
            if rawtext is None:
                rawtext = bytes(bins[0])
                bins = bins[1:]

            rawtext = mdiff.patches(rawtext, bins)
            self._revisioncache = (node, rev, rawtext)

        if flags is None:
            if rev is None:
                rev = self.rev(node)
            flags = self.flags(rev)

        text, validatehash = self._processflags(rawtext, flags, 'read', raw=raw)
        if validatehash:
            self.checkhash(text, node, rev=rev)

        return text

    def hash(self, text, p1, p2):
        """Compute a node hash.

        Available as a function so that subclasses can replace the hash
        as needed.
        """
        return storageutil.hashrevisionsha1(text, p1, p2)

    def _processflags(self, text, flags, operation, raw=False):
        """Inspect revision data flags and applies transforms defined by
        registered flag processors.

        ``text`` - the revision data to process
        ``flags`` - the revision flags
        ``operation`` - the operation being performed (read or write)
        ``raw`` - an optional argument describing if the raw transform should be
        applied.

        This method processes the flags in the order (or reverse order if
        ``operation`` is 'write') defined by REVIDX_FLAGS_ORDER, applying the
        flag processors registered for present flags. The order of flags defined
        in REVIDX_FLAGS_ORDER needs to be stable to allow non-commutativity.

        Returns a 2-tuple of ``(text, validatehash)`` where ``text`` is the
        processed text and ``validatehash`` is a bool indicating whether the
        returned text should be checked for hash integrity.

        Note: If the ``raw`` argument is set, it has precedence over the
        operation and will only update the value of ``validatehash``.
        """
        # fast path: no flag processors will run
        if flags == 0:
            return text, True
        if not operation in ('read', 'write'):
            raise error.ProgrammingError(_("invalid '%s' operation") %
                                         operation)
        # Check all flags are known.
        if flags & ~REVIDX_KNOWN_FLAGS:
            raise error.RevlogError(_("incompatible revision flag '%#x'") %
                                    (flags & ~REVIDX_KNOWN_FLAGS))
        validatehash = True
        # Depending on the operation (read or write), the order might be
        # reversed due to non-commutative transforms.
        orderedflags = REVIDX_FLAGS_ORDER
        if operation == 'write':
            orderedflags = reversed(orderedflags)

        for flag in orderedflags:
            # If a flagprocessor has been registered for a known flag, apply the
            # related operation transform and update result tuple.
            if flag & flags:
                vhash = True

                if flag not in self._flagprocessors:
                    message = _("missing processor for flag '%#x'") % (flag)
                    raise error.RevlogError(message)

                processor = self._flagprocessors[flag]
                if processor is not None:
                    readtransform, writetransform, rawtransform = processor

                    if raw:
                        vhash = rawtransform(self, text)
                    elif operation == 'read':
                        text, vhash = readtransform(self, text)
                    else: # write operation
                        text, vhash = writetransform(self, text)
                validatehash = validatehash and vhash

        return text, validatehash

    def checkhash(self, text, node, p1=None, p2=None, rev=None):
        """Check node hash integrity.

        Available as a function so that subclasses can extend hash mismatch
        behaviors as needed.
        """
        try:
            if p1 is None and p2 is None:
                p1, p2 = self.parents(node)
            if node != self.hash(text, p1, p2):
                # Clear the revision cache on hash failure. The revision cache
                # only stores the raw revision and clearing the cache does have
                # the side-effect that we won't have a cache hit when the raw
                # revision data is accessed. But this case should be rare and
                # it is extra work to teach the cache about the hash
                # verification state.
                if self._revisioncache and self._revisioncache[0] == node:
                    self._revisioncache = None

                revornode = rev
                if revornode is None:
                    revornode = templatefilters.short(hex(node))
                raise error.RevlogError(_("integrity check failed on %s:%s")
                    % (self.indexfile, pycompat.bytestr(revornode)))
        except error.RevlogError:
            if self._censorable and storageutil.iscensoredtext(text):
                raise error.CensoredNodeError(self.indexfile, node, text)
            raise

    def _enforceinlinesize(self, tr, fp=None):
        """Check if the revlog is too big for inline and convert if so.

        This should be called after revisions are added to the revlog. If the
        revlog has grown too large to be an inline revlog, it will convert it
        to use multiple index and data files.
        """
        tiprev = len(self) - 1
        if (not self._inline or
            (self.start(tiprev) + self.length(tiprev)) < _maxinline):
            return

        trinfo = tr.find(self.indexfile)
        if trinfo is None:
            raise error.RevlogError(_("%s not found in the transaction")
                                    % self.indexfile)

        trindex = trinfo[2]
        if trindex is not None:
            dataoff = self.start(trindex)
        else:
            # revlog was stripped at start of transaction, use all leftover data
            trindex = len(self) - 1
            dataoff = self.end(tiprev)

        tr.add(self.datafile, dataoff)

        if fp:
            fp.flush()
            fp.close()

        with self._datafp('w') as df:
            for r in self:
                df.write(self._getsegmentforrevs(r, r)[1])

        with self._indexfp('w') as fp:
            self.version &= ~FLAG_INLINE_DATA
            self._inline = False
            io = self._io
            for i in self:
                e = io.packentry(self.index[i], self.node, self.version, i)
                fp.write(e)

            # the temp file replace the real index when we exit the context
            # manager

        tr.replace(self.indexfile, trindex * self._io.size)
        self._chunkclear()

    def _nodeduplicatecallback(self, transaction, node):
        """called when trying to add a node already stored.
        """

    def addrevision(self, text, transaction, link, p1, p2, cachedelta=None,
                    node=None, flags=REVIDX_DEFAULT_FLAGS, deltacomputer=None):
        """add a revision to the log

        text - the revision data to add
        transaction - the transaction object used for rollback
        link - the linkrev data to add
        p1, p2 - the parent nodeids of the revision
        cachedelta - an optional precomputed delta
        node - nodeid of revision; typically node is not specified, and it is
            computed by default as hash(text, p1, p2), however subclasses might
            use different hashing method (and override checkhash() in such case)
        flags - the known flags to set on the revision
        deltacomputer - an optional deltacomputer instance shared between
            multiple calls
        """
        if link == nullrev:
            raise error.RevlogError(_("attempted to add linkrev -1 to %s")
                                    % self.indexfile)

        if flags:
            node = node or self.hash(text, p1, p2)

        rawtext, validatehash = self._processflags(text, flags, 'write')

        # If the flag processor modifies the revision data, ignore any provided
        # cachedelta.
        if rawtext != text:
            cachedelta = None

        if len(rawtext) > _maxentrysize:
            raise error.RevlogError(
                _("%s: size of %d bytes exceeds maximum revlog storage of 2GiB")
                % (self.indexfile, len(rawtext)))

        node = node or self.hash(rawtext, p1, p2)
        if node in self.nodemap:
            return node

        if validatehash:
            self.checkhash(rawtext, node, p1=p1, p2=p2)

        return self.addrawrevision(rawtext, transaction, link, p1, p2, node,
                                   flags, cachedelta=cachedelta,
                                   deltacomputer=deltacomputer)

    def addrawrevision(self, rawtext, transaction, link, p1, p2, node, flags,
                       cachedelta=None, deltacomputer=None):
        """add a raw revision with known flags, node and parents
        useful when reusing a revision not stored in this revlog (ex: received
        over wire, or read from an external bundle).
        """
        dfh = None
        if not self._inline:
            dfh = self._datafp("a+")
        ifh = self._indexfp("a+")
        try:
            return self._addrevision(node, rawtext, transaction, link, p1, p2,
                                     flags, cachedelta, ifh, dfh,
                                     deltacomputer=deltacomputer)
        finally:
            if dfh:
                dfh.close()
            ifh.close()

    def compress(self, data):
        """Generate a possibly-compressed representation of data."""
        if not data:
            return '', data

        compressed = self._compressor.compress(data)

        if compressed:
            # The revlog compressor added the header in the returned data.
            return '', compressed

        if data[0:1] == '\0':
            return '', data
        return 'u', data

    def decompress(self, data):
        """Decompress a revlog chunk.

        The chunk is expected to begin with a header identifying the
        format type so it can be routed to an appropriate decompressor.
        """
        if not data:
            return data

        # Revlogs are read much more frequently than they are written and many
        # chunks only take microseconds to decompress, so performance is
        # important here.
        #
        # We can make a few assumptions about revlogs:
        #
        # 1) the majority of chunks will be compressed (as opposed to inline
        #    raw data).
        # 2) decompressing *any* data will likely by at least 10x slower than
        #    returning raw inline data.
        # 3) we want to prioritize common and officially supported compression
        #    engines
        #
        # It follows that we want to optimize for "decompress compressed data
        # when encoded with common and officially supported compression engines"
        # case over "raw data" and "data encoded by less common or non-official
        # compression engines." That is why we have the inline lookup first
        # followed by the compengines lookup.
        #
        # According to `hg perfrevlogchunks`, this is ~0.5% faster for zlib
        # compressed chunks. And this matters for changelog and manifest reads.
        t = data[0:1]

        if t == 'x':
            try:
                return _zlibdecompress(data)
            except zlib.error as e:
                raise error.RevlogError(_('revlog decompress error: %s') %
                                        stringutil.forcebytestr(e))
        # '\0' is more common than 'u' so it goes first.
        elif t == '\0':
            return data
        elif t == 'u':
            return util.buffer(data, 1)

        try:
            compressor = self._decompressors[t]
        except KeyError:
            try:
                engine = util.compengines.forrevlogheader(t)
                compressor = engine.revlogcompressor()
                self._decompressors[t] = compressor
            except KeyError:
                raise error.RevlogError(_('unknown compression type %r') % t)

        return compressor.decompress(data)

    def _addrevision(self, node, rawtext, transaction, link, p1, p2, flags,
                     cachedelta, ifh, dfh, alwayscache=False,
                     deltacomputer=None):
        """internal function to add revisions to the log

        see addrevision for argument descriptions.

        note: "addrevision" takes non-raw text, "_addrevision" takes raw text.

        if "deltacomputer" is not provided or None, a defaultdeltacomputer will
        be used.

        invariants:
        - rawtext is optional (can be None); if not set, cachedelta must be set.
          if both are set, they must correspond to each other.
        """
        if node == nullid:
            raise error.RevlogError(_("%s: attempt to add null revision") %
                                    self.indexfile)
        if node == wdirid or node in wdirfilenodeids:
            raise error.RevlogError(_("%s: attempt to add wdir revision") %
                                    self.indexfile)

        if self._inline:
            fh = ifh
        else:
            fh = dfh

        btext = [rawtext]

        curr = len(self)
        prev = curr - 1
        offset = self.end(prev)
        p1r, p2r = self.rev(p1), self.rev(p2)

        # full versions are inserted when the needed deltas
        # become comparable to the uncompressed text
        if rawtext is None:
            # need rawtext size, before changed by flag processors, which is
            # the non-raw size. use revlog explicitly to avoid filelog's extra
            # logic that might remove metadata size.
            textlen = mdiff.patchedsize(revlog.size(self, cachedelta[0]),
                                        cachedelta[1])
        else:
            textlen = len(rawtext)

        if deltacomputer is None:
            deltacomputer = deltautil.deltacomputer(self)

        revinfo = _revisioninfo(node, p1, p2, btext, textlen, cachedelta, flags)

        deltainfo = deltacomputer.finddeltainfo(revinfo, fh)

        e = (offset_type(offset, flags), deltainfo.deltalen, textlen,
             deltainfo.base, link, p1r, p2r, node)
        self.index.append(e)
        self.nodemap[node] = curr

        # Reset the pure node cache start lookup offset to account for new
        # revision.
        if self._nodepos is not None:
            self._nodepos = curr

        entry = self._io.packentry(e, self.node, self.version, curr)
        self._writeentry(transaction, ifh, dfh, entry, deltainfo.data,
                         link, offset)

        rawtext = btext[0]

        if alwayscache and rawtext is None:
            rawtext = deltacomputer.buildtext(revinfo, fh)

        if type(rawtext) == bytes: # only accept immutable objects
            self._revisioncache = (node, curr, rawtext)
        self._chainbasecache[curr] = deltainfo.chainbase
        return node

    def _writeentry(self, transaction, ifh, dfh, entry, data, link, offset):
        # Files opened in a+ mode have inconsistent behavior on various
        # platforms. Windows requires that a file positioning call be made
        # when the file handle transitions between reads and writes. See
        # 3686fa2b8eee and the mixedfilemodewrapper in windows.py. On other
        # platforms, Python or the platform itself can be buggy. Some versions
        # of Solaris have been observed to not append at the end of the file
        # if the file was seeked to before the end. See issue4943 for more.
        #
        # We work around this issue by inserting a seek() before writing.
        # Note: This is likely not necessary on Python 3.
        ifh.seek(0, os.SEEK_END)
        if dfh:
            dfh.seek(0, os.SEEK_END)

        curr = len(self) - 1
        if not self._inline:
            transaction.add(self.datafile, offset)
            transaction.add(self.indexfile, curr * len(entry))
            if data[0]:
                dfh.write(data[0])
            dfh.write(data[1])
            ifh.write(entry)
        else:
            offset += curr * self._io.size
            transaction.add(self.indexfile, offset, curr)
            ifh.write(entry)
            ifh.write(data[0])
            ifh.write(data[1])
            self._enforceinlinesize(transaction, ifh)

    def addgroup(self, deltas, linkmapper, transaction, addrevisioncb=None):
        """
        add a delta group

        given a set of deltas, add them to the revision log. the
        first delta is against its parent, which should be in our
        log, the rest are against the previous delta.

        If ``addrevisioncb`` is defined, it will be called with arguments of
        this revlog and the node that was added.
        """

        nodes = []

        r = len(self)
        end = 0
        if r:
            end = self.end(r - 1)
        ifh = self._indexfp("a+")
        isize = r * self._io.size
        if self._inline:
            transaction.add(self.indexfile, end + isize, r)
            dfh = None
        else:
            transaction.add(self.indexfile, isize, r)
            transaction.add(self.datafile, end)
            dfh = self._datafp("a+")
        def flush():
            if dfh:
                dfh.flush()
            ifh.flush()
        try:
            deltacomputer = deltautil.deltacomputer(self)
            # loop through our set of deltas
            for data in deltas:
                node, p1, p2, linknode, deltabase, delta, flags = data
                link = linkmapper(linknode)
                flags = flags or REVIDX_DEFAULT_FLAGS

                nodes.append(node)

                if node in self.nodemap:
                    self._nodeduplicatecallback(transaction, node)
                    # this can happen if two branches make the same change
                    continue

                for p in (p1, p2):
                    if p not in self.nodemap:
                        raise error.LookupError(p, self.indexfile,
                                                _('unknown parent'))

                if deltabase not in self.nodemap:
                    raise error.LookupError(deltabase, self.indexfile,
                                            _('unknown delta base'))

                baserev = self.rev(deltabase)

                if baserev != nullrev and self.iscensored(baserev):
                    # if base is censored, delta must be full replacement in a
                    # single patch operation
                    hlen = struct.calcsize(">lll")
                    oldlen = self.rawsize(baserev)
                    newlen = len(delta) - hlen
                    if delta[:hlen] != mdiff.replacediffheader(oldlen, newlen):
                        raise error.CensoredBaseError(self.indexfile,
                                                      self.node(baserev))

                if not flags and self._peek_iscensored(baserev, delta, flush):
                    flags |= REVIDX_ISCENSORED

                # We assume consumers of addrevisioncb will want to retrieve
                # the added revision, which will require a call to
                # revision(). revision() will fast path if there is a cache
                # hit. So, we tell _addrevision() to always cache in this case.
                # We're only using addgroup() in the context of changegroup
                # generation so the revision data can always be handled as raw
                # by the flagprocessor.
                self._addrevision(node, None, transaction, link,
                                  p1, p2, flags, (baserev, delta),
                                  ifh, dfh,
                                  alwayscache=bool(addrevisioncb),
                                  deltacomputer=deltacomputer)

                if addrevisioncb:
                    addrevisioncb(self, node)

                if not dfh and not self._inline:
                    # addrevision switched from inline to conventional
                    # reopen the index
                    ifh.close()
                    dfh = self._datafp("a+")
                    ifh = self._indexfp("a+")
        finally:
            if dfh:
                dfh.close()
            ifh.close()

        return nodes

    def iscensored(self, rev):
        """Check if a file revision is censored."""
        if not self._censorable:
            return False

        return self.flags(rev) & REVIDX_ISCENSORED

    def _peek_iscensored(self, baserev, delta, flush):
        """Quickly check if a delta produces a censored revision."""
        if not self._censorable:
            return False

        return storageutil.deltaiscensored(delta, baserev, self.rawsize)

    def getstrippoint(self, minlink):
        """find the minimum rev that must be stripped to strip the linkrev

        Returns a tuple containing the minimum rev and a set of all revs that
        have linkrevs that will be broken by this strip.
        """
        return storageutil.resolvestripinfo(minlink, len(self) - 1,
                                            self.headrevs(),
                                            self.linkrev, self.parentrevs)

    def strip(self, minlink, transaction):
        """truncate the revlog on the first revision with a linkrev >= minlink

        This function is called when we're stripping revision minlink and
        its descendants from the repository.

        We have to remove all revisions with linkrev >= minlink, because
        the equivalent changelog revisions will be renumbered after the
        strip.

        So we truncate the revlog on the first of these revisions, and
        trust that the caller has saved the revisions that shouldn't be
        removed and that it'll re-add them after this truncation.
        """
        if len(self) == 0:
            return

        rev, _ = self.getstrippoint(minlink)
        if rev == len(self):
            return

        # first truncate the files on disk
        end = self.start(rev)
        if not self._inline:
            transaction.add(self.datafile, end)
            end = rev * self._io.size
        else:
            end += rev * self._io.size

        transaction.add(self.indexfile, end)

        # then reset internal state in memory to forget those revisions
        self._revisioncache = None
        self._chaininfocache = {}
        self._chunkclear()
        for x in pycompat.xrange(rev, len(self)):
            del self.nodemap[self.node(x)]

        del self.index[rev:-1]
        self._nodepos = None

    def checksize(self):
        expected = 0
        if len(self):
            expected = max(0, self.end(len(self) - 1))

        try:
            with self._datafp() as f:
                f.seek(0, 2)
                actual = f.tell()
            dd = actual - expected
        except IOError as inst:
            if inst.errno != errno.ENOENT:
                raise
            dd = 0

        try:
            f = self.opener(self.indexfile)
            f.seek(0, 2)
            actual = f.tell()
            f.close()
            s = self._io.size
            i = max(0, actual // s)
            di = actual - (i * s)
            if self._inline:
                databytes = 0
                for r in self:
                    databytes += max(0, self.length(r))
                dd = 0
                di = actual - len(self) * s - databytes
        except IOError as inst:
            if inst.errno != errno.ENOENT:
                raise
            di = 0

        return (dd, di)

    def files(self):
        res = [self.indexfile]
        if not self._inline:
            res.append(self.datafile)
        return res

    def emitrevisions(self, nodes, nodesorder=None, revisiondata=False,
                      assumehaveparentrevisions=False, deltaprevious=False):
        if nodesorder not in ('nodes', 'storage', 'linear', None):
            raise error.ProgrammingError('unhandled value for nodesorder: %s' %
                                         nodesorder)

        if nodesorder is None and not self._generaldelta:
            nodesorder = 'storage'

        return storageutil.emitrevisions(
            self, nodes, nodesorder, revlogrevisiondelta,
            deltaparentfn=self.deltaparent,
            candeltafn=self.candelta,
            rawsizefn=self.rawsize,
            revdifffn=self.revdiff,
            flagsfn=self.flags,
            sendfulltext=not self._storedeltachains,
            revisiondata=revisiondata,
            assumehaveparentrevisions=assumehaveparentrevisions,
            deltaprevious=deltaprevious)

    DELTAREUSEALWAYS = 'always'
    DELTAREUSESAMEREVS = 'samerevs'
    DELTAREUSENEVER = 'never'

    DELTAREUSEFULLADD = 'fulladd'

    DELTAREUSEALL = {'always', 'samerevs', 'never', 'fulladd'}

    def clone(self, tr, destrevlog, addrevisioncb=None,
              deltareuse=DELTAREUSESAMEREVS, deltabothparents=None):
        """Copy this revlog to another, possibly with format changes.

        The destination revlog will contain the same revisions and nodes.
        However, it may not be bit-for-bit identical due to e.g. delta encoding
        differences.

        The ``deltareuse`` argument control how deltas from the existing revlog
        are preserved in the destination revlog. The argument can have the
        following values:

        DELTAREUSEALWAYS
           Deltas will always be reused (if possible), even if the destination
           revlog would not select the same revisions for the delta. This is the
           fastest mode of operation.
        DELTAREUSESAMEREVS
           Deltas will be reused if the destination revlog would pick the same
           revisions for the delta. This mode strikes a balance between speed
           and optimization.
        DELTAREUSENEVER
           Deltas will never be reused. This is the slowest mode of execution.
           This mode can be used to recompute deltas (e.g. if the diff/delta
           algorithm changes).

        Delta computation can be slow, so the choice of delta reuse policy can
        significantly affect run time.

        The default policy (``DELTAREUSESAMEREVS``) strikes a balance between
        two extremes. Deltas will be reused if they are appropriate. But if the
        delta could choose a better revision, it will do so. This means if you
        are converting a non-generaldelta revlog to a generaldelta revlog,
        deltas will be recomputed if the delta's parent isn't a parent of the
        revision.

        In addition to the delta policy, the ``deltabothparents`` argument
        controls whether to compute deltas against both parents for merges.
        By default, the current default is used.
        """
        if deltareuse not in self.DELTAREUSEALL:
            raise ValueError(_('value for deltareuse invalid: %s') % deltareuse)

        if len(destrevlog):
            raise ValueError(_('destination revlog is not empty'))

        if getattr(self, 'filteredrevs', None):
            raise ValueError(_('source revlog has filtered revisions'))
        if getattr(destrevlog, 'filteredrevs', None):
            raise ValueError(_('destination revlog has filtered revisions'))

        # lazydeltabase controls whether to reuse a cached delta, if possible.
        oldlazydeltabase = destrevlog._lazydeltabase
        oldamd = destrevlog._deltabothparents

        try:
            if deltareuse == self.DELTAREUSEALWAYS:
                destrevlog._lazydeltabase = True
            elif deltareuse == self.DELTAREUSESAMEREVS:
                destrevlog._lazydeltabase = False

            destrevlog._deltabothparents = deltabothparents or oldamd

            populatecachedelta = deltareuse in (self.DELTAREUSEALWAYS,
                                                self.DELTAREUSESAMEREVS)

            deltacomputer = deltautil.deltacomputer(destrevlog)
            index = self.index
            for rev in self:
                entry = index[rev]

                # Some classes override linkrev to take filtered revs into
                # account. Use raw entry from index.
                flags = entry[0] & 0xffff
                linkrev = entry[4]
                p1 = index[entry[5]][7]
                p2 = index[entry[6]][7]
                node = entry[7]

                # (Possibly) reuse the delta from the revlog if allowed and
                # the revlog chunk is a delta.
                cachedelta = None
                rawtext = None
                if populatecachedelta:
                    dp = self.deltaparent(rev)
                    if dp != nullrev:
                        cachedelta = (dp, bytes(self._chunk(rev)))

                if not cachedelta:
                    rawtext = self.revision(rev, raw=True)


                if deltareuse == self.DELTAREUSEFULLADD:
                    destrevlog.addrevision(rawtext, tr, linkrev, p1, p2,
                                           cachedelta=cachedelta,
                                           node=node, flags=flags,
                                           deltacomputer=deltacomputer)
                else:
                    ifh = destrevlog.opener(destrevlog.indexfile, 'a+',
                                            checkambig=False)
                    dfh = None
                    if not destrevlog._inline:
                        dfh = destrevlog.opener(destrevlog.datafile, 'a+')
                    try:
                        destrevlog._addrevision(node, rawtext, tr, linkrev, p1,
                                                p2, flags, cachedelta, ifh, dfh,
                                                deltacomputer=deltacomputer)
                    finally:
                        if dfh:
                            dfh.close()
                        ifh.close()

                if addrevisioncb:
                    addrevisioncb(self, rev, node)
        finally:
            destrevlog._lazydeltabase = oldlazydeltabase
            destrevlog._deltabothparents = oldamd

    def censorrevision(self, tr, censornode, tombstone=b''):
        if (self.version & 0xFFFF) == REVLOGV0:
            raise error.RevlogError(_('cannot censor with version %d revlogs') %
                                    self.version)

        censorrev = self.rev(censornode)
        tombstone = storageutil.packmeta({b'censored': tombstone}, b'')

        if len(tombstone) > self.rawsize(censorrev):
            raise error.Abort(_('censor tombstone must be no longer than '
                                'censored data'))

        # Rewriting the revlog in place is hard. Our strategy for censoring is
        # to create a new revlog, copy all revisions to it, then replace the
        # revlogs on transaction close.

        newindexfile = self.indexfile + b'.tmpcensored'
        newdatafile = self.datafile + b'.tmpcensored'

        # This is a bit dangerous. We could easily have a mismatch of state.
        newrl = revlog(self.opener, newindexfile, newdatafile,
                       censorable=True)
        newrl.version = self.version
        newrl._generaldelta = self._generaldelta
        newrl._io = self._io

        for rev in self.revs():
            node = self.node(rev)
            p1, p2 = self.parents(node)

            if rev == censorrev:
                newrl.addrawrevision(tombstone, tr, self.linkrev(censorrev),
                                     p1, p2, censornode, REVIDX_ISCENSORED)

                if newrl.deltaparent(rev) != nullrev:
                    raise error.Abort(_('censored revision stored as delta; '
                                        'cannot censor'),
                                      hint=_('censoring of revlogs is not '
                                             'fully implemented; please report '
                                             'this bug'))
                continue

            if self.iscensored(rev):
                if self.deltaparent(rev) != nullrev:
                    raise error.Abort(_('cannot censor due to censored '
                                        'revision having delta stored'))
                rawtext = self._chunk(rev)
            else:
                rawtext = self.revision(rev, raw=True)

            newrl.addrawrevision(rawtext, tr, self.linkrev(rev), p1, p2, node,
                                 self.flags(rev))

        tr.addbackup(self.indexfile, location='store')
        if not self._inline:
            tr.addbackup(self.datafile, location='store')

        self.opener.rename(newrl.indexfile, self.indexfile)
        if not self._inline:
            self.opener.rename(newrl.datafile, self.datafile)

        self.clearcaches()
        self._loadindex(self.version, None)

    def verifyintegrity(self, state):
        """Verifies the integrity of the revlog.

        Yields ``revlogproblem`` instances describing problems that are
        found.
        """
        dd, di = self.checksize()
        if dd:
            yield revlogproblem(error=_('data length off by %d bytes') % dd)
        if di:
            yield revlogproblem(error=_('index contains %d extra bytes') % di)

        version = self.version & 0xFFFF

        # The verifier tells us what version revlog we should be.
        if version != state['expectedversion']:
            yield revlogproblem(
                warning=_("warning: '%s' uses revlog format %d; expected %d") %
                        (self.indexfile, version, state['expectedversion']))

        state['skipread'] = set()

        for rev in self:
            node = self.node(rev)

            # Verify contents. 4 cases to care about:
            #
            #   common: the most common case
            #   rename: with a rename
            #   meta: file content starts with b'\1\n', the metadata
            #         header defined in filelog.py, but without a rename
            #   ext: content stored externally
            #
            # More formally, their differences are shown below:
            #
            #                       | common | rename | meta  | ext
            #  -------------------------------------------------------
            #   flags()             | 0      | 0      | 0     | not 0
            #   renamed()           | False  | True   | False | ?
            #   rawtext[0:2]=='\1\n'| False  | True   | True  | ?
            #
            # "rawtext" means the raw text stored in revlog data, which
            # could be retrieved by "revision(rev, raw=True)". "text"
            # mentioned below is "revision(rev, raw=False)".
            #
            # There are 3 different lengths stored physically:
            #  1. L1: rawsize, stored in revlog index
            #  2. L2: len(rawtext), stored in revlog data
            #  3. L3: len(text), stored in revlog data if flags==0, or
            #     possibly somewhere else if flags!=0
            #
            # L1 should be equal to L2. L3 could be different from them.
            # "text" may or may not affect commit hash depending on flag
            # processors (see revlog.addflagprocessor).
            #
            #              | common  | rename | meta  | ext
            # -------------------------------------------------
            #    rawsize() | L1      | L1     | L1    | L1
            #       size() | L1      | L2-LM  | L1(*) | L1 (?)
            # len(rawtext) | L2      | L2     | L2    | L2
            #    len(text) | L2      | L2     | L2    | L3
            #  len(read()) | L2      | L2-LM  | L2-LM | L3 (?)
            #
            # LM:  length of metadata, depending on rawtext
            # (*): not ideal, see comment in filelog.size
            # (?): could be "- len(meta)" if the resolved content has
            #      rename metadata
            #
            # Checks needed to be done:
            #  1. length check: L1 == L2, in all cases.
            #  2. hash check: depending on flag processor, we may need to
            #     use either "text" (external), or "rawtext" (in revlog).

            try:
                skipflags = state.get('skipflags', 0)
                if skipflags:
                    skipflags &= self.flags(rev)

                if skipflags:
                    state['skipread'].add(node)
                else:
                    # Side-effect: read content and verify hash.
                    self.revision(node)

                l1 = self.rawsize(rev)
                l2 = len(self.revision(node, raw=True))

                if l1 != l2:
                    yield revlogproblem(
                        error=_('unpacked size is %d, %d expected') % (l2, l1),
                        node=node)

            except error.CensoredNodeError:
                if state['erroroncensored']:
                    yield revlogproblem(error=_('censored file data'),
                                        node=node)
                    state['skipread'].add(node)
            except Exception as e:
                yield revlogproblem(
                    error=_('unpacking %s: %s') % (short(node),
                                                   stringutil.forcebytestr(e)),
                    node=node)
                state['skipread'].add(node)

    def storageinfo(self, exclusivefiles=False, sharedfiles=False,
                    revisionscount=False, trackedsize=False,
                    storedsize=False):
        d = {}

        if exclusivefiles:
            d['exclusivefiles'] = [(self.opener, self.indexfile)]
            if not self._inline:
                d['exclusivefiles'].append((self.opener, self.datafile))

        if sharedfiles:
            d['sharedfiles'] = []

        if revisionscount:
            d['revisionscount'] = len(self)

        if trackedsize:
            d['trackedsize'] = sum(map(self.rawsize, iter(self)))

        if storedsize:
            d['storedsize'] = sum(self.opener.stat(path).st_size
                                  for path in self.files())

        return d
