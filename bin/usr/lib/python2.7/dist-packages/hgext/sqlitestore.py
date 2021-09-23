# sqlitestore.py - Storage backend that uses SQLite
#
# Copyright 2018 Gregory Szorc <gregory.szorc@gmail.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

"""store repository data in SQLite (EXPERIMENTAL)

The sqlitestore extension enables the storage of repository data in SQLite.

This extension is HIGHLY EXPERIMENTAL. There are NO BACKWARDS COMPATIBILITY
GUARANTEES. This means that repositories created with this extension may
only be usable with the exact version of this extension/Mercurial that was
used. The extension attempts to enforce this in order to prevent repository
corruption.

In addition, several features are not yet supported or have known bugs:

* Only some data is stored in SQLite. Changeset, manifest, and other repository
  data is not yet stored in SQLite.
* Transactions are not robust. If the process is aborted at the right time
  during transaction close/rollback, the repository could be in an inconsistent
  state. This problem will diminish once all repository data is tracked by
  SQLite.
* Bundle repositories do not work (the ability to use e.g.
  `hg -R <bundle-file> log` to automatically overlay a bundle on top of the
  existing repository).
* Various other features don't work.

This extension should work for basic clone/pull, update, and commit workflows.
Some history rewriting operations may fail due to lack of support for bundle
repositories.

To use, activate the extension and set the ``storage.new-repo-backend`` config
option to ``sqlite`` to enable new repositories to use SQLite for storage.
"""

# To run the test suite with repos using SQLite by default, execute the
# following:
#
# HGREPOFEATURES="sqlitestore" run-tests.py \
#     --extra-config-opt extensions.sqlitestore= \
#     --extra-config-opt storage.new-repo-backend=sqlite

from __future__ import absolute_import

import hashlib
import sqlite3
import struct
import threading
import zlib

from mercurial.i18n import _
from mercurial.node import (
    nullid,
    nullrev,
    short,
)
from mercurial.thirdparty import (
    attr,
)
from mercurial import (
    ancestor,
    dagop,
    error,
    extensions,
    localrepo,
    mdiff,
    pycompat,
    registrar,
    repository,
    util,
    verify,
)
from mercurial.utils import (
    interfaceutil,
    storageutil,
)

try:
    from mercurial import zstd
    zstd.__version__
except ImportError:
    zstd = None

configtable = {}
configitem = registrar.configitem(configtable)

# experimental config: storage.sqlite.compression
configitem('storage', 'sqlite.compression',
           default='zstd' if zstd else 'zlib')

# Note for extension authors: ONLY specify testedwith = 'ships-with-hg-core' for
# extensions which SHIP WITH MERCURIAL. Non-mainline extensions should
# be specifying the version(s) of Mercurial they are tested with, or
# leave the attribute unspecified.
testedwith = 'ships-with-hg-core'

REQUIREMENT = b'exp-sqlite-001'
REQUIREMENT_ZSTD = b'exp-sqlite-comp-001=zstd'
REQUIREMENT_ZLIB = b'exp-sqlite-comp-001=zlib'
REQUIREMENT_NONE = b'exp-sqlite-comp-001=none'
REQUIREMENT_SHALLOW_FILES = b'exp-sqlite-shallow-files'

CURRENT_SCHEMA_VERSION = 1

COMPRESSION_NONE = 1
COMPRESSION_ZSTD = 2
COMPRESSION_ZLIB = 3

FLAG_CENSORED = 1
FLAG_MISSING_P1 = 2
FLAG_MISSING_P2 = 4

CREATE_SCHEMA = [
    # Deltas are stored as content-indexed blobs.
    # compression column holds COMPRESSION_* constant for how the
    # delta is encoded.

    r'CREATE TABLE delta ('
    r'    id INTEGER PRIMARY KEY, '
    r'    compression INTEGER NOT NULL, '
    r'    hash BLOB UNIQUE ON CONFLICT ABORT, '
    r'    delta BLOB NOT NULL '
    r')',

    # Tracked paths are denormalized to integers to avoid redundant
    # storage of the path name.
    r'CREATE TABLE filepath ('
    r'    id INTEGER PRIMARY KEY, '
    r'    path BLOB NOT NULL '
    r')',

    r'CREATE UNIQUE INDEX filepath_path '
    r'    ON filepath (path)',

    # We have a single table for all file revision data.
    # Each file revision is uniquely described by a (path, rev) and
    # (path, node).
    #
    # Revision data is stored as a pointer to the delta producing this
    # revision and the file revision whose delta should be applied before
    # that one. One can reconstruct the delta chain by recursively following
    # the delta base revision pointers until one encounters NULL.
    #
    # flags column holds bitwise integer flags controlling storage options.
    # These flags are defined by the FLAG_* constants.
    r'CREATE TABLE fileindex ('
    r'    id INTEGER PRIMARY KEY, '
    r'    pathid INTEGER REFERENCES filepath(id), '
    r'    revnum INTEGER NOT NULL, '
    r'    p1rev INTEGER NOT NULL, '
    r'    p2rev INTEGER NOT NULL, '
    r'    linkrev INTEGER NOT NULL, '
    r'    flags INTEGER NOT NULL, '
    r'    deltaid INTEGER REFERENCES delta(id), '
    r'    deltabaseid INTEGER REFERENCES fileindex(id), '
    r'    node BLOB NOT NULL '
    r')',

    r'CREATE UNIQUE INDEX fileindex_pathrevnum '
    r'    ON fileindex (pathid, revnum)',

    r'CREATE UNIQUE INDEX fileindex_pathnode '
    r'    ON fileindex (pathid, node)',

    # Provide a view over all file data for convenience.
    r'CREATE VIEW filedata AS '
    r'SELECT '
    r'    fileindex.id AS id, '
    r'    filepath.id AS pathid, '
    r'    filepath.path AS path, '
    r'    fileindex.revnum AS revnum, '
    r'    fileindex.node AS node, '
    r'    fileindex.p1rev AS p1rev, '
    r'    fileindex.p2rev AS p2rev, '
    r'    fileindex.linkrev AS linkrev, '
    r'    fileindex.flags AS flags, '
    r'    fileindex.deltaid AS deltaid, '
    r'    fileindex.deltabaseid AS deltabaseid '
    r'FROM filepath, fileindex '
    r'WHERE fileindex.pathid=filepath.id',

    r'PRAGMA user_version=%d' % CURRENT_SCHEMA_VERSION,
]

def resolvedeltachain(db, pathid, node, revisioncache,
                      stoprids, zstddctx=None):
    """Resolve a delta chain for a file node."""

    # TODO the "not in ({stops})" here is possibly slowing down the query
    # because it needs to perform the lookup on every recursive invocation.
    # This could possibly be faster if we created a temporary query with
    # baseid "poisoned" to null and limited the recursive filter to
    # "is not null".
    res = db.execute(
        r'WITH RECURSIVE '
        r'    deltachain(deltaid, baseid) AS ('
        r'        SELECT deltaid, deltabaseid FROM fileindex '
        r'            WHERE pathid=? AND node=? '
        r'        UNION ALL '
        r'        SELECT fileindex.deltaid, deltabaseid '
        r'            FROM fileindex, deltachain '
        r'            WHERE '
        r'                fileindex.id=deltachain.baseid '
        r'                AND deltachain.baseid IS NOT NULL '
        r'                AND fileindex.id NOT IN ({stops}) '
        r'    ) '
        r'SELECT deltachain.baseid, compression, delta '
        r'FROM deltachain, delta '
        r'WHERE delta.id=deltachain.deltaid'.format(
            stops=r','.join([r'?'] * len(stoprids))),
        tuple([pathid, node] + list(stoprids.keys())))

    deltas = []
    lastdeltabaseid = None

    for deltabaseid, compression, delta in res:
        lastdeltabaseid = deltabaseid

        if compression == COMPRESSION_ZSTD:
            delta = zstddctx.decompress(delta)
        elif compression == COMPRESSION_NONE:
            delta = delta
        elif compression == COMPRESSION_ZLIB:
            delta = zlib.decompress(delta)
        else:
            raise SQLiteStoreError('unhandled compression type: %d' %
                                   compression)

        deltas.append(delta)

    if lastdeltabaseid in stoprids:
        basetext = revisioncache[stoprids[lastdeltabaseid]]
    else:
        basetext = deltas.pop()

    deltas.reverse()
    fulltext = mdiff.patches(basetext, deltas)

    # SQLite returns buffer instances for blob columns on Python 2. This
    # type can propagate through the delta application layer. Because
    # downstream callers assume revisions are bytes, cast as needed.
    if not isinstance(fulltext, bytes):
        fulltext = bytes(delta)

    return fulltext

def insertdelta(db, compression, hash, delta):
    try:
        return db.execute(
            r'INSERT INTO delta (compression, hash, delta) '
            r'VALUES (?, ?, ?)',
            (compression, hash, delta)).lastrowid
    except sqlite3.IntegrityError:
        return db.execute(
            r'SELECT id FROM delta WHERE hash=?',
            (hash,)).fetchone()[0]

class SQLiteStoreError(error.StorageError):
    pass

@attr.s
class revisionentry(object):
    rid = attr.ib()
    rev = attr.ib()
    node = attr.ib()
    p1rev = attr.ib()
    p2rev = attr.ib()
    p1node = attr.ib()
    p2node = attr.ib()
    linkrev = attr.ib()
    flags = attr.ib()

@interfaceutil.implementer(repository.irevisiondelta)
@attr.s(slots=True)
class sqliterevisiondelta(object):
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
class sqliteproblem(object):
    warning = attr.ib(default=None)
    error = attr.ib(default=None)
    node = attr.ib(default=None)

@interfaceutil.implementer(repository.ifilestorage)
class sqlitefilestore(object):
    """Implements storage for an individual tracked path."""

    def __init__(self, db, path, compression):
        self._db = db
        self._path = path

        self._pathid = None

        # revnum -> node
        self._revtonode = {}
        # node -> revnum
        self._nodetorev = {}
        # node -> data structure
        self._revisions = {}

        self._revisioncache = util.lrucachedict(10)

        self._compengine = compression

        if compression == 'zstd':
            self._cctx = zstd.ZstdCompressor(level=3)
            self._dctx = zstd.ZstdDecompressor()
        else:
            self._cctx = None
            self._dctx = None

        self._refreshindex()

    def _refreshindex(self):
        self._revtonode = {}
        self._nodetorev = {}
        self._revisions = {}

        res = list(self._db.execute(
            r'SELECT id FROM filepath WHERE path=?', (self._path,)))

        if not res:
            self._pathid = None
            return

        self._pathid = res[0][0]

        res = self._db.execute(
            r'SELECT id, revnum, node, p1rev, p2rev, linkrev, flags '
            r'FROM fileindex '
            r'WHERE pathid=? '
            r'ORDER BY revnum ASC',
            (self._pathid,))

        for i, row in enumerate(res):
            rid, rev, node, p1rev, p2rev, linkrev, flags = row

            if i != rev:
                raise SQLiteStoreError(_('sqlite database has inconsistent '
                                         'revision numbers'))

            if p1rev == nullrev:
                p1node = nullid
            else:
                p1node = self._revtonode[p1rev]

            if p2rev == nullrev:
                p2node = nullid
            else:
                p2node = self._revtonode[p2rev]

            entry = revisionentry(
                rid=rid,
                rev=rev,
                node=node,
                p1rev=p1rev,
                p2rev=p2rev,
                p1node=p1node,
                p2node=p2node,
                linkrev=linkrev,
                flags=flags)

            self._revtonode[rev] = node
            self._nodetorev[node] = rev
            self._revisions[node] = entry

    # Start of ifileindex interface.

    def __len__(self):
        return len(self._revisions)

    def __iter__(self):
        return iter(pycompat.xrange(len(self._revisions)))

    def hasnode(self, node):
        if node == nullid:
            return False

        return node in self._nodetorev

    def revs(self, start=0, stop=None):
        return storageutil.iterrevs(len(self._revisions), start=start,
                                    stop=stop)

    def parents(self, node):
        if node == nullid:
            return nullid, nullid

        if node not in self._revisions:
            raise error.LookupError(node, self._path, _('no node'))

        entry = self._revisions[node]
        return entry.p1node, entry.p2node

    def parentrevs(self, rev):
        if rev == nullrev:
            return nullrev, nullrev

        if rev not in self._revtonode:
            raise IndexError(rev)

        entry = self._revisions[self._revtonode[rev]]
        return entry.p1rev, entry.p2rev

    def rev(self, node):
        if node == nullid:
            return nullrev

        if node not in self._nodetorev:
            raise error.LookupError(node, self._path, _('no node'))

        return self._nodetorev[node]

    def node(self, rev):
        if rev == nullrev:
            return nullid

        if rev not in self._revtonode:
            raise IndexError(rev)

        return self._revtonode[rev]

    def lookup(self, node):
        return storageutil.fileidlookup(self, node, self._path)

    def linkrev(self, rev):
        if rev == nullrev:
            return nullrev

        if rev not in self._revtonode:
            raise IndexError(rev)

        entry = self._revisions[self._revtonode[rev]]
        return entry.linkrev

    def iscensored(self, rev):
        if rev == nullrev:
            return False

        if rev not in self._revtonode:
            raise IndexError(rev)

        return self._revisions[self._revtonode[rev]].flags & FLAG_CENSORED

    def commonancestorsheads(self, node1, node2):
        rev1 = self.rev(node1)
        rev2 = self.rev(node2)

        ancestors = ancestor.commonancestorsheads(self.parentrevs, rev1, rev2)
        return pycompat.maplist(self.node, ancestors)

    def descendants(self, revs):
        # TODO we could implement this using a recursive SQL query, which
        # might be faster.
        return dagop.descendantrevs(revs, self.revs, self.parentrevs)

    def heads(self, start=None, stop=None):
        if start is None and stop is None:
            if not len(self):
                return [nullid]

        startrev = self.rev(start) if start is not None else nullrev
        stoprevs = {self.rev(n) for n in stop or []}

        revs = dagop.headrevssubset(self.revs, self.parentrevs,
                                    startrev=startrev, stoprevs=stoprevs)

        return [self.node(rev) for rev in revs]

    def children(self, node):
        rev = self.rev(node)

        res = self._db.execute(
            r'SELECT'
            r'  node '
            r'  FROM filedata '
            r'  WHERE path=? AND (p1rev=? OR p2rev=?) '
            r'  ORDER BY revnum ASC',
            (self._path, rev, rev))

        return [row[0] for row in res]

    # End of ifileindex interface.

    # Start of ifiledata interface.

    def size(self, rev):
        if rev == nullrev:
            return 0

        if rev not in self._revtonode:
            raise IndexError(rev)

        node = self._revtonode[rev]

        if self.renamed(node):
            return len(self.read(node))

        return len(self.revision(node))

    def revision(self, node, raw=False, _verifyhash=True):
        if node in (nullid, nullrev):
            return b''

        if isinstance(node, int):
            node = self.node(node)

        if node not in self._nodetorev:
            raise error.LookupError(node, self._path, _('no node'))

        if node in self._revisioncache:
            return self._revisioncache[node]

        # Because we have a fulltext revision cache, we are able to
        # short-circuit delta chain traversal and decompression as soon as
        # we encounter a revision in the cache.

        stoprids = {self._revisions[n].rid: n
                    for n in self._revisioncache}

        if not stoprids:
            stoprids[-1] = None

        fulltext = resolvedeltachain(self._db, self._pathid, node,
                                     self._revisioncache, stoprids,
                                     zstddctx=self._dctx)

        # Don't verify hashes if parent nodes were rewritten, as the hash
        # wouldn't verify.
        if self._revisions[node].flags & (FLAG_MISSING_P1 | FLAG_MISSING_P2):
            _verifyhash = False

        if _verifyhash:
            self._checkhash(fulltext, node)
            self._revisioncache[node] = fulltext

        return fulltext

    def read(self, node):
        return storageutil.filtermetadata(self.revision(node))

    def renamed(self, node):
        return storageutil.filerevisioncopied(self, node)

    def cmp(self, node, fulltext):
        return not storageutil.filedataequivalent(self, node, fulltext)

    def emitrevisions(self, nodes, nodesorder=None, revisiondata=False,
                      assumehaveparentrevisions=False, deltaprevious=False):
        if nodesorder not in ('nodes', 'storage', 'linear', None):
            raise error.ProgrammingError('unhandled value for nodesorder: %s' %
                                         nodesorder)

        nodes = [n for n in nodes if n != nullid]

        if not nodes:
            return

        # TODO perform in a single query.
        res = self._db.execute(
            r'SELECT revnum, deltaid FROM fileindex '
            r'WHERE pathid=? '
            r'    AND node in (%s)' % (r','.join([r'?'] * len(nodes))),
            tuple([self._pathid] + nodes))

        deltabases = {}

        for rev, deltaid in res:
            res = self._db.execute(
                r'SELECT revnum from fileindex WHERE pathid=? AND deltaid=?',
                (self._pathid, deltaid))
            deltabases[rev] = res.fetchone()[0]

        # TODO define revdifffn so we can use delta from storage.
        for delta in storageutil.emitrevisions(
            self, nodes, nodesorder, sqliterevisiondelta,
            deltaparentfn=deltabases.__getitem__,
            revisiondata=revisiondata,
            assumehaveparentrevisions=assumehaveparentrevisions,
            deltaprevious=deltaprevious):

            yield delta

    # End of ifiledata interface.

    # Start of ifilemutation interface.

    def add(self, filedata, meta, transaction, linkrev, p1, p2):
        if meta or filedata.startswith(b'\x01\n'):
            filedata = storageutil.packmeta(meta, filedata)

        return self.addrevision(filedata, transaction, linkrev, p1, p2)

    def addrevision(self, revisiondata, transaction, linkrev, p1, p2, node=None,
                    flags=0, cachedelta=None):
        if flags:
            raise SQLiteStoreError(_('flags not supported on revisions'))

        validatehash = node is not None
        node = node or storageutil.hashrevisionsha1(revisiondata, p1, p2)

        if validatehash:
            self._checkhash(revisiondata, node, p1, p2)

        if node in self._nodetorev:
            return node

        node = self._addrawrevision(node, revisiondata, transaction, linkrev,
                                    p1, p2)

        self._revisioncache[node] = revisiondata
        return node

    def addgroup(self, deltas, linkmapper, transaction, addrevisioncb=None,
                 maybemissingparents=False):
        nodes = []

        for node, p1, p2, linknode, deltabase, delta, wireflags in deltas:
            storeflags = 0

            if wireflags & repository.REVISION_FLAG_CENSORED:
                storeflags |= FLAG_CENSORED

            if wireflags & ~repository.REVISION_FLAG_CENSORED:
                raise SQLiteStoreError('unhandled revision flag')

            if maybemissingparents:
                if p1 != nullid and not self.hasnode(p1):
                    p1 = nullid
                    storeflags |= FLAG_MISSING_P1

                if p2 != nullid and not self.hasnode(p2):
                    p2 = nullid
                    storeflags |= FLAG_MISSING_P2

            baserev = self.rev(deltabase)

            # If base is censored, delta must be full replacement in a single
            # patch operation.
            if baserev != nullrev and self.iscensored(baserev):
                hlen = struct.calcsize('>lll')
                oldlen = len(self.revision(deltabase, raw=True,
                                           _verifyhash=False))
                newlen = len(delta) - hlen

                if delta[:hlen] != mdiff.replacediffheader(oldlen, newlen):
                    raise error.CensoredBaseError(self._path,
                                                  deltabase)

            if (not (storeflags & FLAG_CENSORED)
                and storageutil.deltaiscensored(
                    delta, baserev, lambda x: len(self.revision(x, raw=True)))):
                storeflags |= FLAG_CENSORED

            linkrev = linkmapper(linknode)

            nodes.append(node)

            if node in self._revisions:
                # Possibly reset parents to make them proper.
                entry = self._revisions[node]

                if entry.flags & FLAG_MISSING_P1 and p1 != nullid:
                    entry.p1node = p1
                    entry.p1rev = self._nodetorev[p1]
                    entry.flags &= ~FLAG_MISSING_P1

                    self._db.execute(
                        r'UPDATE fileindex SET p1rev=?, flags=? '
                        r'WHERE id=?',
                        (self._nodetorev[p1], entry.flags, entry.rid))

                if entry.flags & FLAG_MISSING_P2 and p2 != nullid:
                    entry.p2node = p2
                    entry.p2rev = self._nodetorev[p2]
                    entry.flags &= ~FLAG_MISSING_P2

                    self._db.execute(
                        r'UPDATE fileindex SET p2rev=?, flags=? '
                        r'WHERE id=?',
                        (self._nodetorev[p1], entry.flags, entry.rid))

                continue

            if deltabase == nullid:
                text = mdiff.patch(b'', delta)
                storedelta = None
            else:
                text = None
                storedelta = (deltabase, delta)

            self._addrawrevision(node, text, transaction, linkrev, p1, p2,
                                 storedelta=storedelta, flags=storeflags)

            if addrevisioncb:
                addrevisioncb(self, node)

        return nodes

    def censorrevision(self, tr, censornode, tombstone=b''):
        tombstone = storageutil.packmeta({b'censored': tombstone}, b'')

        # This restriction is cargo culted from revlogs and makes no sense for
        # SQLite, since columns can be resized at will.
        if len(tombstone) > len(self.revision(censornode, raw=True)):
            raise error.Abort(_('censor tombstone must be no longer than '
                                'censored data'))

        # We need to replace the censored revision's data with the tombstone.
        # But replacing that data will have implications for delta chains that
        # reference it.
        #
        # While "better," more complex strategies are possible, we do something
        # simple: we find delta chain children of the censored revision and we
        # replace those incremental deltas with fulltexts of their corresponding
        # revision. Then we delete the now-unreferenced delta and original
        # revision and insert a replacement.

        # Find the delta to be censored.
        censoreddeltaid = self._db.execute(
            r'SELECT deltaid FROM fileindex WHERE id=?',
            (self._revisions[censornode].rid,)).fetchone()[0]

        # Find all its delta chain children.
        # TODO once we support storing deltas for !files, we'll need to look
        # for those delta chains too.
        rows = list(self._db.execute(
            r'SELECT id, pathid, node FROM fileindex '
            r'WHERE deltabaseid=? OR deltaid=?',
            (censoreddeltaid, censoreddeltaid)))

        for row in rows:
            rid, pathid, node = row

            fulltext = resolvedeltachain(self._db, pathid, node, {}, {-1: None},
                                         zstddctx=self._dctx)

            deltahash = hashlib.sha1(fulltext).digest()

            if self._compengine == 'zstd':
                deltablob = self._cctx.compress(fulltext)
                compression = COMPRESSION_ZSTD
            elif self._compengine == 'zlib':
                deltablob = zlib.compress(fulltext)
                compression = COMPRESSION_ZLIB
            elif self._compengine == 'none':
                deltablob = fulltext
                compression = COMPRESSION_NONE
            else:
                raise error.ProgrammingError('unhandled compression engine: %s'
                                             % self._compengine)

            if len(deltablob) >= len(fulltext):
                deltablob = fulltext
                compression = COMPRESSION_NONE

            deltaid = insertdelta(self._db, compression, deltahash, deltablob)

            self._db.execute(
                r'UPDATE fileindex SET deltaid=?, deltabaseid=NULL '
                r'WHERE id=?', (deltaid, rid))

        # Now create the tombstone delta and replace the delta on the censored
        # node.
        deltahash = hashlib.sha1(tombstone).digest()
        tombstonedeltaid = insertdelta(self._db, COMPRESSION_NONE,
                                       deltahash, tombstone)

        flags = self._revisions[censornode].flags
        flags |= FLAG_CENSORED

        self._db.execute(
            r'UPDATE fileindex SET flags=?, deltaid=?, deltabaseid=NULL '
            r'WHERE pathid=? AND node=?',
            (flags, tombstonedeltaid, self._pathid, censornode))

        self._db.execute(
            r'DELETE FROM delta WHERE id=?', (censoreddeltaid,))

        self._refreshindex()
        self._revisioncache.clear()

    def getstrippoint(self, minlink):
        return storageutil.resolvestripinfo(minlink, len(self) - 1,
                                            [self.rev(n) for n in self.heads()],
                                            self.linkrev,
                                            self.parentrevs)

    def strip(self, minlink, transaction):
        if not len(self):
            return

        rev, _ignored = self.getstrippoint(minlink)

        if rev == len(self):
            return

        for rev in self.revs(rev):
            self._db.execute(
                r'DELETE FROM fileindex WHERE pathid=? AND node=?',
                (self._pathid, self.node(rev)))

        # TODO how should we garbage collect data in delta table?

        self._refreshindex()

    # End of ifilemutation interface.

    # Start of ifilestorage interface.

    def files(self):
        return []

    def storageinfo(self, exclusivefiles=False, sharedfiles=False,
                    revisionscount=False, trackedsize=False,
                    storedsize=False):
        d = {}

        if exclusivefiles:
            d['exclusivefiles'] = []

        if sharedfiles:
            # TODO list sqlite file(s) here.
            d['sharedfiles'] = []

        if revisionscount:
            d['revisionscount'] = len(self)

        if trackedsize:
            d['trackedsize'] = sum(len(self.revision(node))
                                       for node in self._nodetorev)

        if storedsize:
            # TODO implement this?
            d['storedsize'] = None

        return d

    def verifyintegrity(self, state):
        state['skipread'] = set()

        for rev in self:
            node = self.node(rev)

            try:
                self.revision(node)
            except Exception as e:
                yield sqliteproblem(
                    error=_('unpacking %s: %s') % (short(node), e),
                    node=node)

                state['skipread'].add(node)

    # End of ifilestorage interface.

    def _checkhash(self, fulltext, node, p1=None, p2=None):
        if p1 is None and p2 is None:
            p1, p2 = self.parents(node)

        if node == storageutil.hashrevisionsha1(fulltext, p1, p2):
            return

        try:
            del self._revisioncache[node]
        except KeyError:
            pass

        if storageutil.iscensoredtext(fulltext):
            raise error.CensoredNodeError(self._path, node, fulltext)

        raise SQLiteStoreError(_('integrity check failed on %s') %
                               self._path)

    def _addrawrevision(self, node, revisiondata, transaction, linkrev,
                        p1, p2, storedelta=None, flags=0):
        if self._pathid is None:
            res = self._db.execute(
                r'INSERT INTO filepath (path) VALUES (?)', (self._path,))
            self._pathid = res.lastrowid

        # For simplicity, always store a delta against p1.
        # TODO we need a lot more logic here to make behavior reasonable.

        if storedelta:
            deltabase, delta = storedelta

            if isinstance(deltabase, int):
                deltabase = self.node(deltabase)

        else:
            assert revisiondata is not None
            deltabase = p1

            if deltabase == nullid:
                delta = revisiondata
            else:
                delta = mdiff.textdiff(self.revision(self.rev(deltabase)),
                                       revisiondata)

        # File index stores a pointer to its delta and the parent delta.
        # The parent delta is stored via a pointer to the fileindex PK.
        if deltabase == nullid:
            baseid = None
        else:
            baseid = self._revisions[deltabase].rid

        # Deltas are stored with a hash of their content. This allows
        # us to de-duplicate. The table is configured to ignore conflicts
        # and it is faster to just insert and silently noop than to look
        # first.
        deltahash = hashlib.sha1(delta).digest()

        if self._compengine == 'zstd':
            deltablob = self._cctx.compress(delta)
            compression = COMPRESSION_ZSTD
        elif self._compengine == 'zlib':
            deltablob = zlib.compress(delta)
            compression = COMPRESSION_ZLIB
        elif self._compengine == 'none':
            deltablob = delta
            compression = COMPRESSION_NONE
        else:
            raise error.ProgrammingError('unhandled compression engine: %s' %
                                         self._compengine)

        # Don't store compressed data if it isn't practical.
        if len(deltablob) >= len(delta):
            deltablob = delta
            compression = COMPRESSION_NONE

        deltaid = insertdelta(self._db, compression, deltahash, deltablob)

        rev = len(self)

        if p1 == nullid:
            p1rev = nullrev
        else:
            p1rev = self._nodetorev[p1]

        if p2 == nullid:
            p2rev = nullrev
        else:
            p2rev = self._nodetorev[p2]

        rid = self._db.execute(
            r'INSERT INTO fileindex ('
            r'    pathid, revnum, node, p1rev, p2rev, linkrev, flags, '
            r'    deltaid, deltabaseid) '
            r'    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (self._pathid, rev, node, p1rev, p2rev, linkrev, flags,
             deltaid, baseid)
        ).lastrowid

        entry = revisionentry(
            rid=rid,
            rev=rev,
            node=node,
            p1rev=p1rev,
            p2rev=p2rev,
            p1node=p1,
            p2node=p2,
            linkrev=linkrev,
            flags=flags)

        self._nodetorev[node] = rev
        self._revtonode[rev] = node
        self._revisions[node] = entry

        return node

class sqliterepository(localrepo.localrepository):
    def cancopy(self):
        return False

    def transaction(self, *args, **kwargs):
        current = self.currenttransaction()

        tr = super(sqliterepository, self).transaction(*args, **kwargs)

        if current:
            return tr

        self._dbconn.execute(r'BEGIN TRANSACTION')

        def committransaction(_):
            self._dbconn.commit()

        tr.addfinalize('sqlitestore', committransaction)

        return tr

    @property
    def _dbconn(self):
        # SQLite connections can only be used on the thread that created
        # them. In most cases, this "just works." However, hgweb uses
        # multiple threads.
        tid = threading.current_thread().ident

        if self._db:
            if self._db[0] == tid:
                return self._db[1]

        db = makedb(self.svfs.join('db.sqlite'))
        self._db = (tid, db)

        return db

def makedb(path):
    """Construct a database handle for a database at path."""

    db = sqlite3.connect(path)
    db.text_factory = bytes

    res = db.execute(r'PRAGMA user_version').fetchone()[0]

    # New database.
    if res == 0:
        for statement in CREATE_SCHEMA:
            db.execute(statement)

        db.commit()

    elif res == CURRENT_SCHEMA_VERSION:
        pass

    else:
        raise error.Abort(_('sqlite database has unrecognized version'))

    db.execute(r'PRAGMA journal_mode=WAL')

    return db

def featuresetup(ui, supported):
    supported.add(REQUIREMENT)

    if zstd:
        supported.add(REQUIREMENT_ZSTD)

    supported.add(REQUIREMENT_ZLIB)
    supported.add(REQUIREMENT_NONE)
    supported.add(REQUIREMENT_SHALLOW_FILES)
    supported.add(repository.NARROW_REQUIREMENT)

def newreporequirements(orig, ui, createopts):
    if createopts['backend'] != 'sqlite':
        return orig(ui, createopts)

    # This restriction can be lifted once we have more confidence.
    if 'sharedrepo' in createopts:
        raise error.Abort(_('shared repositories not supported with SQLite '
                            'store'))

    # This filtering is out of an abundance of caution: we want to ensure
    # we honor creation options and we do that by annotating exactly the
    # creation options we recognize.
    known = {
        'narrowfiles',
        'backend',
        'shallowfilestore',
    }

    unsupported = set(createopts) - known
    if unsupported:
        raise error.Abort(_('SQLite store does not support repo creation '
                            'option: %s') % ', '.join(sorted(unsupported)))

    # Since we're a hybrid store that still relies on revlogs, we fall back
    # to using the revlogv1 backend's storage requirements then adding our
    # own requirement.
    createopts['backend'] = 'revlogv1'
    requirements = orig(ui, createopts)
    requirements.add(REQUIREMENT)

    compression = ui.config('storage', 'sqlite.compression')

    if compression == 'zstd' and not zstd:
        raise error.Abort(_('storage.sqlite.compression set to "zstd" but '
                            'zstandard compression not available to this '
                            'Mercurial install'))

    if compression == 'zstd':
        requirements.add(REQUIREMENT_ZSTD)
    elif compression == 'zlib':
        requirements.add(REQUIREMENT_ZLIB)
    elif compression == 'none':
        requirements.add(REQUIREMENT_NONE)
    else:
        raise error.Abort(_('unknown compression engine defined in '
                            'storage.sqlite.compression: %s') % compression)

    if createopts.get('shallowfilestore'):
        requirements.add(REQUIREMENT_SHALLOW_FILES)

    return requirements

@interfaceutil.implementer(repository.ilocalrepositoryfilestorage)
class sqlitefilestorage(object):
    """Repository file storage backed by SQLite."""
    def file(self, path):
        if path[0] == b'/':
            path = path[1:]

        if REQUIREMENT_ZSTD in self.requirements:
            compression = 'zstd'
        elif REQUIREMENT_ZLIB in self.requirements:
            compression = 'zlib'
        elif REQUIREMENT_NONE in self.requirements:
            compression = 'none'
        else:
            raise error.Abort(_('unable to determine what compression engine '
                                'to use for SQLite storage'))

        return sqlitefilestore(self._dbconn, path, compression)

def makefilestorage(orig, requirements, features, **kwargs):
    """Produce a type conforming to ``ilocalrepositoryfilestorage``."""
    if REQUIREMENT in requirements:
        if REQUIREMENT_SHALLOW_FILES in requirements:
            features.add(repository.REPO_FEATURE_SHALLOW_FILE_STORAGE)

        return sqlitefilestorage
    else:
        return orig(requirements=requirements, features=features, **kwargs)

def makemain(orig, ui, requirements, **kwargs):
    if REQUIREMENT in requirements:
        if REQUIREMENT_ZSTD in requirements and not zstd:
            raise error.Abort(_('repository uses zstandard compression, which '
                                'is not available to this Mercurial install'))

        return sqliterepository

    return orig(requirements=requirements, **kwargs)

def verifierinit(orig, self, *args, **kwargs):
    orig(self, *args, **kwargs)

    # We don't care that files in the store don't align with what is
    # advertised. So suppress these warnings.
    self.warnorphanstorefiles = False

def extsetup(ui):
    localrepo.featuresetupfuncs.add(featuresetup)
    extensions.wrapfunction(localrepo, 'newreporequirements',
                            newreporequirements)
    extensions.wrapfunction(localrepo, 'makefilestorage',
                            makefilestorage)
    extensions.wrapfunction(localrepo, 'makemain',
                            makemain)
    extensions.wrapfunction(verify.verifier, '__init__',
                            verifierinit)

def reposetup(ui, repo):
    if isinstance(repo, sqliterepository):
        repo._db = None

    # TODO check for bundlerepository?
