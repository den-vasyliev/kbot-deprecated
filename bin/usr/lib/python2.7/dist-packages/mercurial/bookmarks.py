# Mercurial bookmark support code
#
# Copyright 2008 David Soria Parra <dsp@php.net>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import errno
import struct

from .i18n import _
from .node import (
    bin,
    hex,
    short,
    wdirid,
)
from . import (
    encoding,
    error,
    obsutil,
    pycompat,
    scmutil,
    txnutil,
    util,
)

# label constants
# until 3.5, bookmarks.current was the advertised name, not
# bookmarks.active, so we must use both to avoid breaking old
# custom styles
activebookmarklabel = 'bookmarks.active bookmarks.current'

def _getbkfile(repo):
    """Hook so that extensions that mess with the store can hook bm storage.

    For core, this just handles wether we should see pending
    bookmarks or the committed ones. Other extensions (like share)
    may need to tweak this behavior further.
    """
    fp, pending = txnutil.trypending(repo.root, repo.vfs, 'bookmarks')
    return fp

class bmstore(object):
    """Storage for bookmarks.

    This object should do all bookmark-related reads and writes, so
    that it's fairly simple to replace the storage underlying
    bookmarks without having to clone the logic surrounding
    bookmarks. This type also should manage the active bookmark, if
    any.

    This particular bmstore implementation stores bookmarks as
    {hash}\s{name}\n (the same format as localtags) in
    .hg/bookmarks. The mapping is stored as {name: nodeid}.
    """

    def __init__(self, repo):
        self._repo = repo
        self._refmap = refmap = {}  # refspec: node
        self._nodemap = nodemap = {}  # node: sorted([refspec, ...])
        self._clean = True
        self._aclean = True
        nm = repo.changelog.nodemap
        tonode = bin # force local lookup
        try:
            with _getbkfile(repo) as bkfile:
                for line in bkfile:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        sha, refspec = line.split(' ', 1)
                        node = tonode(sha)
                        if node in nm:
                            refspec = encoding.tolocal(refspec)
                            refmap[refspec] = node
                            nrefs = nodemap.get(node)
                            if nrefs is None:
                                nodemap[node] = [refspec]
                            else:
                                nrefs.append(refspec)
                                if nrefs[-2] > refspec:
                                    # bookmarks weren't sorted before 4.5
                                    nrefs.sort()
                    except (TypeError, ValueError):
                        # TypeError:
                        # - bin(...)
                        # ValueError:
                        # - node in nm, for non-20-bytes entry
                        # - split(...), for string without ' '
                        repo.ui.warn(_('malformed line in .hg/bookmarks: %r\n')
                                     % pycompat.bytestr(line))
        except IOError as inst:
            if inst.errno != errno.ENOENT:
                raise
        self._active = _readactive(repo, self)

    @property
    def active(self):
        return self._active

    @active.setter
    def active(self, mark):
        if mark is not None and mark not in self._refmap:
            raise AssertionError('bookmark %s does not exist!' % mark)

        self._active = mark
        self._aclean = False

    def __len__(self):
        return len(self._refmap)

    def __iter__(self):
        return iter(self._refmap)

    def iteritems(self):
        return self._refmap.iteritems()

    def items(self):
        return self._refmap.items()

    # TODO: maybe rename to allnames()?
    def keys(self):
        return self._refmap.keys()

    # TODO: maybe rename to allnodes()? but nodes would have to be deduplicated
    # could be self._nodemap.keys()
    def values(self):
        return self._refmap.values()

    def __contains__(self, mark):
        return mark in self._refmap

    def __getitem__(self, mark):
        return self._refmap[mark]

    def get(self, mark, default=None):
        return self._refmap.get(mark, default)

    def _set(self, mark, node):
        self._clean = False
        if mark in self._refmap:
            self._del(mark)
        self._refmap[mark] = node
        nrefs = self._nodemap.get(node)
        if nrefs is None:
            self._nodemap[node] = [mark]
        else:
            nrefs.append(mark)
            nrefs.sort()

    def _del(self, mark):
        self._clean = False
        node = self._refmap.pop(mark)
        nrefs = self._nodemap[node]
        if len(nrefs) == 1:
            assert nrefs[0] == mark
            del self._nodemap[node]
        else:
            nrefs.remove(mark)

    def names(self, node):
        """Return a sorted list of bookmarks pointing to the specified node"""
        return self._nodemap.get(node, [])

    def changectx(self, mark):
        node = self._refmap[mark]
        return self._repo[node]

    def applychanges(self, repo, tr, changes):
        """Apply a list of changes to bookmarks
        """
        bmchanges = tr.changes.get('bookmarks')
        for name, node in changes:
            old = self._refmap.get(name)
            if node is None:
                self._del(name)
            else:
                self._set(name, node)
            if bmchanges is not None:
                # if a previous value exist preserve the "initial" value
                previous = bmchanges.get(name)
                if previous is not None:
                    old = previous[0]
                bmchanges[name] = (old, node)
        self._recordchange(tr)

    def _recordchange(self, tr):
        """record that bookmarks have been changed in a transaction

        The transaction is then responsible for updating the file content."""
        tr.addfilegenerator('bookmarks', ('bookmarks',), self._write,
                            location='plain')
        tr.hookargs['bookmark_moved'] = '1'

    def _writerepo(self, repo):
        """Factored out for extensibility"""
        rbm = repo._bookmarks
        if rbm.active not in self._refmap:
            rbm.active = None
            rbm._writeactive()

        with repo.wlock():
            file_ = repo.vfs('bookmarks', 'w', atomictemp=True,
                             checkambig=True)
            try:
                self._write(file_)
            except: # re-raises
                file_.discard()
                raise
            finally:
                file_.close()

    def _writeactive(self):
        if self._aclean:
            return
        with self._repo.wlock():
            if self._active is not None:
                f = self._repo.vfs('bookmarks.current', 'w', atomictemp=True,
                                   checkambig=True)
                try:
                    f.write(encoding.fromlocal(self._active))
                finally:
                    f.close()
            else:
                self._repo.vfs.tryunlink('bookmarks.current')
        self._aclean = True

    def _write(self, fp):
        for name, node in sorted(self._refmap.iteritems()):
            fp.write("%s %s\n" % (hex(node), encoding.fromlocal(name)))
        self._clean = True
        self._repo.invalidatevolatilesets()

    def expandname(self, bname):
        if bname == '.':
            if self.active:
                return self.active
            else:
                raise error.RepoLookupError(_("no active bookmark"))
        return bname

    def checkconflict(self, mark, force=False, target=None):
        """check repo for a potential clash of mark with an existing bookmark,
        branch, or hash

        If target is supplied, then check that we are moving the bookmark
        forward.

        If force is supplied, then forcibly move the bookmark to a new commit
        regardless if it is a move forward.

        If divergent bookmark are to be deleted, they will be returned as list.
        """
        cur = self._repo['.'].node()
        if mark in self._refmap and not force:
            if target:
                if self._refmap[mark] == target and target == cur:
                    # re-activating a bookmark
                    return []
                rev = self._repo[target].rev()
                anc = self._repo.changelog.ancestors([rev])
                bmctx = self.changectx(mark)
                divs = [self._refmap[b] for b in self._refmap
                        if b.split('@', 1)[0] == mark.split('@', 1)[0]]

                # allow resolving a single divergent bookmark even if moving
                # the bookmark across branches when a revision is specified
                # that contains a divergent bookmark
                if bmctx.rev() not in anc and target in divs:
                    return divergent2delete(self._repo, [target], mark)

                deletefrom = [b for b in divs
                              if self._repo[b].rev() in anc or b == target]
                delbms = divergent2delete(self._repo, deletefrom, mark)
                if validdest(self._repo, bmctx, self._repo[target]):
                    self._repo.ui.status(
                        _("moving bookmark '%s' forward from %s\n") %
                        (mark, short(bmctx.node())))
                    return delbms
            raise error.Abort(_("bookmark '%s' already exists "
                                "(use -f to force)") % mark)
        if ((mark in self._repo.branchmap() or
             mark == self._repo.dirstate.branch()) and not force):
            raise error.Abort(
                _("a bookmark cannot have the name of an existing branch"))
        if len(mark) > 3 and not force:
            try:
                shadowhash = scmutil.isrevsymbol(self._repo, mark)
            except error.LookupError:  # ambiguous identifier
                shadowhash = False
            if shadowhash:
                self._repo.ui.warn(
                    _("bookmark %s matches a changeset hash\n"
                      "(did you leave a -r out of an 'hg bookmark' "
                      "command?)\n")
                    % mark)
        return []

def _readactive(repo, marks):
    """
    Get the active bookmark. We can have an active bookmark that updates
    itself as we commit. This function returns the name of that bookmark.
    It is stored in .hg/bookmarks.current
    """
    mark = None
    try:
        file = repo.vfs('bookmarks.current')
    except IOError as inst:
        if inst.errno != errno.ENOENT:
            raise
        return None
    try:
        # No readline() in osutil.posixfile, reading everything is
        # cheap.
        # Note that it's possible for readlines() here to raise
        # IOError, since we might be reading the active mark over
        # static-http which only tries to load the file when we try
        # to read from it.
        mark = encoding.tolocal((file.readlines() or [''])[0])
        if mark == '' or mark not in marks:
            mark = None
    except IOError as inst:
        if inst.errno != errno.ENOENT:
            raise
        return None
    finally:
        file.close()
    return mark

def activate(repo, mark):
    """
    Set the given bookmark to be 'active', meaning that this bookmark will
    follow new commits that are made.
    The name is recorded in .hg/bookmarks.current
    """
    repo._bookmarks.active = mark
    repo._bookmarks._writeactive()

def deactivate(repo):
    """
    Unset the active bookmark in this repository.
    """
    repo._bookmarks.active = None
    repo._bookmarks._writeactive()

def isactivewdirparent(repo):
    """
    Tell whether the 'active' bookmark (the one that follows new commits)
    points to one of the parents of the current working directory (wdir).

    While this is normally the case, it can on occasion be false; for example,
    immediately after a pull, the active bookmark can be moved to point
    to a place different than the wdir. This is solved by running `hg update`.
    """
    mark = repo._activebookmark
    marks = repo._bookmarks
    parents = [p.node() for p in repo[None].parents()]
    return (mark in marks and marks[mark] in parents)

def divergent2delete(repo, deletefrom, bm):
    """find divergent versions of bm on nodes in deletefrom.

    the list of bookmark to delete."""
    todelete = []
    marks = repo._bookmarks
    divergent = [b for b in marks if b.split('@', 1)[0] == bm.split('@', 1)[0]]
    for mark in divergent:
        if mark == '@' or '@' not in mark:
            # can't be divergent by definition
            continue
        if mark and marks[mark] in deletefrom:
            if mark != bm:
                todelete.append(mark)
    return todelete

def headsforactive(repo):
    """Given a repo with an active bookmark, return divergent bookmark nodes.

    Args:
      repo: A repository with an active bookmark.

    Returns:
      A list of binary node ids that is the full list of other
      revisions with bookmarks divergent from the active bookmark. If
      there were no divergent bookmarks, then this list will contain
      only one entry.
    """
    if not repo._activebookmark:
        raise ValueError(
            'headsforactive() only makes sense with an active bookmark')
    name = repo._activebookmark.split('@', 1)[0]
    heads = []
    for mark, n in repo._bookmarks.iteritems():
        if mark.split('@', 1)[0] == name:
            heads.append(n)
    return heads

def calculateupdate(ui, repo):
    '''Return a tuple (activemark, movemarkfrom) indicating the active bookmark
    and where to move the active bookmark from, if needed.'''
    checkout, movemarkfrom = None, None
    activemark = repo._activebookmark
    if isactivewdirparent(repo):
        movemarkfrom = repo['.'].node()
    elif activemark:
        ui.status(_("updating to active bookmark %s\n") % activemark)
        checkout = activemark
    return (checkout, movemarkfrom)

def update(repo, parents, node):
    deletefrom = parents
    marks = repo._bookmarks
    active = marks.active
    if not active:
        return False

    bmchanges = []
    if marks[active] in parents:
        new = repo[node]
        divs = [marks.changectx(b) for b in marks
                if b.split('@', 1)[0] == active.split('@', 1)[0]]
        anc = repo.changelog.ancestors([new.rev()])
        deletefrom = [b.node() for b in divs if b.rev() in anc or b == new]
        if validdest(repo, marks.changectx(active), new):
            bmchanges.append((active, new.node()))

    for bm in divergent2delete(repo, deletefrom, active):
        bmchanges.append((bm, None))

    if bmchanges:
        with repo.lock(), repo.transaction('bookmark') as tr:
            marks.applychanges(repo, tr, bmchanges)
    return bool(bmchanges)

def listbinbookmarks(repo):
    # We may try to list bookmarks on a repo type that does not
    # support it (e.g., statichttprepository).
    marks = getattr(repo, '_bookmarks', {})

    hasnode = repo.changelog.hasnode
    for k, v in marks.iteritems():
        # don't expose local divergent bookmarks
        if hasnode(v) and ('@' not in k or k.endswith('@')):
            yield k, v

def listbookmarks(repo):
    d = {}
    for book, node in listbinbookmarks(repo):
        d[book] = hex(node)
    return d

def pushbookmark(repo, key, old, new):
    with repo.wlock(), repo.lock(), repo.transaction('bookmarks') as tr:
        marks = repo._bookmarks
        existing = hex(marks.get(key, ''))
        if existing != old and existing != new:
            return False
        if new == '':
            changes = [(key, None)]
        else:
            if new not in repo:
                return False
            changes = [(key, repo[new].node())]
        marks.applychanges(repo, tr, changes)
        return True

def comparebookmarks(repo, srcmarks, dstmarks, targets=None):
    '''Compare bookmarks between srcmarks and dstmarks

    This returns tuple "(addsrc, adddst, advsrc, advdst, diverge,
    differ, invalid)", each are list of bookmarks below:

    :addsrc:  added on src side (removed on dst side, perhaps)
    :adddst:  added on dst side (removed on src side, perhaps)
    :advsrc:  advanced on src side
    :advdst:  advanced on dst side
    :diverge: diverge
    :differ:  changed, but changeset referred on src is unknown on dst
    :invalid: unknown on both side
    :same:    same on both side

    Each elements of lists in result tuple is tuple "(bookmark name,
    changeset ID on source side, changeset ID on destination
    side)". Each changeset IDs are 40 hexadecimal digit string or
    None.

    Changeset IDs of tuples in "addsrc", "adddst", "differ" or
     "invalid" list may be unknown for repo.

    If "targets" is specified, only bookmarks listed in it are
    examined.
    '''

    if targets:
        bset = set(targets)
    else:
        srcmarkset = set(srcmarks)
        dstmarkset = set(dstmarks)
        bset = srcmarkset | dstmarkset

    results = ([], [], [], [], [], [], [], [])
    addsrc = results[0].append
    adddst = results[1].append
    advsrc = results[2].append
    advdst = results[3].append
    diverge = results[4].append
    differ = results[5].append
    invalid = results[6].append
    same = results[7].append

    for b in sorted(bset):
        if b not in srcmarks:
            if b in dstmarks:
                adddst((b, None, dstmarks[b]))
            else:
                invalid((b, None, None))
        elif b not in dstmarks:
            addsrc((b, srcmarks[b], None))
        else:
            scid = srcmarks[b]
            dcid = dstmarks[b]
            if scid == dcid:
                same((b, scid, dcid))
            elif scid in repo and dcid in repo:
                sctx = repo[scid]
                dctx = repo[dcid]
                if sctx.rev() < dctx.rev():
                    if validdest(repo, sctx, dctx):
                        advdst((b, scid, dcid))
                    else:
                        diverge((b, scid, dcid))
                else:
                    if validdest(repo, dctx, sctx):
                        advsrc((b, scid, dcid))
                    else:
                        diverge((b, scid, dcid))
            else:
                # it is too expensive to examine in detail, in this case
                differ((b, scid, dcid))

    return results

def _diverge(ui, b, path, localmarks, remotenode):
    '''Return appropriate diverged bookmark for specified ``path``

    This returns None, if it is failed to assign any divergent
    bookmark name.

    This reuses already existing one with "@number" suffix, if it
    refers ``remotenode``.
    '''
    if b == '@':
        b = ''
    # try to use an @pathalias suffix
    # if an @pathalias already exists, we overwrite (update) it
    if path.startswith("file:"):
        path = util.url(path).path
    for p, u in ui.configitems("paths"):
        if u.startswith("file:"):
            u = util.url(u).path
        if path == u:
            return '%s@%s' % (b, p)

    # assign a unique "@number" suffix newly
    for x in range(1, 100):
        n = '%s@%d' % (b, x)
        if n not in localmarks or localmarks[n] == remotenode:
            return n

    return None

def unhexlifybookmarks(marks):
    binremotemarks = {}
    for name, node in marks.items():
        binremotemarks[name] = bin(node)
    return binremotemarks

_binaryentry = struct.Struct('>20sH')

def binaryencode(bookmarks):
    """encode a '(bookmark, node)' iterable into a binary stream

    the binary format is:

        <node><bookmark-length><bookmark-name>

    :node: is a 20 bytes binary node,
    :bookmark-length: an unsigned short,
    :bookmark-name: the name of the bookmark (of length <bookmark-length>)

    wdirid (all bits set) will be used as a special value for "missing"
    """
    binarydata = []
    for book, node in bookmarks:
        if not node: # None or ''
            node = wdirid
        binarydata.append(_binaryentry.pack(node, len(book)))
        binarydata.append(book)
    return ''.join(binarydata)

def binarydecode(stream):
    """decode a binary stream into an '(bookmark, node)' iterable

    the binary format is:

        <node><bookmark-length><bookmark-name>

    :node: is a 20 bytes binary node,
    :bookmark-length: an unsigned short,
    :bookmark-name: the name of the bookmark (of length <bookmark-length>))

    wdirid (all bits set) will be used as a special value for "missing"
    """
    entrysize = _binaryentry.size
    books = []
    while True:
        entry = stream.read(entrysize)
        if len(entry) < entrysize:
            if entry:
                raise error.Abort(_('bad bookmark stream'))
            break
        node, length = _binaryentry.unpack(entry)
        bookmark = stream.read(length)
        if len(bookmark) < length:
            if entry:
                raise error.Abort(_('bad bookmark stream'))
        if node == wdirid:
            node = None
        books.append((bookmark, node))
    return books

def updatefromremote(ui, repo, remotemarks, path, trfunc, explicit=()):
    ui.debug("checking for updated bookmarks\n")
    localmarks = repo._bookmarks
    (addsrc, adddst, advsrc, advdst, diverge, differ, invalid, same
    ) = comparebookmarks(repo, remotemarks, localmarks)

    status = ui.status
    warn = ui.warn
    if ui.configbool('ui', 'quietbookmarkmove'):
        status = warn = ui.debug

    explicit = set(explicit)
    changed = []
    for b, scid, dcid in addsrc:
        if scid in repo: # add remote bookmarks for changes we already have
            changed.append((b, scid, status,
                            _("adding remote bookmark %s\n") % (b)))
        elif b in explicit:
            explicit.remove(b)
            ui.warn(_("remote bookmark %s points to locally missing %s\n")
                    % (b, hex(scid)[:12]))

    for b, scid, dcid in advsrc:
        changed.append((b, scid, status,
                        _("updating bookmark %s\n") % (b)))
    # remove normal movement from explicit set
    explicit.difference_update(d[0] for d in changed)

    for b, scid, dcid in diverge:
        if b in explicit:
            explicit.discard(b)
            changed.append((b, scid, status,
                            _("importing bookmark %s\n") % (b)))
        else:
            db = _diverge(ui, b, path, localmarks, scid)
            if db:
                changed.append((db, scid, warn,
                                _("divergent bookmark %s stored as %s\n") %
                                (b, db)))
            else:
                warn(_("warning: failed to assign numbered name "
                       "to divergent bookmark %s\n") % (b))
    for b, scid, dcid in adddst + advdst:
        if b in explicit:
            explicit.discard(b)
            changed.append((b, scid, status,
                            _("importing bookmark %s\n") % (b)))
    for b, scid, dcid in differ:
        if b in explicit:
            explicit.remove(b)
            ui.warn(_("remote bookmark %s points to locally missing %s\n")
                    % (b, hex(scid)[:12]))

    if changed:
        tr = trfunc()
        changes = []
        for b, node, writer, msg in sorted(changed):
            changes.append((b, node))
            writer(msg)
        localmarks.applychanges(repo, tr, changes)

def incoming(ui, repo, peer):
    '''Show bookmarks incoming from other to repo
    '''
    ui.status(_("searching for changed bookmarks\n"))

    with peer.commandexecutor() as e:
        remotemarks = unhexlifybookmarks(e.callcommand('listkeys', {
            'namespace': 'bookmarks',
        }).result())

    r = comparebookmarks(repo, remotemarks, repo._bookmarks)
    addsrc, adddst, advsrc, advdst, diverge, differ, invalid, same = r

    incomings = []
    if ui.debugflag:
        getid = lambda id: id
    else:
        getid = lambda id: id[:12]
    if ui.verbose:
        def add(b, id, st):
            incomings.append("   %-25s %s %s\n" % (b, getid(id), st))
    else:
        def add(b, id, st):
            incomings.append("   %-25s %s\n" % (b, getid(id)))
    for b, scid, dcid in addsrc:
        # i18n: "added" refers to a bookmark
        add(b, hex(scid), _('added'))
    for b, scid, dcid in advsrc:
        # i18n: "advanced" refers to a bookmark
        add(b, hex(scid), _('advanced'))
    for b, scid, dcid in diverge:
        # i18n: "diverged" refers to a bookmark
        add(b, hex(scid), _('diverged'))
    for b, scid, dcid in differ:
        # i18n: "changed" refers to a bookmark
        add(b, hex(scid), _('changed'))

    if not incomings:
        ui.status(_("no changed bookmarks found\n"))
        return 1

    for s in sorted(incomings):
        ui.write(s)

    return 0

def outgoing(ui, repo, other):
    '''Show bookmarks outgoing from repo to other
    '''
    ui.status(_("searching for changed bookmarks\n"))

    remotemarks = unhexlifybookmarks(other.listkeys('bookmarks'))
    r = comparebookmarks(repo, repo._bookmarks, remotemarks)
    addsrc, adddst, advsrc, advdst, diverge, differ, invalid, same = r

    outgoings = []
    if ui.debugflag:
        getid = lambda id: id
    else:
        getid = lambda id: id[:12]
    if ui.verbose:
        def add(b, id, st):
            outgoings.append("   %-25s %s %s\n" % (b, getid(id), st))
    else:
        def add(b, id, st):
            outgoings.append("   %-25s %s\n" % (b, getid(id)))
    for b, scid, dcid in addsrc:
        # i18n: "added refers to a bookmark
        add(b, hex(scid), _('added'))
    for b, scid, dcid in adddst:
        # i18n: "deleted" refers to a bookmark
        add(b, ' ' * 40, _('deleted'))
    for b, scid, dcid in advsrc:
        # i18n: "advanced" refers to a bookmark
        add(b, hex(scid), _('advanced'))
    for b, scid, dcid in diverge:
        # i18n: "diverged" refers to a bookmark
        add(b, hex(scid), _('diverged'))
    for b, scid, dcid in differ:
        # i18n: "changed" refers to a bookmark
        add(b, hex(scid), _('changed'))

    if not outgoings:
        ui.status(_("no changed bookmarks found\n"))
        return 1

    for s in sorted(outgoings):
        ui.write(s)

    return 0

def summary(repo, peer):
    '''Compare bookmarks between repo and other for "hg summary" output

    This returns "(# of incoming, # of outgoing)" tuple.
    '''
    with peer.commandexecutor() as e:
        remotemarks = unhexlifybookmarks(e.callcommand('listkeys', {
            'namespace': 'bookmarks',
        }).result())

    r = comparebookmarks(repo, remotemarks, repo._bookmarks)
    addsrc, adddst, advsrc, advdst, diverge, differ, invalid, same = r
    return (len(addsrc), len(adddst))

def validdest(repo, old, new):
    """Is the new bookmark destination a valid update from the old one"""
    repo = repo.unfiltered()
    if old == new:
        # Old == new -> nothing to update.
        return False
    elif not old:
        # old is nullrev, anything is valid.
        # (new != nullrev has been excluded by the previous check)
        return True
    elif repo.obsstore:
        return new.node() in obsutil.foreground(repo, [old.node()])
    else:
        # still an independent clause as it is lazier (and therefore faster)
        return old.isancestorof(new)

def checkformat(repo, mark):
    """return a valid version of a potential bookmark name

    Raises an abort error if the bookmark name is not valid.
    """
    mark = mark.strip()
    if not mark:
        raise error.Abort(_("bookmark names cannot consist entirely of "
                            "whitespace"))
    scmutil.checknewlabel(repo, mark, 'bookmark')
    return mark

def delete(repo, tr, names):
    """remove a mark from the bookmark store

    Raises an abort error if mark does not exist.
    """
    marks = repo._bookmarks
    changes = []
    for mark in names:
        if mark not in marks:
            raise error.Abort(_("bookmark '%s' does not exist") % mark)
        if mark == repo._activebookmark:
            deactivate(repo)
        changes.append((mark, None))
    marks.applychanges(repo, tr, changes)

def rename(repo, tr, old, new, force=False, inactive=False):
    """rename a bookmark from old to new

    If force is specified, then the new name can overwrite an existing
    bookmark.

    If inactive is specified, then do not activate the new bookmark.

    Raises an abort error if old is not in the bookmark store.
    """
    marks = repo._bookmarks
    mark = checkformat(repo, new)
    if old not in marks:
        raise error.Abort(_("bookmark '%s' does not exist") % old)
    changes = []
    for bm in marks.checkconflict(mark, force):
        changes.append((bm, None))
    changes.extend([(mark, marks[old]), (old, None)])
    marks.applychanges(repo, tr, changes)
    if repo._activebookmark == old and not inactive:
        activate(repo, mark)

def addbookmarks(repo, tr, names, rev=None, force=False, inactive=False):
    """add a list of bookmarks

    If force is specified, then the new name can overwrite an existing
    bookmark.

    If inactive is specified, then do not activate any bookmark. Otherwise, the
    first bookmark is activated.

    Raises an abort error if old is not in the bookmark store.
    """
    marks = repo._bookmarks
    cur = repo['.'].node()
    newact = None
    changes = []
    hiddenrev = None

    # unhide revs if any
    if rev:
        repo = scmutil.unhidehashlikerevs(repo, [rev], 'nowarn')

    for mark in names:
        mark = checkformat(repo, mark)
        if newact is None:
            newact = mark
        if inactive and mark == repo._activebookmark:
            deactivate(repo)
            return
        tgt = cur
        if rev:
            ctx = scmutil.revsingle(repo, rev)
            if ctx.hidden():
                hiddenrev = ctx.hex()[:12]
            tgt = ctx.node()
        for bm in marks.checkconflict(mark, force, tgt):
            changes.append((bm, None))
        changes.append((mark, tgt))

    if hiddenrev:
        repo.ui.warn(_("bookmarking hidden changeset %s\n") % hiddenrev)

        if ctx.obsolete():
            msg = obsutil._getfilteredreason(repo, "%s" % hiddenrev, ctx)
            repo.ui.warn("(%s)\n" % msg)

    marks.applychanges(repo, tr, changes)
    if not inactive and cur == marks[newact] and not rev:
        activate(repo, newact)
    elif cur != tgt and newact == repo._activebookmark:
        deactivate(repo)

def _printbookmarks(ui, repo, fm, bmarks):
    """private method to print bookmarks

    Provides a way for extensions to control how bookmarks are printed (e.g.
    prepend or postpend names)
    """
    hexfn = fm.hexfunc
    if len(bmarks) == 0 and fm.isplain():
        ui.status(_("no bookmarks set\n"))
    for bmark, (n, prefix, label) in sorted(bmarks.iteritems()):
        fm.startitem()
        fm.context(repo=repo)
        if not ui.quiet:
            fm.plain(' %s ' % prefix, label=label)
        fm.write('bookmark', '%s', bmark, label=label)
        pad = " " * (25 - encoding.colwidth(bmark))
        fm.condwrite(not ui.quiet, 'rev node', pad + ' %d:%s',
                     repo.changelog.rev(n), hexfn(n), label=label)
        fm.data(active=(activebookmarklabel in label))
        fm.plain('\n')

def printbookmarks(ui, repo, fm, names=None):
    """print bookmarks by the given formatter

    Provides a way for extensions to control how bookmarks are printed.
    """
    marks = repo._bookmarks
    bmarks = {}
    for bmark in (names or marks):
        if bmark not in marks:
            raise error.Abort(_("bookmark '%s' does not exist") % bmark)
        active = repo._activebookmark
        if bmark == active:
            prefix, label = '*', activebookmarklabel
        else:
            prefix, label = ' ', ''

        bmarks[bmark] = (marks[bmark], prefix, label)
    _printbookmarks(ui, repo, fm, bmarks)

def preparehookargs(name, old, new):
    if new is None:
        new = ''
    if old is None:
        old = ''
    return {'bookmark': name,
            'node': hex(new),
            'oldnode': hex(old)}
