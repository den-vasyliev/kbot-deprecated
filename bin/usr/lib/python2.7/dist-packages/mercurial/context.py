# context.py - changeset and file context objects for mercurial
#
# Copyright 2006, 2007 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import errno
import filecmp
import os
import stat

from .i18n import _
from .node import (
    addednodeid,
    hex,
    modifiednodeid,
    nullid,
    nullrev,
    short,
    wdirfilenodeids,
    wdirid,
)
from . import (
    dagop,
    encoding,
    error,
    fileset,
    match as matchmod,
    obsolete as obsmod,
    patch,
    pathutil,
    phases,
    pycompat,
    repoview,
    scmutil,
    sparse,
    subrepo,
    subrepoutil,
    util,
)
from .utils import (
    dateutil,
    stringutil,
)

propertycache = util.propertycache

class basectx(object):
    """A basectx object represents the common logic for its children:
    changectx: read-only context that is already present in the repo,
    workingctx: a context that represents the working directory and can
                be committed,
    memctx: a context that represents changes in-memory and can also
            be committed."""

    def __init__(self, repo):
        self._repo = repo

    def __bytes__(self):
        return short(self.node())

    __str__ = encoding.strmethod(__bytes__)

    def __repr__(self):
        return r"<%s %s>" % (type(self).__name__, str(self))

    def __eq__(self, other):
        try:
            return type(self) == type(other) and self._rev == other._rev
        except AttributeError:
            return False

    def __ne__(self, other):
        return not (self == other)

    def __contains__(self, key):
        return key in self._manifest

    def __getitem__(self, key):
        return self.filectx(key)

    def __iter__(self):
        return iter(self._manifest)

    def _buildstatusmanifest(self, status):
        """Builds a manifest that includes the given status results, if this is
        a working copy context. For non-working copy contexts, it just returns
        the normal manifest."""
        return self.manifest()

    def _matchstatus(self, other, match):
        """This internal method provides a way for child objects to override the
        match operator.
        """
        return match

    def _buildstatus(self, other, s, match, listignored, listclean,
                     listunknown):
        """build a status with respect to another context"""
        # Load earliest manifest first for caching reasons. More specifically,
        # if you have revisions 1000 and 1001, 1001 is probably stored as a
        # delta against 1000. Thus, if you read 1000 first, we'll reconstruct
        # 1000 and cache it so that when you read 1001, we just need to apply a
        # delta to what's in the cache. So that's one full reconstruction + one
        # delta application.
        mf2 = None
        if self.rev() is not None and self.rev() < other.rev():
            mf2 = self._buildstatusmanifest(s)
        mf1 = other._buildstatusmanifest(s)
        if mf2 is None:
            mf2 = self._buildstatusmanifest(s)

        modified, added = [], []
        removed = []
        clean = []
        deleted, unknown, ignored = s.deleted, s.unknown, s.ignored
        deletedset = set(deleted)
        d = mf1.diff(mf2, match=match, clean=listclean)
        for fn, value in d.iteritems():
            if fn in deletedset:
                continue
            if value is None:
                clean.append(fn)
                continue
            (node1, flag1), (node2, flag2) = value
            if node1 is None:
                added.append(fn)
            elif node2 is None:
                removed.append(fn)
            elif flag1 != flag2:
                modified.append(fn)
            elif node2 not in wdirfilenodeids:
                # When comparing files between two commits, we save time by
                # not comparing the file contents when the nodeids differ.
                # Note that this means we incorrectly report a reverted change
                # to a file as a modification.
                modified.append(fn)
            elif self[fn].cmp(other[fn]):
                modified.append(fn)
            else:
                clean.append(fn)

        if removed:
            # need to filter files if they are already reported as removed
            unknown = [fn for fn in unknown if fn not in mf1 and
                                               (not match or match(fn))]
            ignored = [fn for fn in ignored if fn not in mf1 and
                                               (not match or match(fn))]
            # if they're deleted, don't report them as removed
            removed = [fn for fn in removed if fn not in deletedset]

        return scmutil.status(modified, added, removed, deleted, unknown,
                              ignored, clean)

    @propertycache
    def substate(self):
        return subrepoutil.state(self, self._repo.ui)

    def subrev(self, subpath):
        return self.substate[subpath][1]

    def rev(self):
        return self._rev
    def node(self):
        return self._node
    def hex(self):
        return hex(self.node())
    def manifest(self):
        return self._manifest
    def manifestctx(self):
        return self._manifestctx
    def repo(self):
        return self._repo
    def phasestr(self):
        return phases.phasenames[self.phase()]
    def mutable(self):
        return self.phase() > phases.public

    def matchfileset(self, expr, badfn=None):
        return fileset.match(self, expr, badfn=badfn)

    def obsolete(self):
        """True if the changeset is obsolete"""
        return self.rev() in obsmod.getrevs(self._repo, 'obsolete')

    def extinct(self):
        """True if the changeset is extinct"""
        return self.rev() in obsmod.getrevs(self._repo, 'extinct')

    def orphan(self):
        """True if the changeset is not obsolete, but its ancestor is"""
        return self.rev() in obsmod.getrevs(self._repo, 'orphan')

    def phasedivergent(self):
        """True if the changeset tries to be a successor of a public changeset

        Only non-public and non-obsolete changesets may be phase-divergent.
        """
        return self.rev() in obsmod.getrevs(self._repo, 'phasedivergent')

    def contentdivergent(self):
        """Is a successor of a changeset with multiple possible successor sets

        Only non-public and non-obsolete changesets may be content-divergent.
        """
        return self.rev() in obsmod.getrevs(self._repo, 'contentdivergent')

    def isunstable(self):
        """True if the changeset is either orphan, phase-divergent or
        content-divergent"""
        return self.orphan() or self.phasedivergent() or self.contentdivergent()

    def instabilities(self):
        """return the list of instabilities affecting this changeset.

        Instabilities are returned as strings. possible values are:
        - orphan,
        - phase-divergent,
        - content-divergent.
        """
        instabilities = []
        if self.orphan():
            instabilities.append('orphan')
        if self.phasedivergent():
            instabilities.append('phase-divergent')
        if self.contentdivergent():
            instabilities.append('content-divergent')
        return instabilities

    def parents(self):
        """return contexts for each parent changeset"""
        return self._parents

    def p1(self):
        return self._parents[0]

    def p2(self):
        parents = self._parents
        if len(parents) == 2:
            return parents[1]
        return self._repo[nullrev]

    def _fileinfo(self, path):
        if r'_manifest' in self.__dict__:
            try:
                return self._manifest[path], self._manifest.flags(path)
            except KeyError:
                raise error.ManifestLookupError(self._node, path,
                                                _('not found in manifest'))
        if r'_manifestdelta' in self.__dict__ or path in self.files():
            if path in self._manifestdelta:
                return (self._manifestdelta[path],
                        self._manifestdelta.flags(path))
        mfl = self._repo.manifestlog
        try:
            node, flag = mfl[self._changeset.manifest].find(path)
        except KeyError:
            raise error.ManifestLookupError(self._node, path,
                                            _('not found in manifest'))

        return node, flag

    def filenode(self, path):
        return self._fileinfo(path)[0]

    def flags(self, path):
        try:
            return self._fileinfo(path)[1]
        except error.LookupError:
            return ''

    def sub(self, path, allowcreate=True):
        '''return a subrepo for the stored revision of path, never wdir()'''
        return subrepo.subrepo(self, path, allowcreate=allowcreate)

    def nullsub(self, path, pctx):
        return subrepo.nullsubrepo(self, path, pctx)

    def workingsub(self, path):
        '''return a subrepo for the stored revision, or wdir if this is a wdir
        context.
        '''
        return subrepo.subrepo(self, path, allowwdir=True)

    def match(self, pats=None, include=None, exclude=None, default='glob',
              listsubrepos=False, badfn=None):
        r = self._repo
        return matchmod.match(r.root, r.getcwd(), pats,
                              include, exclude, default,
                              auditor=r.nofsauditor, ctx=self,
                              listsubrepos=listsubrepos, badfn=badfn)

    def diff(self, ctx2=None, match=None, changes=None, opts=None,
             losedatafn=None, prefix='', relroot='', copy=None,
             hunksfilterfn=None):
        """Returns a diff generator for the given contexts and matcher"""
        if ctx2 is None:
            ctx2 = self.p1()
        if ctx2 is not None:
            ctx2 = self._repo[ctx2]
        return patch.diff(self._repo, ctx2, self, match=match, changes=changes,
                          opts=opts, losedatafn=losedatafn, prefix=prefix,
                          relroot=relroot, copy=copy,
                          hunksfilterfn=hunksfilterfn)

    def dirs(self):
        return self._manifest.dirs()

    def hasdir(self, dir):
        return self._manifest.hasdir(dir)

    def status(self, other=None, match=None, listignored=False,
               listclean=False, listunknown=False, listsubrepos=False):
        """return status of files between two nodes or node and working
        directory.

        If other is None, compare this node with working directory.

        returns (modified, added, removed, deleted, unknown, ignored, clean)
        """

        ctx1 = self
        ctx2 = self._repo[other]

        # This next code block is, admittedly, fragile logic that tests for
        # reversing the contexts and wouldn't need to exist if it weren't for
        # the fast (and common) code path of comparing the working directory
        # with its first parent.
        #
        # What we're aiming for here is the ability to call:
        #
        # workingctx.status(parentctx)
        #
        # If we always built the manifest for each context and compared those,
        # then we'd be done. But the special case of the above call means we
        # just copy the manifest of the parent.
        reversed = False
        if (not isinstance(ctx1, changectx)
            and isinstance(ctx2, changectx)):
            reversed = True
            ctx1, ctx2 = ctx2, ctx1

        match = self._repo.narrowmatch(match)
        match = ctx2._matchstatus(ctx1, match)
        r = scmutil.status([], [], [], [], [], [], [])
        r = ctx2._buildstatus(ctx1, r, match, listignored, listclean,
                              listunknown)

        if reversed:
            # Reverse added and removed. Clear deleted, unknown and ignored as
            # these make no sense to reverse.
            r = scmutil.status(r.modified, r.removed, r.added, [], [], [],
                               r.clean)

        if listsubrepos:
            for subpath, sub in scmutil.itersubrepos(ctx1, ctx2):
                try:
                    rev2 = ctx2.subrev(subpath)
                except KeyError:
                    # A subrepo that existed in node1 was deleted between
                    # node1 and node2 (inclusive). Thus, ctx2's substate
                    # won't contain that subpath. The best we can do ignore it.
                    rev2 = None
                submatch = matchmod.subdirmatcher(subpath, match)
                s = sub.status(rev2, match=submatch, ignored=listignored,
                               clean=listclean, unknown=listunknown,
                               listsubrepos=True)
                for rfiles, sfiles in zip(r, s):
                    rfiles.extend("%s/%s" % (subpath, f) for f in sfiles)

        for l in r:
            l.sort()

        return r

class changectx(basectx):
    """A changecontext object makes access to data related to a particular
    changeset convenient. It represents a read-only context already present in
    the repo."""
    def __init__(self, repo, rev, node):
        super(changectx, self).__init__(repo)
        self._rev = rev
        self._node = node

    def __hash__(self):
        try:
            return hash(self._rev)
        except AttributeError:
            return id(self)

    def __nonzero__(self):
        return self._rev != nullrev

    __bool__ = __nonzero__

    @propertycache
    def _changeset(self):
        return self._repo.changelog.changelogrevision(self.rev())

    @propertycache
    def _manifest(self):
        return self._manifestctx.read()

    @property
    def _manifestctx(self):
        return self._repo.manifestlog[self._changeset.manifest]

    @propertycache
    def _manifestdelta(self):
        return self._manifestctx.readdelta()

    @propertycache
    def _parents(self):
        repo = self._repo
        p1, p2 = repo.changelog.parentrevs(self._rev)
        if p2 == nullrev:
            return [repo[p1]]
        return [repo[p1], repo[p2]]

    def changeset(self):
        c = self._changeset
        return (
            c.manifest,
            c.user,
            c.date,
            c.files,
            c.description,
            c.extra,
        )
    def manifestnode(self):
        return self._changeset.manifest

    def user(self):
        return self._changeset.user
    def date(self):
        return self._changeset.date
    def files(self):
        return self._changeset.files
    def description(self):
        return self._changeset.description
    def branch(self):
        return encoding.tolocal(self._changeset.extra.get("branch"))
    def closesbranch(self):
        return 'close' in self._changeset.extra
    def extra(self):
        """Return a dict of extra information."""
        return self._changeset.extra
    def tags(self):
        """Return a list of byte tag names"""
        return self._repo.nodetags(self._node)
    def bookmarks(self):
        """Return a list of byte bookmark names."""
        return self._repo.nodebookmarks(self._node)
    def phase(self):
        return self._repo._phasecache.phase(self._repo, self._rev)
    def hidden(self):
        return self._rev in repoview.filterrevs(self._repo, 'visible')

    def isinmemory(self):
        return False

    def children(self):
        """return list of changectx contexts for each child changeset.

        This returns only the immediate child changesets. Use descendants() to
        recursively walk children.
        """
        c = self._repo.changelog.children(self._node)
        return [self._repo[x] for x in c]

    def ancestors(self):
        for a in self._repo.changelog.ancestors([self._rev]):
            yield self._repo[a]

    def descendants(self):
        """Recursively yield all children of the changeset.

        For just the immediate children, use children()
        """
        for d in self._repo.changelog.descendants([self._rev]):
            yield self._repo[d]

    def filectx(self, path, fileid=None, filelog=None):
        """get a file context from this changeset"""
        if fileid is None:
            fileid = self.filenode(path)
        return filectx(self._repo, path, fileid=fileid,
                       changectx=self, filelog=filelog)

    def ancestor(self, c2, warn=False):
        """return the "best" ancestor context of self and c2

        If there are multiple candidates, it will show a message and check
        merge.preferancestor configuration before falling back to the
        revlog ancestor."""
        # deal with workingctxs
        n2 = c2._node
        if n2 is None:
            n2 = c2._parents[0]._node
        cahs = self._repo.changelog.commonancestorsheads(self._node, n2)
        if not cahs:
            anc = nullid
        elif len(cahs) == 1:
            anc = cahs[0]
        else:
            # experimental config: merge.preferancestor
            for r in self._repo.ui.configlist('merge', 'preferancestor'):
                try:
                    ctx = scmutil.revsymbol(self._repo, r)
                except error.RepoLookupError:
                    continue
                anc = ctx.node()
                if anc in cahs:
                    break
            else:
                anc = self._repo.changelog.ancestor(self._node, n2)
            if warn:
                self._repo.ui.status(
                    (_("note: using %s as ancestor of %s and %s\n") %
                     (short(anc), short(self._node), short(n2))) +
                    ''.join(_("      alternatively, use --config "
                              "merge.preferancestor=%s\n") %
                            short(n) for n in sorted(cahs) if n != anc))
        return self._repo[anc]

    def isancestorof(self, other):
        """True if this changeset is an ancestor of other"""
        return self._repo.changelog.isancestorrev(self._rev, other._rev)

    def walk(self, match):
        '''Generates matching file names.'''

        # Wrap match.bad method to have message with nodeid
        def bad(fn, msg):
            # The manifest doesn't know about subrepos, so don't complain about
            # paths into valid subrepos.
            if any(fn == s or fn.startswith(s + '/')
                   for s in self.substate):
                return
            match.bad(fn, _('no such file in rev %s') % self)

        m = matchmod.badmatch(self._repo.narrowmatch(match), bad)
        return self._manifest.walk(m)

    def matches(self, match):
        return self.walk(match)

class basefilectx(object):
    """A filecontext object represents the common logic for its children:
    filectx: read-only access to a filerevision that is already present
             in the repo,
    workingfilectx: a filecontext that represents files from the working
                    directory,
    memfilectx: a filecontext that represents files in-memory,
    """
    @propertycache
    def _filelog(self):
        return self._repo.file(self._path)

    @propertycache
    def _changeid(self):
        if r'_changeid' in self.__dict__:
            return self._changeid
        elif r'_changectx' in self.__dict__:
            return self._changectx.rev()
        elif r'_descendantrev' in self.__dict__:
            # this file context was created from a revision with a known
            # descendant, we can (lazily) correct for linkrev aliases
            return self._adjustlinkrev(self._descendantrev)
        else:
            return self._filelog.linkrev(self._filerev)

    @propertycache
    def _filenode(self):
        if r'_fileid' in self.__dict__:
            return self._filelog.lookup(self._fileid)
        else:
            return self._changectx.filenode(self._path)

    @propertycache
    def _filerev(self):
        return self._filelog.rev(self._filenode)

    @propertycache
    def _repopath(self):
        return self._path

    def __nonzero__(self):
        try:
            self._filenode
            return True
        except error.LookupError:
            # file is missing
            return False

    __bool__ = __nonzero__

    def __bytes__(self):
        try:
            return "%s@%s" % (self.path(), self._changectx)
        except error.LookupError:
            return "%s@???" % self.path()

    __str__ = encoding.strmethod(__bytes__)

    def __repr__(self):
        return r"<%s %s>" % (type(self).__name__, str(self))

    def __hash__(self):
        try:
            return hash((self._path, self._filenode))
        except AttributeError:
            return id(self)

    def __eq__(self, other):
        try:
            return (type(self) == type(other) and self._path == other._path
                    and self._filenode == other._filenode)
        except AttributeError:
            return False

    def __ne__(self, other):
        return not (self == other)

    def filerev(self):
        return self._filerev
    def filenode(self):
        return self._filenode
    @propertycache
    def _flags(self):
        return self._changectx.flags(self._path)
    def flags(self):
        return self._flags
    def filelog(self):
        return self._filelog
    def rev(self):
        return self._changeid
    def linkrev(self):
        return self._filelog.linkrev(self._filerev)
    def node(self):
        return self._changectx.node()
    def hex(self):
        return self._changectx.hex()
    def user(self):
        return self._changectx.user()
    def date(self):
        return self._changectx.date()
    def files(self):
        return self._changectx.files()
    def description(self):
        return self._changectx.description()
    def branch(self):
        return self._changectx.branch()
    def extra(self):
        return self._changectx.extra()
    def phase(self):
        return self._changectx.phase()
    def phasestr(self):
        return self._changectx.phasestr()
    def obsolete(self):
        return self._changectx.obsolete()
    def instabilities(self):
        return self._changectx.instabilities()
    def manifest(self):
        return self._changectx.manifest()
    def changectx(self):
        return self._changectx
    def renamed(self):
        return self._copied
    def repo(self):
        return self._repo
    def size(self):
        return len(self.data())

    def path(self):
        return self._path

    def isbinary(self):
        try:
            return stringutil.binary(self.data())
        except IOError:
            return False
    def isexec(self):
        return 'x' in self.flags()
    def islink(self):
        return 'l' in self.flags()

    def isabsent(self):
        """whether this filectx represents a file not in self._changectx

        This is mainly for merge code to detect change/delete conflicts. This is
        expected to be True for all subclasses of basectx."""
        return False

    _customcmp = False
    def cmp(self, fctx):
        """compare with other file context

        returns True if different than fctx.
        """
        if fctx._customcmp:
            return fctx.cmp(self)

        if (fctx._filenode is None
            and (self._repo._encodefilterpats
                 # if file data starts with '\1\n', empty metadata block is
                 # prepended, which adds 4 bytes to filelog.size().
                 or self.size() - 4 == fctx.size())
            or self.size() == fctx.size()):
            return self._filelog.cmp(self._filenode, fctx.data())

        return True

    def _adjustlinkrev(self, srcrev, inclusive=False):
        """return the first ancestor of <srcrev> introducing <fnode>

        If the linkrev of the file revision does not point to an ancestor of
        srcrev, we'll walk down the ancestors until we find one introducing
        this file revision.

        :srcrev: the changeset revision we search ancestors from
        :inclusive: if true, the src revision will also be checked
        """
        repo = self._repo
        cl = repo.unfiltered().changelog
        mfl = repo.manifestlog
        # fetch the linkrev
        lkr = self.linkrev()
        if srcrev == lkr:
            return lkr
        # hack to reuse ancestor computation when searching for renames
        memberanc = getattr(self, '_ancestrycontext', None)
        iteranc = None
        if srcrev is None:
            # wctx case, used by workingfilectx during mergecopy
            revs = [p.rev() for p in self._repo[None].parents()]
            inclusive = True # we skipped the real (revless) source
        else:
            revs = [srcrev]
        if memberanc is None:
            memberanc = iteranc = cl.ancestors(revs, lkr,
                                               inclusive=inclusive)
        # check if this linkrev is an ancestor of srcrev
        if lkr not in memberanc:
            if iteranc is None:
                iteranc = cl.ancestors(revs, lkr, inclusive=inclusive)
            fnode = self._filenode
            path = self._path
            for a in iteranc:
                ac = cl.read(a) # get changeset data (we avoid object creation)
                if path in ac[3]: # checking the 'files' field.
                    # The file has been touched, check if the content is
                    # similar to the one we search for.
                    if fnode == mfl[ac[0]].readfast().get(path):
                        return a
            # In theory, we should never get out of that loop without a result.
            # But if manifest uses a buggy file revision (not children of the
            # one it replaces) we could. Such a buggy situation will likely
            # result is crash somewhere else at to some point.
        return lkr

    def introrev(self):
        """return the rev of the changeset which introduced this file revision

        This method is different from linkrev because it take into account the
        changeset the filectx was created from. It ensures the returned
        revision is one of its ancestors. This prevents bugs from
        'linkrev-shadowing' when a file revision is used by multiple
        changesets.
        """
        attrs = vars(self)
        hastoprev = (r'_changeid' in attrs or r'_changectx' in attrs)
        if hastoprev:
            return self._adjustlinkrev(self.rev(), inclusive=True)
        else:
            return self.linkrev()

    def introfilectx(self):
        """Return filectx having identical contents, but pointing to the
        changeset revision where this filectx was introduced"""
        introrev = self.introrev()
        if self.rev() == introrev:
            return self
        return self.filectx(self.filenode(), changeid=introrev)

    def _parentfilectx(self, path, fileid, filelog):
        """create parent filectx keeping ancestry info for _adjustlinkrev()"""
        fctx = filectx(self._repo, path, fileid=fileid, filelog=filelog)
        if r'_changeid' in vars(self) or r'_changectx' in vars(self):
            # If self is associated with a changeset (probably explicitly
            # fed), ensure the created filectx is associated with a
            # changeset that is an ancestor of self.changectx.
            # This lets us later use _adjustlinkrev to get a correct link.
            fctx._descendantrev = self.rev()
            fctx._ancestrycontext = getattr(self, '_ancestrycontext', None)
        elif r'_descendantrev' in vars(self):
            # Otherwise propagate _descendantrev if we have one associated.
            fctx._descendantrev = self._descendantrev
            fctx._ancestrycontext = getattr(self, '_ancestrycontext', None)
        return fctx

    def parents(self):
        _path = self._path
        fl = self._filelog
        parents = self._filelog.parents(self._filenode)
        pl = [(_path, node, fl) for node in parents if node != nullid]

        r = fl.renamed(self._filenode)
        if r:
            # - In the simple rename case, both parent are nullid, pl is empty.
            # - In case of merge, only one of the parent is null id and should
            # be replaced with the rename information. This parent is -always-
            # the first one.
            #
            # As null id have always been filtered out in the previous list
            # comprehension, inserting to 0 will always result in "replacing
            # first nullid parent with rename information.
            pl.insert(0, (r[0], r[1], self._repo.file(r[0])))

        return [self._parentfilectx(path, fnode, l) for path, fnode, l in pl]

    def p1(self):
        return self.parents()[0]

    def p2(self):
        p = self.parents()
        if len(p) == 2:
            return p[1]
        return filectx(self._repo, self._path, fileid=-1, filelog=self._filelog)

    def annotate(self, follow=False, skiprevs=None, diffopts=None):
        """Returns a list of annotateline objects for each line in the file

        - line.fctx is the filectx of the node where that line was last changed
        - line.lineno is the line number at the first appearance in the managed
          file
        - line.text is the data on that line (including newline character)
        """
        getlog = util.lrucachefunc(lambda x: self._repo.file(x))

        def parents(f):
            # Cut _descendantrev here to mitigate the penalty of lazy linkrev
            # adjustment. Otherwise, p._adjustlinkrev() would walk changelog
            # from the topmost introrev (= srcrev) down to p.linkrev() if it
            # isn't an ancestor of the srcrev.
            f._changeid
            pl = f.parents()

            # Don't return renamed parents if we aren't following.
            if not follow:
                pl = [p for p in pl if p.path() == f.path()]

            # renamed filectx won't have a filelog yet, so set it
            # from the cache to save time
            for p in pl:
                if not r'_filelog' in p.__dict__:
                    p._filelog = getlog(p.path())

            return pl

        # use linkrev to find the first changeset where self appeared
        base = self.introfilectx()
        if getattr(base, '_ancestrycontext', None) is None:
            cl = self._repo.changelog
            if base.rev() is None:
                # wctx is not inclusive, but works because _ancestrycontext
                # is used to test filelog revisions
                ac = cl.ancestors([p.rev() for p in base.parents()],
                                  inclusive=True)
            else:
                ac = cl.ancestors([base.rev()], inclusive=True)
            base._ancestrycontext = ac

        return dagop.annotate(base, parents, skiprevs=skiprevs,
                              diffopts=diffopts)

    def ancestors(self, followfirst=False):
        visit = {}
        c = self
        if followfirst:
            cut = 1
        else:
            cut = None

        while True:
            for parent in c.parents()[:cut]:
                visit[(parent.linkrev(), parent.filenode())] = parent
            if not visit:
                break
            c = visit.pop(max(visit))
            yield c

    def decodeddata(self):
        """Returns `data()` after running repository decoding filters.

        This is often equivalent to how the data would be expressed on disk.
        """
        return self._repo.wwritedata(self.path(), self.data())

class filectx(basefilectx):
    """A filecontext object makes access to data related to a particular
       filerevision convenient."""
    def __init__(self, repo, path, changeid=None, fileid=None,
                 filelog=None, changectx=None):
        """changeid must be a revision number, if specified.
           fileid can be a file revision or node."""
        self._repo = repo
        self._path = path

        assert (changeid is not None
                or fileid is not None
                or changectx is not None), \
                ("bad args: changeid=%r, fileid=%r, changectx=%r"
                 % (changeid, fileid, changectx))

        if filelog is not None:
            self._filelog = filelog

        if changeid is not None:
            self._changeid = changeid
        if changectx is not None:
            self._changectx = changectx
        if fileid is not None:
            self._fileid = fileid

    @propertycache
    def _changectx(self):
        try:
            return self._repo[self._changeid]
        except error.FilteredRepoLookupError:
            # Linkrev may point to any revision in the repository.  When the
            # repository is filtered this may lead to `filectx` trying to build
            # `changectx` for filtered revision. In such case we fallback to
            # creating `changectx` on the unfiltered version of the reposition.
            # This fallback should not be an issue because `changectx` from
            # `filectx` are not used in complex operations that care about
            # filtering.
            #
            # This fallback is a cheap and dirty fix that prevent several
            # crashes. It does not ensure the behavior is correct. However the
            # behavior was not correct before filtering either and "incorrect
            # behavior" is seen as better as "crash"
            #
            # Linkrevs have several serious troubles with filtering that are
            # complicated to solve. Proper handling of the issue here should be
            # considered when solving linkrev issue are on the table.
            return self._repo.unfiltered()[self._changeid]

    def filectx(self, fileid, changeid=None):
        '''opens an arbitrary revision of the file without
        opening a new filelog'''
        return filectx(self._repo, self._path, fileid=fileid,
                       filelog=self._filelog, changeid=changeid)

    def rawdata(self):
        return self._filelog.revision(self._filenode, raw=True)

    def rawflags(self):
        """low-level revlog flags"""
        return self._filelog.flags(self._filerev)

    def data(self):
        try:
            return self._filelog.read(self._filenode)
        except error.CensoredNodeError:
            if self._repo.ui.config("censor", "policy") == "ignore":
                return ""
            raise error.Abort(_("censored node: %s") % short(self._filenode),
                             hint=_("set censor.policy to ignore errors"))

    def size(self):
        return self._filelog.size(self._filerev)

    @propertycache
    def _copied(self):
        """check if file was actually renamed in this changeset revision

        If rename logged in file revision, we report copy for changeset only
        if file revisions linkrev points back to the changeset in question
        or both changeset parents contain different file revisions.
        """

        renamed = self._filelog.renamed(self._filenode)
        if not renamed:
            return None

        if self.rev() == self.linkrev():
            return renamed

        name = self.path()
        fnode = self._filenode
        for p in self._changectx.parents():
            try:
                if fnode == p.filenode(name):
                    return None
            except error.LookupError:
                pass
        return renamed

    def children(self):
        # hard for renames
        c = self._filelog.children(self._filenode)
        return [filectx(self._repo, self._path, fileid=x,
                        filelog=self._filelog) for x in c]

class committablectx(basectx):
    """A committablectx object provides common functionality for a context that
    wants the ability to commit, e.g. workingctx or memctx."""
    def __init__(self, repo, text="", user=None, date=None, extra=None,
                 changes=None):
        super(committablectx, self).__init__(repo)
        self._rev = None
        self._node = None
        self._text = text
        if date:
            self._date = dateutil.parsedate(date)
        if user:
            self._user = user
        if changes:
            self._status = changes

        self._extra = {}
        if extra:
            self._extra = extra.copy()
        if 'branch' not in self._extra:
            try:
                branch = encoding.fromlocal(self._repo.dirstate.branch())
            except UnicodeDecodeError:
                raise error.Abort(_('branch name not in UTF-8!'))
            self._extra['branch'] = branch
        if self._extra['branch'] == '':
            self._extra['branch'] = 'default'

    def __bytes__(self):
        return bytes(self._parents[0]) + "+"

    __str__ = encoding.strmethod(__bytes__)

    def __nonzero__(self):
        return True

    __bool__ = __nonzero__

    def _buildflagfunc(self):
        # Create a fallback function for getting file flags when the
        # filesystem doesn't support them

        copiesget = self._repo.dirstate.copies().get
        parents = self.parents()
        if len(parents) < 2:
            # when we have one parent, it's easy: copy from parent
            man = parents[0].manifest()
            def func(f):
                f = copiesget(f, f)
                return man.flags(f)
        else:
            # merges are tricky: we try to reconstruct the unstored
            # result from the merge (issue1802)
            p1, p2 = parents
            pa = p1.ancestor(p2)
            m1, m2, ma = p1.manifest(), p2.manifest(), pa.manifest()

            def func(f):
                f = copiesget(f, f) # may be wrong for merges with copies
                fl1, fl2, fla = m1.flags(f), m2.flags(f), ma.flags(f)
                if fl1 == fl2:
                    return fl1
                if fl1 == fla:
                    return fl2
                if fl2 == fla:
                    return fl1
                return '' # punt for conflicts

        return func

    @propertycache
    def _flagfunc(self):
        return self._repo.dirstate.flagfunc(self._buildflagfunc)

    @propertycache
    def _status(self):
        return self._repo.status()

    @propertycache
    def _user(self):
        return self._repo.ui.username()

    @propertycache
    def _date(self):
        ui = self._repo.ui
        date = ui.configdate('devel', 'default-date')
        if date is None:
            date = dateutil.makedate()
        return date

    def subrev(self, subpath):
        return None

    def manifestnode(self):
        return None
    def user(self):
        return self._user or self._repo.ui.username()
    def date(self):
        return self._date
    def description(self):
        return self._text
    def files(self):
        return sorted(self._status.modified + self._status.added +
                      self._status.removed)

    def modified(self):
        return self._status.modified
    def added(self):
        return self._status.added
    def removed(self):
        return self._status.removed
    def deleted(self):
        return self._status.deleted
    def branch(self):
        return encoding.tolocal(self._extra['branch'])
    def closesbranch(self):
        return 'close' in self._extra
    def extra(self):
        return self._extra

    def isinmemory(self):
        return False

    def tags(self):
        return []

    def bookmarks(self):
        b = []
        for p in self.parents():
            b.extend(p.bookmarks())
        return b

    def phase(self):
        phase = phases.draft # default phase to draft
        for p in self.parents():
            phase = max(phase, p.phase())
        return phase

    def hidden(self):
        return False

    def children(self):
        return []

    def flags(self, path):
        if r'_manifest' in self.__dict__:
            try:
                return self._manifest.flags(path)
            except KeyError:
                return ''

        try:
            return self._flagfunc(path)
        except OSError:
            return ''

    def ancestor(self, c2):
        """return the "best" ancestor context of self and c2"""
        return self._parents[0].ancestor(c2) # punt on two parents for now

    def walk(self, match):
        '''Generates matching file names.'''
        return sorted(self._repo.dirstate.walk(self._repo.narrowmatch(match),
                                               subrepos=sorted(self.substate),
                                               unknown=True, ignored=False))

    def matches(self, match):
        match = self._repo.narrowmatch(match)
        ds = self._repo.dirstate
        return sorted(f for f in ds.matches(match) if ds[f] != 'r')

    def ancestors(self):
        for p in self._parents:
            yield p
        for a in self._repo.changelog.ancestors(
            [p.rev() for p in self._parents]):
            yield self._repo[a]

    def markcommitted(self, node):
        """Perform post-commit cleanup necessary after committing this ctx

        Specifically, this updates backing stores this working context
        wraps to reflect the fact that the changes reflected by this
        workingctx have been committed.  For example, it marks
        modified and added files as normal in the dirstate.

        """

        with self._repo.dirstate.parentchange():
            for f in self.modified() + self.added():
                self._repo.dirstate.normal(f)
            for f in self.removed():
                self._repo.dirstate.drop(f)
            self._repo.dirstate.setparents(node)

        # write changes out explicitly, because nesting wlock at
        # runtime may prevent 'wlock.release()' in 'repo.commit()'
        # from immediately doing so for subsequent changing files
        self._repo.dirstate.write(self._repo.currenttransaction())

    def dirty(self, missing=False, merge=True, branch=True):
        return False

class workingctx(committablectx):
    """A workingctx object makes access to data related to
    the current working directory convenient.
    date - any valid date string or (unixtime, offset), or None.
    user - username string, or None.
    extra - a dictionary of extra values, or None.
    changes - a list of file lists as returned by localrepo.status()
               or None to use the repository status.
    """
    def __init__(self, repo, text="", user=None, date=None, extra=None,
                 changes=None):
        super(workingctx, self).__init__(repo, text, user, date, extra, changes)

    def __iter__(self):
        d = self._repo.dirstate
        for f in d:
            if d[f] != 'r':
                yield f

    def __contains__(self, key):
        return self._repo.dirstate[key] not in "?r"

    def hex(self):
        return hex(wdirid)

    @propertycache
    def _parents(self):
        p = self._repo.dirstate.parents()
        if p[1] == nullid:
            p = p[:-1]
        # use unfiltered repo to delay/avoid loading obsmarkers
        unfi = self._repo.unfiltered()
        return [changectx(self._repo, unfi.changelog.rev(n), n) for n in p]

    def _fileinfo(self, path):
        # populate __dict__['_manifest'] as workingctx has no _manifestdelta
        self._manifest
        return super(workingctx, self)._fileinfo(path)

    def filectx(self, path, filelog=None):
        """get a file context from the working directory"""
        return workingfilectx(self._repo, path, workingctx=self,
                              filelog=filelog)

    def dirty(self, missing=False, merge=True, branch=True):
        "check whether a working directory is modified"
        # check subrepos first
        for s in sorted(self.substate):
            if self.sub(s).dirty(missing=missing):
                return True
        # check current working dir
        return ((merge and self.p2()) or
                (branch and self.branch() != self.p1().branch()) or
                self.modified() or self.added() or self.removed() or
                (missing and self.deleted()))

    def add(self, list, prefix=""):
        with self._repo.wlock():
            ui, ds = self._repo.ui, self._repo.dirstate
            uipath = lambda f: ds.pathto(pathutil.join(prefix, f))
            rejected = []
            lstat = self._repo.wvfs.lstat
            for f in list:
                # ds.pathto() returns an absolute file when this is invoked from
                # the keyword extension.  That gets flagged as non-portable on
                # Windows, since it contains the drive letter and colon.
                scmutil.checkportable(ui, os.path.join(prefix, f))
                try:
                    st = lstat(f)
                except OSError:
                    ui.warn(_("%s does not exist!\n") % uipath(f))
                    rejected.append(f)
                    continue
                limit = ui.configbytes('ui', 'large-file-limit')
                if limit != 0 and st.st_size > limit:
                    ui.warn(_("%s: up to %d MB of RAM may be required "
                              "to manage this file\n"
                              "(use 'hg revert %s' to cancel the "
                              "pending addition)\n")
                            % (f, 3 * st.st_size // 1000000, uipath(f)))
                if not (stat.S_ISREG(st.st_mode) or stat.S_ISLNK(st.st_mode)):
                    ui.warn(_("%s not added: only files and symlinks "
                              "supported currently\n") % uipath(f))
                    rejected.append(f)
                elif ds[f] in 'amn':
                    ui.warn(_("%s already tracked!\n") % uipath(f))
                elif ds[f] == 'r':
                    ds.normallookup(f)
                else:
                    ds.add(f)
            return rejected

    def forget(self, files, prefix=""):
        with self._repo.wlock():
            ds = self._repo.dirstate
            uipath = lambda f: ds.pathto(pathutil.join(prefix, f))
            rejected = []
            for f in files:
                if f not in self._repo.dirstate:
                    self._repo.ui.warn(_("%s not tracked!\n") % uipath(f))
                    rejected.append(f)
                elif self._repo.dirstate[f] != 'a':
                    self._repo.dirstate.remove(f)
                else:
                    self._repo.dirstate.drop(f)
            return rejected

    def undelete(self, list):
        pctxs = self.parents()
        with self._repo.wlock():
            ds = self._repo.dirstate
            for f in list:
                if self._repo.dirstate[f] != 'r':
                    self._repo.ui.warn(_("%s not removed!\n") % ds.pathto(f))
                else:
                    fctx = f in pctxs[0] and pctxs[0][f] or pctxs[1][f]
                    t = fctx.data()
                    self._repo.wwrite(f, t, fctx.flags())
                    self._repo.dirstate.normal(f)

    def copy(self, source, dest):
        try:
            st = self._repo.wvfs.lstat(dest)
        except OSError as err:
            if err.errno != errno.ENOENT:
                raise
            self._repo.ui.warn(_("%s does not exist!\n")
                               % self._repo.dirstate.pathto(dest))
            return
        if not (stat.S_ISREG(st.st_mode) or stat.S_ISLNK(st.st_mode)):
            self._repo.ui.warn(_("copy failed: %s is not a file or a "
                                 "symbolic link\n")
                               % self._repo.dirstate.pathto(dest))
        else:
            with self._repo.wlock():
                if self._repo.dirstate[dest] in '?':
                    self._repo.dirstate.add(dest)
                elif self._repo.dirstate[dest] in 'r':
                    self._repo.dirstate.normallookup(dest)
                self._repo.dirstate.copy(source, dest)

    def match(self, pats=None, include=None, exclude=None, default='glob',
              listsubrepos=False, badfn=None):
        r = self._repo

        # Only a case insensitive filesystem needs magic to translate user input
        # to actual case in the filesystem.
        icasefs = not util.fscasesensitive(r.root)
        return matchmod.match(r.root, r.getcwd(), pats, include, exclude,
                              default, auditor=r.auditor, ctx=self,
                              listsubrepos=listsubrepos, badfn=badfn,
                              icasefs=icasefs)

    def _filtersuspectsymlink(self, files):
        if not files or self._repo.dirstate._checklink:
            return files

        # Symlink placeholders may get non-symlink-like contents
        # via user error or dereferencing by NFS or Samba servers,
        # so we filter out any placeholders that don't look like a
        # symlink
        sane = []
        for f in files:
            if self.flags(f) == 'l':
                d = self[f].data()
                if (d == '' or len(d) >= 1024 or '\n' in d
                    or stringutil.binary(d)):
                    self._repo.ui.debug('ignoring suspect symlink placeholder'
                                        ' "%s"\n' % f)
                    continue
            sane.append(f)
        return sane

    def _checklookup(self, files):
        # check for any possibly clean files
        if not files:
            return [], [], []

        modified = []
        deleted = []
        fixup = []
        pctx = self._parents[0]
        # do a full compare of any files that might have changed
        for f in sorted(files):
            try:
                # This will return True for a file that got replaced by a
                # directory in the interim, but fixing that is pretty hard.
                if (f not in pctx or self.flags(f) != pctx.flags(f)
                    or pctx[f].cmp(self[f])):
                    modified.append(f)
                else:
                    fixup.append(f)
            except (IOError, OSError):
                # A file become inaccessible in between? Mark it as deleted,
                # matching dirstate behavior (issue5584).
                # The dirstate has more complex behavior around whether a
                # missing file matches a directory, etc, but we don't need to
                # bother with that: if f has made it to this point, we're sure
                # it's in the dirstate.
                deleted.append(f)

        return modified, deleted, fixup

    def _poststatusfixup(self, status, fixup):
        """update dirstate for files that are actually clean"""
        poststatus = self._repo.postdsstatus()
        if fixup or poststatus:
            try:
                oldid = self._repo.dirstate.identity()

                # updating the dirstate is optional
                # so we don't wait on the lock
                # wlock can invalidate the dirstate, so cache normal _after_
                # taking the lock
                with self._repo.wlock(False):
                    if self._repo.dirstate.identity() == oldid:
                        if fixup:
                            normal = self._repo.dirstate.normal
                            for f in fixup:
                                normal(f)
                            # write changes out explicitly, because nesting
                            # wlock at runtime may prevent 'wlock.release()'
                            # after this block from doing so for subsequent
                            # changing files
                            tr = self._repo.currenttransaction()
                            self._repo.dirstate.write(tr)

                        if poststatus:
                            for ps in poststatus:
                                ps(self, status)
                    else:
                        # in this case, writing changes out breaks
                        # consistency, because .hg/dirstate was
                        # already changed simultaneously after last
                        # caching (see also issue5584 for detail)
                        self._repo.ui.debug('skip updating dirstate: '
                                            'identity mismatch\n')
            except error.LockError:
                pass
            finally:
                # Even if the wlock couldn't be grabbed, clear out the list.
                self._repo.clearpostdsstatus()

    def _dirstatestatus(self, match, ignored=False, clean=False, unknown=False):
        '''Gets the status from the dirstate -- internal use only.'''
        subrepos = []
        if '.hgsub' in self:
            subrepos = sorted(self.substate)
        cmp, s = self._repo.dirstate.status(match, subrepos, ignored=ignored,
                                            clean=clean, unknown=unknown)

        # check for any possibly clean files
        fixup = []
        if cmp:
            modified2, deleted2, fixup = self._checklookup(cmp)
            s.modified.extend(modified2)
            s.deleted.extend(deleted2)

            if fixup and clean:
                s.clean.extend(fixup)

        self._poststatusfixup(s, fixup)

        if match.always():
            # cache for performance
            if s.unknown or s.ignored or s.clean:
                # "_status" is cached with list*=False in the normal route
                self._status = scmutil.status(s.modified, s.added, s.removed,
                                              s.deleted, [], [], [])
            else:
                self._status = s

        return s

    @propertycache
    def _manifest(self):
        """generate a manifest corresponding to the values in self._status

        This reuse the file nodeid from parent, but we use special node
        identifiers for added and modified files. This is used by manifests
        merge to see that files are different and by update logic to avoid
        deleting newly added files.
        """
        return self._buildstatusmanifest(self._status)

    def _buildstatusmanifest(self, status):
        """Builds a manifest that includes the given status results."""
        parents = self.parents()

        man = parents[0].manifest().copy()

        ff = self._flagfunc
        for i, l in ((addednodeid, status.added),
                     (modifiednodeid, status.modified)):
            for f in l:
                man[f] = i
                try:
                    man.setflag(f, ff(f))
                except OSError:
                    pass

        for f in status.deleted + status.removed:
            if f in man:
                del man[f]

        return man

    def _buildstatus(self, other, s, match, listignored, listclean,
                     listunknown):
        """build a status with respect to another context

        This includes logic for maintaining the fast path of status when
        comparing the working directory against its parent, which is to skip
        building a new manifest if self (working directory) is not comparing
        against its parent (repo['.']).
        """
        s = self._dirstatestatus(match, listignored, listclean, listunknown)
        # Filter out symlinks that, in the case of FAT32 and NTFS filesystems,
        # might have accidentally ended up with the entire contents of the file
        # they are supposed to be linking to.
        s.modified[:] = self._filtersuspectsymlink(s.modified)
        if other != self._repo['.']:
            s = super(workingctx, self)._buildstatus(other, s, match,
                                                     listignored, listclean,
                                                     listunknown)
        return s

    def _matchstatus(self, other, match):
        """override the match method with a filter for directory patterns

        We use inheritance to customize the match.bad method only in cases of
        workingctx since it belongs only to the working directory when
        comparing against the parent changeset.

        If we aren't comparing against the working directory's parent, then we
        just use the default match object sent to us.
        """
        if other != self._repo['.']:
            def bad(f, msg):
                # 'f' may be a directory pattern from 'match.files()',
                # so 'f not in ctx1' is not enough
                if f not in other and not other.hasdir(f):
                    self._repo.ui.warn('%s: %s\n' %
                                       (self._repo.dirstate.pathto(f), msg))
            match.bad = bad
        return match

    def markcommitted(self, node):
        super(workingctx, self).markcommitted(node)

        sparse.aftercommit(self._repo, node)

class committablefilectx(basefilectx):
    """A committablefilectx provides common functionality for a file context
    that wants the ability to commit, e.g. workingfilectx or memfilectx."""
    def __init__(self, repo, path, filelog=None, ctx=None):
        self._repo = repo
        self._path = path
        self._changeid = None
        self._filerev = self._filenode = None

        if filelog is not None:
            self._filelog = filelog
        if ctx:
            self._changectx = ctx

    def __nonzero__(self):
        return True

    __bool__ = __nonzero__

    def linkrev(self):
        # linked to self._changectx no matter if file is modified or not
        return self.rev()

    def parents(self):
        '''return parent filectxs, following copies if necessary'''
        def filenode(ctx, path):
            return ctx._manifest.get(path, nullid)

        path = self._path
        fl = self._filelog
        pcl = self._changectx._parents
        renamed = self.renamed()

        if renamed:
            pl = [renamed + (None,)]
        else:
            pl = [(path, filenode(pcl[0], path), fl)]

        for pc in pcl[1:]:
            pl.append((path, filenode(pc, path), fl))

        return [self._parentfilectx(p, fileid=n, filelog=l)
                for p, n, l in pl if n != nullid]

    def children(self):
        return []

class workingfilectx(committablefilectx):
    """A workingfilectx object makes access to data related to a particular
       file in the working directory convenient."""
    def __init__(self, repo, path, filelog=None, workingctx=None):
        super(workingfilectx, self).__init__(repo, path, filelog, workingctx)

    @propertycache
    def _changectx(self):
        return workingctx(self._repo)

    def data(self):
        return self._repo.wread(self._path)
    def renamed(self):
        rp = self._repo.dirstate.copied(self._path)
        if not rp:
            return None
        return rp, self._changectx._parents[0]._manifest.get(rp, nullid)

    def size(self):
        return self._repo.wvfs.lstat(self._path).st_size
    def date(self):
        t, tz = self._changectx.date()
        try:
            return (self._repo.wvfs.lstat(self._path)[stat.ST_MTIME], tz)
        except OSError as err:
            if err.errno != errno.ENOENT:
                raise
            return (t, tz)

    def exists(self):
        return self._repo.wvfs.exists(self._path)

    def lexists(self):
        return self._repo.wvfs.lexists(self._path)

    def audit(self):
        return self._repo.wvfs.audit(self._path)

    def cmp(self, fctx):
        """compare with other file context

        returns True if different than fctx.
        """
        # fctx should be a filectx (not a workingfilectx)
        # invert comparison to reuse the same code path
        return fctx.cmp(self)

    def remove(self, ignoremissing=False):
        """wraps unlink for a repo's working directory"""
        rmdir = self._repo.ui.configbool('experimental', 'removeemptydirs')
        self._repo.wvfs.unlinkpath(self._path, ignoremissing=ignoremissing,
                                   rmdir=rmdir)

    def write(self, data, flags, backgroundclose=False, **kwargs):
        """wraps repo.wwrite"""
        self._repo.wwrite(self._path, data, flags,
                          backgroundclose=backgroundclose,
                          **kwargs)

    def markcopied(self, src):
        """marks this file a copy of `src`"""
        if self._repo.dirstate[self._path] in "nma":
            self._repo.dirstate.copy(src, self._path)

    def clearunknown(self):
        """Removes conflicting items in the working directory so that
        ``write()`` can be called successfully.
        """
        wvfs = self._repo.wvfs
        f = self._path
        wvfs.audit(f)
        if self._repo.ui.configbool('experimental', 'merge.checkpathconflicts'):
            # remove files under the directory as they should already be
            # warned and backed up
            if wvfs.isdir(f) and not wvfs.islink(f):
                wvfs.rmtree(f, forcibly=True)
            for p in reversed(list(util.finddirs(f))):
                if wvfs.isfileorlink(p):
                    wvfs.unlink(p)
                    break
        else:
            # don't remove files if path conflicts are not processed
            if wvfs.isdir(f) and not wvfs.islink(f):
                wvfs.removedirs(f)

    def setflags(self, l, x):
        self._repo.wvfs.setflags(self._path, l, x)

class overlayworkingctx(committablectx):
    """Wraps another mutable context with a write-back cache that can be
    converted into a commit context.

    self._cache[path] maps to a dict with keys: {
        'exists': bool?
        'date': date?
        'data': str?
        'flags': str?
        'copied': str? (path or None)
    }
    If `exists` is True, `flags` must be non-None and 'date' is non-None. If it
    is `False`, the file was deleted.
    """

    def __init__(self, repo):
        super(overlayworkingctx, self).__init__(repo)
        self.clean()

    def setbase(self, wrappedctx):
        self._wrappedctx = wrappedctx
        self._parents = [wrappedctx]
        # Drop old manifest cache as it is now out of date.
        # This is necessary when, e.g., rebasing several nodes with one
        # ``overlayworkingctx`` (e.g. with --collapse).
        util.clearcachedproperty(self, '_manifest')

    def data(self, path):
        if self.isdirty(path):
            if self._cache[path]['exists']:
                if self._cache[path]['data']:
                    return self._cache[path]['data']
                else:
                    # Must fallback here, too, because we only set flags.
                    return self._wrappedctx[path].data()
            else:
                raise error.ProgrammingError("No such file or directory: %s" %
                                             path)
        else:
            return self._wrappedctx[path].data()

    @propertycache
    def _manifest(self):
        parents = self.parents()
        man = parents[0].manifest().copy()

        flag = self._flagfunc
        for path in self.added():
            man[path] = addednodeid
            man.setflag(path, flag(path))
        for path in self.modified():
            man[path] = modifiednodeid
            man.setflag(path, flag(path))
        for path in self.removed():
            del man[path]
        return man

    @propertycache
    def _flagfunc(self):
        def f(path):
            return self._cache[path]['flags']
        return f

    def files(self):
        return sorted(self.added() + self.modified() + self.removed())

    def modified(self):
        return [f for f in self._cache.keys() if self._cache[f]['exists'] and
                self._existsinparent(f)]

    def added(self):
        return [f for f in self._cache.keys() if self._cache[f]['exists'] and
                not self._existsinparent(f)]

    def removed(self):
        return [f for f in self._cache.keys() if
                not self._cache[f]['exists'] and self._existsinparent(f)]

    def isinmemory(self):
        return True

    def filedate(self, path):
        if self.isdirty(path):
            return self._cache[path]['date']
        else:
            return self._wrappedctx[path].date()

    def markcopied(self, path, origin):
        if self.isdirty(path):
            self._cache[path]['copied'] = origin
        else:
            raise error.ProgrammingError('markcopied() called on clean context')

    def copydata(self, path):
        if self.isdirty(path):
            return self._cache[path]['copied']
        else:
            raise error.ProgrammingError('copydata() called on clean context')

    def flags(self, path):
        if self.isdirty(path):
            if self._cache[path]['exists']:
                return self._cache[path]['flags']
            else:
                raise error.ProgrammingError("No such file or directory: %s" %
                                             self._path)
        else:
            return self._wrappedctx[path].flags()

    def __contains__(self, key):
        if key in self._cache:
            return self._cache[key]['exists']
        return key in self.p1()

    def _existsinparent(self, path):
        try:
            # ``commitctx` raises a ``ManifestLookupError`` if a path does not
            # exist, unlike ``workingctx``, which returns a ``workingfilectx``
            # with an ``exists()`` function.
            self._wrappedctx[path]
            return True
        except error.ManifestLookupError:
            return False

    def _auditconflicts(self, path):
        """Replicates conflict checks done by wvfs.write().

        Since we never write to the filesystem and never call `applyupdates` in
        IMM, we'll never check that a path is actually writable -- e.g., because
        it adds `a/foo`, but `a` is actually a file in the other commit.
        """
        def fail(path, component):
            # p1() is the base and we're receiving "writes" for p2()'s
            # files.
            if 'l' in self.p1()[component].flags():
                raise error.Abort("error: %s conflicts with symlink %s "
                                  "in %s." % (path, component,
                                              self.p1().rev()))
            else:
                raise error.Abort("error: '%s' conflicts with file '%s' in "
                                  "%s." % (path, component,
                                           self.p1().rev()))

        # Test that each new directory to be created to write this path from p2
        # is not a file in p1.
        components = path.split('/')
        for i in pycompat.xrange(len(components)):
            component = "/".join(components[0:i])
            if component in self:
                fail(path, component)

        # Test the other direction -- that this path from p2 isn't a directory
        # in p1 (test that p1 doesn't have any paths matching `path/*`).
        match = self.match(pats=[path + '/'], default=b'path')
        matches = self.p1().manifest().matches(match)
        mfiles = matches.keys()
        if len(mfiles) > 0:
            if len(mfiles) == 1 and mfiles[0] == path:
                return
            # omit the files which are deleted in current IMM wctx
            mfiles = [m for m in mfiles if m in self]
            if not mfiles:
                return
            raise error.Abort("error: file '%s' cannot be written because "
                              " '%s/' is a folder in %s (containing %d "
                              "entries: %s)"
                              % (path, path, self.p1(), len(mfiles),
                                 ', '.join(mfiles)))

    def write(self, path, data, flags='', **kwargs):
        if data is None:
            raise error.ProgrammingError("data must be non-None")
        self._auditconflicts(path)
        self._markdirty(path, exists=True, data=data, date=dateutil.makedate(),
                        flags=flags)

    def setflags(self, path, l, x):
        flag = ''
        if l:
            flag = 'l'
        elif x:
            flag = 'x'
        self._markdirty(path, exists=True, date=dateutil.makedate(),
                        flags=flag)

    def remove(self, path):
        self._markdirty(path, exists=False)

    def exists(self, path):
        """exists behaves like `lexists`, but needs to follow symlinks and
        return False if they are broken.
        """
        if self.isdirty(path):
            # If this path exists and is a symlink, "follow" it by calling
            # exists on the destination path.
            if (self._cache[path]['exists'] and
                        'l' in self._cache[path]['flags']):
                return self.exists(self._cache[path]['data'].strip())
            else:
                return self._cache[path]['exists']

        return self._existsinparent(path)

    def lexists(self, path):
        """lexists returns True if the path exists"""
        if self.isdirty(path):
            return self._cache[path]['exists']

        return self._existsinparent(path)

    def size(self, path):
        if self.isdirty(path):
            if self._cache[path]['exists']:
                return len(self._cache[path]['data'])
            else:
                raise error.ProgrammingError("No such file or directory: %s" %
                                             self._path)
        return self._wrappedctx[path].size()

    def tomemctx(self, text, branch=None, extra=None, date=None, parents=None,
                 user=None, editor=None):
        """Converts this ``overlayworkingctx`` into a ``memctx`` ready to be
        committed.

        ``text`` is the commit message.
        ``parents`` (optional) are rev numbers.
        """
        # Default parents to the wrapped contexts' if not passed.
        if parents is None:
            parents = self._wrappedctx.parents()
            if len(parents) == 1:
                parents = (parents[0], None)

        # ``parents`` is passed as rev numbers; convert to ``commitctxs``.
        if parents[1] is None:
            parents = (self._repo[parents[0]], None)
        else:
            parents = (self._repo[parents[0]], self._repo[parents[1]])

        files = self._cache.keys()
        def getfile(repo, memctx, path):
            if self._cache[path]['exists']:
                return memfilectx(repo, memctx, path,
                                  self._cache[path]['data'],
                                  'l' in self._cache[path]['flags'],
                                  'x' in self._cache[path]['flags'],
                                  self._cache[path]['copied'])
            else:
                # Returning None, but including the path in `files`, is
                # necessary for memctx to register a deletion.
                return None
        return memctx(self._repo, parents, text, files, getfile, date=date,
                      extra=extra, user=user, branch=branch, editor=editor)

    def isdirty(self, path):
        return path in self._cache

    def isempty(self):
        # We need to discard any keys that are actually clean before the empty
        # commit check.
        self._compact()
        return len(self._cache) == 0

    def clean(self):
        self._cache = {}

    def _compact(self):
        """Removes keys from the cache that are actually clean, by comparing
        them with the underlying context.

        This can occur during the merge process, e.g. by passing --tool :local
        to resolve a conflict.
        """
        keys = []
        for path in self._cache.keys():
            cache = self._cache[path]
            try:
                underlying = self._wrappedctx[path]
                if (underlying.data() == cache['data'] and
                            underlying.flags() == cache['flags']):
                    keys.append(path)
            except error.ManifestLookupError:
                # Path not in the underlying manifest (created).
                continue

        for path in keys:
            del self._cache[path]
        return keys

    def _markdirty(self, path, exists, data=None, date=None, flags=''):
        # data not provided, let's see if we already have some; if not, let's
        # grab it from our underlying context, so that we always have data if
        # the file is marked as existing.
        if exists and data is None:
            oldentry = self._cache.get(path) or {}
            data = oldentry.get('data') or self._wrappedctx[path].data()

        self._cache[path] = {
            'exists': exists,
            'data': data,
            'date': date,
            'flags': flags,
            'copied': None,
        }

    def filectx(self, path, filelog=None):
        return overlayworkingfilectx(self._repo, path, parent=self,
                                     filelog=filelog)

class overlayworkingfilectx(committablefilectx):
    """Wrap a ``workingfilectx`` but intercepts all writes into an in-memory
    cache, which can be flushed through later by calling ``flush()``."""

    def __init__(self, repo, path, filelog=None, parent=None):
        super(overlayworkingfilectx, self).__init__(repo, path, filelog,
                                                    parent)
        self._repo = repo
        self._parent = parent
        self._path = path

    def cmp(self, fctx):
        return self.data() != fctx.data()

    def changectx(self):
        return self._parent

    def data(self):
        return self._parent.data(self._path)

    def date(self):
        return self._parent.filedate(self._path)

    def exists(self):
        return self.lexists()

    def lexists(self):
        return self._parent.exists(self._path)

    def renamed(self):
        path = self._parent.copydata(self._path)
        if not path:
            return None
        return path, self._changectx._parents[0]._manifest.get(path, nullid)

    def size(self):
        return self._parent.size(self._path)

    def markcopied(self, origin):
        self._parent.markcopied(self._path, origin)

    def audit(self):
        pass

    def flags(self):
        return self._parent.flags(self._path)

    def setflags(self, islink, isexec):
        return self._parent.setflags(self._path, islink, isexec)

    def write(self, data, flags, backgroundclose=False, **kwargs):
        return self._parent.write(self._path, data, flags, **kwargs)

    def remove(self, ignoremissing=False):
        return self._parent.remove(self._path)

    def clearunknown(self):
        pass

class workingcommitctx(workingctx):
    """A workingcommitctx object makes access to data related to
    the revision being committed convenient.

    This hides changes in the working directory, if they aren't
    committed in this context.
    """
    def __init__(self, repo, changes,
                 text="", user=None, date=None, extra=None):
        super(workingcommitctx, self).__init__(repo, text, user, date, extra,
                                               changes)

    def _dirstatestatus(self, match, ignored=False, clean=False, unknown=False):
        """Return matched files only in ``self._status``

        Uncommitted files appear "clean" via this context, even if
        they aren't actually so in the working directory.
        """
        if clean:
            clean = [f for f in self._manifest if f not in self._changedset]
        else:
            clean = []
        return scmutil.status([f for f in self._status.modified if match(f)],
                              [f for f in self._status.added if match(f)],
                              [f for f in self._status.removed if match(f)],
                              [], [], [], clean)

    @propertycache
    def _changedset(self):
        """Return the set of files changed in this context
        """
        changed = set(self._status.modified)
        changed.update(self._status.added)
        changed.update(self._status.removed)
        return changed

def makecachingfilectxfn(func):
    """Create a filectxfn that caches based on the path.

    We can't use util.cachefunc because it uses all arguments as the cache
    key and this creates a cycle since the arguments include the repo and
    memctx.
    """
    cache = {}

    def getfilectx(repo, memctx, path):
        if path not in cache:
            cache[path] = func(repo, memctx, path)
        return cache[path]

    return getfilectx

def memfilefromctx(ctx):
    """Given a context return a memfilectx for ctx[path]

    This is a convenience method for building a memctx based on another
    context.
    """
    def getfilectx(repo, memctx, path):
        fctx = ctx[path]
        # this is weird but apparently we only keep track of one parent
        # (why not only store that instead of a tuple?)
        copied = fctx.renamed()
        if copied:
            copied = copied[0]
        return memfilectx(repo, memctx, path, fctx.data(),
                          islink=fctx.islink(), isexec=fctx.isexec(),
                          copied=copied)

    return getfilectx

def memfilefrompatch(patchstore):
    """Given a patch (e.g. patchstore object) return a memfilectx

    This is a convenience method for building a memctx based on a patchstore.
    """
    def getfilectx(repo, memctx, path):
        data, mode, copied = patchstore.getfile(path)
        if data is None:
            return None
        islink, isexec = mode
        return memfilectx(repo, memctx, path, data, islink=islink,
                          isexec=isexec, copied=copied)

    return getfilectx

class memctx(committablectx):
    """Use memctx to perform in-memory commits via localrepo.commitctx().

    Revision information is supplied at initialization time while
    related files data and is made available through a callback
    mechanism.  'repo' is the current localrepo, 'parents' is a
    sequence of two parent revisions identifiers (pass None for every
    missing parent), 'text' is the commit message and 'files' lists
    names of files touched by the revision (normalized and relative to
    repository root).

    filectxfn(repo, memctx, path) is a callable receiving the
    repository, the current memctx object and the normalized path of
    requested file, relative to repository root. It is fired by the
    commit function for every file in 'files', but calls order is
    undefined. If the file is available in the revision being
    committed (updated or added), filectxfn returns a memfilectx
    object. If the file was removed, filectxfn return None for recent
    Mercurial. Moved files are represented by marking the source file
    removed and the new file added with copy information (see
    memfilectx).

    user receives the committer name and defaults to current
    repository username, date is the commit date in any format
    supported by dateutil.parsedate() and defaults to current date, extra
    is a dictionary of metadata or is left empty.
    """

    # Mercurial <= 3.1 expects the filectxfn to raise IOError for missing files.
    # Extensions that need to retain compatibility across Mercurial 3.1 can use
    # this field to determine what to do in filectxfn.
    _returnnoneformissingfiles = True

    def __init__(self, repo, parents, text, files, filectxfn, user=None,
                 date=None, extra=None, branch=None, editor=False):
        super(memctx, self).__init__(repo, text, user, date, extra)
        self._rev = None
        self._node = None
        parents = [(p or nullid) for p in parents]
        p1, p2 = parents
        self._parents = [self._repo[p] for p in (p1, p2)]
        files = sorted(set(files))
        self._files = files
        if branch is not None:
            self._extra['branch'] = encoding.fromlocal(branch)
        self.substate = {}

        if isinstance(filectxfn, patch.filestore):
            filectxfn = memfilefrompatch(filectxfn)
        elif not callable(filectxfn):
            # if store is not callable, wrap it in a function
            filectxfn = memfilefromctx(filectxfn)

        # memoizing increases performance for e.g. vcs convert scenarios.
        self._filectxfn = makecachingfilectxfn(filectxfn)

        if editor:
            self._text = editor(self._repo, self, [])
            self._repo.savecommitmessage(self._text)

    def filectx(self, path, filelog=None):
        """get a file context from the working directory

        Returns None if file doesn't exist and should be removed."""
        return self._filectxfn(self._repo, self, path)

    def commit(self):
        """commit context to the repo"""
        return self._repo.commitctx(self)

    @propertycache
    def _manifest(self):
        """generate a manifest based on the return values of filectxfn"""

        # keep this simple for now; just worry about p1
        pctx = self._parents[0]
        man = pctx.manifest().copy()

        for f in self._status.modified:
            man[f] = modifiednodeid

        for f in self._status.added:
            man[f] = addednodeid

        for f in self._status.removed:
            if f in man:
                del man[f]

        return man

    @propertycache
    def _status(self):
        """Calculate exact status from ``files`` specified at construction
        """
        man1 = self.p1().manifest()
        p2 = self._parents[1]
        # "1 < len(self._parents)" can't be used for checking
        # existence of the 2nd parent, because "memctx._parents" is
        # explicitly initialized by the list, of which length is 2.
        if p2.node() != nullid:
            man2 = p2.manifest()
            managing = lambda f: f in man1 or f in man2
        else:
            managing = lambda f: f in man1

        modified, added, removed = [], [], []
        for f in self._files:
            if not managing(f):
                added.append(f)
            elif self[f]:
                modified.append(f)
            else:
                removed.append(f)

        return scmutil.status(modified, added, removed, [], [], [], [])

class memfilectx(committablefilectx):
    """memfilectx represents an in-memory file to commit.

    See memctx and committablefilectx for more details.
    """
    def __init__(self, repo, changectx, path, data, islink=False,
                 isexec=False, copied=None):
        """
        path is the normalized file path relative to repository root.
        data is the file content as a string.
        islink is True if the file is a symbolic link.
        isexec is True if the file is executable.
        copied is the source file path if current file was copied in the
        revision being committed, or None."""
        super(memfilectx, self).__init__(repo, path, None, changectx)
        self._data = data
        if islink:
            self._flags = 'l'
        elif isexec:
            self._flags = 'x'
        else:
            self._flags = ''
        self._copied = None
        if copied:
            self._copied = (copied, nullid)

    def cmp(self, fctx):
        return self.data() != fctx.data()

    def data(self):
        return self._data

    def remove(self, ignoremissing=False):
        """wraps unlink for a repo's working directory"""
        # need to figure out what to do here
        del self._changectx[self._path]

    def write(self, data, flags, **kwargs):
        """wraps repo.wwrite"""
        self._data = data


class metadataonlyctx(committablectx):
    """Like memctx but it's reusing the manifest of different commit.
    Intended to be used by lightweight operations that are creating
    metadata-only changes.

    Revision information is supplied at initialization time.  'repo' is the
    current localrepo, 'ctx' is original revision which manifest we're reuisng
    'parents' is a sequence of two parent revisions identifiers (pass None for
    every missing parent), 'text' is the commit.

    user receives the committer name and defaults to current repository
    username, date is the commit date in any format supported by
    dateutil.parsedate() and defaults to current date, extra is a dictionary of
    metadata or is left empty.
    """
    def __init__(self, repo, originalctx, parents=None, text=None, user=None,
                 date=None, extra=None, editor=False):
        if text is None:
            text = originalctx.description()
        super(metadataonlyctx, self).__init__(repo, text, user, date, extra)
        self._rev = None
        self._node = None
        self._originalctx = originalctx
        self._manifestnode = originalctx.manifestnode()
        if parents is None:
            parents = originalctx.parents()
        else:
            parents = [repo[p] for p in parents if p is not None]
        parents = parents[:]
        while len(parents) < 2:
            parents.append(repo[nullid])
        p1, p2 = self._parents = parents

        # sanity check to ensure that the reused manifest parents are
        # manifests of our commit parents
        mp1, mp2 = self.manifestctx().parents
        if p1 != nullid and p1.manifestnode() != mp1:
            raise RuntimeError(r"can't reuse the manifest: its p1 "
                               r"doesn't match the new ctx p1")
        if p2 != nullid and p2.manifestnode() != mp2:
            raise RuntimeError(r"can't reuse the manifest: "
                               r"its p2 doesn't match the new ctx p2")

        self._files = originalctx.files()
        self.substate = {}

        if editor:
            self._text = editor(self._repo, self, [])
            self._repo.savecommitmessage(self._text)

    def manifestnode(self):
        return self._manifestnode

    @property
    def _manifestctx(self):
        return self._repo.manifestlog[self._manifestnode]

    def filectx(self, path, filelog=None):
        return self._originalctx.filectx(path, filelog=filelog)

    def commit(self):
        """commit context to the repo"""
        return self._repo.commitctx(self)

    @property
    def _manifest(self):
        return self._originalctx.manifest()

    @propertycache
    def _status(self):
        """Calculate exact status from ``files`` specified in the ``origctx``
        and parents manifests.
        """
        man1 = self.p1().manifest()
        p2 = self._parents[1]
        # "1 < len(self._parents)" can't be used for checking
        # existence of the 2nd parent, because "metadataonlyctx._parents" is
        # explicitly initialized by the list, of which length is 2.
        if p2.node() != nullid:
            man2 = p2.manifest()
            managing = lambda f: f in man1 or f in man2
        else:
            managing = lambda f: f in man1

        modified, added, removed = [], [], []
        for f in self._files:
            if not managing(f):
                added.append(f)
            elif f in self:
                modified.append(f)
            else:
                removed.append(f)

        return scmutil.status(modified, added, removed, [], [], [], [])

class arbitraryfilectx(object):
    """Allows you to use filectx-like functions on a file in an arbitrary
    location on disk, possibly not in the working directory.
    """
    def __init__(self, path, repo=None):
        # Repo is optional because contrib/simplemerge uses this class.
        self._repo = repo
        self._path = path

    def cmp(self, fctx):
        # filecmp follows symlinks whereas `cmp` should not, so skip the fast
        # path if either side is a symlink.
        symlinks = ('l' in self.flags() or 'l' in fctx.flags())
        if not symlinks and isinstance(fctx, workingfilectx) and self._repo:
            # Add a fast-path for merge if both sides are disk-backed.
            # Note that filecmp uses the opposite return values (True if same)
            # from our cmp functions (True if different).
            return not filecmp.cmp(self.path(), self._repo.wjoin(fctx.path()))
        return self.data() != fctx.data()

    def path(self):
        return self._path

    def flags(self):
        return ''

    def data(self):
        return util.readfile(self._path)

    def decodeddata(self):
        with open(self._path, "rb") as f:
            return f.read()

    def remove(self):
        util.unlink(self._path)

    def write(self, data, flags, **kwargs):
        assert not flags
        with open(self._path, "wb") as f:
            f.write(data)
