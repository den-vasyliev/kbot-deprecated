# convcmd - convert extension commands definition
#
# Copyright 2005-2007 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.
from __future__ import absolute_import

import collections
import os
import shutil

from mercurial.i18n import _
from mercurial import (
    encoding,
    error,
    hg,
    pycompat,
    scmutil,
    util,
)
from mercurial.utils import dateutil

from . import (
    bzr,
    common,
    cvs,
    darcs,
    filemap,
    git,
    gnuarch,
    hg as hgconvert,
    monotone,
    p4,
    subversion,
)

mapfile = common.mapfile
MissingTool = common.MissingTool
NoRepo = common.NoRepo
SKIPREV = common.SKIPREV

bzr_source = bzr.bzr_source
convert_cvs = cvs.convert_cvs
convert_git = git.convert_git
darcs_source = darcs.darcs_source
gnuarch_source = gnuarch.gnuarch_source
mercurial_sink = hgconvert.mercurial_sink
mercurial_source = hgconvert.mercurial_source
monotone_source = monotone.monotone_source
p4_source = p4.p4_source
svn_sink = subversion.svn_sink
svn_source = subversion.svn_source

orig_encoding = 'ascii'

def recode(s):
    if isinstance(s, pycompat.unicode):
        return s.encode(pycompat.sysstr(orig_encoding), 'replace')
    else:
        return s.decode('utf-8').encode(
            pycompat.sysstr(orig_encoding), 'replace')

def mapbranch(branch, branchmap):
    '''
    >>> bmap = {b'default': b'branch1'}
    >>> for i in [b'', None]:
    ...     mapbranch(i, bmap)
    'branch1'
    'branch1'
    >>> bmap = {b'None': b'branch2'}
    >>> for i in [b'', None]:
    ...     mapbranch(i, bmap)
    'branch2'
    'branch2'
    >>> bmap = {b'None': b'branch3', b'default': b'branch4'}
    >>> for i in [b'None', b'', None, b'default', b'branch5']:
    ...     mapbranch(i, bmap)
    'branch3'
    'branch4'
    'branch4'
    'branch4'
    'branch5'
    '''
    # If branch is None or empty, this commit is coming from the source
    # repository's default branch and destined for the default branch in the
    # destination repository. For such commits, using a literal "default"
    # in branchmap below allows the user to map "default" to an alternate
    # default branch in the destination repository.
    branch = branchmap.get(branch or 'default', branch)
    # At some point we used "None" literal to denote the default branch,
    # attempt to use that for backward compatibility.
    if (not branch):
        branch = branchmap.get('None', branch)
    return branch

source_converters = [
    ('cvs', convert_cvs, 'branchsort'),
    ('git', convert_git, 'branchsort'),
    ('svn', svn_source, 'branchsort'),
    ('hg', mercurial_source, 'sourcesort'),
    ('darcs', darcs_source, 'branchsort'),
    ('mtn', monotone_source, 'branchsort'),
    ('gnuarch', gnuarch_source, 'branchsort'),
    ('bzr', bzr_source, 'branchsort'),
    ('p4', p4_source, 'branchsort'),
    ]

sink_converters = [
    ('hg', mercurial_sink),
    ('svn', svn_sink),
    ]

def convertsource(ui, path, type, revs):
    exceptions = []
    if type and type not in [s[0] for s in source_converters]:
        raise error.Abort(_('%s: invalid source repository type') % type)
    for name, source, sortmode in source_converters:
        try:
            if not type or name == type:
                return source(ui, name, path, revs), sortmode
        except (NoRepo, MissingTool) as inst:
            exceptions.append(inst)
    if not ui.quiet:
        for inst in exceptions:
            ui.write("%s\n" % pycompat.bytestr(inst))
    raise error.Abort(_('%s: missing or unsupported repository') % path)

def convertsink(ui, path, type):
    if type and type not in [s[0] for s in sink_converters]:
        raise error.Abort(_('%s: invalid destination repository type') % type)
    for name, sink in sink_converters:
        try:
            if not type or name == type:
                return sink(ui, name, path)
        except NoRepo as inst:
            ui.note(_("convert: %s\n") % inst)
        except MissingTool as inst:
            raise error.Abort('%s\n' % inst)
    raise error.Abort(_('%s: unknown repository type') % path)

class progresssource(object):
    def __init__(self, ui, source, filecount):
        self.ui = ui
        self.source = source
        self.progress = ui.makeprogress(_('getting files'), unit=_('files'),
                                        total=filecount)

    def getfile(self, file, rev):
        self.progress.increment(item=file)
        return self.source.getfile(file, rev)

    def targetfilebelongstosource(self, targetfilename):
        return self.source.targetfilebelongstosource(targetfilename)

    def lookuprev(self, rev):
        return self.source.lookuprev(rev)

    def close(self):
        self.progress.complete()

class converter(object):
    def __init__(self, ui, source, dest, revmapfile, opts):

        self.source = source
        self.dest = dest
        self.ui = ui
        self.opts = opts
        self.commitcache = {}
        self.authors = {}
        self.authorfile = None

        # Record converted revisions persistently: maps source revision
        # ID to target revision ID (both strings).  (This is how
        # incremental conversions work.)
        self.map = mapfile(ui, revmapfile)

        # Read first the dst author map if any
        authorfile = self.dest.authorfile()
        if authorfile and os.path.exists(authorfile):
            self.readauthormap(authorfile)
        # Extend/Override with new author map if necessary
        if opts.get('authormap'):
            self.readauthormap(opts.get('authormap'))
            self.authorfile = self.dest.authorfile()

        self.splicemap = self.parsesplicemap(opts.get('splicemap'))
        self.branchmap = mapfile(ui, opts.get('branchmap'))

    def parsesplicemap(self, path):
        """ check and validate the splicemap format and
            return a child/parents dictionary.
            Format checking has two parts.
            1. generic format which is same across all source types
            2. specific format checking which may be different for
               different source type.  This logic is implemented in
               checkrevformat function in source files like
               hg.py, subversion.py etc.
        """

        if not path:
            return {}
        m = {}
        try:
            fp = open(path, 'rb')
            for i, line in enumerate(util.iterfile(fp)):
                line = line.splitlines()[0].rstrip()
                if not line:
                    # Ignore blank lines
                    continue
                # split line
                lex = common.shlexer(data=line, whitespace=',')
                line = list(lex)
                # check number of parents
                if not (2 <= len(line) <= 3):
                    raise error.Abort(_('syntax error in %s(%d): child parent1'
                                       '[,parent2] expected') % (path, i + 1))
                for part in line:
                    self.source.checkrevformat(part)
                child, p1, p2 = line[0], line[1:2], line[2:]
                if p1 == p2:
                    m[child] = p1
                else:
                    m[child] = p1 + p2
         # if file does not exist or error reading, exit
        except IOError:
            raise error.Abort(_('splicemap file not found or error reading %s:')
                               % path)
        return m


    def walktree(self, heads):
        '''Return a mapping that identifies the uncommitted parents of every
        uncommitted changeset.'''
        visit = list(heads)
        known = set()
        parents = {}
        numcommits = self.source.numcommits()
        progress = self.ui.makeprogress(_('scanning'), unit=_('revisions'),
                                        total=numcommits)
        while visit:
            n = visit.pop(0)
            if n in known:
                continue
            if n in self.map:
                m = self.map[n]
                if m == SKIPREV or self.dest.hascommitfrommap(m):
                    continue
            known.add(n)
            progress.update(len(known))
            commit = self.cachecommit(n)
            parents[n] = []
            for p in commit.parents:
                parents[n].append(p)
                visit.append(p)
        progress.complete()

        return parents

    def mergesplicemap(self, parents, splicemap):
        """A splicemap redefines child/parent relationships. Check the
        map contains valid revision identifiers and merge the new
        links in the source graph.
        """
        for c in sorted(splicemap):
            if c not in parents:
                if not self.dest.hascommitforsplicemap(self.map.get(c, c)):
                    # Could be in source but not converted during this run
                    self.ui.warn(_('splice map revision %s is not being '
                                   'converted, ignoring\n') % c)
                continue
            pc = []
            for p in splicemap[c]:
                # We do not have to wait for nodes already in dest.
                if self.dest.hascommitforsplicemap(self.map.get(p, p)):
                    continue
                # Parent is not in dest and not being converted, not good
                if p not in parents:
                    raise error.Abort(_('unknown splice map parent: %s') % p)
                pc.append(p)
            parents[c] = pc

    def toposort(self, parents, sortmode):
        '''Return an ordering such that every uncommitted changeset is
        preceded by all its uncommitted ancestors.'''

        def mapchildren(parents):
            """Return a (children, roots) tuple where 'children' maps parent
            revision identifiers to children ones, and 'roots' is the list of
            revisions without parents. 'parents' must be a mapping of revision
            identifier to its parents ones.
            """
            visit = collections.deque(sorted(parents))
            seen = set()
            children = {}
            roots = []

            while visit:
                n = visit.popleft()
                if n in seen:
                    continue
                seen.add(n)
                # Ensure that nodes without parents are present in the
                # 'children' mapping.
                children.setdefault(n, [])
                hasparent = False
                for p in parents[n]:
                    if p not in self.map:
                        visit.append(p)
                        hasparent = True
                    children.setdefault(p, []).append(n)
                if not hasparent:
                    roots.append(n)

            return children, roots

        # Sort functions are supposed to take a list of revisions which
        # can be converted immediately and pick one

        def makebranchsorter():
            """If the previously converted revision has a child in the
            eligible revisions list, pick it. Return the list head
            otherwise. Branch sort attempts to minimize branch
            switching, which is harmful for Mercurial backend
            compression.
            """
            prev = [None]
            def picknext(nodes):
                next = nodes[0]
                for n in nodes:
                    if prev[0] in parents[n]:
                        next = n
                        break
                prev[0] = next
                return next
            return picknext

        def makesourcesorter():
            """Source specific sort."""
            keyfn = lambda n: self.commitcache[n].sortkey
            def picknext(nodes):
                return sorted(nodes, key=keyfn)[0]
            return picknext

        def makeclosesorter():
            """Close order sort."""
            keyfn = lambda n: ('close' not in self.commitcache[n].extra,
                               self.commitcache[n].sortkey)
            def picknext(nodes):
                return sorted(nodes, key=keyfn)[0]
            return picknext

        def makedatesorter():
            """Sort revisions by date."""
            dates = {}
            def getdate(n):
                if n not in dates:
                    dates[n] = dateutil.parsedate(self.commitcache[n].date)
                return dates[n]

            def picknext(nodes):
                return min([(getdate(n), n) for n in nodes])[1]

            return picknext

        if sortmode == 'branchsort':
            picknext = makebranchsorter()
        elif sortmode == 'datesort':
            picknext = makedatesorter()
        elif sortmode == 'sourcesort':
            picknext = makesourcesorter()
        elif sortmode == 'closesort':
            picknext = makeclosesorter()
        else:
            raise error.Abort(_('unknown sort mode: %s') % sortmode)

        children, actives = mapchildren(parents)

        s = []
        pendings = {}
        while actives:
            n = picknext(actives)
            actives.remove(n)
            s.append(n)

            # Update dependents list
            for c in children.get(n, []):
                if c not in pendings:
                    pendings[c] = [p for p in parents[c] if p not in self.map]
                try:
                    pendings[c].remove(n)
                except ValueError:
                    raise error.Abort(_('cycle detected between %s and %s')
                                       % (recode(c), recode(n)))
                if not pendings[c]:
                    # Parents are converted, node is eligible
                    actives.insert(0, c)
                    pendings[c] = None

        if len(s) != len(parents):
            raise error.Abort(_("not all revisions were sorted"))

        return s

    def writeauthormap(self):
        authorfile = self.authorfile
        if authorfile:
            self.ui.status(_('writing author map file %s\n') % authorfile)
            ofile = open(authorfile, 'wb+')
            for author in self.authors:
                ofile.write(util.tonativeeol("%s=%s\n"
                                             % (author, self.authors[author])))
            ofile.close()

    def readauthormap(self, authorfile):
        afile = open(authorfile, 'rb')
        for line in afile:

            line = line.strip()
            if not line or line.startswith('#'):
                continue

            try:
                srcauthor, dstauthor = line.split('=', 1)
            except ValueError:
                msg = _('ignoring bad line in author map file %s: %s\n')
                self.ui.warn(msg % (authorfile, line.rstrip()))
                continue

            srcauthor = srcauthor.strip()
            dstauthor = dstauthor.strip()
            if self.authors.get(srcauthor) in (None, dstauthor):
                msg = _('mapping author %s to %s\n')
                self.ui.debug(msg % (srcauthor, dstauthor))
                self.authors[srcauthor] = dstauthor
                continue

            m = _('overriding mapping for author %s, was %s, will be %s\n')
            self.ui.status(m % (srcauthor, self.authors[srcauthor], dstauthor))

        afile.close()

    def cachecommit(self, rev):
        commit = self.source.getcommit(rev)
        commit.author = self.authors.get(commit.author, commit.author)
        commit.branch = mapbranch(commit.branch, self.branchmap)
        self.commitcache[rev] = commit
        return commit

    def copy(self, rev):
        commit = self.commitcache[rev]
        full = self.opts.get('full')
        changes = self.source.getchanges(rev, full)
        if isinstance(changes, bytes):
            if changes == SKIPREV:
                dest = SKIPREV
            else:
                dest = self.map[changes]
            self.map[rev] = dest
            return
        files, copies, cleanp2 = changes
        pbranches = []
        if commit.parents:
            for prev in commit.parents:
                if prev not in self.commitcache:
                    self.cachecommit(prev)
                pbranches.append((self.map[prev],
                                  self.commitcache[prev].branch))
        self.dest.setbranch(commit.branch, pbranches)
        try:
            parents = self.splicemap[rev]
            self.ui.status(_('spliced in %s as parents of %s\n') %
                           (_(' and ').join(parents), rev))
            parents = [self.map.get(p, p) for p in parents]
        except KeyError:
            parents = [b[0] for b in pbranches]
            parents.extend(self.map[x]
                           for x in commit.optparents
                           if x in self.map)
        if len(pbranches) != 2:
            cleanp2 = set()
        if len(parents) < 3:
            source = progresssource(self.ui, self.source, len(files))
        else:
            # For an octopus merge, we end up traversing the list of
            # changed files N-1 times. This tweak to the number of
            # files makes it so the progress bar doesn't overflow
            # itself.
            source = progresssource(self.ui, self.source,
                                    len(files) * (len(parents) - 1))
        newnode = self.dest.putcommit(files, copies, parents, commit,
                                      source, self.map, full, cleanp2)
        source.close()
        self.source.converted(rev, newnode)
        self.map[rev] = newnode

    def convert(self, sortmode):
        try:
            self.source.before()
            self.dest.before()
            self.source.setrevmap(self.map)
            self.ui.status(_("scanning source...\n"))
            heads = self.source.getheads()
            parents = self.walktree(heads)
            self.mergesplicemap(parents, self.splicemap)
            self.ui.status(_("sorting...\n"))
            t = self.toposort(parents, sortmode)
            num = len(t)
            c = None

            self.ui.status(_("converting...\n"))
            progress = self.ui.makeprogress(_('converting'),
                                            unit=_('revisions'), total=len(t))
            for i, c in enumerate(t):
                num -= 1
                desc = self.commitcache[c].desc
                if "\n" in desc:
                    desc = desc.splitlines()[0]
                # convert log message to local encoding without using
                # tolocal() because the encoding.encoding convert()
                # uses is 'utf-8'
                self.ui.status("%d %s\n" % (num, recode(desc)))
                self.ui.note(_("source: %s\n") % recode(c))
                progress.update(i)
                self.copy(c)
            progress.complete()

            if not self.ui.configbool('convert', 'skiptags'):
                tags = self.source.gettags()
                ctags = {}
                for k in tags:
                    v = tags[k]
                    if self.map.get(v, SKIPREV) != SKIPREV:
                        ctags[k] = self.map[v]

                if c and ctags:
                    nrev, tagsparent = self.dest.puttags(ctags)
                    if nrev and tagsparent:
                        # write another hash correspondence to override the
                        # previous one so we don't end up with extra tag heads
                        tagsparents = [e for e in self.map.iteritems()
                                       if e[1] == tagsparent]
                        if tagsparents:
                            self.map[tagsparents[0][0]] = nrev

            bookmarks = self.source.getbookmarks()
            cbookmarks = {}
            for k in bookmarks:
                v = bookmarks[k]
                if self.map.get(v, SKIPREV) != SKIPREV:
                    cbookmarks[k] = self.map[v]

            if c and cbookmarks:
                self.dest.putbookmarks(cbookmarks)

            self.writeauthormap()
        finally:
            self.cleanup()

    def cleanup(self):
        try:
            self.dest.after()
        finally:
            self.source.after()
        self.map.close()

def convert(ui, src, dest=None, revmapfile=None, **opts):
    opts = pycompat.byteskwargs(opts)
    global orig_encoding
    orig_encoding = encoding.encoding
    encoding.encoding = 'UTF-8'

    # support --authors as an alias for --authormap
    if not opts.get('authormap'):
        opts['authormap'] = opts.get('authors')

    if not dest:
        dest = hg.defaultdest(src) + "-hg"
        ui.status(_("assuming destination %s\n") % dest)

    destc = convertsink(ui, dest, opts.get('dest_type'))
    destc = scmutil.wrapconvertsink(destc)

    try:
        srcc, defaultsort = convertsource(ui, src, opts.get('source_type'),
                                          opts.get('rev'))
    except Exception:
        for path in destc.created:
            shutil.rmtree(path, True)
        raise

    sortmodes = ('branchsort', 'datesort', 'sourcesort', 'closesort')
    sortmode = [m for m in sortmodes if opts.get(m)]
    if len(sortmode) > 1:
        raise error.Abort(_('more than one sort mode specified'))
    if sortmode:
        sortmode = sortmode[0]
    else:
        sortmode = defaultsort

    if sortmode == 'sourcesort' and not srcc.hasnativeorder():
        raise error.Abort(_('--sourcesort is not supported by this data source')
                         )
    if sortmode == 'closesort' and not srcc.hasnativeclose():
        raise error.Abort(_('--closesort is not supported by this data source'))

    fmap = opts.get('filemap')
    if fmap:
        srcc = filemap.filemap_source(ui, srcc, fmap)
        destc.setfilemapmode(True)

    if not revmapfile:
        revmapfile = destc.revmapfile()

    c = converter(ui, srcc, destc, revmapfile, opts)
    c.convert(sortmode)
