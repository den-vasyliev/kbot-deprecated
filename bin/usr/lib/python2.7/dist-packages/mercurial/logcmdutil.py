# logcmdutil.py - utility for log-like commands
#
# Copyright 2005-2007 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import itertools
import os

from .i18n import _
from .node import (
    nullid,
    wdirid,
    wdirrev,
)

from . import (
    dagop,
    error,
    formatter,
    graphmod,
    match as matchmod,
    mdiff,
    patch,
    pathutil,
    pycompat,
    revset,
    revsetlang,
    scmutil,
    smartset,
    templatekw,
    templater,
    util,
)
from .utils import (
    dateutil,
    stringutil,
)

def getlimit(opts):
    """get the log limit according to option -l/--limit"""
    limit = opts.get('limit')
    if limit:
        try:
            limit = int(limit)
        except ValueError:
            raise error.Abort(_('limit must be a positive integer'))
        if limit <= 0:
            raise error.Abort(_('limit must be positive'))
    else:
        limit = None
    return limit

def diffordiffstat(ui, repo, diffopts, node1, node2, match,
                   changes=None, stat=False, fp=None, prefix='',
                   root='', listsubrepos=False, hunksfilterfn=None):
    '''show diff or diffstat.'''
    if root:
        relroot = pathutil.canonpath(repo.root, repo.getcwd(), root)
    else:
        relroot = ''
    if relroot != '':
        # XXX relative roots currently don't work if the root is within a
        # subrepo
        uirelroot = match.uipath(relroot)
        relroot += '/'
        for matchroot in match.files():
            if not matchroot.startswith(relroot):
                ui.warn(_('warning: %s not inside relative root %s\n') % (
                    match.uipath(matchroot), uirelroot))

    if stat:
        diffopts = diffopts.copy(context=0, noprefix=False)
        width = 80
        if not ui.plain():
            width = ui.termwidth()

    chunks = repo[node2].diff(repo[node1], match, changes, opts=diffopts,
                              prefix=prefix, relroot=relroot,
                              hunksfilterfn=hunksfilterfn)

    if fp is not None or ui.canwritewithoutlabels():
        out = fp or ui
        if stat:
            chunks = [patch.diffstat(util.iterlines(chunks), width=width)]
        for chunk in util.filechunkiter(util.chunkbuffer(chunks)):
            out.write(chunk)
    else:
        if stat:
            chunks = patch.diffstatui(util.iterlines(chunks), width=width)
        else:
            chunks = patch.difflabel(lambda chunks, **kwargs: chunks, chunks,
                                     opts=diffopts)
        if ui.canbatchlabeledwrites():
            def gen():
                for chunk, label in chunks:
                    yield ui.label(chunk, label=label)
            for chunk in util.filechunkiter(util.chunkbuffer(gen())):
                ui.write(chunk)
        else:
            for chunk, label in chunks:
                ui.write(chunk, label=label)

    if listsubrepos:
        ctx1 = repo[node1]
        ctx2 = repo[node2]
        for subpath, sub in scmutil.itersubrepos(ctx1, ctx2):
            tempnode2 = node2
            try:
                if node2 is not None:
                    tempnode2 = ctx2.substate[subpath][1]
            except KeyError:
                # A subrepo that existed in node1 was deleted between node1 and
                # node2 (inclusive). Thus, ctx2's substate won't contain that
                # subpath. The best we can do is to ignore it.
                tempnode2 = None
            submatch = matchmod.subdirmatcher(subpath, match)
            sub.diff(ui, diffopts, tempnode2, submatch, changes=changes,
                     stat=stat, fp=fp, prefix=prefix)

class changesetdiffer(object):
    """Generate diff of changeset with pre-configured filtering functions"""

    def _makefilematcher(self, ctx):
        return scmutil.matchall(ctx.repo())

    def _makehunksfilter(self, ctx):
        return None

    def showdiff(self, ui, ctx, diffopts, stat=False):
        repo = ctx.repo()
        node = ctx.node()
        prev = ctx.p1().node()
        diffordiffstat(ui, repo, diffopts, prev, node,
                       match=self._makefilematcher(ctx), stat=stat,
                       hunksfilterfn=self._makehunksfilter(ctx))

def changesetlabels(ctx):
    labels = ['log.changeset', 'changeset.%s' % ctx.phasestr()]
    if ctx.obsolete():
        labels.append('changeset.obsolete')
    if ctx.isunstable():
        labels.append('changeset.unstable')
        for instability in ctx.instabilities():
            labels.append('instability.%s' % instability)
    return ' '.join(labels)

class changesetprinter(object):
    '''show changeset information when templating not requested.'''

    def __init__(self, ui, repo, differ=None, diffopts=None, buffered=False):
        self.ui = ui
        self.repo = repo
        self.buffered = buffered
        self._differ = differ or changesetdiffer()
        self._diffopts = patch.diffallopts(ui, diffopts)
        self._includestat = diffopts and diffopts.get('stat')
        self._includediff = diffopts and diffopts.get('patch')
        self.header = {}
        self.hunk = {}
        self.lastheader = None
        self.footer = None
        self._columns = templatekw.getlogcolumns()

    def flush(self, ctx):
        rev = ctx.rev()
        if rev in self.header:
            h = self.header[rev]
            if h != self.lastheader:
                self.lastheader = h
                self.ui.write(h)
            del self.header[rev]
        if rev in self.hunk:
            self.ui.write(self.hunk[rev])
            del self.hunk[rev]

    def close(self):
        if self.footer:
            self.ui.write(self.footer)

    def show(self, ctx, copies=None, **props):
        props = pycompat.byteskwargs(props)
        if self.buffered:
            self.ui.pushbuffer(labeled=True)
            self._show(ctx, copies, props)
            self.hunk[ctx.rev()] = self.ui.popbuffer()
        else:
            self._show(ctx, copies, props)

    def _show(self, ctx, copies, props):
        '''show a single changeset or file revision'''
        changenode = ctx.node()

        if self.ui.quiet:
            self.ui.write("%s\n" % scmutil.formatchangeid(ctx),
                          label='log.node')
            return

        columns = self._columns
        self.ui.write(columns['changeset'] % scmutil.formatchangeid(ctx),
                      label=changesetlabels(ctx))

        # branches are shown first before any other names due to backwards
        # compatibility
        branch = ctx.branch()
        # don't show the default branch name
        if branch != 'default':
            self.ui.write(columns['branch'] % branch, label='log.branch')

        for nsname, ns in self.repo.names.iteritems():
            # branches has special logic already handled above, so here we just
            # skip it
            if nsname == 'branches':
                continue
            # we will use the templatename as the color name since those two
            # should be the same
            for name in ns.names(self.repo, changenode):
                self.ui.write(ns.logfmt % name,
                              label='log.%s' % ns.colorname)
        if self.ui.debugflag:
            self.ui.write(columns['phase'] % ctx.phasestr(), label='log.phase')
        for pctx in scmutil.meaningfulparents(self.repo, ctx):
            label = 'log.parent changeset.%s' % pctx.phasestr()
            self.ui.write(columns['parent'] % scmutil.formatchangeid(pctx),
                          label=label)

        if self.ui.debugflag:
            mnode = ctx.manifestnode()
            if mnode is None:
                mnode = wdirid
                mrev = wdirrev
            else:
                mrev = self.repo.manifestlog.rev(mnode)
            self.ui.write(columns['manifest']
                          % scmutil.formatrevnode(self.ui, mrev, mnode),
                          label='ui.debug log.manifest')
        self.ui.write(columns['user'] % ctx.user(), label='log.user')
        self.ui.write(columns['date'] % dateutil.datestr(ctx.date()),
                      label='log.date')

        if ctx.isunstable():
            instabilities = ctx.instabilities()
            self.ui.write(columns['instability'] % ', '.join(instabilities),
                          label='log.instability')

        elif ctx.obsolete():
            self._showobsfate(ctx)

        self._exthook(ctx)

        if self.ui.debugflag:
            files = ctx.p1().status(ctx)[:3]
            for key, value in zip(['files', 'files+', 'files-'], files):
                if value:
                    self.ui.write(columns[key] % " ".join(value),
                                  label='ui.debug log.files')
        elif ctx.files() and self.ui.verbose:
            self.ui.write(columns['files'] % " ".join(ctx.files()),
                          label='ui.note log.files')
        if copies and self.ui.verbose:
            copies = ['%s (%s)' % c for c in copies]
            self.ui.write(columns['copies'] % ' '.join(copies),
                          label='ui.note log.copies')

        extra = ctx.extra()
        if extra and self.ui.debugflag:
            for key, value in sorted(extra.items()):
                self.ui.write(columns['extra']
                              % (key, stringutil.escapestr(value)),
                              label='ui.debug log.extra')

        description = ctx.description().strip()
        if description:
            if self.ui.verbose:
                self.ui.write(_("description:\n"),
                              label='ui.note log.description')
                self.ui.write(description,
                              label='ui.note log.description')
                self.ui.write("\n\n")
            else:
                self.ui.write(columns['summary'] % description.splitlines()[0],
                              label='log.summary')
        self.ui.write("\n")

        self._showpatch(ctx)

    def _showobsfate(self, ctx):
        # TODO: do not depend on templater
        tres = formatter.templateresources(self.repo.ui, self.repo)
        t = formatter.maketemplater(self.repo.ui, '{join(obsfate, "\n")}',
                                    defaults=templatekw.keywords,
                                    resources=tres)
        obsfate = t.renderdefault({'ctx': ctx}).splitlines()

        if obsfate:
            for obsfateline in obsfate:
                self.ui.write(self._columns['obsolete'] % obsfateline,
                              label='log.obsfate')

    def _exthook(self, ctx):
        '''empty method used by extension as a hook point
        '''

    def _showpatch(self, ctx):
        if self._includestat:
            self._differ.showdiff(self.ui, ctx, self._diffopts, stat=True)
        if self._includestat and self._includediff:
            self.ui.write("\n")
        if self._includediff:
            self._differ.showdiff(self.ui, ctx, self._diffopts, stat=False)
        if self._includestat or self._includediff:
            self.ui.write("\n")

class changesetformatter(changesetprinter):
    """Format changeset information by generic formatter"""

    def __init__(self, ui, repo, fm, differ=None, diffopts=None,
                 buffered=False):
        changesetprinter.__init__(self, ui, repo, differ, diffopts, buffered)
        self._diffopts = patch.difffeatureopts(ui, diffopts, git=True)
        self._fm = fm

    def close(self):
        self._fm.end()

    def _show(self, ctx, copies, props):
        '''show a single changeset or file revision'''
        fm = self._fm
        fm.startitem()
        fm.context(ctx=ctx)
        fm.data(rev=scmutil.intrev(ctx),
                node=fm.hexfunc(scmutil.binnode(ctx)))

        if self.ui.quiet:
            return

        fm.data(branch=ctx.branch(),
                phase=ctx.phasestr(),
                user=ctx.user(),
                date=fm.formatdate(ctx.date()),
                desc=ctx.description(),
                bookmarks=fm.formatlist(ctx.bookmarks(), name='bookmark'),
                tags=fm.formatlist(ctx.tags(), name='tag'),
                parents=fm.formatlist([fm.hexfunc(c.node())
                                       for c in ctx.parents()], name='node'))

        if self.ui.debugflag:
            fm.data(manifest=fm.hexfunc(ctx.manifestnode() or wdirid),
                    extra=fm.formatdict(ctx.extra()))

            files = ctx.p1().status(ctx)
            fm.data(modified=fm.formatlist(files[0], name='file'),
                    added=fm.formatlist(files[1], name='file'),
                    removed=fm.formatlist(files[2], name='file'))

        elif self.ui.verbose:
            fm.data(files=fm.formatlist(ctx.files(), name='file'))
            if copies:
                fm.data(copies=fm.formatdict(copies,
                                             key='name', value='source'))

        if self._includestat:
            self.ui.pushbuffer()
            self._differ.showdiff(self.ui, ctx, self._diffopts, stat=True)
            fm.data(diffstat=self.ui.popbuffer())
        if self._includediff:
            self.ui.pushbuffer()
            self._differ.showdiff(self.ui, ctx, self._diffopts, stat=False)
            fm.data(diff=self.ui.popbuffer())

class changesettemplater(changesetprinter):
    '''format changeset information.

    Note: there are a variety of convenience functions to build a
    changesettemplater for common cases. See functions such as:
    maketemplater, changesetdisplayer, buildcommittemplate, or other
    functions that use changesest_templater.
    '''

    # Arguments before "buffered" used to be positional. Consider not
    # adding/removing arguments before "buffered" to not break callers.
    def __init__(self, ui, repo, tmplspec, differ=None, diffopts=None,
                 buffered=False):
        changesetprinter.__init__(self, ui, repo, differ, diffopts, buffered)
        # tres is shared with _graphnodeformatter()
        self._tresources = tres = formatter.templateresources(ui, repo)
        self.t = formatter.loadtemplater(ui, tmplspec,
                                         defaults=templatekw.keywords,
                                         resources=tres,
                                         cache=templatekw.defaulttempl)
        self._counter = itertools.count()

        self._tref = tmplspec.ref
        self._parts = {'header': '', 'footer': '',
                       tmplspec.ref: tmplspec.ref,
                       'docheader': '', 'docfooter': '',
                       'separator': ''}
        if tmplspec.mapfile:
            # find correct templates for current mode, for backward
            # compatibility with 'log -v/-q/--debug' using a mapfile
            tmplmodes = [
                (True, ''),
                (self.ui.verbose, '_verbose'),
                (self.ui.quiet, '_quiet'),
                (self.ui.debugflag, '_debug'),
            ]
            for mode, postfix in tmplmodes:
                for t in self._parts:
                    cur = t + postfix
                    if mode and cur in self.t:
                        self._parts[t] = cur
        else:
            partnames = [p for p in self._parts.keys() if p != tmplspec.ref]
            m = formatter.templatepartsmap(tmplspec, self.t, partnames)
            self._parts.update(m)

        if self._parts['docheader']:
            self.ui.write(self.t.render(self._parts['docheader'], {}))

    def close(self):
        if self._parts['docfooter']:
            if not self.footer:
                self.footer = ""
            self.footer += self.t.render(self._parts['docfooter'], {})
        return super(changesettemplater, self).close()

    def _show(self, ctx, copies, props):
        '''show a single changeset or file revision'''
        props = props.copy()
        props['ctx'] = ctx
        props['index'] = index = next(self._counter)
        props['revcache'] = {'copies': copies}

        # write separator, which wouldn't work well with the header part below
        # since there's inherently a conflict between header (across items) and
        # separator (per item)
        if self._parts['separator'] and index > 0:
            self.ui.write(self.t.render(self._parts['separator'], {}))

        # write header
        if self._parts['header']:
            h = self.t.render(self._parts['header'], props)
            if self.buffered:
                self.header[ctx.rev()] = h
            else:
                if self.lastheader != h:
                    self.lastheader = h
                    self.ui.write(h)

        # write changeset metadata, then patch if requested
        key = self._parts[self._tref]
        self.ui.write(self.t.render(key, props))
        self._showpatch(ctx)

        if self._parts['footer']:
            if not self.footer:
                self.footer = self.t.render(self._parts['footer'], props)

def templatespec(tmpl, mapfile):
    if pycompat.ispy3:
        assert not isinstance(tmpl, str), 'tmpl must not be a str'
    if mapfile:
        return formatter.templatespec('changeset', tmpl, mapfile)
    else:
        return formatter.templatespec('', tmpl, None)

def _lookuptemplate(ui, tmpl, style):
    """Find the template matching the given template spec or style

    See formatter.lookuptemplate() for details.
    """

    # ui settings
    if not tmpl and not style: # template are stronger than style
        tmpl = ui.config('ui', 'logtemplate')
        if tmpl:
            return templatespec(templater.unquotestring(tmpl), None)
        else:
            style = util.expandpath(ui.config('ui', 'style'))

    if not tmpl and style:
        mapfile = style
        if not os.path.split(mapfile)[0]:
            mapname = (templater.templatepath('map-cmdline.' + mapfile)
                       or templater.templatepath(mapfile))
            if mapname:
                mapfile = mapname
        return templatespec(None, mapfile)

    if not tmpl:
        return templatespec(None, None)

    return formatter.lookuptemplate(ui, 'changeset', tmpl)

def maketemplater(ui, repo, tmpl, buffered=False):
    """Create a changesettemplater from a literal template 'tmpl'
    byte-string."""
    spec = templatespec(tmpl, None)
    return changesettemplater(ui, repo, spec, buffered=buffered)

def changesetdisplayer(ui, repo, opts, differ=None, buffered=False):
    """show one changeset using template or regular display.

    Display format will be the first non-empty hit of:
    1. option 'template'
    2. option 'style'
    3. [ui] setting 'logtemplate'
    4. [ui] setting 'style'
    If all of these values are either the unset or the empty string,
    regular display via changesetprinter() is done.
    """
    postargs = (differ, opts, buffered)
    if opts.get('template') == 'json':
        fm = ui.formatter('log', opts)
        return changesetformatter(ui, repo, fm, *postargs)

    spec = _lookuptemplate(ui, opts.get('template'), opts.get('style'))

    if not spec.ref and not spec.tmpl and not spec.mapfile:
        return changesetprinter(ui, repo, *postargs)

    return changesettemplater(ui, repo, spec, *postargs)

def _makematcher(repo, revs, pats, opts):
    """Build matcher and expanded patterns from log options

    If --follow, revs are the revisions to follow from.

    Returns (match, pats, slowpath) where
    - match: a matcher built from the given pats and -I/-X opts
    - pats: patterns used (globs are expanded on Windows)
    - slowpath: True if patterns aren't as simple as scanning filelogs
    """
    # pats/include/exclude are passed to match.match() directly in
    # _matchfiles() revset but walkchangerevs() builds its matcher with
    # scmutil.match(). The difference is input pats are globbed on
    # platforms without shell expansion (windows).
    wctx = repo[None]
    match, pats = scmutil.matchandpats(wctx, pats, opts)
    slowpath = match.anypats() or (not match.always() and opts.get('removed'))
    if not slowpath:
        follow = opts.get('follow') or opts.get('follow_first')
        startctxs = []
        if follow and opts.get('rev'):
            startctxs = [repo[r] for r in revs]
        for f in match.files():
            if follow and startctxs:
                # No idea if the path was a directory at that revision, so
                # take the slow path.
                if any(f not in c for c in startctxs):
                    slowpath = True
                    continue
            elif follow and f not in wctx:
                # If the file exists, it may be a directory, so let it
                # take the slow path.
                if os.path.exists(repo.wjoin(f)):
                    slowpath = True
                    continue
                else:
                    raise error.Abort(_('cannot follow file not in parent '
                                        'revision: "%s"') % f)
            filelog = repo.file(f)
            if not filelog:
                # A zero count may be a directory or deleted file, so
                # try to find matching entries on the slow path.
                if follow:
                    raise error.Abort(
                        _('cannot follow nonexistent file: "%s"') % f)
                slowpath = True

        # We decided to fall back to the slowpath because at least one
        # of the paths was not a file. Check to see if at least one of them
        # existed in history - in that case, we'll continue down the
        # slowpath; otherwise, we can turn off the slowpath
        if slowpath:
            for path in match.files():
                if path == '.' or path in repo.store:
                    break
            else:
                slowpath = False

    return match, pats, slowpath

def _fileancestors(repo, revs, match, followfirst):
    fctxs = []
    for r in revs:
        ctx = repo[r]
        fctxs.extend(ctx[f].introfilectx() for f in ctx.walk(match))

    # When displaying a revision with --patch --follow FILE, we have
    # to know which file of the revision must be diffed. With
    # --follow, we want the names of the ancestors of FILE in the
    # revision, stored in "fcache". "fcache" is populated as a side effect
    # of the graph traversal.
    fcache = {}
    def filematcher(ctx):
        return scmutil.matchfiles(repo, fcache.get(ctx.rev(), []))

    def revgen():
        for rev, cs in dagop.filectxancestors(fctxs, followfirst=followfirst):
            fcache[rev] = [c.path() for c in cs]
            yield rev
    return smartset.generatorset(revgen(), iterasc=False), filematcher

def _makenofollowfilematcher(repo, pats, opts):
    '''hook for extensions to override the filematcher for non-follow cases'''
    return None

_opt2logrevset = {
    'no_merges':        ('not merge()', None),
    'only_merges':      ('merge()', None),
    '_matchfiles':      (None, '_matchfiles(%ps)'),
    'date':             ('date(%s)', None),
    'branch':           ('branch(%s)', '%lr'),
    '_patslog':         ('filelog(%s)', '%lr'),
    'keyword':          ('keyword(%s)', '%lr'),
    'prune':            ('ancestors(%s)', 'not %lr'),
    'user':             ('user(%s)', '%lr'),
}

def _makerevset(repo, match, pats, slowpath, opts):
    """Return a revset string built from log options and file patterns"""
    opts = dict(opts)
    # follow or not follow?
    follow = opts.get('follow') or opts.get('follow_first')

    # branch and only_branch are really aliases and must be handled at
    # the same time
    opts['branch'] = opts.get('branch', []) + opts.get('only_branch', [])
    opts['branch'] = [repo.lookupbranch(b) for b in opts['branch']]

    if slowpath:
        # See walkchangerevs() slow path.
        #
        # pats/include/exclude cannot be represented as separate
        # revset expressions as their filtering logic applies at file
        # level. For instance "-I a -X b" matches a revision touching
        # "a" and "b" while "file(a) and not file(b)" does
        # not. Besides, filesets are evaluated against the working
        # directory.
        matchargs = ['r:', 'd:relpath']
        for p in pats:
            matchargs.append('p:' + p)
        for p in opts.get('include', []):
            matchargs.append('i:' + p)
        for p in opts.get('exclude', []):
            matchargs.append('x:' + p)
        opts['_matchfiles'] = matchargs
    elif not follow:
        opts['_patslog'] = list(pats)

    expr = []
    for op, val in sorted(opts.iteritems()):
        if not val:
            continue
        if op not in _opt2logrevset:
            continue
        revop, listop = _opt2logrevset[op]
        if revop and '%' not in revop:
            expr.append(revop)
        elif not listop:
            expr.append(revsetlang.formatspec(revop, val))
        else:
            if revop:
                val = [revsetlang.formatspec(revop, v) for v in val]
            expr.append(revsetlang.formatspec(listop, val))

    if expr:
        expr = '(' + ' and '.join(expr) + ')'
    else:
        expr = None
    return expr

def _initialrevs(repo, opts):
    """Return the initial set of revisions to be filtered or followed"""
    follow = opts.get('follow') or opts.get('follow_first')
    if opts.get('rev'):
        revs = scmutil.revrange(repo, opts['rev'])
    elif follow and repo.dirstate.p1() == nullid:
        revs = smartset.baseset()
    elif follow:
        revs = repo.revs('.')
    else:
        revs = smartset.spanset(repo)
        revs.reverse()
    return revs

def getrevs(repo, pats, opts):
    """Return (revs, differ) where revs is a smartset

    differ is a changesetdiffer with pre-configured file matcher.
    """
    follow = opts.get('follow') or opts.get('follow_first')
    followfirst = opts.get('follow_first')
    limit = getlimit(opts)
    revs = _initialrevs(repo, opts)
    if not revs:
        return smartset.baseset(), None
    match, pats, slowpath = _makematcher(repo, revs, pats, opts)
    filematcher = None
    if follow:
        if slowpath or match.always():
            revs = dagop.revancestors(repo, revs, followfirst=followfirst)
        else:
            revs, filematcher = _fileancestors(repo, revs, match, followfirst)
        revs.reverse()
    if filematcher is None:
        filematcher = _makenofollowfilematcher(repo, pats, opts)
    if filematcher is None:
        def filematcher(ctx):
            return match

    expr = _makerevset(repo, match, pats, slowpath, opts)
    if opts.get('graph') and opts.get('rev'):
        # User-specified revs might be unsorted, but don't sort before
        # _makerevset because it might depend on the order of revs
        if not (revs.isdescending() or revs.istopo()):
            revs.sort(reverse=True)
    if expr:
        matcher = revset.match(None, expr)
        revs = matcher(repo, revs)
    if limit is not None:
        revs = revs.slice(0, limit)

    differ = changesetdiffer()
    differ._makefilematcher = filematcher
    return revs, differ

def _parselinerangeopt(repo, opts):
    """Parse --line-range log option and return a list of tuples (filename,
    (fromline, toline)).
    """
    linerangebyfname = []
    for pat in opts.get('line_range', []):
        try:
            pat, linerange = pat.rsplit(',', 1)
        except ValueError:
            raise error.Abort(_('malformatted line-range pattern %s') % pat)
        try:
            fromline, toline = map(int, linerange.split(':'))
        except ValueError:
            raise error.Abort(_("invalid line range for %s") % pat)
        msg = _("line range pattern '%s' must match exactly one file") % pat
        fname = scmutil.parsefollowlinespattern(repo, None, pat, msg)
        linerangebyfname.append(
            (fname, util.processlinerange(fromline, toline)))
    return linerangebyfname

def getlinerangerevs(repo, userrevs, opts):
    """Return (revs, differ).

    "revs" are revisions obtained by processing "line-range" log options and
    walking block ancestors of each specified file/line-range.

    "differ" is a changesetdiffer with pre-configured file matcher and hunks
    filter.
    """
    wctx = repo[None]

    # Two-levels map of "rev -> file ctx -> [line range]".
    linerangesbyrev = {}
    for fname, (fromline, toline) in _parselinerangeopt(repo, opts):
        if fname not in wctx:
            raise error.Abort(_('cannot follow file not in parent '
                                'revision: "%s"') % fname)
        fctx = wctx.filectx(fname)
        for fctx, linerange in dagop.blockancestors(fctx, fromline, toline):
            rev = fctx.introrev()
            if rev not in userrevs:
                continue
            linerangesbyrev.setdefault(
                rev, {}).setdefault(
                    fctx.path(), []).append(linerange)

    def nofilterhunksfn(fctx, hunks):
        return hunks

    def hunksfilter(ctx):
        fctxlineranges = linerangesbyrev.get(ctx.rev())
        if fctxlineranges is None:
            return nofilterhunksfn

        def filterfn(fctx, hunks):
            lineranges = fctxlineranges.get(fctx.path())
            if lineranges is not None:
                for hr, lines in hunks:
                    if hr is None: # binary
                        yield hr, lines
                        continue
                    if any(mdiff.hunkinrange(hr[2:], lr)
                           for lr in lineranges):
                        yield hr, lines
            else:
                for hunk in hunks:
                    yield hunk

        return filterfn

    def filematcher(ctx):
        files = list(linerangesbyrev.get(ctx.rev(), []))
        return scmutil.matchfiles(repo, files)

    revs = sorted(linerangesbyrev, reverse=True)

    differ = changesetdiffer()
    differ._makefilematcher = filematcher
    differ._makehunksfilter = hunksfilter
    return revs, differ

def _graphnodeformatter(ui, displayer):
    spec = ui.config('ui', 'graphnodetemplate')
    if not spec:
        return templatekw.getgraphnode  # fast path for "{graphnode}"

    spec = templater.unquotestring(spec)
    if isinstance(displayer, changesettemplater):
        # reuse cache of slow templates
        tres = displayer._tresources
    else:
        tres = formatter.templateresources(ui)
    templ = formatter.maketemplater(ui, spec, defaults=templatekw.keywords,
                                    resources=tres)
    def formatnode(repo, ctx):
        props = {'ctx': ctx, 'repo': repo}
        return templ.renderdefault(props)
    return formatnode

def displaygraph(ui, repo, dag, displayer, edgefn, getrenamed=None, props=None):
    props = props or {}
    formatnode = _graphnodeformatter(ui, displayer)
    state = graphmod.asciistate()
    styles = state['styles']

    # only set graph styling if HGPLAIN is not set.
    if ui.plain('graph'):
        # set all edge styles to |, the default pre-3.8 behaviour
        styles.update(dict.fromkeys(styles, '|'))
    else:
        edgetypes = {
            'parent': graphmod.PARENT,
            'grandparent': graphmod.GRANDPARENT,
            'missing': graphmod.MISSINGPARENT
        }
        for name, key in edgetypes.items():
            # experimental config: experimental.graphstyle.*
            styles[key] = ui.config('experimental', 'graphstyle.%s' % name,
                                    styles[key])
            if not styles[key]:
                styles[key] = None

        # experimental config: experimental.graphshorten
        state['graphshorten'] = ui.configbool('experimental', 'graphshorten')

    for rev, type, ctx, parents in dag:
        char = formatnode(repo, ctx)
        copies = None
        if getrenamed and ctx.rev():
            copies = []
            for fn in ctx.files():
                rename = getrenamed(fn, ctx.rev())
                if rename:
                    copies.append((fn, rename))
        edges = edgefn(type, char, state, rev, parents)
        firstedge = next(edges)
        width = firstedge[2]
        displayer.show(ctx, copies=copies,
                       graphwidth=width, **pycompat.strkwargs(props))
        lines = displayer.hunk.pop(rev).split('\n')
        if not lines[-1]:
            del lines[-1]
        displayer.flush(ctx)
        for type, char, width, coldata in itertools.chain([firstedge], edges):
            graphmod.ascii(ui, state, type, char, lines, coldata)
            lines = []
    displayer.close()

def displaygraphrevs(ui, repo, revs, displayer, getrenamed):
    revdag = graphmod.dagwalker(repo, revs)
    displaygraph(ui, repo, revdag, displayer, graphmod.asciiedges, getrenamed)

def displayrevs(ui, repo, revs, displayer, getrenamed):
    for rev in revs:
        ctx = repo[rev]
        copies = None
        if getrenamed is not None and rev:
            copies = []
            for fn in ctx.files():
                rename = getrenamed(fn, rev)
                if rename:
                    copies.append((fn, rename))
        displayer.show(ctx, copies=copies)
        displayer.flush(ctx)
    displayer.close()

def checkunsupportedgraphflags(pats, opts):
    for op in ["newest_first"]:
        if op in opts and opts[op]:
            raise error.Abort(_("-G/--graph option is incompatible with --%s")
                             % op.replace("_", "-"))

def graphrevs(repo, nodes, opts):
    limit = getlimit(opts)
    nodes.reverse()
    if limit is not None:
        nodes = nodes[:limit]
    return graphmod.nodes(repo, nodes)
