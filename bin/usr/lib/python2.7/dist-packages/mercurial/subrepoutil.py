# subrepoutil.py - sub-repository operations and substate handling
#
# Copyright 2009-2010 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import errno
import os
import posixpath
import re

from .i18n import _
from . import (
    config,
    error,
    filemerge,
    pathutil,
    phases,
    util,
)
from .utils import (
    stringutil,
)

nullstate = ('', '', 'empty')

def state(ctx, ui):
    """return a state dict, mapping subrepo paths configured in .hgsub
    to tuple: (source from .hgsub, revision from .hgsubstate, kind
    (key in types dict))
    """
    p = config.config()
    repo = ctx.repo()
    def read(f, sections=None, remap=None):
        if f in ctx:
            try:
                data = ctx[f].data()
            except IOError as err:
                if err.errno != errno.ENOENT:
                    raise
                # handle missing subrepo spec files as removed
                ui.warn(_("warning: subrepo spec file \'%s\' not found\n") %
                        repo.pathto(f))
                return
            p.parse(f, data, sections, remap, read)
        else:
            raise error.Abort(_("subrepo spec file \'%s\' not found") %
                             repo.pathto(f))
    if '.hgsub' in ctx:
        read('.hgsub')

    for path, src in ui.configitems('subpaths'):
        p.set('subpaths', path, src, ui.configsource('subpaths', path))

    rev = {}
    if '.hgsubstate' in ctx:
        try:
            for i, l in enumerate(ctx['.hgsubstate'].data().splitlines()):
                l = l.lstrip()
                if not l:
                    continue
                try:
                    revision, path = l.split(" ", 1)
                except ValueError:
                    raise error.Abort(_("invalid subrepository revision "
                                       "specifier in \'%s\' line %d")
                                     % (repo.pathto('.hgsubstate'), (i + 1)))
                rev[path] = revision
        except IOError as err:
            if err.errno != errno.ENOENT:
                raise

    def remap(src):
        for pattern, repl in p.items('subpaths'):
            # Turn r'C:\foo\bar' into r'C:\\foo\\bar' since re.sub
            # does a string decode.
            repl = stringutil.escapestr(repl)
            # However, we still want to allow back references to go
            # through unharmed, so we turn r'\\1' into r'\1'. Again,
            # extra escapes are needed because re.sub string decodes.
            repl = re.sub(br'\\\\([0-9]+)', br'\\\1', repl)
            try:
                src = re.sub(pattern, repl, src, 1)
            except re.error as e:
                raise error.Abort(_("bad subrepository pattern in %s: %s")
                                 % (p.source('subpaths', pattern),
                                    stringutil.forcebytestr(e)))
        return src

    state = {}
    for path, src in p[''].items():
        kind = 'hg'
        if src.startswith('['):
            if ']' not in src:
                raise error.Abort(_('missing ] in subrepository source'))
            kind, src = src.split(']', 1)
            kind = kind[1:]
            src = src.lstrip() # strip any extra whitespace after ']'

        if not util.url(src).isabs():
            parent = _abssource(repo, abort=False)
            if parent:
                parent = util.url(parent)
                parent.path = posixpath.join(parent.path or '', src)
                parent.path = posixpath.normpath(parent.path)
                joined = bytes(parent)
                # Remap the full joined path and use it if it changes,
                # else remap the original source.
                remapped = remap(joined)
                if remapped == joined:
                    src = remap(src)
                else:
                    src = remapped

        src = remap(src)
        state[util.pconvert(path)] = (src.strip(), rev.get(path, ''), kind)

    return state

def writestate(repo, state):
    """rewrite .hgsubstate in (outer) repo with these subrepo states"""
    lines = ['%s %s\n' % (state[s][1], s) for s in sorted(state)
                                                if state[s][1] != nullstate[1]]
    repo.wwrite('.hgsubstate', ''.join(lines), '')

def submerge(repo, wctx, mctx, actx, overwrite, labels=None):
    """delegated from merge.applyupdates: merging of .hgsubstate file
    in working context, merging context and ancestor context"""
    if mctx == actx: # backwards?
        actx = wctx.p1()
    s1 = wctx.substate
    s2 = mctx.substate
    sa = actx.substate
    sm = {}

    repo.ui.debug("subrepo merge %s %s %s\n" % (wctx, mctx, actx))

    def debug(s, msg, r=""):
        if r:
            r = "%s:%s:%s" % r
        repo.ui.debug("  subrepo %s: %s %s\n" % (s, msg, r))

    promptssrc = filemerge.partextras(labels)
    for s, l in sorted(s1.iteritems()):
        prompts = None
        a = sa.get(s, nullstate)
        ld = l # local state with possible dirty flag for compares
        if wctx.sub(s).dirty():
            ld = (l[0], l[1] + "+")
        if wctx == actx: # overwrite
            a = ld

        prompts = promptssrc.copy()
        prompts['s'] = s
        if s in s2:
            r = s2[s]
            if ld == r or r == a: # no change or local is newer
                sm[s] = l
                continue
            elif ld == a: # other side changed
                debug(s, "other changed, get", r)
                wctx.sub(s).get(r, overwrite)
                sm[s] = r
            elif ld[0] != r[0]: # sources differ
                prompts['lo'] = l[0]
                prompts['ro'] = r[0]
                if repo.ui.promptchoice(
                    _(' subrepository sources for %(s)s differ\n'
                      'use (l)ocal%(l)s source (%(lo)s)'
                      ' or (r)emote%(o)s source (%(ro)s)?'
                      '$$ &Local $$ &Remote') % prompts, 0):
                    debug(s, "prompt changed, get", r)
                    wctx.sub(s).get(r, overwrite)
                    sm[s] = r
            elif ld[1] == a[1]: # local side is unchanged
                debug(s, "other side changed, get", r)
                wctx.sub(s).get(r, overwrite)
                sm[s] = r
            else:
                debug(s, "both sides changed")
                srepo = wctx.sub(s)
                prompts['sl'] = srepo.shortid(l[1])
                prompts['sr'] = srepo.shortid(r[1])
                option = repo.ui.promptchoice(
                    _(' subrepository %(s)s diverged (local revision: %(sl)s, '
                      'remote revision: %(sr)s)\n'
                      '(M)erge, keep (l)ocal%(l)s or keep (r)emote%(o)s?'
                      '$$ &Merge $$ &Local $$ &Remote')
                    % prompts, 0)
                if option == 0:
                    wctx.sub(s).merge(r)
                    sm[s] = l
                    debug(s, "merge with", r)
                elif option == 1:
                    sm[s] = l
                    debug(s, "keep local subrepo revision", l)
                else:
                    wctx.sub(s).get(r, overwrite)
                    sm[s] = r
                    debug(s, "get remote subrepo revision", r)
        elif ld == a: # remote removed, local unchanged
            debug(s, "remote removed, remove")
            wctx.sub(s).remove()
        elif a == nullstate: # not present in remote or ancestor
            debug(s, "local added, keep")
            sm[s] = l
            continue
        else:
            if repo.ui.promptchoice(
                _(' local%(l)s changed subrepository %(s)s'
                  ' which remote%(o)s removed\n'
                  'use (c)hanged version or (d)elete?'
                  '$$ &Changed $$ &Delete') % prompts, 0):
                debug(s, "prompt remove")
                wctx.sub(s).remove()

    for s, r in sorted(s2.items()):
        prompts = None
        if s in s1:
            continue
        elif s not in sa:
            debug(s, "remote added, get", r)
            mctx.sub(s).get(r)
            sm[s] = r
        elif r != sa[s]:
            prompts = promptssrc.copy()
            prompts['s'] = s
            if repo.ui.promptchoice(
                _(' remote%(o)s changed subrepository %(s)s'
                  ' which local%(l)s removed\n'
                  'use (c)hanged version or (d)elete?'
                  '$$ &Changed $$ &Delete') % prompts, 0) == 0:
                debug(s, "prompt recreate", r)
                mctx.sub(s).get(r)
                sm[s] = r

    # record merged .hgsubstate
    writestate(repo, sm)
    return sm

def precommit(ui, wctx, status, match, force=False):
    """Calculate .hgsubstate changes that should be applied before committing

    Returns (subs, commitsubs, newstate) where
    - subs: changed subrepos (including dirty ones)
    - commitsubs: dirty subrepos which the caller needs to commit recursively
    - newstate: new state dict which the caller must write to .hgsubstate

    This also updates the given status argument.
    """
    subs = []
    commitsubs = set()
    newstate = wctx.substate.copy()

    # only manage subrepos and .hgsubstate if .hgsub is present
    if '.hgsub' in wctx:
        # we'll decide whether to track this ourselves, thanks
        for c in status.modified, status.added, status.removed:
            if '.hgsubstate' in c:
                c.remove('.hgsubstate')

        # compare current state to last committed state
        # build new substate based on last committed state
        oldstate = wctx.p1().substate
        for s in sorted(newstate.keys()):
            if not match(s):
                # ignore working copy, use old state if present
                if s in oldstate:
                    newstate[s] = oldstate[s]
                    continue
                if not force:
                    raise error.Abort(
                        _("commit with new subrepo %s excluded") % s)
            dirtyreason = wctx.sub(s).dirtyreason(True)
            if dirtyreason:
                if not ui.configbool('ui', 'commitsubrepos'):
                    raise error.Abort(dirtyreason,
                        hint=_("use --subrepos for recursive commit"))
                subs.append(s)
                commitsubs.add(s)
            else:
                bs = wctx.sub(s).basestate()
                newstate[s] = (newstate[s][0], bs, newstate[s][2])
                if oldstate.get(s, (None, None, None))[1] != bs:
                    subs.append(s)

        # check for removed subrepos
        for p in wctx.parents():
            r = [s for s in p.substate if s not in newstate]
            subs += [s for s in r if match(s)]
        if subs:
            if (not match('.hgsub') and
                '.hgsub' in (wctx.modified() + wctx.added())):
                raise error.Abort(_("can't commit subrepos without .hgsub"))
            status.modified.insert(0, '.hgsubstate')

    elif '.hgsub' in status.removed:
        # clean up .hgsubstate when .hgsub is removed
        if ('.hgsubstate' in wctx and
            '.hgsubstate' not in (status.modified + status.added +
                                  status.removed)):
            status.removed.insert(0, '.hgsubstate')

    return subs, commitsubs, newstate

def reporelpath(repo):
    """return path to this (sub)repo as seen from outermost repo"""
    parent = repo
    while util.safehasattr(parent, '_subparent'):
        parent = parent._subparent
    return repo.root[len(pathutil.normasprefix(parent.root)):]

def subrelpath(sub):
    """return path to this subrepo as seen from outermost repo"""
    return sub._relpath

def _abssource(repo, push=False, abort=True):
    """return pull/push path of repo - either based on parent repo .hgsub info
    or on the top repo config. Abort or return None if no source found."""
    if util.safehasattr(repo, '_subparent'):
        source = util.url(repo._subsource)
        if source.isabs():
            return bytes(source)
        source.path = posixpath.normpath(source.path)
        parent = _abssource(repo._subparent, push, abort=False)
        if parent:
            parent = util.url(util.pconvert(parent))
            parent.path = posixpath.join(parent.path or '', source.path)
            parent.path = posixpath.normpath(parent.path)
            return bytes(parent)
    else: # recursion reached top repo
        path = None
        if util.safehasattr(repo, '_subtoppath'):
            path = repo._subtoppath
        elif push and repo.ui.config('paths', 'default-push'):
            path = repo.ui.config('paths', 'default-push')
        elif repo.ui.config('paths', 'default'):
            path = repo.ui.config('paths', 'default')
        elif repo.shared():
            # chop off the .hg component to get the default path form.  This has
            # already run through vfsmod.vfs(..., realpath=True), so it doesn't
            # have problems with 'C:'
            return os.path.dirname(repo.sharedpath)
        if path:
            # issue5770: 'C:\' and 'C:' are not equivalent paths.  The former is
            # as expected: an absolute path to the root of the C: drive.  The
            # latter is a relative path, and works like so:
            #
            #   C:\>cd C:\some\path
            #   C:\>D:
            #   D:\>python -c "import os; print os.path.abspath('C:')"
            #   C:\some\path
            #
            #   D:\>python -c "import os; print os.path.abspath('C:relative')"
            #   C:\some\path\relative
            if util.hasdriveletter(path):
                if len(path) == 2 or path[2:3] not in br'\/':
                    path = os.path.abspath(path)
            return path

    if abort:
        raise error.Abort(_("default path for subrepository not found"))

def newcommitphase(ui, ctx):
    commitphase = phases.newcommitphase(ui)
    substate = getattr(ctx, "substate", None)
    if not substate:
        return commitphase
    check = ui.config('phases', 'checksubrepos')
    if check not in ('ignore', 'follow', 'abort'):
        raise error.Abort(_('invalid phases.checksubrepos configuration: %s')
                         % (check))
    if check == 'ignore':
        return commitphase
    maxphase = phases.public
    maxsub = None
    for s in sorted(substate):
        sub = ctx.sub(s)
        subphase = sub.phase(substate[s][1])
        if maxphase < subphase:
            maxphase = subphase
            maxsub = s
    if commitphase < maxphase:
        if check == 'abort':
            raise error.Abort(_("can't commit in %s phase"
                               " conflicting %s from subrepository %s") %
                             (phases.phasenames[commitphase],
                              phases.phasenames[maxphase], maxsub))
        ui.warn(_("warning: changes are committed in"
                  " %s phase from subrepository %s\n") %
                (phases.phasenames[maxphase], maxsub))
        return maxphase
    return commitphase
