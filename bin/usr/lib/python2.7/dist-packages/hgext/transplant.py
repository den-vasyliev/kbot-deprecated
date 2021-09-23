# Patch transplanting extension for Mercurial
#
# Copyright 2006, 2007 Brendan Cully <brendan@kublai.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

'''command to transplant changesets from another branch

This extension allows you to transplant changes to another parent revision,
possibly in another repository. The transplant is done using 'diff' patches.

Transplanted patches are recorded in .hg/transplant/transplants, as a
map from a changeset hash to its hash in the source repository.
'''
from __future__ import absolute_import

import os

from mercurial.i18n import _
from mercurial import (
    bundlerepo,
    cmdutil,
    error,
    exchange,
    hg,
    logcmdutil,
    match,
    merge,
    node as nodemod,
    patch,
    pycompat,
    registrar,
    revlog,
    revset,
    scmutil,
    smartset,
    util,
    vfs as vfsmod,
)
from mercurial.utils import (
    procutil,
    stringutil,
)

class TransplantError(error.Abort):
    pass

cmdtable = {}
command = registrar.command(cmdtable)
# Note for extension authors: ONLY specify testedwith = 'ships-with-hg-core' for
# extensions which SHIP WITH MERCURIAL. Non-mainline extensions should
# be specifying the version(s) of Mercurial they are tested with, or
# leave the attribute unspecified.
testedwith = 'ships-with-hg-core'

configtable = {}
configitem = registrar.configitem(configtable)

configitem('transplant', 'filter',
    default=None,
)
configitem('transplant', 'log',
    default=None,
)

class transplantentry(object):
    def __init__(self, lnode, rnode):
        self.lnode = lnode
        self.rnode = rnode

class transplants(object):
    def __init__(self, path=None, transplantfile=None, opener=None):
        self.path = path
        self.transplantfile = transplantfile
        self.opener = opener

        if not opener:
            self.opener = vfsmod.vfs(self.path)
        self.transplants = {}
        self.dirty = False
        self.read()

    def read(self):
        abspath = os.path.join(self.path, self.transplantfile)
        if self.transplantfile and os.path.exists(abspath):
            for line in self.opener.read(self.transplantfile).splitlines():
                lnode, rnode = map(revlog.bin, line.split(':'))
                list = self.transplants.setdefault(rnode, [])
                list.append(transplantentry(lnode, rnode))

    def write(self):
        if self.dirty and self.transplantfile:
            if not os.path.isdir(self.path):
                os.mkdir(self.path)
            fp = self.opener(self.transplantfile, 'w')
            for list in self.transplants.itervalues():
                for t in list:
                    l, r = map(nodemod.hex, (t.lnode, t.rnode))
                    fp.write(l + ':' + r + '\n')
            fp.close()
        self.dirty = False

    def get(self, rnode):
        return self.transplants.get(rnode) or []

    def set(self, lnode, rnode):
        list = self.transplants.setdefault(rnode, [])
        list.append(transplantentry(lnode, rnode))
        self.dirty = True

    def remove(self, transplant):
        list = self.transplants.get(transplant.rnode)
        if list:
            del list[list.index(transplant)]
            self.dirty = True

class transplanter(object):
    def __init__(self, ui, repo, opts):
        self.ui = ui
        self.path = repo.vfs.join('transplant')
        self.opener = vfsmod.vfs(self.path)
        self.transplants = transplants(self.path, 'transplants',
                                       opener=self.opener)
        def getcommiteditor():
            editform = cmdutil.mergeeditform(repo[None], 'transplant')
            return cmdutil.getcommiteditor(editform=editform,
                                           **pycompat.strkwargs(opts))
        self.getcommiteditor = getcommiteditor

    def applied(self, repo, node, parent):
        '''returns True if a node is already an ancestor of parent
        or is parent or has already been transplanted'''
        if hasnode(repo, parent):
            parentrev = repo.changelog.rev(parent)
        if hasnode(repo, node):
            rev = repo.changelog.rev(node)
            reachable = repo.changelog.ancestors([parentrev], rev,
                                                 inclusive=True)
            if rev in reachable:
                return True
        for t in self.transplants.get(node):
            # it might have been stripped
            if not hasnode(repo, t.lnode):
                self.transplants.remove(t)
                return False
            lnoderev = repo.changelog.rev(t.lnode)
            if lnoderev in repo.changelog.ancestors([parentrev], lnoderev,
                                                    inclusive=True):
                return True
        return False

    def apply(self, repo, source, revmap, merges, opts=None):
        '''apply the revisions in revmap one by one in revision order'''
        if opts is None:
            opts = {}
        revs = sorted(revmap)
        p1, p2 = repo.dirstate.parents()
        pulls = []
        diffopts = patch.difffeatureopts(self.ui, opts)
        diffopts.git = True

        lock = tr = None
        try:
            lock = repo.lock()
            tr = repo.transaction('transplant')
            for rev in revs:
                node = revmap[rev]
                revstr = '%d:%s' % (rev, nodemod.short(node))

                if self.applied(repo, node, p1):
                    self.ui.warn(_('skipping already applied revision %s\n') %
                                 revstr)
                    continue

                parents = source.changelog.parents(node)
                if not (opts.get('filter') or opts.get('log')):
                    # If the changeset parent is the same as the
                    # wdir's parent, just pull it.
                    if parents[0] == p1:
                        pulls.append(node)
                        p1 = node
                        continue
                    if pulls:
                        if source != repo:
                            exchange.pull(repo, source.peer(), heads=pulls)
                        merge.update(repo, pulls[-1], branchmerge=False,
                                     force=False)
                        p1, p2 = repo.dirstate.parents()
                        pulls = []

                domerge = False
                if node in merges:
                    # pulling all the merge revs at once would mean we
                    # couldn't transplant after the latest even if
                    # transplants before them fail.
                    domerge = True
                    if not hasnode(repo, node):
                        exchange.pull(repo, source.peer(), heads=[node])

                skipmerge = False
                if parents[1] != revlog.nullid:
                    if not opts.get('parent'):
                        self.ui.note(_('skipping merge changeset %d:%s\n')
                                     % (rev, nodemod.short(node)))
                        skipmerge = True
                    else:
                        parent = source.lookup(opts['parent'])
                        if parent not in parents:
                            raise error.Abort(_('%s is not a parent of %s') %
                                              (nodemod.short(parent),
                                               nodemod.short(node)))
                else:
                    parent = parents[0]

                if skipmerge:
                    patchfile = None
                else:
                    fd, patchfile = pycompat.mkstemp(prefix='hg-transplant-')
                    fp = os.fdopen(fd, r'wb')
                    gen = patch.diff(source, parent, node, opts=diffopts)
                    for chunk in gen:
                        fp.write(chunk)
                    fp.close()

                del revmap[rev]
                if patchfile or domerge:
                    try:
                        try:
                            n = self.applyone(repo, node,
                                              source.changelog.read(node),
                                              patchfile, merge=domerge,
                                              log=opts.get('log'),
                                              filter=opts.get('filter'))
                        except TransplantError:
                            # Do not rollback, it is up to the user to
                            # fix the merge or cancel everything
                            tr.close()
                            raise
                        if n and domerge:
                            self.ui.status(_('%s merged at %s\n') % (revstr,
                                      nodemod.short(n)))
                        elif n:
                            self.ui.status(_('%s transplanted to %s\n')
                                           % (nodemod.short(node),
                                              nodemod.short(n)))
                    finally:
                        if patchfile:
                            os.unlink(patchfile)
            tr.close()
            if pulls:
                exchange.pull(repo, source.peer(), heads=pulls)
                merge.update(repo, pulls[-1], branchmerge=False, force=False)
        finally:
            self.saveseries(revmap, merges)
            self.transplants.write()
            if tr:
                tr.release()
            if lock:
                lock.release()

    def filter(self, filter, node, changelog, patchfile):
        '''arbitrarily rewrite changeset before applying it'''

        self.ui.status(_('filtering %s\n') % patchfile)
        user, date, msg = (changelog[1], changelog[2], changelog[4])
        fd, headerfile = pycompat.mkstemp(prefix='hg-transplant-')
        fp = os.fdopen(fd, r'wb')
        fp.write("# HG changeset patch\n")
        fp.write("# User %s\n" % user)
        fp.write("# Date %d %d\n" % date)
        fp.write(msg + '\n')
        fp.close()

        try:
            self.ui.system('%s %s %s' % (filter,
                                         procutil.shellquote(headerfile),
                                         procutil.shellquote(patchfile)),
                           environ={'HGUSER': changelog[1],
                                    'HGREVISION': nodemod.hex(node),
                                    },
                           onerr=error.Abort, errprefix=_('filter failed'),
                           blockedtag='transplant_filter')
            user, date, msg = self.parselog(open(headerfile, 'rb'))[1:4]
        finally:
            os.unlink(headerfile)

        return (user, date, msg)

    def applyone(self, repo, node, cl, patchfile, merge=False, log=False,
                 filter=None):
        '''apply the patch in patchfile to the repository as a transplant'''
        (manifest, user, (time, timezone), files, message) = cl[:5]
        date = "%d %d" % (time, timezone)
        extra = {'transplant_source': node}
        if filter:
            (user, date, message) = self.filter(filter, node, cl, patchfile)

        if log:
            # we don't translate messages inserted into commits
            message += '\n(transplanted from %s)' % nodemod.hex(node)

        self.ui.status(_('applying %s\n') % nodemod.short(node))
        self.ui.note('%s %s\n%s\n' % (user, date, message))

        if not patchfile and not merge:
            raise error.Abort(_('can only omit patchfile if merging'))
        if patchfile:
            try:
                files = set()
                patch.patch(self.ui, repo, patchfile, files=files, eolmode=None)
                files = list(files)
            except Exception as inst:
                seriespath = os.path.join(self.path, 'series')
                if os.path.exists(seriespath):
                    os.unlink(seriespath)
                p1 = repo.dirstate.p1()
                p2 = node
                self.log(user, date, message, p1, p2, merge=merge)
                self.ui.write(stringutil.forcebytestr(inst) + '\n')
                raise TransplantError(_('fix up the working directory and run '
                                        'hg transplant --continue'))
        else:
            files = None
        if merge:
            p1, p2 = repo.dirstate.parents()
            repo.setparents(p1, node)
            m = match.always(repo.root, '')
        else:
            m = match.exact(repo.root, '', files)

        n = repo.commit(message, user, date, extra=extra, match=m,
                        editor=self.getcommiteditor())
        if not n:
            self.ui.warn(_('skipping emptied changeset %s\n') %
                           nodemod.short(node))
            return None
        if not merge:
            self.transplants.set(n, node)

        return n

    def canresume(self):
        return os.path.exists(os.path.join(self.path, 'journal'))

    def resume(self, repo, source, opts):
        '''recover last transaction and apply remaining changesets'''
        if os.path.exists(os.path.join(self.path, 'journal')):
            n, node = self.recover(repo, source, opts)
            if n:
                self.ui.status(_('%s transplanted as %s\n') %
                                 (nodemod.short(node),
                                  nodemod.short(n)))
            else:
                self.ui.status(_('%s skipped due to empty diff\n')
                               % (nodemod.short(node),))
        seriespath = os.path.join(self.path, 'series')
        if not os.path.exists(seriespath):
            self.transplants.write()
            return
        nodes, merges = self.readseries()
        revmap = {}
        for n in nodes:
            revmap[source.changelog.rev(n)] = n
        os.unlink(seriespath)

        self.apply(repo, source, revmap, merges, opts)

    def recover(self, repo, source, opts):
        '''commit working directory using journal metadata'''
        node, user, date, message, parents = self.readlog()
        merge = False

        if not user or not date or not message or not parents[0]:
            raise error.Abort(_('transplant log file is corrupt'))

        parent = parents[0]
        if len(parents) > 1:
            if opts.get('parent'):
                parent = source.lookup(opts['parent'])
                if parent not in parents:
                    raise error.Abort(_('%s is not a parent of %s') %
                                      (nodemod.short(parent),
                                       nodemod.short(node)))
            else:
                merge = True

        extra = {'transplant_source': node}
        try:
            p1, p2 = repo.dirstate.parents()
            if p1 != parent:
                raise error.Abort(_('working directory not at transplant '
                                   'parent %s') % nodemod.hex(parent))
            if merge:
                repo.setparents(p1, parents[1])
            modified, added, removed, deleted = repo.status()[:4]
            if merge or modified or added or removed or deleted:
                n = repo.commit(message, user, date, extra=extra,
                                editor=self.getcommiteditor())
                if not n:
                    raise error.Abort(_('commit failed'))
                if not merge:
                    self.transplants.set(n, node)
            else:
                n = None
            self.unlog()

            return n, node
        finally:
            # TODO: get rid of this meaningless try/finally enclosing.
            # this is kept only to reduce changes in a patch.
            pass

    def readseries(self):
        nodes = []
        merges = []
        cur = nodes
        for line in self.opener.read('series').splitlines():
            if line.startswith('# Merges'):
                cur = merges
                continue
            cur.append(revlog.bin(line))

        return (nodes, merges)

    def saveseries(self, revmap, merges):
        if not revmap:
            return

        if not os.path.isdir(self.path):
            os.mkdir(self.path)
        series = self.opener('series', 'w')
        for rev in sorted(revmap):
            series.write(nodemod.hex(revmap[rev]) + '\n')
        if merges:
            series.write('# Merges\n')
            for m in merges:
                series.write(nodemod.hex(m) + '\n')
        series.close()

    def parselog(self, fp):
        parents = []
        message = []
        node = revlog.nullid
        inmsg = False
        user = None
        date = None
        for line in fp.read().splitlines():
            if inmsg:
                message.append(line)
            elif line.startswith('# User '):
                user = line[7:]
            elif line.startswith('# Date '):
                date = line[7:]
            elif line.startswith('# Node ID '):
                node = revlog.bin(line[10:])
            elif line.startswith('# Parent '):
                parents.append(revlog.bin(line[9:]))
            elif not line.startswith('# '):
                inmsg = True
                message.append(line)
        if None in (user, date):
            raise error.Abort(_("filter corrupted changeset (no user or date)"))
        return (node, user, date, '\n'.join(message), parents)

    def log(self, user, date, message, p1, p2, merge=False):
        '''journal changelog metadata for later recover'''

        if not os.path.isdir(self.path):
            os.mkdir(self.path)
        fp = self.opener('journal', 'w')
        fp.write('# User %s\n' % user)
        fp.write('# Date %s\n' % date)
        fp.write('# Node ID %s\n' % nodemod.hex(p2))
        fp.write('# Parent ' + nodemod.hex(p1) + '\n')
        if merge:
            fp.write('# Parent ' + nodemod.hex(p2) + '\n')
        fp.write(message.rstrip() + '\n')
        fp.close()

    def readlog(self):
        return self.parselog(self.opener('journal'))

    def unlog(self):
        '''remove changelog journal'''
        absdst = os.path.join(self.path, 'journal')
        if os.path.exists(absdst):
            os.unlink(absdst)

    def transplantfilter(self, repo, source, root):
        def matchfn(node):
            if self.applied(repo, node, root):
                return False
            if source.changelog.parents(node)[1] != revlog.nullid:
                return False
            extra = source.changelog.read(node)[5]
            cnode = extra.get('transplant_source')
            if cnode and self.applied(repo, cnode, root):
                return False
            return True

        return matchfn

def hasnode(repo, node):
    try:
        return repo.changelog.rev(node) is not None
    except error.StorageError:
        return False

def browserevs(ui, repo, nodes, opts):
    '''interactively transplant changesets'''
    displayer = logcmdutil.changesetdisplayer(ui, repo, opts)
    transplants = []
    merges = []
    prompt = _('apply changeset? [ynmpcq?]:'
               '$$ &yes, transplant this changeset'
               '$$ &no, skip this changeset'
               '$$ &merge at this changeset'
               '$$ show &patch'
               '$$ &commit selected changesets'
               '$$ &quit and cancel transplant'
               '$$ &? (show this help)')
    for node in nodes:
        displayer.show(repo[node])
        action = None
        while not action:
            choice = ui.promptchoice(prompt)
            action = 'ynmpcq?'[choice:choice + 1]
            if action == '?':
                for c, t in ui.extractchoices(prompt)[1]:
                    ui.write('%s: %s\n' % (c, t))
                action = None
            elif action == 'p':
                parent = repo.changelog.parents(node)[0]
                for chunk in patch.diff(repo, parent, node):
                    ui.write(chunk)
                action = None
        if action == 'y':
            transplants.append(node)
        elif action == 'm':
            merges.append(node)
        elif action == 'c':
            break
        elif action == 'q':
            transplants = ()
            merges = ()
            break
    displayer.close()
    return (transplants, merges)

@command('transplant',
    [('s', 'source', '', _('transplant changesets from REPO'), _('REPO')),
    ('b', 'branch', [], _('use this source changeset as head'), _('REV')),
    ('a', 'all', None, _('pull all changesets up to the --branch revisions')),
    ('p', 'prune', [], _('skip over REV'), _('REV')),
    ('m', 'merge', [], _('merge at REV'), _('REV')),
    ('', 'parent', '',
     _('parent to choose when transplanting merge'), _('REV')),
    ('e', 'edit', False, _('invoke editor on commit messages')),
    ('', 'log', None, _('append transplant info to log message')),
    ('c', 'continue', None, _('continue last transplant session '
                              'after fixing conflicts')),
    ('', 'filter', '',
     _('filter changesets through command'), _('CMD'))],
    _('hg transplant [-s REPO] [-b BRANCH [-a]] [-p REV] '
      '[-m REV] [REV]...'),
    helpcategory=command.CATEGORY_CHANGE_MANAGEMENT)
def transplant(ui, repo, *revs, **opts):
    '''transplant changesets from another branch

    Selected changesets will be applied on top of the current working
    directory with the log of the original changeset. The changesets
    are copied and will thus appear twice in the history with different
    identities.

    Consider using the graft command if everything is inside the same
    repository - it will use merges and will usually give a better result.
    Use the rebase extension if the changesets are unpublished and you want
    to move them instead of copying them.

    If --log is specified, log messages will have a comment appended
    of the form::

      (transplanted from CHANGESETHASH)

    You can rewrite the changelog message with the --filter option.
    Its argument will be invoked with the current changelog message as
    $1 and the patch as $2.

    --source/-s specifies another repository to use for selecting changesets,
    just as if it temporarily had been pulled.
    If --branch/-b is specified, these revisions will be used as
    heads when deciding which changesets to transplant, just as if only
    these revisions had been pulled.
    If --all/-a is specified, all the revisions up to the heads specified
    with --branch will be transplanted.

    Example:

    - transplant all changes up to REV on top of your current revision::

        hg transplant --branch REV --all

    You can optionally mark selected transplanted changesets as merge
    changesets. You will not be prompted to transplant any ancestors
    of a merged transplant, and you can merge descendants of them
    normally instead of transplanting them.

    Merge changesets may be transplanted directly by specifying the
    proper parent changeset by calling :hg:`transplant --parent`.

    If no merges or revisions are provided, :hg:`transplant` will
    start an interactive changeset browser.

    If a changeset application fails, you can fix the merge by hand
    and then resume where you left off by calling :hg:`transplant
    --continue/-c`.
    '''
    with repo.wlock():
        return _dotransplant(ui, repo, *revs, **opts)

def _dotransplant(ui, repo, *revs, **opts):
    def incwalk(repo, csets, match=util.always):
        for node in csets:
            if match(node):
                yield node

    def transplantwalk(repo, dest, heads, match=util.always):
        '''Yield all nodes that are ancestors of a head but not ancestors
        of dest.
        If no heads are specified, the heads of repo will be used.'''
        if not heads:
            heads = repo.heads()
        ancestors = []
        ctx = repo[dest]
        for head in heads:
            ancestors.append(ctx.ancestor(repo[head]).node())
        for node in repo.changelog.nodesbetween(ancestors, heads)[0]:
            if match(node):
                yield node

    def checkopts(opts, revs):
        if opts.get('continue'):
            if opts.get('branch') or opts.get('all') or opts.get('merge'):
                raise error.Abort(_('--continue is incompatible with '
                                   '--branch, --all and --merge'))
            return
        if not (opts.get('source') or revs or
                opts.get('merge') or opts.get('branch')):
            raise error.Abort(_('no source URL, branch revision, or revision '
                               'list provided'))
        if opts.get('all'):
            if not opts.get('branch'):
                raise error.Abort(_('--all requires a branch revision'))
            if revs:
                raise error.Abort(_('--all is incompatible with a '
                                   'revision list'))

    opts = pycompat.byteskwargs(opts)
    checkopts(opts, revs)

    if not opts.get('log'):
        # deprecated config: transplant.log
        opts['log'] = ui.config('transplant', 'log')
    if not opts.get('filter'):
        # deprecated config: transplant.filter
        opts['filter'] = ui.config('transplant', 'filter')

    tp = transplanter(ui, repo, opts)

    p1, p2 = repo.dirstate.parents()
    if len(repo) > 0 and p1 == revlog.nullid:
        raise error.Abort(_('no revision checked out'))
    if opts.get('continue'):
        if not tp.canresume():
            raise error.Abort(_('no transplant to continue'))
    else:
        cmdutil.checkunfinished(repo)
        if p2 != revlog.nullid:
            raise error.Abort(_('outstanding uncommitted merges'))
        m, a, r, d = repo.status()[:4]
        if m or a or r or d:
            raise error.Abort(_('outstanding local changes'))

    sourcerepo = opts.get('source')
    if sourcerepo:
        peer = hg.peer(repo, opts, ui.expandpath(sourcerepo))
        heads = pycompat.maplist(peer.lookup, opts.get('branch', ()))
        target = set(heads)
        for r in revs:
            try:
                target.add(peer.lookup(r))
            except error.RepoError:
                pass
        source, csets, cleanupfn = bundlerepo.getremotechanges(ui, repo, peer,
                                    onlyheads=sorted(target), force=True)
    else:
        source = repo
        heads = pycompat.maplist(source.lookup, opts.get('branch', ()))
        cleanupfn = None

    try:
        if opts.get('continue'):
            tp.resume(repo, source, opts)
            return

        tf = tp.transplantfilter(repo, source, p1)
        if opts.get('prune'):
            prune = set(source[r].node()
                        for r in scmutil.revrange(source, opts.get('prune')))
            matchfn = lambda x: tf(x) and x not in prune
        else:
            matchfn = tf
        merges = pycompat.maplist(source.lookup, opts.get('merge', ()))
        revmap = {}
        if revs:
            for r in scmutil.revrange(source, revs):
                revmap[int(r)] = source[r].node()
        elif opts.get('all') or not merges:
            if source != repo:
                alltransplants = incwalk(source, csets, match=matchfn)
            else:
                alltransplants = transplantwalk(source, p1, heads,
                                                match=matchfn)
            if opts.get('all'):
                revs = alltransplants
            else:
                revs, newmerges = browserevs(ui, source, alltransplants, opts)
                merges.extend(newmerges)
            for r in revs:
                revmap[source.changelog.rev(r)] = r
        for r in merges:
            revmap[source.changelog.rev(r)] = r

        tp.apply(repo, source, revmap, merges, opts)
    finally:
        if cleanupfn:
            cleanupfn()

revsetpredicate = registrar.revsetpredicate()

@revsetpredicate('transplanted([set])')
def revsettransplanted(repo, subset, x):
    """Transplanted changesets in set, or all transplanted changesets.
    """
    if x:
        s = revset.getset(repo, subset, x)
    else:
        s = subset
    return smartset.baseset([r for r in s if
        repo[r].extra().get('transplant_source')])

templatekeyword = registrar.templatekeyword()

@templatekeyword('transplanted', requires={'ctx'})
def kwtransplanted(context, mapping):
    """String. The node identifier of the transplanted
    changeset if any."""
    ctx = context.resource(mapping, 'ctx')
    n = ctx.extra().get('transplant_source')
    return n and nodemod.hex(n) or ''

def extsetup(ui):
    cmdutil.unfinishedstates.append(
        ['transplant/journal', True, False, _('transplant in progress'),
         _("use 'hg transplant --continue' or 'hg update' to abort")])

# tell hggettext to extract docstrings from these functions:
i18nfunctions = [revsettransplanted, kwtransplanted]
