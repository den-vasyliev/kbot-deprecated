# shelve.py - save/restore working directory state
#
# Copyright 2013 Facebook, Inc.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

"""save and restore changes to the working directory

The "hg shelve" command saves changes made to the working directory
and reverts those changes, resetting the working directory to a clean
state.

Later on, the "hg unshelve" command restores the changes saved by "hg
shelve". Changes can be restored even after updating to a different
parent, in which case Mercurial's merge machinery will resolve any
conflicts if necessary.

You can have more than one shelved change outstanding at a time; each
shelved change has a distinct name. For details, see the help for "hg
shelve".
"""
from __future__ import absolute_import

import collections
import errno
import itertools
import stat

from mercurial.i18n import _
from mercurial import (
    bookmarks,
    bundle2,
    bundlerepo,
    changegroup,
    cmdutil,
    discovery,
    error,
    exchange,
    hg,
    lock as lockmod,
    mdiff,
    merge,
    narrowspec,
    node as nodemod,
    patch,
    phases,
    pycompat,
    registrar,
    repair,
    scmutil,
    templatefilters,
    util,
    vfs as vfsmod,
)

from . import (
    rebase,
)
from mercurial.utils import (
    dateutil,
    stringutil,
)

cmdtable = {}
command = registrar.command(cmdtable)
# Note for extension authors: ONLY specify testedwith = 'ships-with-hg-core' for
# extensions which SHIP WITH MERCURIAL. Non-mainline extensions should
# be specifying the version(s) of Mercurial they are tested with, or
# leave the attribute unspecified.
testedwith = 'ships-with-hg-core'

configtable = {}
configitem = registrar.configitem(configtable)

configitem('shelve', 'maxbackups',
    default=10,
)

backupdir = 'shelve-backup'
shelvedir = 'shelved'
shelvefileextensions = ['hg', 'patch', 'shelve']
# universal extension is present in all types of shelves
patchextension = 'patch'

# we never need the user, so we use a
# generic user for all shelve operations
shelveuser = 'shelve@localhost'

class shelvedfile(object):
    """Helper for the file storing a single shelve

    Handles common functions on shelve files (.hg/.patch) using
    the vfs layer"""
    def __init__(self, repo, name, filetype=None):
        self.repo = repo
        self.name = name
        self.vfs = vfsmod.vfs(repo.vfs.join(shelvedir))
        self.backupvfs = vfsmod.vfs(repo.vfs.join(backupdir))
        self.ui = self.repo.ui
        if filetype:
            self.fname = name + '.' + filetype
        else:
            self.fname = name

    def exists(self):
        return self.vfs.exists(self.fname)

    def filename(self):
        return self.vfs.join(self.fname)

    def backupfilename(self):
        def gennames(base):
            yield base
            base, ext = base.rsplit('.', 1)
            for i in itertools.count(1):
                yield '%s-%d.%s' % (base, i, ext)

        name = self.backupvfs.join(self.fname)
        for n in gennames(name):
            if not self.backupvfs.exists(n):
                return n

    def movetobackup(self):
        if not self.backupvfs.isdir():
            self.backupvfs.makedir()
        util.rename(self.filename(), self.backupfilename())

    def stat(self):
        return self.vfs.stat(self.fname)

    def opener(self, mode='rb'):
        try:
            return self.vfs(self.fname, mode)
        except IOError as err:
            if err.errno != errno.ENOENT:
                raise
            raise error.Abort(_("shelved change '%s' not found") % self.name)

    def applybundle(self):
        fp = self.opener()
        try:
            targetphase = phases.internal
            if not phases.supportinternal(self.repo):
                targetphase = phases.secret
            gen = exchange.readbundle(self.repo.ui, fp, self.fname, self.vfs)
            pretip = self.repo['tip']
            tr = self.repo.currenttransaction()
            bundle2.applybundle(self.repo, gen, tr,
                                source='unshelve',
                                url='bundle:' + self.vfs.join(self.fname),
                                targetphase=targetphase)
            shelvectx = self.repo['tip']
            if pretip == shelvectx:
                shelverev = tr.changes['revduplicates'][-1]
                shelvectx = self.repo[shelverev]
            return shelvectx
        finally:
            fp.close()

    def bundlerepo(self):
        path = self.vfs.join(self.fname)
        return bundlerepo.instance(self.repo.baseui,
                                   'bundle://%s+%s' % (self.repo.root, path))

    def writebundle(self, bases, node):
        cgversion = changegroup.safeversion(self.repo)
        if cgversion == '01':
            btype = 'HG10BZ'
            compression = None
        else:
            btype = 'HG20'
            compression = 'BZ'

        repo = self.repo.unfiltered()

        outgoing = discovery.outgoing(repo, missingroots=bases,
                                      missingheads=[node])
        cg = changegroup.makechangegroup(repo, outgoing, cgversion, 'shelve')

        bundle2.writebundle(self.ui, cg, self.fname, btype, self.vfs,
                                compression=compression)

    def writeinfo(self, info):
        scmutil.simplekeyvaluefile(self.vfs, self.fname).write(info)

    def readinfo(self):
        return scmutil.simplekeyvaluefile(self.vfs, self.fname).read()

class shelvedstate(object):
    """Handle persistence during unshelving operations.

    Handles saving and restoring a shelved state. Ensures that different
    versions of a shelved state are possible and handles them appropriately.
    """
    _version = 2
    _filename = 'shelvedstate'
    _keep = 'keep'
    _nokeep = 'nokeep'
    # colon is essential to differentiate from a real bookmark name
    _noactivebook = ':no-active-bookmark'

    @classmethod
    def _verifyandtransform(cls, d):
        """Some basic shelvestate syntactic verification and transformation"""
        try:
            d['originalwctx'] = nodemod.bin(d['originalwctx'])
            d['pendingctx'] = nodemod.bin(d['pendingctx'])
            d['parents'] = [nodemod.bin(h)
                            for h in d['parents'].split(' ')]
            d['nodestoremove'] = [nodemod.bin(h)
                                  for h in d['nodestoremove'].split(' ')]
        except (ValueError, TypeError, KeyError) as err:
            raise error.CorruptedState(pycompat.bytestr(err))

    @classmethod
    def _getversion(cls, repo):
        """Read version information from shelvestate file"""
        fp = repo.vfs(cls._filename)
        try:
            version = int(fp.readline().strip())
        except ValueError as err:
            raise error.CorruptedState(pycompat.bytestr(err))
        finally:
            fp.close()
        return version

    @classmethod
    def _readold(cls, repo):
        """Read the old position-based version of a shelvestate file"""
        # Order is important, because old shelvestate file uses it
        # to detemine values of fields (i.g. name is on the second line,
        # originalwctx is on the third and so forth). Please do not change.
        keys = ['version', 'name', 'originalwctx', 'pendingctx', 'parents',
                'nodestoremove', 'branchtorestore', 'keep', 'activebook']
        # this is executed only seldomly, so it is not a big deal
        # that we open this file twice
        fp = repo.vfs(cls._filename)
        d = {}
        try:
            for key in keys:
                d[key] = fp.readline().strip()
        finally:
            fp.close()
        return d

    @classmethod
    def load(cls, repo):
        version = cls._getversion(repo)
        if version < cls._version:
            d = cls._readold(repo)
        elif version == cls._version:
            d = scmutil.simplekeyvaluefile(repo.vfs, cls._filename)\
                       .read(firstlinenonkeyval=True)
        else:
            raise error.Abort(_('this version of shelve is incompatible '
                                'with the version used in this repo'))

        cls._verifyandtransform(d)
        try:
            obj = cls()
            obj.name = d['name']
            obj.wctx = repo[d['originalwctx']]
            obj.pendingctx = repo[d['pendingctx']]
            obj.parents = d['parents']
            obj.nodestoremove = d['nodestoremove']
            obj.branchtorestore = d.get('branchtorestore', '')
            obj.keep = d.get('keep') == cls._keep
            obj.activebookmark = ''
            if d.get('activebook', '') != cls._noactivebook:
                obj.activebookmark = d.get('activebook', '')
        except (error.RepoLookupError, KeyError) as err:
            raise error.CorruptedState(pycompat.bytestr(err))

        return obj

    @classmethod
    def save(cls, repo, name, originalwctx, pendingctx, nodestoremove,
             branchtorestore, keep=False, activebook=''):
        info = {
            "name": name,
            "originalwctx": nodemod.hex(originalwctx.node()),
            "pendingctx": nodemod.hex(pendingctx.node()),
            "parents": ' '.join([nodemod.hex(p)
                                 for p in repo.dirstate.parents()]),
            "nodestoremove": ' '.join([nodemod.hex(n)
                                      for n in nodestoremove]),
            "branchtorestore": branchtorestore,
            "keep": cls._keep if keep else cls._nokeep,
            "activebook": activebook or cls._noactivebook
        }
        scmutil.simplekeyvaluefile(repo.vfs, cls._filename)\
               .write(info, firstline=("%d" % cls._version))

    @classmethod
    def clear(cls, repo):
        repo.vfs.unlinkpath(cls._filename, ignoremissing=True)

def cleanupoldbackups(repo):
    vfs = vfsmod.vfs(repo.vfs.join(backupdir))
    maxbackups = repo.ui.configint('shelve', 'maxbackups')
    hgfiles = [f for f in vfs.listdir()
               if f.endswith('.' + patchextension)]
    hgfiles = sorted([(vfs.stat(f)[stat.ST_MTIME], f) for f in hgfiles])
    if maxbackups > 0 and maxbackups < len(hgfiles):
        bordermtime = hgfiles[-maxbackups][0]
    else:
        bordermtime = None
    for mtime, f in hgfiles[:len(hgfiles) - maxbackups]:
        if mtime == bordermtime:
            # keep it, because timestamp can't decide exact order of backups
            continue
        base = f[:-(1 + len(patchextension))]
        for ext in shelvefileextensions:
            vfs.tryunlink(base + '.' + ext)

def _backupactivebookmark(repo):
    activebookmark = repo._activebookmark
    if activebookmark:
        bookmarks.deactivate(repo)
    return activebookmark

def _restoreactivebookmark(repo, mark):
    if mark:
        bookmarks.activate(repo, mark)

def _aborttransaction(repo):
    '''Abort current transaction for shelve/unshelve, but keep dirstate
    '''
    tr = repo.currenttransaction()
    dirstatebackupname = 'dirstate.shelve'
    narrowspecbackupname = 'narrowspec.shelve'
    repo.dirstate.savebackup(tr, dirstatebackupname)
    narrowspec.savebackup(repo, narrowspecbackupname)
    tr.abort()
    narrowspec.restorebackup(repo, narrowspecbackupname)
    repo.dirstate.restorebackup(None, dirstatebackupname)

def getshelvename(repo, parent, opts):
    """Decide on the name this shelve is going to have"""
    def gennames():
        yield label
        for i in itertools.count(1):
            yield '%s-%02d' % (label, i)
    name = opts.get('name')
    label = repo._activebookmark or parent.branch() or 'default'
    # slashes aren't allowed in filenames, therefore we rename it
    label = label.replace('/', '_')
    label = label.replace('\\', '_')
    # filenames must not start with '.' as it should not be hidden
    if label.startswith('.'):
        label = label.replace('.', '_', 1)

    if name:
        if shelvedfile(repo, name, patchextension).exists():
            e = _("a shelved change named '%s' already exists") % name
            raise error.Abort(e)

        # ensure we are not creating a subdirectory or a hidden file
        if '/' in name or '\\' in name:
            raise error.Abort(_('shelved change names can not contain slashes'))
        if name.startswith('.'):
            raise error.Abort(_("shelved change names can not start with '.'"))

    else:
        for n in gennames():
            if not shelvedfile(repo, n, patchextension).exists():
                name = n
                break

    return name

def mutableancestors(ctx):
    """return all mutable ancestors for ctx (included)

    Much faster than the revset ancestors(ctx) & draft()"""
    seen = {nodemod.nullrev}
    visit = collections.deque()
    visit.append(ctx)
    while visit:
        ctx = visit.popleft()
        yield ctx.node()
        for parent in ctx.parents():
            rev = parent.rev()
            if rev not in seen:
                seen.add(rev)
                if parent.mutable():
                    visit.append(parent)

def getcommitfunc(extra, interactive, editor=False):
    def commitfunc(ui, repo, message, match, opts):
        hasmq = util.safehasattr(repo, 'mq')
        if hasmq:
            saved, repo.mq.checkapplied = repo.mq.checkapplied, False

        targetphase = phases.internal
        if not phases.supportinternal(repo):
            targetphase = phases.secret
        overrides = {('phases', 'new-commit'): targetphase}
        try:
            editor_ = False
            if editor:
                editor_ = cmdutil.getcommiteditor(editform='shelve.shelve',
                                                  **pycompat.strkwargs(opts))
            with repo.ui.configoverride(overrides):
                return repo.commit(message, shelveuser, opts.get('date'),
                                   match, editor=editor_, extra=extra)
        finally:
            if hasmq:
                repo.mq.checkapplied = saved

    def interactivecommitfunc(ui, repo, *pats, **opts):
        opts = pycompat.byteskwargs(opts)
        match = scmutil.match(repo['.'], pats, {})
        message = opts['message']
        return commitfunc(ui, repo, message, match, opts)

    return interactivecommitfunc if interactive else commitfunc

def _nothingtoshelvemessaging(ui, repo, pats, opts):
    stat = repo.status(match=scmutil.match(repo[None], pats, opts))
    if stat.deleted:
        ui.status(_("nothing changed (%d missing files, see "
                    "'hg status')\n") % len(stat.deleted))
    else:
        ui.status(_("nothing changed\n"))

def _shelvecreatedcommit(repo, node, name):
    info = {'node': nodemod.hex(node)}
    shelvedfile(repo, name, 'shelve').writeinfo(info)
    bases = list(mutableancestors(repo[node]))
    shelvedfile(repo, name, 'hg').writebundle(bases, node)
    with shelvedfile(repo, name, patchextension).opener('wb') as fp:
        cmdutil.exportfile(repo, [node], fp, opts=mdiff.diffopts(git=True))

def _includeunknownfiles(repo, pats, opts, extra):
    s = repo.status(match=scmutil.match(repo[None], pats, opts),
                    unknown=True)
    if s.unknown:
        extra['shelve_unknown'] = '\0'.join(s.unknown)
        repo[None].add(s.unknown)

def _finishshelve(repo):
    if phases.supportinternal(repo):
        backupname = 'dirstate.shelve'
        tr = repo.currenttransaction()
        repo.dirstate.savebackup(tr, backupname)
        tr.close()
        repo.dirstate.restorebackup(None, backupname)
    else:
        _aborttransaction(repo)

def createcmd(ui, repo, pats, opts):
    """subcommand that creates a new shelve"""
    with repo.wlock():
        cmdutil.checkunfinished(repo)
        return _docreatecmd(ui, repo, pats, opts)

def _docreatecmd(ui, repo, pats, opts):
    wctx = repo[None]
    parents = wctx.parents()
    if len(parents) > 1:
        raise error.Abort(_('cannot shelve while merging'))
    parent = parents[0]
    origbranch = wctx.branch()

    if parent.node() != nodemod.nullid:
        desc = "changes to: %s" % parent.description().split('\n', 1)[0]
    else:
        desc = '(changes in empty repository)'

    if not opts.get('message'):
        opts['message'] = desc

    lock = tr = activebookmark = None
    try:
        lock = repo.lock()

        # use an uncommitted transaction to generate the bundle to avoid
        # pull races. ensure we don't print the abort message to stderr.
        tr = repo.transaction('commit', report=lambda x: None)

        interactive = opts.get('interactive', False)
        includeunknown = (opts.get('unknown', False) and
                          not opts.get('addremove', False))

        name = getshelvename(repo, parent, opts)
        activebookmark = _backupactivebookmark(repo)
        extra = {'internal': 'shelve'}
        if includeunknown:
            _includeunknownfiles(repo, pats, opts, extra)

        if _iswctxonnewbranch(repo) and not _isbareshelve(pats, opts):
            # In non-bare shelve we don't store newly created branch
            # at bundled commit
            repo.dirstate.setbranch(repo['.'].branch())

        commitfunc = getcommitfunc(extra, interactive, editor=True)
        if not interactive:
            node = cmdutil.commit(ui, repo, commitfunc, pats, opts)
        else:
            node = cmdutil.dorecord(ui, repo, commitfunc, None,
                                    False, cmdutil.recordfilter, *pats,
                                    **pycompat.strkwargs(opts))
        if not node:
            _nothingtoshelvemessaging(ui, repo, pats, opts)
            return 1

        _shelvecreatedcommit(repo, node, name)

        if ui.formatted():
            desc = stringutil.ellipsis(desc, ui.termwidth())
        ui.status(_('shelved as %s\n') % name)
        hg.update(repo, parent.node())
        if origbranch != repo['.'].branch() and not _isbareshelve(pats, opts):
            repo.dirstate.setbranch(origbranch)

        _finishshelve(repo)
    finally:
        _restoreactivebookmark(repo, activebookmark)
        lockmod.release(tr, lock)

def _isbareshelve(pats, opts):
    return (not pats
            and not opts.get('interactive', False)
            and not opts.get('include', False)
            and not opts.get('exclude', False))

def _iswctxonnewbranch(repo):
    return repo[None].branch() != repo['.'].branch()

def cleanupcmd(ui, repo):
    """subcommand that deletes all shelves"""

    with repo.wlock():
        for (name, _type) in repo.vfs.readdir(shelvedir):
            suffix = name.rsplit('.', 1)[-1]
            if suffix in shelvefileextensions:
                shelvedfile(repo, name).movetobackup()
            cleanupoldbackups(repo)

def deletecmd(ui, repo, pats):
    """subcommand that deletes a specific shelve"""
    if not pats:
        raise error.Abort(_('no shelved changes specified!'))
    with repo.wlock():
        try:
            for name in pats:
                for suffix in shelvefileextensions:
                    shfile = shelvedfile(repo, name, suffix)
                    # patch file is necessary, as it should
                    # be present for any kind of shelve,
                    # but the .hg file is optional as in future we
                    # will add obsolete shelve with does not create a
                    # bundle
                    if shfile.exists() or suffix == patchextension:
                        shfile.movetobackup()
            cleanupoldbackups(repo)
        except OSError as err:
            if err.errno != errno.ENOENT:
                raise
            raise error.Abort(_("shelved change '%s' not found") % name)

def listshelves(repo):
    """return all shelves in repo as list of (time, filename)"""
    try:
        names = repo.vfs.readdir(shelvedir)
    except OSError as err:
        if err.errno != errno.ENOENT:
            raise
        return []
    info = []
    for (name, _type) in names:
        pfx, sfx = name.rsplit('.', 1)
        if not pfx or sfx != patchextension:
            continue
        st = shelvedfile(repo, name).stat()
        info.append((st[stat.ST_MTIME], shelvedfile(repo, pfx).filename()))
    return sorted(info, reverse=True)

def listcmd(ui, repo, pats, opts):
    """subcommand that displays the list of shelves"""
    pats = set(pats)
    width = 80
    if not ui.plain():
        width = ui.termwidth()
    namelabel = 'shelve.newest'
    ui.pager('shelve')
    for mtime, name in listshelves(repo):
        sname = util.split(name)[1]
        if pats and sname not in pats:
            continue
        ui.write(sname, label=namelabel)
        namelabel = 'shelve.name'
        if ui.quiet:
            ui.write('\n')
            continue
        ui.write(' ' * (16 - len(sname)))
        used = 16
        date = dateutil.makedate(mtime)
        age = '(%s)' % templatefilters.age(date, abbrev=True)
        ui.write(age, label='shelve.age')
        ui.write(' ' * (12 - len(age)))
        used += 12
        with open(name + '.' + patchextension, 'rb') as fp:
            while True:
                line = fp.readline()
                if not line:
                    break
                if not line.startswith('#'):
                    desc = line.rstrip()
                    if ui.formatted():
                        desc = stringutil.ellipsis(desc, width - used)
                    ui.write(desc)
                    break
            ui.write('\n')
            if not (opts['patch'] or opts['stat']):
                continue
            difflines = fp.readlines()
            if opts['patch']:
                for chunk, label in patch.difflabel(iter, difflines):
                    ui.write(chunk, label=label)
            if opts['stat']:
                for chunk, label in patch.diffstatui(difflines, width=width):
                    ui.write(chunk, label=label)

def patchcmds(ui, repo, pats, opts):
    """subcommand that displays shelves"""
    if len(pats) == 0:
        shelves = listshelves(repo)
        if not shelves:
            raise error.Abort(_("there are no shelves to show"))
        mtime, name = shelves[0]
        sname = util.split(name)[1]
        pats = [sname]

    for shelfname in pats:
        if not shelvedfile(repo, shelfname, patchextension).exists():
            raise error.Abort(_("cannot find shelf %s") % shelfname)

    listcmd(ui, repo, pats, opts)

def checkparents(repo, state):
    """check parent while resuming an unshelve"""
    if state.parents != repo.dirstate.parents():
        raise error.Abort(_('working directory parents do not match unshelve '
                           'state'))

def pathtofiles(repo, files):
    cwd = repo.getcwd()
    return [repo.pathto(f, cwd) for f in files]

def unshelveabort(ui, repo, state, opts):
    """subcommand that abort an in-progress unshelve"""
    with repo.lock():
        try:
            checkparents(repo, state)

            merge.update(repo, state.pendingctx, branchmerge=False, force=True)
            if (state.activebookmark
                    and state.activebookmark in repo._bookmarks):
                bookmarks.activate(repo, state.activebookmark)

            if repo.vfs.exists('unshelverebasestate'):
                repo.vfs.rename('unshelverebasestate', 'rebasestate')
                rebase.clearstatus(repo)

            mergefiles(ui, repo, state.wctx, state.pendingctx)
            if not phases.supportinternal(repo):
                repair.strip(ui, repo, state.nodestoremove, backup=False,
                             topic='shelve')
        finally:
            shelvedstate.clear(repo)
            ui.warn(_("unshelve of '%s' aborted\n") % state.name)

def mergefiles(ui, repo, wctx, shelvectx):
    """updates to wctx and merges the changes from shelvectx into the
    dirstate."""
    with ui.configoverride({('ui', 'quiet'): True}):
        hg.update(repo, wctx.node())
        files = []
        files.extend(shelvectx.files())
        files.extend(shelvectx.parents()[0].files())

        # revert will overwrite unknown files, so move them out of the way
        for file in repo.status(unknown=True).unknown:
            if file in files:
                util.rename(file, scmutil.origpath(ui, repo, file))
        ui.pushbuffer(True)
        cmdutil.revert(ui, repo, shelvectx, repo.dirstate.parents(),
                       *pathtofiles(repo, files),
                       **{r'no_backup': True})
        ui.popbuffer()

def restorebranch(ui, repo, branchtorestore):
    if branchtorestore and branchtorestore != repo.dirstate.branch():
        repo.dirstate.setbranch(branchtorestore)
        ui.status(_('marked working directory as branch %s\n')
                  % branchtorestore)

def unshelvecleanup(ui, repo, name, opts):
    """remove related files after an unshelve"""
    if not opts.get('keep'):
        for filetype in shelvefileextensions:
            shfile = shelvedfile(repo, name, filetype)
            if shfile.exists():
                shfile.movetobackup()
        cleanupoldbackups(repo)

def unshelvecontinue(ui, repo, state, opts):
    """subcommand to continue an in-progress unshelve"""
    # We're finishing off a merge. First parent is our original
    # parent, second is the temporary "fake" commit we're unshelving.
    with repo.lock():
        checkparents(repo, state)
        ms = merge.mergestate.read(repo)
        if list(ms.unresolved()):
            raise error.Abort(
                _("unresolved conflicts, can't continue"),
                hint=_("see 'hg resolve', then 'hg unshelve --continue'"))

        shelvectx = repo[state.parents[1]]
        pendingctx = state.pendingctx

        with repo.dirstate.parentchange():
            repo.setparents(state.pendingctx.node(), nodemod.nullid)
            repo.dirstate.write(repo.currenttransaction())

        targetphase = phases.internal
        if not phases.supportinternal(repo):
            targetphase = phases.secret
        overrides = {('phases', 'new-commit'): targetphase}
        with repo.ui.configoverride(overrides, 'unshelve'):
            with repo.dirstate.parentchange():
                repo.setparents(state.parents[0], nodemod.nullid)
                newnode = repo.commit(text=shelvectx.description(),
                                      extra=shelvectx.extra(),
                                      user=shelvectx.user(),
                                      date=shelvectx.date())

        if newnode is None:
            # If it ended up being a no-op commit, then the normal
            # merge state clean-up path doesn't happen, so do it
            # here. Fix issue5494
            merge.mergestate.clean(repo)
            shelvectx = state.pendingctx
            msg = _('note: unshelved changes already existed '
                    'in the working copy\n')
            ui.status(msg)
        else:
            # only strip the shelvectx if we produced one
            state.nodestoremove.append(newnode)
            shelvectx = repo[newnode]

        hg.updaterepo(repo, pendingctx.node(), overwrite=False)

        if repo.vfs.exists('unshelverebasestate'):
            repo.vfs.rename('unshelverebasestate', 'rebasestate')
            rebase.clearstatus(repo)

        mergefiles(ui, repo, state.wctx, shelvectx)
        restorebranch(ui, repo, state.branchtorestore)

        if not phases.supportinternal(repo):
            repair.strip(ui, repo, state.nodestoremove, backup=False,
                         topic='shelve')
        _restoreactivebookmark(repo, state.activebookmark)
        shelvedstate.clear(repo)
        unshelvecleanup(ui, repo, state.name, opts)
        ui.status(_("unshelve of '%s' complete\n") % state.name)

def _commitworkingcopychanges(ui, repo, opts, tmpwctx):
    """Temporarily commit working copy changes before moving unshelve commit"""
    # Store pending changes in a commit and remember added in case a shelve
    # contains unknown files that are part of the pending change
    s = repo.status()
    addedbefore = frozenset(s.added)
    if not (s.modified or s.added or s.removed):
        return tmpwctx, addedbefore
    ui.status(_("temporarily committing pending changes "
                "(restore with 'hg unshelve --abort')\n"))
    extra = {'internal': 'shelve'}
    commitfunc = getcommitfunc(extra=extra, interactive=False,
                               editor=False)
    tempopts = {}
    tempopts['message'] = "pending changes temporary commit"
    tempopts['date'] = opts.get('date')
    with ui.configoverride({('ui', 'quiet'): True}):
        node = cmdutil.commit(ui, repo, commitfunc, [], tempopts)
    tmpwctx = repo[node]
    return tmpwctx, addedbefore

def _unshelverestorecommit(ui, repo, basename):
    """Recreate commit in the repository during the unshelve"""
    repo = repo.unfiltered()
    node = None
    if shelvedfile(repo, basename, 'shelve').exists():
        node = shelvedfile(repo, basename, 'shelve').readinfo()['node']
    if node is None or node not in repo:
        with ui.configoverride({('ui', 'quiet'): True}):
            shelvectx = shelvedfile(repo, basename, 'hg').applybundle()
        # We might not strip the unbundled changeset, so we should keep track of
        # the unshelve node in case we need to reuse it (eg: unshelve --keep)
        if node is None:
            info = {'node': nodemod.hex(shelvectx.node())}
            shelvedfile(repo, basename, 'shelve').writeinfo(info)
    else:
        shelvectx = repo[node]

    return repo, shelvectx

def _rebaserestoredcommit(ui, repo, opts, tr, oldtiprev, basename, pctx,
                          tmpwctx, shelvectx, branchtorestore,
                          activebookmark):
    """Rebase restored commit from its original location to a destination"""
    # If the shelve is not immediately on top of the commit
    # we'll be merging with, rebase it to be on top.
    if tmpwctx.node() == shelvectx.parents()[0].node():
        return shelvectx

    overrides = {
        ('ui', 'forcemerge'): opts.get('tool', ''),
        ('phases', 'new-commit'): phases.secret,
    }
    with repo.ui.configoverride(overrides, 'unshelve'):
        ui.status(_('rebasing shelved changes\n'))
        stats = merge.graft(repo, shelvectx, shelvectx.p1(),
                           labels=['shelve', 'working-copy'],
                           keepconflictparent=True)
        if stats.unresolvedcount:
            tr.close()

            nodestoremove = [repo.changelog.node(rev)
                             for rev in pycompat.xrange(oldtiprev, len(repo))]
            shelvedstate.save(repo, basename, pctx, tmpwctx, nodestoremove,
                              branchtorestore, opts.get('keep'), activebookmark)
            raise error.InterventionRequired(
                _("unresolved conflicts (see 'hg resolve', then "
                  "'hg unshelve --continue')"))

        with repo.dirstate.parentchange():
            repo.setparents(tmpwctx.node(), nodemod.nullid)
            newnode = repo.commit(text=shelvectx.description(),
                                  extra=shelvectx.extra(),
                                  user=shelvectx.user(),
                                  date=shelvectx.date())

        if newnode is None:
            # If it ended up being a no-op commit, then the normal
            # merge state clean-up path doesn't happen, so do it
            # here. Fix issue5494
            merge.mergestate.clean(repo)
            shelvectx = tmpwctx
            msg = _('note: unshelved changes already existed '
                    'in the working copy\n')
            ui.status(msg)
        else:
            shelvectx = repo[newnode]
            hg.updaterepo(repo, tmpwctx.node(), False)

    return shelvectx

def _forgetunknownfiles(repo, shelvectx, addedbefore):
    # Forget any files that were unknown before the shelve, unknown before
    # unshelve started, but are now added.
    shelveunknown = shelvectx.extra().get('shelve_unknown')
    if not shelveunknown:
        return
    shelveunknown = frozenset(shelveunknown.split('\0'))
    addedafter = frozenset(repo.status().added)
    toforget = (addedafter & shelveunknown) - addedbefore
    repo[None].forget(toforget)

def _finishunshelve(repo, oldtiprev, tr, activebookmark):
    _restoreactivebookmark(repo, activebookmark)
    # The transaction aborting will strip all the commits for us,
    # but it doesn't update the inmemory structures, so addchangegroup
    # hooks still fire and try to operate on the missing commits.
    # Clean up manually to prevent this.
    repo.unfiltered().changelog.strip(oldtiprev, tr)
    _aborttransaction(repo)

def _checkunshelveuntrackedproblems(ui, repo, shelvectx):
    """Check potential problems which may result from working
    copy having untracked changes."""
    wcdeleted = set(repo.status().deleted)
    shelvetouched = set(shelvectx.files())
    intersection = wcdeleted.intersection(shelvetouched)
    if intersection:
        m = _("shelved change touches missing files")
        hint = _("run hg status to see which files are missing")
        raise error.Abort(m, hint=hint)

@command('unshelve',
         [('a', 'abort', None,
           _('abort an incomplete unshelve operation')),
          ('c', 'continue', None,
           _('continue an incomplete unshelve operation')),
          ('k', 'keep', None,
           _('keep shelve after unshelving')),
          ('n', 'name', '',
           _('restore shelved change with given name'), _('NAME')),
          ('t', 'tool', '', _('specify merge tool')),
          ('', 'date', '',
           _('set date for temporary commits (DEPRECATED)'), _('DATE'))],
         _('hg unshelve [[-n] SHELVED]'),
         helpcategory=command.CATEGORY_WORKING_DIRECTORY)
def unshelve(ui, repo, *shelved, **opts):
    """restore a shelved change to the working directory

    This command accepts an optional name of a shelved change to
    restore. If none is given, the most recent shelved change is used.

    If a shelved change is applied successfully, the bundle that
    contains the shelved changes is moved to a backup location
    (.hg/shelve-backup).

    Since you can restore a shelved change on top of an arbitrary
    commit, it is possible that unshelving will result in a conflict
    between your changes and the commits you are unshelving onto. If
    this occurs, you must resolve the conflict, then use
    ``--continue`` to complete the unshelve operation. (The bundle
    will not be moved until you successfully complete the unshelve.)

    (Alternatively, you can use ``--abort`` to abandon an unshelve
    that causes a conflict. This reverts the unshelved changes, and
    leaves the bundle in place.)

    If bare shelved change(when no files are specified, without interactive,
    include and exclude option) was done on newly created branch it would
    restore branch information to the working directory.

    After a successful unshelve, the shelved changes are stored in a
    backup directory. Only the N most recent backups are kept. N
    defaults to 10 but can be overridden using the ``shelve.maxbackups``
    configuration option.

    .. container:: verbose

       Timestamp in seconds is used to decide order of backups. More
       than ``maxbackups`` backups are kept, if same timestamp
       prevents from deciding exact order of them, for safety.
    """
    with repo.wlock():
        return _dounshelve(ui, repo, *shelved, **opts)

def _dounshelve(ui, repo, *shelved, **opts):
    opts = pycompat.byteskwargs(opts)
    abortf = opts.get('abort')
    continuef = opts.get('continue')
    if not abortf and not continuef:
        cmdutil.checkunfinished(repo)
    shelved = list(shelved)
    if opts.get("name"):
        shelved.append(opts["name"])

    if abortf or continuef:
        if abortf and continuef:
            raise error.Abort(_('cannot use both abort and continue'))
        if shelved:
            raise error.Abort(_('cannot combine abort/continue with '
                               'naming a shelved change'))
        if abortf and opts.get('tool', False):
            ui.warn(_('tool option will be ignored\n'))

        try:
            state = shelvedstate.load(repo)
            if opts.get('keep') is None:
                opts['keep'] = state.keep
        except IOError as err:
            if err.errno != errno.ENOENT:
                raise
            cmdutil.wrongtooltocontinue(repo, _('unshelve'))
        except error.CorruptedState as err:
            ui.debug(pycompat.bytestr(err) + '\n')
            if continuef:
                msg = _('corrupted shelved state file')
                hint = _('please run hg unshelve --abort to abort unshelve '
                         'operation')
                raise error.Abort(msg, hint=hint)
            elif abortf:
                msg = _('could not read shelved state file, your working copy '
                        'may be in an unexpected state\nplease update to some '
                        'commit\n')
                ui.warn(msg)
                shelvedstate.clear(repo)
            return

        if abortf:
            return unshelveabort(ui, repo, state, opts)
        elif continuef:
            return unshelvecontinue(ui, repo, state, opts)
    elif len(shelved) > 1:
        raise error.Abort(_('can only unshelve one change at a time'))
    elif not shelved:
        shelved = listshelves(repo)
        if not shelved:
            raise error.Abort(_('no shelved changes to apply!'))
        basename = util.split(shelved[0][1])[1]
        ui.status(_("unshelving change '%s'\n") % basename)
    else:
        basename = shelved[0]

    if not shelvedfile(repo, basename, patchextension).exists():
        raise error.Abort(_("shelved change '%s' not found") % basename)

    repo = repo.unfiltered()
    lock = tr = None
    try:
        lock = repo.lock()
        tr = repo.transaction('unshelve', report=lambda x: None)
        oldtiprev = len(repo)

        pctx = repo['.']
        tmpwctx = pctx
        # The goal is to have a commit structure like so:
        # ...-> pctx -> tmpwctx -> shelvectx
        # where tmpwctx is an optional commit with the user's pending changes
        # and shelvectx is the unshelved changes. Then we merge it all down
        # to the original pctx.

        activebookmark = _backupactivebookmark(repo)
        tmpwctx, addedbefore = _commitworkingcopychanges(ui, repo, opts,
                                                         tmpwctx)
        repo, shelvectx = _unshelverestorecommit(ui, repo, basename)
        _checkunshelveuntrackedproblems(ui, repo, shelvectx)
        branchtorestore = ''
        if shelvectx.branch() != shelvectx.p1().branch():
            branchtorestore = shelvectx.branch()

        shelvectx = _rebaserestoredcommit(ui, repo, opts, tr, oldtiprev,
                                          basename, pctx, tmpwctx,
                                          shelvectx, branchtorestore,
                                          activebookmark)
        overrides = {('ui', 'forcemerge'): opts.get('tool', '')}
        with ui.configoverride(overrides, 'unshelve'):
            mergefiles(ui, repo, pctx, shelvectx)
        restorebranch(ui, repo, branchtorestore)
        _forgetunknownfiles(repo, shelvectx, addedbefore)

        shelvedstate.clear(repo)
        _finishunshelve(repo, oldtiprev, tr, activebookmark)
        unshelvecleanup(ui, repo, basename, opts)
    finally:
        if tr:
            tr.release()
        lockmod.release(lock)

@command('shelve',
         [('A', 'addremove', None,
           _('mark new/missing files as added/removed before shelving')),
          ('u', 'unknown', None,
           _('store unknown files in the shelve')),
          ('', 'cleanup', None,
           _('delete all shelved changes')),
          ('', 'date', '',
           _('shelve with the specified commit date'), _('DATE')),
          ('d', 'delete', None,
           _('delete the named shelved change(s)')),
          ('e', 'edit', False,
           _('invoke editor on commit messages')),
          ('l', 'list', None,
           _('list current shelves')),
          ('m', 'message', '',
           _('use text as shelve message'), _('TEXT')),
          ('n', 'name', '',
           _('use the given name for the shelved commit'), _('NAME')),
          ('p', 'patch', None,
           _('output patches for changes (provide the names of the shelved '
             'changes as positional arguments)')),
          ('i', 'interactive', None,
           _('interactive mode, only works while creating a shelve')),
          ('', 'stat', None,
           _('output diffstat-style summary of changes (provide the names of '
             'the shelved changes as positional arguments)')
           )] + cmdutil.walkopts,
         _('hg shelve [OPTION]... [FILE]...'),
         helpcategory=command.CATEGORY_WORKING_DIRECTORY)
def shelvecmd(ui, repo, *pats, **opts):
    '''save and set aside changes from the working directory

    Shelving takes files that "hg status" reports as not clean, saves
    the modifications to a bundle (a shelved change), and reverts the
    files so that their state in the working directory becomes clean.

    To restore these changes to the working directory, using "hg
    unshelve"; this will work even if you switch to a different
    commit.

    When no files are specified, "hg shelve" saves all not-clean
    files. If specific files or directories are named, only changes to
    those files are shelved.

    In bare shelve (when no files are specified, without interactive,
    include and exclude option), shelving remembers information if the
    working directory was on newly created branch, in other words working
    directory was on different branch than its first parent. In this
    situation unshelving restores branch information to the working directory.

    Each shelved change has a name that makes it easier to find later.
    The name of a shelved change defaults to being based on the active
    bookmark, or if there is no active bookmark, the current named
    branch.  To specify a different name, use ``--name``.

    To see a list of existing shelved changes, use the ``--list``
    option. For each shelved change, this will print its name, age,
    and description; use ``--patch`` or ``--stat`` for more details.

    To delete specific shelved changes, use ``--delete``. To delete
    all shelved changes, use ``--cleanup``.
    '''
    opts = pycompat.byteskwargs(opts)
    allowables = [
        ('addremove', {'create'}), # 'create' is pseudo action
        ('unknown', {'create'}),
        ('cleanup', {'cleanup'}),
#       ('date', {'create'}), # ignored for passing '--date "0 0"' in tests
        ('delete', {'delete'}),
        ('edit', {'create'}),
        ('list', {'list'}),
        ('message', {'create'}),
        ('name', {'create'}),
        ('patch', {'patch', 'list'}),
        ('stat', {'stat', 'list'}),
    ]
    def checkopt(opt):
        if opts.get(opt):
            for i, allowable in allowables:
                if opts[i] and opt not in allowable:
                    raise error.Abort(_("options '--%s' and '--%s' may not be "
                                       "used together") % (opt, i))
            return True
    if checkopt('cleanup'):
        if pats:
            raise error.Abort(_("cannot specify names when using '--cleanup'"))
        return cleanupcmd(ui, repo)
    elif checkopt('delete'):
        return deletecmd(ui, repo, pats)
    elif checkopt('list'):
        return listcmd(ui, repo, pats, opts)
    elif checkopt('patch') or checkopt('stat'):
        return patchcmds(ui, repo, pats, opts)
    else:
        return createcmd(ui, repo, pats, opts)

def extsetup(ui):
    cmdutil.unfinishedstates.append(
        [shelvedstate._filename, False, False,
         _('unshelve already in progress'),
         _("use 'hg unshelve --continue' or 'hg unshelve --abort'")])
    cmdutil.afterresolvedstates.append(
        [shelvedstate._filename, _('hg unshelve --continue')])
