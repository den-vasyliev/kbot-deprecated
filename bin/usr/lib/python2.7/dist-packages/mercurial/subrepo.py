# subrepo.py - sub-repository classes and factory
#
# Copyright 2009-2010 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import copy
import errno
import hashlib
import os
import posixpath
import re
import stat
import subprocess
import sys
import tarfile
import xml.dom.minidom

from .i18n import _
from . import (
    cmdutil,
    encoding,
    error,
    exchange,
    logcmdutil,
    match as matchmod,
    node,
    pathutil,
    phases,
    pycompat,
    scmutil,
    subrepoutil,
    util,
    vfs as vfsmod,
)
from .utils import (
    dateutil,
    procutil,
    stringutil,
)

hg = None
reporelpath = subrepoutil.reporelpath
subrelpath = subrepoutil.subrelpath
_abssource = subrepoutil._abssource
propertycache = util.propertycache

def _expandedabspath(path):
    '''
    get a path or url and if it is a path expand it and return an absolute path
    '''
    expandedpath = util.urllocalpath(util.expandpath(path))
    u = util.url(expandedpath)
    if not u.scheme:
        path = util.normpath(os.path.abspath(u.path))
    return path

def _getstorehashcachename(remotepath):
    '''get a unique filename for the store hash cache of a remote repository'''
    return node.hex(hashlib.sha1(_expandedabspath(remotepath)).digest())[0:12]

class SubrepoAbort(error.Abort):
    """Exception class used to avoid handling a subrepo error more than once"""
    def __init__(self, *args, **kw):
        self.subrepo = kw.pop(r'subrepo', None)
        self.cause = kw.pop(r'cause', None)
        error.Abort.__init__(self, *args, **kw)

def annotatesubrepoerror(func):
    def decoratedmethod(self, *args, **kargs):
        try:
            res = func(self, *args, **kargs)
        except SubrepoAbort as ex:
            # This exception has already been handled
            raise ex
        except error.Abort as ex:
            subrepo = subrelpath(self)
            errormsg = (stringutil.forcebytestr(ex) + ' '
                        + _('(in subrepository "%s")') % subrepo)
            # avoid handling this exception by raising a SubrepoAbort exception
            raise SubrepoAbort(errormsg, hint=ex.hint, subrepo=subrepo,
                               cause=sys.exc_info())
        return res
    return decoratedmethod

def _updateprompt(ui, sub, dirty, local, remote):
    if dirty:
        msg = (_(' subrepository sources for %s differ\n'
                 'use (l)ocal source (%s) or (r)emote source (%s)?'
                 '$$ &Local $$ &Remote')
               % (subrelpath(sub), local, remote))
    else:
        msg = (_(' subrepository sources for %s differ (in checked out '
                 'version)\n'
                 'use (l)ocal source (%s) or (r)emote source (%s)?'
                 '$$ &Local $$ &Remote')
               % (subrelpath(sub), local, remote))
    return ui.promptchoice(msg, 0)

def _sanitize(ui, vfs, ignore):
    for dirname, dirs, names in vfs.walk():
        for i, d in enumerate(dirs):
            if d.lower() == ignore:
                del dirs[i]
                break
        if vfs.basename(dirname).lower() != '.hg':
            continue
        for f in names:
            if f.lower() == 'hgrc':
                ui.warn(_("warning: removing potentially hostile 'hgrc' "
                          "in '%s'\n") % vfs.join(dirname))
                vfs.unlink(vfs.reljoin(dirname, f))

def _auditsubrepopath(repo, path):
    # sanity check for potentially unsafe paths such as '~' and '$FOO'
    if path.startswith('~') or '$' in path or util.expandpath(path) != path:
        raise error.Abort(_('subrepo path contains illegal component: %s')
                          % path)
    # auditor doesn't check if the path itself is a symlink
    pathutil.pathauditor(repo.root)(path)
    if repo.wvfs.islink(path):
        raise error.Abort(_("subrepo '%s' traverses symbolic link") % path)

SUBREPO_ALLOWED_DEFAULTS = {
    'hg': True,
    'git': False,
    'svn': False,
}

def _checktype(ui, kind):
    # subrepos.allowed is a master kill switch. If disabled, subrepos are
    # disabled period.
    if not ui.configbool('subrepos', 'allowed', True):
        raise error.Abort(_('subrepos not enabled'),
                          hint=_("see 'hg help config.subrepos' for details"))

    default = SUBREPO_ALLOWED_DEFAULTS.get(kind, False)
    if not ui.configbool('subrepos', '%s:allowed' % kind, default):
        raise error.Abort(_('%s subrepos not allowed') % kind,
                          hint=_("see 'hg help config.subrepos' for details"))

    if kind not in types:
        raise error.Abort(_('unknown subrepo type %s') % kind)

def subrepo(ctx, path, allowwdir=False, allowcreate=True):
    """return instance of the right subrepo class for subrepo in path"""
    # subrepo inherently violates our import layering rules
    # because it wants to make repo objects from deep inside the stack
    # so we manually delay the circular imports to not break
    # scripts that don't use our demand-loading
    global hg
    from . import hg as h
    hg = h

    repo = ctx.repo()
    _auditsubrepopath(repo, path)
    state = ctx.substate[path]
    _checktype(repo.ui, state[2])
    if allowwdir:
        state = (state[0], ctx.subrev(path), state[2])
    return types[state[2]](ctx, path, state[:2], allowcreate)

def nullsubrepo(ctx, path, pctx):
    """return an empty subrepo in pctx for the extant subrepo in ctx"""
    # subrepo inherently violates our import layering rules
    # because it wants to make repo objects from deep inside the stack
    # so we manually delay the circular imports to not break
    # scripts that don't use our demand-loading
    global hg
    from . import hg as h
    hg = h

    repo = ctx.repo()
    _auditsubrepopath(repo, path)
    state = ctx.substate[path]
    _checktype(repo.ui, state[2])
    subrev = ''
    if state[2] == 'hg':
        subrev = "0" * 40
    return types[state[2]](pctx, path, (state[0], subrev), True)

# subrepo classes need to implement the following abstract class:

class abstractsubrepo(object):

    def __init__(self, ctx, path):
        """Initialize abstractsubrepo part

        ``ctx`` is the context referring this subrepository in the
        parent repository.

        ``path`` is the path to this subrepository as seen from
        innermost repository.
        """
        self.ui = ctx.repo().ui
        self._ctx = ctx
        self._path = path

    def addwebdirpath(self, serverpath, webconf):
        """Add the hgwebdir entries for this subrepo, and any of its subrepos.

        ``serverpath`` is the path component of the URL for this repo.

        ``webconf`` is the dictionary of hgwebdir entries.
        """
        pass

    def storeclean(self, path):
        """
        returns true if the repository has not changed since it was last
        cloned from or pushed to a given repository.
        """
        return False

    def dirty(self, ignoreupdate=False, missing=False):
        """returns true if the dirstate of the subrepo is dirty or does not
        match current stored state. If ignoreupdate is true, only check
        whether the subrepo has uncommitted changes in its dirstate.  If missing
        is true, check for deleted files.
        """
        raise NotImplementedError

    def dirtyreason(self, ignoreupdate=False, missing=False):
        """return reason string if it is ``dirty()``

        Returned string should have enough information for the message
        of exception.

        This returns None, otherwise.
        """
        if self.dirty(ignoreupdate=ignoreupdate, missing=missing):
            return _('uncommitted changes in subrepository "%s"'
                     ) % subrelpath(self)

    def bailifchanged(self, ignoreupdate=False, hint=None):
        """raise Abort if subrepository is ``dirty()``
        """
        dirtyreason = self.dirtyreason(ignoreupdate=ignoreupdate,
                                       missing=True)
        if dirtyreason:
            raise error.Abort(dirtyreason, hint=hint)

    def basestate(self):
        """current working directory base state, disregarding .hgsubstate
        state and working directory modifications"""
        raise NotImplementedError

    def checknested(self, path):
        """check if path is a subrepository within this repository"""
        return False

    def commit(self, text, user, date):
        """commit the current changes to the subrepo with the given
        log message. Use given user and date if possible. Return the
        new state of the subrepo.
        """
        raise NotImplementedError

    def phase(self, state):
        """returns phase of specified state in the subrepository.
        """
        return phases.public

    def remove(self):
        """remove the subrepo

        (should verify the dirstate is not dirty first)
        """
        raise NotImplementedError

    def get(self, state, overwrite=False):
        """run whatever commands are needed to put the subrepo into
        this state
        """
        raise NotImplementedError

    def merge(self, state):
        """merge currently-saved state with the new state."""
        raise NotImplementedError

    def push(self, opts):
        """perform whatever action is analogous to 'hg push'

        This may be a no-op on some systems.
        """
        raise NotImplementedError

    def add(self, ui, match, prefix, explicitonly, **opts):
        return []

    def addremove(self, matcher, prefix, opts):
        self.ui.warn("%s: %s" % (prefix, _("addremove is not supported")))
        return 1

    def cat(self, match, fm, fntemplate, prefix, **opts):
        return 1

    def status(self, rev2, **opts):
        return scmutil.status([], [], [], [], [], [], [])

    def diff(self, ui, diffopts, node2, match, prefix, **opts):
        pass

    def outgoing(self, ui, dest, opts):
        return 1

    def incoming(self, ui, source, opts):
        return 1

    def files(self):
        """return filename iterator"""
        raise NotImplementedError

    def filedata(self, name, decode):
        """return file data, optionally passed through repo decoders"""
        raise NotImplementedError

    def fileflags(self, name):
        """return file flags"""
        return ''

    def matchfileset(self, expr, badfn=None):
        """Resolve the fileset expression for this repo"""
        return matchmod.nevermatcher(self.wvfs.base, '', badfn=badfn)

    def printfiles(self, ui, m, fm, fmt, subrepos):
        """handle the files command for this subrepo"""
        return 1

    def archive(self, archiver, prefix, match=None, decode=True):
        if match is not None:
            files = [f for f in self.files() if match(f)]
        else:
            files = self.files()
        total = len(files)
        relpath = subrelpath(self)
        progress = self.ui.makeprogress(_('archiving (%s)') % relpath,
                                        unit=_('files'), total=total)
        progress.update(0)
        for name in files:
            flags = self.fileflags(name)
            mode = 'x' in flags and 0o755 or 0o644
            symlink = 'l' in flags
            archiver.addfile(prefix + self._path + '/' + name,
                             mode, symlink, self.filedata(name, decode))
            progress.increment()
        progress.complete()
        return total

    def walk(self, match):
        '''
        walk recursively through the directory tree, finding all files
        matched by the match function
        '''

    def forget(self, match, prefix, dryrun, interactive):
        return ([], [])

    def removefiles(self, matcher, prefix, after, force, subrepos,
                    dryrun, warnings):
        """remove the matched files from the subrepository and the filesystem,
        possibly by force and/or after the file has been removed from the
        filesystem.  Return 0 on success, 1 on any warning.
        """
        warnings.append(_("warning: removefiles not implemented (%s)")
                        % self._path)
        return 1

    def revert(self, substate, *pats, **opts):
        self.ui.warn(_('%s: reverting %s subrepos is unsupported\n') \
            % (substate[0], substate[2]))
        return []

    def shortid(self, revid):
        return revid

    def unshare(self):
        '''
        convert this repository from shared to normal storage.
        '''

    def verify(self):
        '''verify the integrity of the repository.  Return 0 on success or
        warning, 1 on any error.
        '''
        return 0

    @propertycache
    def wvfs(self):
        """return vfs to access the working directory of this subrepository
        """
        return vfsmod.vfs(self._ctx.repo().wvfs.join(self._path))

    @propertycache
    def _relpath(self):
        """return path to this subrepository as seen from outermost repository
        """
        return self.wvfs.reljoin(reporelpath(self._ctx.repo()), self._path)

class hgsubrepo(abstractsubrepo):
    def __init__(self, ctx, path, state, allowcreate):
        super(hgsubrepo, self).__init__(ctx, path)
        self._state = state
        r = ctx.repo()
        root = r.wjoin(path)
        create = allowcreate and not r.wvfs.exists('%s/.hg' % path)
        # repository constructor does expand variables in path, which is
        # unsafe since subrepo path might come from untrusted source.
        if os.path.realpath(util.expandpath(root)) != root:
            raise error.Abort(_('subrepo path contains illegal component: %s')
                              % path)
        self._repo = hg.repository(r.baseui, root, create=create)
        if self._repo.root != root:
            raise error.ProgrammingError('failed to reject unsafe subrepo '
                                         'path: %s (expanded to %s)'
                                         % (root, self._repo.root))

        # Propagate the parent's --hidden option
        if r is r.unfiltered():
            self._repo = self._repo.unfiltered()

        self.ui = self._repo.ui
        for s, k in [('ui', 'commitsubrepos')]:
            v = r.ui.config(s, k)
            if v:
                self.ui.setconfig(s, k, v, 'subrepo')
        # internal config: ui._usedassubrepo
        self.ui.setconfig('ui', '_usedassubrepo', 'True', 'subrepo')
        self._initrepo(r, state[0], create)

    @annotatesubrepoerror
    def addwebdirpath(self, serverpath, webconf):
        cmdutil.addwebdirpath(self._repo, subrelpath(self), webconf)

    def storeclean(self, path):
        with self._repo.lock():
            return self._storeclean(path)

    def _storeclean(self, path):
        clean = True
        itercache = self._calcstorehash(path)
        for filehash in self._readstorehashcache(path):
            if filehash != next(itercache, None):
                clean = False
                break
        if clean:
            # if not empty:
            # the cached and current pull states have a different size
            clean = next(itercache, None) is None
        return clean

    def _calcstorehash(self, remotepath):
        '''calculate a unique "store hash"

        This method is used to to detect when there are changes that may
        require a push to a given remote path.'''
        # sort the files that will be hashed in increasing (likely) file size
        filelist = ('bookmarks', 'store/phaseroots', 'store/00changelog.i')
        yield '# %s\n' % _expandedabspath(remotepath)
        vfs = self._repo.vfs
        for relname in filelist:
            filehash = node.hex(hashlib.sha1(vfs.tryread(relname)).digest())
            yield '%s = %s\n' % (relname, filehash)

    @propertycache
    def _cachestorehashvfs(self):
        return vfsmod.vfs(self._repo.vfs.join('cache/storehash'))

    def _readstorehashcache(self, remotepath):
        '''read the store hash cache for a given remote repository'''
        cachefile = _getstorehashcachename(remotepath)
        return self._cachestorehashvfs.tryreadlines(cachefile, 'r')

    def _cachestorehash(self, remotepath):
        '''cache the current store hash

        Each remote repo requires its own store hash cache, because a subrepo
        store may be "clean" versus a given remote repo, but not versus another
        '''
        cachefile = _getstorehashcachename(remotepath)
        with self._repo.lock():
            storehash = list(self._calcstorehash(remotepath))
            vfs = self._cachestorehashvfs
            vfs.writelines(cachefile, storehash, mode='wb', notindexed=True)

    def _getctx(self):
        '''fetch the context for this subrepo revision, possibly a workingctx
        '''
        if self._ctx.rev() is None:
            return self._repo[None]  # workingctx if parent is workingctx
        else:
            rev = self._state[1]
            return self._repo[rev]

    @annotatesubrepoerror
    def _initrepo(self, parentrepo, source, create):
        self._repo._subparent = parentrepo
        self._repo._subsource = source

        if create:
            lines = ['[paths]\n']

            def addpathconfig(key, value):
                if value:
                    lines.append('%s = %s\n' % (key, value))
                    self.ui.setconfig('paths', key, value, 'subrepo')

            defpath = _abssource(self._repo, abort=False)
            defpushpath = _abssource(self._repo, True, abort=False)
            addpathconfig('default', defpath)
            if defpath != defpushpath:
                addpathconfig('default-push', defpushpath)

            self._repo.vfs.write('hgrc', util.tonativeeol(''.join(lines)))

    @annotatesubrepoerror
    def add(self, ui, match, prefix, explicitonly, **opts):
        return cmdutil.add(ui, self._repo, match,
                           self.wvfs.reljoin(prefix, self._path),
                           explicitonly, **opts)

    @annotatesubrepoerror
    def addremove(self, m, prefix, opts):
        # In the same way as sub directories are processed, once in a subrepo,
        # always entry any of its subrepos.  Don't corrupt the options that will
        # be used to process sibling subrepos however.
        opts = copy.copy(opts)
        opts['subrepos'] = True
        return scmutil.addremove(self._repo, m,
                                 self.wvfs.reljoin(prefix, self._path), opts)

    @annotatesubrepoerror
    def cat(self, match, fm, fntemplate, prefix, **opts):
        rev = self._state[1]
        ctx = self._repo[rev]
        return cmdutil.cat(self.ui, self._repo, ctx, match, fm, fntemplate,
                           prefix, **opts)

    @annotatesubrepoerror
    def status(self, rev2, **opts):
        try:
            rev1 = self._state[1]
            ctx1 = self._repo[rev1]
            ctx2 = self._repo[rev2]
            return self._repo.status(ctx1, ctx2, **opts)
        except error.RepoLookupError as inst:
            self.ui.warn(_('warning: error "%s" in subrepository "%s"\n')
                         % (inst, subrelpath(self)))
            return scmutil.status([], [], [], [], [], [], [])

    @annotatesubrepoerror
    def diff(self, ui, diffopts, node2, match, prefix, **opts):
        try:
            node1 = node.bin(self._state[1])
            # We currently expect node2 to come from substate and be
            # in hex format
            if node2 is not None:
                node2 = node.bin(node2)
            logcmdutil.diffordiffstat(ui, self._repo, diffopts,
                                      node1, node2, match,
                                      prefix=posixpath.join(prefix, self._path),
                                      listsubrepos=True, **opts)
        except error.RepoLookupError as inst:
            self.ui.warn(_('warning: error "%s" in subrepository "%s"\n')
                          % (inst, subrelpath(self)))

    @annotatesubrepoerror
    def archive(self, archiver, prefix, match=None, decode=True):
        self._get(self._state + ('hg',))
        files = self.files()
        if match:
            files = [f for f in files if match(f)]
        rev = self._state[1]
        ctx = self._repo[rev]
        scmutil.prefetchfiles(self._repo, [ctx.rev()],
                              scmutil.matchfiles(self._repo, files))
        total = abstractsubrepo.archive(self, archiver, prefix, match)
        for subpath in ctx.substate:
            s = subrepo(ctx, subpath, True)
            submatch = matchmod.subdirmatcher(subpath, match)
            total += s.archive(archiver, prefix + self._path + '/', submatch,
                               decode)
        return total

    @annotatesubrepoerror
    def dirty(self, ignoreupdate=False, missing=False):
        r = self._state[1]
        if r == '' and not ignoreupdate: # no state recorded
            return True
        w = self._repo[None]
        if r != w.p1().hex() and not ignoreupdate:
            # different version checked out
            return True
        return w.dirty(missing=missing) # working directory changed

    def basestate(self):
        return self._repo['.'].hex()

    def checknested(self, path):
        return self._repo._checknested(self._repo.wjoin(path))

    @annotatesubrepoerror
    def commit(self, text, user, date):
        # don't bother committing in the subrepo if it's only been
        # updated
        if not self.dirty(True):
            return self._repo['.'].hex()
        self.ui.debug("committing subrepo %s\n" % subrelpath(self))
        n = self._repo.commit(text, user, date)
        if not n:
            return self._repo['.'].hex() # different version checked out
        return node.hex(n)

    @annotatesubrepoerror
    def phase(self, state):
        return self._repo[state or '.'].phase()

    @annotatesubrepoerror
    def remove(self):
        # we can't fully delete the repository as it may contain
        # local-only history
        self.ui.note(_('removing subrepo %s\n') % subrelpath(self))
        hg.clean(self._repo, node.nullid, False)

    def _get(self, state):
        source, revision, kind = state
        parentrepo = self._repo._subparent

        if revision in self._repo.unfiltered():
            # Allow shared subrepos tracked at null to setup the sharedpath
            if len(self._repo) != 0 or not parentrepo.shared():
                return True
        self._repo._subsource = source
        srcurl = _abssource(self._repo)

        # Defer creating the peer until after the status message is logged, in
        # case there are network problems.
        getpeer = lambda: hg.peer(self._repo, {}, srcurl)

        if len(self._repo) == 0:
            # use self._repo.vfs instead of self.wvfs to remove .hg only
            self._repo.vfs.rmtree()

            # A remote subrepo could be shared if there is a local copy
            # relative to the parent's share source.  But clone pooling doesn't
            # assemble the repos in a tree, so that can't be consistently done.
            # A simpler option is for the user to configure clone pooling, and
            # work with that.
            if parentrepo.shared() and hg.islocal(srcurl):
                self.ui.status(_('sharing subrepo %s from %s\n')
                               % (subrelpath(self), srcurl))
                shared = hg.share(self._repo._subparent.baseui,
                                  getpeer(), self._repo.root,
                                  update=False, bookmarks=False)
                self._repo = shared.local()
            else:
                # TODO: find a common place for this and this code in the
                # share.py wrap of the clone command.
                if parentrepo.shared():
                    pool = self.ui.config('share', 'pool')
                    if pool:
                        pool = util.expandpath(pool)

                    shareopts = {
                        'pool': pool,
                        'mode': self.ui.config('share', 'poolnaming'),
                    }
                else:
                    shareopts = {}

                self.ui.status(_('cloning subrepo %s from %s\n')
                               % (subrelpath(self), util.hidepassword(srcurl)))
                other, cloned = hg.clone(self._repo._subparent.baseui, {},
                                         getpeer(), self._repo.root,
                                         update=False, shareopts=shareopts)
                self._repo = cloned.local()
            self._initrepo(parentrepo, source, create=True)
            self._cachestorehash(srcurl)
        else:
            self.ui.status(_('pulling subrepo %s from %s\n')
                           % (subrelpath(self), util.hidepassword(srcurl)))
            cleansub = self.storeclean(srcurl)
            exchange.pull(self._repo, getpeer())
            if cleansub:
                # keep the repo clean after pull
                self._cachestorehash(srcurl)
        return False

    @annotatesubrepoerror
    def get(self, state, overwrite=False):
        inrepo = self._get(state)
        source, revision, kind = state
        repo = self._repo
        repo.ui.debug("getting subrepo %s\n" % self._path)
        if inrepo:
            urepo = repo.unfiltered()
            ctx = urepo[revision]
            if ctx.hidden():
                urepo.ui.warn(
                    _('revision %s in subrepository "%s" is hidden\n') \
                    % (revision[0:12], self._path))
                repo = urepo
        hg.updaterepo(repo, revision, overwrite)

    @annotatesubrepoerror
    def merge(self, state):
        self._get(state)
        cur = self._repo['.']
        dst = self._repo[state[1]]
        anc = dst.ancestor(cur)

        def mergefunc():
            if anc == cur and dst.branch() == cur.branch():
                self.ui.debug('updating subrepository "%s"\n'
                              % subrelpath(self))
                hg.update(self._repo, state[1])
            elif anc == dst:
                self.ui.debug('skipping subrepository "%s"\n'
                              % subrelpath(self))
            else:
                self.ui.debug('merging subrepository "%s"\n' % subrelpath(self))
                hg.merge(self._repo, state[1], remind=False)

        wctx = self._repo[None]
        if self.dirty():
            if anc != dst:
                if _updateprompt(self.ui, self, wctx.dirty(), cur, dst):
                    mergefunc()
            else:
                mergefunc()
        else:
            mergefunc()

    @annotatesubrepoerror
    def push(self, opts):
        force = opts.get('force')
        newbranch = opts.get('new_branch')
        ssh = opts.get('ssh')

        # push subrepos depth-first for coherent ordering
        c = self._repo['.']
        subs = c.substate # only repos that are committed
        for s in sorted(subs):
            if c.sub(s).push(opts) == 0:
                return False

        dsturl = _abssource(self._repo, True)
        if not force:
            if self.storeclean(dsturl):
                self.ui.status(
                    _('no changes made to subrepo %s since last push to %s\n')
                    % (subrelpath(self), util.hidepassword(dsturl)))
                return None
        self.ui.status(_('pushing subrepo %s to %s\n') %
            (subrelpath(self), util.hidepassword(dsturl)))
        other = hg.peer(self._repo, {'ssh': ssh}, dsturl)
        res = exchange.push(self._repo, other, force, newbranch=newbranch)

        # the repo is now clean
        self._cachestorehash(dsturl)
        return res.cgresult

    @annotatesubrepoerror
    def outgoing(self, ui, dest, opts):
        if 'rev' in opts or 'branch' in opts:
            opts = copy.copy(opts)
            opts.pop('rev', None)
            opts.pop('branch', None)
        return hg.outgoing(ui, self._repo, _abssource(self._repo, True), opts)

    @annotatesubrepoerror
    def incoming(self, ui, source, opts):
        if 'rev' in opts or 'branch' in opts:
            opts = copy.copy(opts)
            opts.pop('rev', None)
            opts.pop('branch', None)
        return hg.incoming(ui, self._repo, _abssource(self._repo, False), opts)

    @annotatesubrepoerror
    def files(self):
        rev = self._state[1]
        ctx = self._repo[rev]
        return ctx.manifest().keys()

    def filedata(self, name, decode):
        rev = self._state[1]
        data = self._repo[rev][name].data()
        if decode:
            data = self._repo.wwritedata(name, data)
        return data

    def fileflags(self, name):
        rev = self._state[1]
        ctx = self._repo[rev]
        return ctx.flags(name)

    @annotatesubrepoerror
    def printfiles(self, ui, m, fm, fmt, subrepos):
        # If the parent context is a workingctx, use the workingctx here for
        # consistency.
        if self._ctx.rev() is None:
            ctx = self._repo[None]
        else:
            rev = self._state[1]
            ctx = self._repo[rev]
        return cmdutil.files(ui, ctx, m, fm, fmt, subrepos)

    @annotatesubrepoerror
    def matchfileset(self, expr, badfn=None):
        repo = self._repo
        if self._ctx.rev() is None:
            ctx = repo[None]
        else:
            rev = self._state[1]
            ctx = repo[rev]

        matchers = [ctx.matchfileset(expr, badfn=badfn)]

        for subpath in ctx.substate:
            sub = ctx.sub(subpath)

            try:
                sm = sub.matchfileset(expr, badfn=badfn)
                pm = matchmod.prefixdirmatcher(repo.root, repo.getcwd(),
                                               subpath, sm, badfn=badfn)
                matchers.append(pm)
            except error.LookupError:
                self.ui.status(_("skipping missing subrepository: %s\n")
                               % self.wvfs.reljoin(reporelpath(self), subpath))
        if len(matchers) == 1:
            return matchers[0]
        return matchmod.unionmatcher(matchers)

    def walk(self, match):
        ctx = self._repo[None]
        return ctx.walk(match)

    @annotatesubrepoerror
    def forget(self, match, prefix, dryrun, interactive):
        return cmdutil.forget(self.ui, self._repo, match,
                              self.wvfs.reljoin(prefix, self._path),
                              True, dryrun=dryrun, interactive=interactive)

    @annotatesubrepoerror
    def removefiles(self, matcher, prefix, after, force, subrepos,
                    dryrun, warnings):
        return cmdutil.remove(self.ui, self._repo, matcher,
                              self.wvfs.reljoin(prefix, self._path),
                              after, force, subrepos, dryrun)

    @annotatesubrepoerror
    def revert(self, substate, *pats, **opts):
        # reverting a subrepo is a 2 step process:
        # 1. if the no_backup is not set, revert all modified
        #    files inside the subrepo
        # 2. update the subrepo to the revision specified in
        #    the corresponding substate dictionary
        self.ui.status(_('reverting subrepo %s\n') % substate[0])
        if not opts.get(r'no_backup'):
            # Revert all files on the subrepo, creating backups
            # Note that this will not recursively revert subrepos
            # We could do it if there was a set:subrepos() predicate
            opts = opts.copy()
            opts[r'date'] = None
            opts[r'rev'] = substate[1]

            self.filerevert(*pats, **opts)

        # Update the repo to the revision specified in the given substate
        if not opts.get(r'dry_run'):
            self.get(substate, overwrite=True)

    def filerevert(self, *pats, **opts):
        ctx = self._repo[opts[r'rev']]
        parents = self._repo.dirstate.parents()
        if opts.get(r'all'):
            pats = ['set:modified()']
        else:
            pats = []
        cmdutil.revert(self.ui, self._repo, ctx, parents, *pats, **opts)

    def shortid(self, revid):
        return revid[:12]

    @annotatesubrepoerror
    def unshare(self):
        # subrepo inherently violates our import layering rules
        # because it wants to make repo objects from deep inside the stack
        # so we manually delay the circular imports to not break
        # scripts that don't use our demand-loading
        global hg
        from . import hg as h
        hg = h

        # Nothing prevents a user from sharing in a repo, and then making that a
        # subrepo.  Alternately, the previous unshare attempt may have failed
        # part way through.  So recurse whether or not this layer is shared.
        if self._repo.shared():
            self.ui.status(_("unsharing subrepo '%s'\n") % self._relpath)

        hg.unshare(self.ui, self._repo)

    def verify(self):
        try:
            rev = self._state[1]
            ctx = self._repo.unfiltered()[rev]
            if ctx.hidden():
                # Since hidden revisions aren't pushed/pulled, it seems worth an
                # explicit warning.
                ui = self._repo.ui
                ui.warn(_("subrepo '%s' is hidden in revision %s\n") %
                        (self._relpath, node.short(self._ctx.node())))
            return 0
        except error.RepoLookupError:
            # A missing subrepo revision may be a case of needing to pull it, so
            # don't treat this as an error.
            self._repo.ui.warn(_("subrepo '%s' not found in revision %s\n") %
                               (self._relpath, node.short(self._ctx.node())))
            return 0

    @propertycache
    def wvfs(self):
        """return own wvfs for efficiency and consistency
        """
        return self._repo.wvfs

    @propertycache
    def _relpath(self):
        """return path to this subrepository as seen from outermost repository
        """
        # Keep consistent dir separators by avoiding vfs.join(self._path)
        return reporelpath(self._repo)

class svnsubrepo(abstractsubrepo):
    def __init__(self, ctx, path, state, allowcreate):
        super(svnsubrepo, self).__init__(ctx, path)
        self._state = state
        self._exe = procutil.findexe('svn')
        if not self._exe:
            raise error.Abort(_("'svn' executable not found for subrepo '%s'")
                             % self._path)

    def _svncommand(self, commands, filename='', failok=False):
        cmd = [self._exe]
        extrakw = {}
        if not self.ui.interactive():
            # Making stdin be a pipe should prevent svn from behaving
            # interactively even if we can't pass --non-interactive.
            extrakw[r'stdin'] = subprocess.PIPE
            # Starting in svn 1.5 --non-interactive is a global flag
            # instead of being per-command, but we need to support 1.4 so
            # we have to be intelligent about what commands take
            # --non-interactive.
            if commands[0] in ('update', 'checkout', 'commit'):
                cmd.append('--non-interactive')
        cmd.extend(commands)
        if filename is not None:
            path = self.wvfs.reljoin(self._ctx.repo().origroot,
                                     self._path, filename)
            cmd.append(path)
        env = dict(encoding.environ)
        # Avoid localized output, preserve current locale for everything else.
        lc_all = env.get('LC_ALL')
        if lc_all:
            env['LANG'] = lc_all
            del env['LC_ALL']
        env['LC_MESSAGES'] = 'C'
        p = subprocess.Popen(pycompat.rapply(procutil.tonativestr, cmd),
                             bufsize=-1, close_fds=procutil.closefds,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             universal_newlines=True,
                             env=procutil.tonativeenv(env), **extrakw)
        stdout, stderr = p.communicate()
        stderr = stderr.strip()
        if not failok:
            if p.returncode:
                raise error.Abort(stderr or 'exited with code %d'
                                  % p.returncode)
            if stderr:
                self.ui.warn(stderr + '\n')
        return stdout, stderr

    @propertycache
    def _svnversion(self):
        output, err = self._svncommand(['--version', '--quiet'], filename=None)
        m = re.search(br'^(\d+)\.(\d+)', output)
        if not m:
            raise error.Abort(_('cannot retrieve svn tool version'))
        return (int(m.group(1)), int(m.group(2)))

    def _svnmissing(self):
        return not self.wvfs.exists('.svn')

    def _wcrevs(self):
        # Get the working directory revision as well as the last
        # commit revision so we can compare the subrepo state with
        # both. We used to store the working directory one.
        output, err = self._svncommand(['info', '--xml'])
        doc = xml.dom.minidom.parseString(output)
        entries = doc.getElementsByTagName('entry')
        lastrev, rev = '0', '0'
        if entries:
            rev = str(entries[0].getAttribute('revision')) or '0'
            commits = entries[0].getElementsByTagName('commit')
            if commits:
                lastrev = str(commits[0].getAttribute('revision')) or '0'
        return (lastrev, rev)

    def _wcrev(self):
        return self._wcrevs()[0]

    def _wcchanged(self):
        """Return (changes, extchanges, missing) where changes is True
        if the working directory was changed, extchanges is
        True if any of these changes concern an external entry and missing
        is True if any change is a missing entry.
        """
        output, err = self._svncommand(['status', '--xml'])
        externals, changes, missing = [], [], []
        doc = xml.dom.minidom.parseString(output)
        for e in doc.getElementsByTagName('entry'):
            s = e.getElementsByTagName('wc-status')
            if not s:
                continue
            item = s[0].getAttribute('item')
            props = s[0].getAttribute('props')
            path = e.getAttribute('path')
            if item == 'external':
                externals.append(path)
            elif item == 'missing':
                missing.append(path)
            if (item not in ('', 'normal', 'unversioned', 'external')
                or props not in ('', 'none', 'normal')):
                changes.append(path)
        for path in changes:
            for ext in externals:
                if path == ext or path.startswith(ext + pycompat.ossep):
                    return True, True, bool(missing)
        return bool(changes), False, bool(missing)

    @annotatesubrepoerror
    def dirty(self, ignoreupdate=False, missing=False):
        if self._svnmissing():
            return self._state[1] != ''
        wcchanged = self._wcchanged()
        changed = wcchanged[0] or (missing and wcchanged[2])
        if not changed:
            if self._state[1] in self._wcrevs() or ignoreupdate:
                return False
        return True

    def basestate(self):
        lastrev, rev = self._wcrevs()
        if lastrev != rev:
            # Last committed rev is not the same than rev. We would
            # like to take lastrev but we do not know if the subrepo
            # URL exists at lastrev.  Test it and fallback to rev it
            # is not there.
            try:
                self._svncommand(['list', '%s@%s' % (self._state[0], lastrev)])
                return lastrev
            except error.Abort:
                pass
        return rev

    @annotatesubrepoerror
    def commit(self, text, user, date):
        # user and date are out of our hands since svn is centralized
        changed, extchanged, missing = self._wcchanged()
        if not changed:
            return self.basestate()
        if extchanged:
            # Do not try to commit externals
            raise error.Abort(_('cannot commit svn externals'))
        if missing:
            # svn can commit with missing entries but aborting like hg
            # seems a better approach.
            raise error.Abort(_('cannot commit missing svn entries'))
        commitinfo, err = self._svncommand(['commit', '-m', text])
        self.ui.status(commitinfo)
        newrev = re.search('Committed revision ([0-9]+).', commitinfo)
        if not newrev:
            if not commitinfo.strip():
                # Sometimes, our definition of "changed" differs from
                # svn one. For instance, svn ignores missing files
                # when committing. If there are only missing files, no
                # commit is made, no output and no error code.
                raise error.Abort(_('failed to commit svn changes'))
            raise error.Abort(commitinfo.splitlines()[-1])
        newrev = newrev.groups()[0]
        self.ui.status(self._svncommand(['update', '-r', newrev])[0])
        return newrev

    @annotatesubrepoerror
    def remove(self):
        if self.dirty():
            self.ui.warn(_('not removing repo %s because '
                           'it has changes.\n') % self._path)
            return
        self.ui.note(_('removing subrepo %s\n') % self._path)

        self.wvfs.rmtree(forcibly=True)
        try:
            pwvfs = self._ctx.repo().wvfs
            pwvfs.removedirs(pwvfs.dirname(self._path))
        except OSError:
            pass

    @annotatesubrepoerror
    def get(self, state, overwrite=False):
        if overwrite:
            self._svncommand(['revert', '--recursive'])
        args = ['checkout']
        if self._svnversion >= (1, 5):
            args.append('--force')
        # The revision must be specified at the end of the URL to properly
        # update to a directory which has since been deleted and recreated.
        args.append('%s@%s' % (state[0], state[1]))

        # SEC: check that the ssh url is safe
        util.checksafessh(state[0])

        status, err = self._svncommand(args, failok=True)
        _sanitize(self.ui, self.wvfs, '.svn')
        if not re.search('Checked out revision [0-9]+.', status):
            if ('is already a working copy for a different URL' in err
                and (self._wcchanged()[:2] == (False, False))):
                # obstructed but clean working copy, so just blow it away.
                self.remove()
                self.get(state, overwrite=False)
                return
            raise error.Abort((status or err).splitlines()[-1])
        self.ui.status(status)

    @annotatesubrepoerror
    def merge(self, state):
        old = self._state[1]
        new = state[1]
        wcrev = self._wcrev()
        if new != wcrev:
            dirty = old == wcrev or self._wcchanged()[0]
            if _updateprompt(self.ui, self, dirty, wcrev, new):
                self.get(state, False)

    def push(self, opts):
        # push is a no-op for SVN
        return True

    @annotatesubrepoerror
    def files(self):
        output = self._svncommand(['list', '--recursive', '--xml'])[0]
        doc = xml.dom.minidom.parseString(output)
        paths = []
        for e in doc.getElementsByTagName('entry'):
            kind = pycompat.bytestr(e.getAttribute('kind'))
            if kind != 'file':
                continue
            name = ''.join(c.data for c
                           in e.getElementsByTagName('name')[0].childNodes
                           if c.nodeType == c.TEXT_NODE)
            paths.append(name.encode('utf-8'))
        return paths

    def filedata(self, name, decode):
        return self._svncommand(['cat'], name)[0]


class gitsubrepo(abstractsubrepo):
    def __init__(self, ctx, path, state, allowcreate):
        super(gitsubrepo, self).__init__(ctx, path)
        self._state = state
        self._abspath = ctx.repo().wjoin(path)
        self._subparent = ctx.repo()
        self._ensuregit()

    def _ensuregit(self):
        try:
            self._gitexecutable = 'git'
            out, err = self._gitnodir(['--version'])
        except OSError as e:
            genericerror = _("error executing git for subrepo '%s': %s")
            notfoundhint = _("check git is installed and in your PATH")
            if e.errno != errno.ENOENT:
                raise error.Abort(genericerror % (
                    self._path, encoding.strtolocal(e.strerror)))
            elif pycompat.iswindows:
                try:
                    self._gitexecutable = 'git.cmd'
                    out, err = self._gitnodir(['--version'])
                except OSError as e2:
                    if e2.errno == errno.ENOENT:
                        raise error.Abort(_("couldn't find 'git' or 'git.cmd'"
                            " for subrepo '%s'") % self._path,
                            hint=notfoundhint)
                    else:
                        raise error.Abort(genericerror % (self._path,
                            encoding.strtolocal(e2.strerror)))
            else:
                raise error.Abort(_("couldn't find git for subrepo '%s'")
                    % self._path, hint=notfoundhint)
        versionstatus = self._checkversion(out)
        if versionstatus == 'unknown':
            self.ui.warn(_('cannot retrieve git version\n'))
        elif versionstatus == 'abort':
            raise error.Abort(_('git subrepo requires at least 1.6.0 or later'))
        elif versionstatus == 'warning':
            self.ui.warn(_('git subrepo requires at least 1.6.0 or later\n'))

    @staticmethod
    def _gitversion(out):
        m = re.search(br'^git version (\d+)\.(\d+)\.(\d+)', out)
        if m:
            return (int(m.group(1)), int(m.group(2)), int(m.group(3)))

        m = re.search(br'^git version (\d+)\.(\d+)', out)
        if m:
            return (int(m.group(1)), int(m.group(2)), 0)

        return -1

    @staticmethod
    def _checkversion(out):
        '''ensure git version is new enough

        >>> _checkversion = gitsubrepo._checkversion
        >>> _checkversion(b'git version 1.6.0')
        'ok'
        >>> _checkversion(b'git version 1.8.5')
        'ok'
        >>> _checkversion(b'git version 1.4.0')
        'abort'
        >>> _checkversion(b'git version 1.5.0')
        'warning'
        >>> _checkversion(b'git version 1.9-rc0')
        'ok'
        >>> _checkversion(b'git version 1.9.0.265.g81cdec2')
        'ok'
        >>> _checkversion(b'git version 1.9.0.GIT')
        'ok'
        >>> _checkversion(b'git version 12345')
        'unknown'
        >>> _checkversion(b'no')
        'unknown'
        '''
        version = gitsubrepo._gitversion(out)
        # git 1.4.0 can't work at all, but 1.5.X can in at least some cases,
        # despite the docstring comment.  For now, error on 1.4.0, warn on
        # 1.5.0 but attempt to continue.
        if version == -1:
            return 'unknown'
        if version < (1, 5, 0):
            return 'abort'
        elif version < (1, 6, 0):
            return 'warning'
        return 'ok'

    def _gitcommand(self, commands, env=None, stream=False):
        return self._gitdir(commands, env=env, stream=stream)[0]

    def _gitdir(self, commands, env=None, stream=False):
        return self._gitnodir(commands, env=env, stream=stream,
                              cwd=self._abspath)

    def _gitnodir(self, commands, env=None, stream=False, cwd=None):
        """Calls the git command

        The methods tries to call the git command. versions prior to 1.6.0
        are not supported and very probably fail.
        """
        self.ui.debug('%s: git %s\n' % (self._relpath, ' '.join(commands)))
        if env is None:
            env = encoding.environ.copy()
        # disable localization for Git output (issue5176)
        env['LC_ALL'] = 'C'
        # fix for Git CVE-2015-7545
        if 'GIT_ALLOW_PROTOCOL' not in env:
            env['GIT_ALLOW_PROTOCOL'] = 'file:git:http:https:ssh'
        # unless ui.quiet is set, print git's stderr,
        # which is mostly progress and useful info
        errpipe = None
        if self.ui.quiet:
            errpipe = open(os.devnull, 'w')
        if self.ui._colormode and len(commands) and commands[0] == "diff":
            # insert the argument in the front,
            # the end of git diff arguments is used for paths
            commands.insert(1, '--color')
        p = subprocess.Popen(pycompat.rapply(procutil.tonativestr,
                                             [self._gitexecutable] + commands),
                             bufsize=-1,
                             cwd=pycompat.rapply(procutil.tonativestr, cwd),
                             env=procutil.tonativeenv(env),
                             close_fds=procutil.closefds,
                             stdout=subprocess.PIPE, stderr=errpipe)
        if stream:
            return p.stdout, None

        retdata = p.stdout.read().strip()
        # wait for the child to exit to avoid race condition.
        p.wait()

        if p.returncode != 0 and p.returncode != 1:
            # there are certain error codes that are ok
            command = commands[0]
            if command in ('cat-file', 'symbolic-ref'):
                return retdata, p.returncode
            # for all others, abort
            raise error.Abort(_('git %s error %d in %s') %
                             (command, p.returncode, self._relpath))

        return retdata, p.returncode

    def _gitmissing(self):
        return not self.wvfs.exists('.git')

    def _gitstate(self):
        return self._gitcommand(['rev-parse', 'HEAD'])

    def _gitcurrentbranch(self):
        current, err = self._gitdir(['symbolic-ref', 'HEAD', '--quiet'])
        if err:
            current = None
        return current

    def _gitremote(self, remote):
        out = self._gitcommand(['remote', 'show', '-n', remote])
        line = out.split('\n')[1]
        i = line.index('URL: ') + len('URL: ')
        return line[i:]

    def _githavelocally(self, revision):
        out, code = self._gitdir(['cat-file', '-e', revision])
        return code == 0

    def _gitisancestor(self, r1, r2):
        base = self._gitcommand(['merge-base', r1, r2])
        return base == r1

    def _gitisbare(self):
        return self._gitcommand(['config', '--bool', 'core.bare']) == 'true'

    def _gitupdatestat(self):
        """This must be run before git diff-index.
        diff-index only looks at changes to file stat;
        this command looks at file contents and updates the stat."""
        self._gitcommand(['update-index', '-q', '--refresh'])

    def _gitbranchmap(self):
        '''returns 2 things:
        a map from git branch to revision
        a map from revision to branches'''
        branch2rev = {}
        rev2branch = {}

        out = self._gitcommand(['for-each-ref', '--format',
                                '%(objectname) %(refname)'])
        for line in out.split('\n'):
            revision, ref = line.split(' ')
            if (not ref.startswith('refs/heads/') and
                not ref.startswith('refs/remotes/')):
                continue
            if ref.startswith('refs/remotes/') and ref.endswith('/HEAD'):
                continue # ignore remote/HEAD redirects
            branch2rev[ref] = revision
            rev2branch.setdefault(revision, []).append(ref)
        return branch2rev, rev2branch

    def _gittracking(self, branches):
        'return map of remote branch to local tracking branch'
        # assumes no more than one local tracking branch for each remote
        tracking = {}
        for b in branches:
            if b.startswith('refs/remotes/'):
                continue
            bname = b.split('/', 2)[2]
            remote = self._gitcommand(['config', 'branch.%s.remote' % bname])
            if remote:
                ref = self._gitcommand(['config', 'branch.%s.merge' % bname])
                tracking['refs/remotes/%s/%s' %
                         (remote, ref.split('/', 2)[2])] = b
        return tracking

    def _abssource(self, source):
        if '://' not in source:
            # recognize the scp syntax as an absolute source
            colon = source.find(':')
            if colon != -1 and '/' not in source[:colon]:
                return source
        self._subsource = source
        return _abssource(self)

    def _fetch(self, source, revision):
        if self._gitmissing():
            # SEC: check for safe ssh url
            util.checksafessh(source)

            source = self._abssource(source)
            self.ui.status(_('cloning subrepo %s from %s\n') %
                            (self._relpath, source))
            self._gitnodir(['clone', source, self._abspath])
        if self._githavelocally(revision):
            return
        self.ui.status(_('pulling subrepo %s from %s\n') %
                        (self._relpath, self._gitremote('origin')))
        # try only origin: the originally cloned repo
        self._gitcommand(['fetch'])
        if not self._githavelocally(revision):
            raise error.Abort(_('revision %s does not exist in subrepository '
                                '"%s"\n') % (revision, self._relpath))

    @annotatesubrepoerror
    def dirty(self, ignoreupdate=False, missing=False):
        if self._gitmissing():
            return self._state[1] != ''
        if self._gitisbare():
            return True
        if not ignoreupdate and self._state[1] != self._gitstate():
            # different version checked out
            return True
        # check for staged changes or modified files; ignore untracked files
        self._gitupdatestat()
        out, code = self._gitdir(['diff-index', '--quiet', 'HEAD'])
        return code == 1

    def basestate(self):
        return self._gitstate()

    @annotatesubrepoerror
    def get(self, state, overwrite=False):
        source, revision, kind = state
        if not revision:
            self.remove()
            return
        self._fetch(source, revision)
        # if the repo was set to be bare, unbare it
        if self._gitisbare():
            self._gitcommand(['config', 'core.bare', 'false'])
            if self._gitstate() == revision:
                self._gitcommand(['reset', '--hard', 'HEAD'])
                return
        elif self._gitstate() == revision:
            if overwrite:
                # first reset the index to unmark new files for commit, because
                # reset --hard will otherwise throw away files added for commit,
                # not just unmark them.
                self._gitcommand(['reset', 'HEAD'])
                self._gitcommand(['reset', '--hard', 'HEAD'])
            return
        branch2rev, rev2branch = self._gitbranchmap()

        def checkout(args):
            cmd = ['checkout']
            if overwrite:
                # first reset the index to unmark new files for commit, because
                # the -f option will otherwise throw away files added for
                # commit, not just unmark them.
                self._gitcommand(['reset', 'HEAD'])
                cmd.append('-f')
            self._gitcommand(cmd + args)
            _sanitize(self.ui, self.wvfs, '.git')

        def rawcheckout():
            # no branch to checkout, check it out with no branch
            self.ui.warn(_('checking out detached HEAD in '
                           'subrepository "%s"\n') % self._relpath)
            self.ui.warn(_('check out a git branch if you intend '
                            'to make changes\n'))
            checkout(['-q', revision])

        if revision not in rev2branch:
            rawcheckout()
            return
        branches = rev2branch[revision]
        firstlocalbranch = None
        for b in branches:
            if b == 'refs/heads/master':
                # master trumps all other branches
                checkout(['refs/heads/master'])
                return
            if not firstlocalbranch and not b.startswith('refs/remotes/'):
                firstlocalbranch = b
        if firstlocalbranch:
            checkout([firstlocalbranch])
            return

        tracking = self._gittracking(branch2rev.keys())
        # choose a remote branch already tracked if possible
        remote = branches[0]
        if remote not in tracking:
            for b in branches:
                if b in tracking:
                    remote = b
                    break

        if remote not in tracking:
            # create a new local tracking branch
            local = remote.split('/', 3)[3]
            checkout(['-b', local, remote])
        elif self._gitisancestor(branch2rev[tracking[remote]], remote):
            # When updating to a tracked remote branch,
            # if the local tracking branch is downstream of it,
            # a normal `git pull` would have performed a "fast-forward merge"
            # which is equivalent to updating the local branch to the remote.
            # Since we are only looking at branching at update, we need to
            # detect this situation and perform this action lazily.
            if tracking[remote] != self._gitcurrentbranch():
                checkout([tracking[remote]])
            self._gitcommand(['merge', '--ff', remote])
            _sanitize(self.ui, self.wvfs, '.git')
        else:
            # a real merge would be required, just checkout the revision
            rawcheckout()

    @annotatesubrepoerror
    def commit(self, text, user, date):
        if self._gitmissing():
            raise error.Abort(_("subrepo %s is missing") % self._relpath)
        cmd = ['commit', '-a', '-m', text]
        env = encoding.environ.copy()
        if user:
            cmd += ['--author', user]
        if date:
            # git's date parser silently ignores when seconds < 1e9
            # convert to ISO8601
            env['GIT_AUTHOR_DATE'] = dateutil.datestr(date,
                                                  '%Y-%m-%dT%H:%M:%S %1%2')
        self._gitcommand(cmd, env=env)
        # make sure commit works otherwise HEAD might not exist under certain
        # circumstances
        return self._gitstate()

    @annotatesubrepoerror
    def merge(self, state):
        source, revision, kind = state
        self._fetch(source, revision)
        base = self._gitcommand(['merge-base', revision, self._state[1]])
        self._gitupdatestat()
        out, code = self._gitdir(['diff-index', '--quiet', 'HEAD'])

        def mergefunc():
            if base == revision:
                self.get(state) # fast forward merge
            elif base != self._state[1]:
                self._gitcommand(['merge', '--no-commit', revision])
            _sanitize(self.ui, self.wvfs, '.git')

        if self.dirty():
            if self._gitstate() != revision:
                dirty = self._gitstate() == self._state[1] or code != 0
                if _updateprompt(self.ui, self, dirty,
                                 self._state[1][:7], revision[:7]):
                    mergefunc()
        else:
            mergefunc()

    @annotatesubrepoerror
    def push(self, opts):
        force = opts.get('force')

        if not self._state[1]:
            return True
        if self._gitmissing():
            raise error.Abort(_("subrepo %s is missing") % self._relpath)
        # if a branch in origin contains the revision, nothing to do
        branch2rev, rev2branch = self._gitbranchmap()
        if self._state[1] in rev2branch:
            for b in rev2branch[self._state[1]]:
                if b.startswith('refs/remotes/origin/'):
                    return True
        for b, revision in branch2rev.iteritems():
            if b.startswith('refs/remotes/origin/'):
                if self._gitisancestor(self._state[1], revision):
                    return True
        # otherwise, try to push the currently checked out branch
        cmd = ['push']
        if force:
            cmd.append('--force')

        current = self._gitcurrentbranch()
        if current:
            # determine if the current branch is even useful
            if not self._gitisancestor(self._state[1], current):
                self.ui.warn(_('unrelated git branch checked out '
                               'in subrepository "%s"\n') % self._relpath)
                return False
            self.ui.status(_('pushing branch %s of subrepository "%s"\n') %
                           (current.split('/', 2)[2], self._relpath))
            ret = self._gitdir(cmd + ['origin', current])
            return ret[1] == 0
        else:
            self.ui.warn(_('no branch checked out in subrepository "%s"\n'
                           'cannot push revision %s\n') %
                          (self._relpath, self._state[1]))
            return False

    @annotatesubrepoerror
    def add(self, ui, match, prefix, explicitonly, **opts):
        if self._gitmissing():
            return []

        s = self.status(None, unknown=True, clean=True)

        tracked = set()
        # dirstates 'amn' warn, 'r' is added again
        for l in (s.modified, s.added, s.deleted, s.clean):
            tracked.update(l)

        # Unknown files not of interest will be rejected by the matcher
        files = s.unknown
        files.extend(match.files())

        rejected = []

        files = [f for f in sorted(set(files)) if match(f)]
        for f in files:
            exact = match.exact(f)
            command = ["add"]
            if exact:
                command.append("-f") #should be added, even if ignored
            if ui.verbose or not exact:
                ui.status(_('adding %s\n') % match.rel(f))

            if f in tracked:  # hg prints 'adding' even if already tracked
                if exact:
                    rejected.append(f)
                continue
            if not opts.get(r'dry_run'):
                self._gitcommand(command + [f])

        for f in rejected:
            ui.warn(_("%s already tracked!\n") % match.abs(f))

        return rejected

    @annotatesubrepoerror
    def remove(self):
        if self._gitmissing():
            return
        if self.dirty():
            self.ui.warn(_('not removing repo %s because '
                           'it has changes.\n') % self._relpath)
            return
        # we can't fully delete the repository as it may contain
        # local-only history
        self.ui.note(_('removing subrepo %s\n') % self._relpath)
        self._gitcommand(['config', 'core.bare', 'true'])
        for f, kind in self.wvfs.readdir():
            if f == '.git':
                continue
            if kind == stat.S_IFDIR:
                self.wvfs.rmtree(f)
            else:
                self.wvfs.unlink(f)

    def archive(self, archiver, prefix, match=None, decode=True):
        total = 0
        source, revision = self._state
        if not revision:
            return total
        self._fetch(source, revision)

        # Parse git's native archive command.
        # This should be much faster than manually traversing the trees
        # and objects with many subprocess calls.
        tarstream = self._gitcommand(['archive', revision], stream=True)
        tar = tarfile.open(fileobj=tarstream, mode=r'r|')
        relpath = subrelpath(self)
        progress = self.ui.makeprogress(_('archiving (%s)') % relpath,
                                        unit=_('files'))
        progress.update(0)
        for info in tar:
            if info.isdir():
                continue
            if match and not match(info.name):
                continue
            if info.issym():
                data = info.linkname
            else:
                data = tar.extractfile(info).read()
            archiver.addfile(prefix + self._path + '/' + info.name,
                             info.mode, info.issym(), data)
            total += 1
            progress.increment()
        progress.complete()
        return total


    @annotatesubrepoerror
    def cat(self, match, fm, fntemplate, prefix, **opts):
        rev = self._state[1]
        if match.anypats():
            return 1 #No support for include/exclude yet

        if not match.files():
            return 1

        # TODO: add support for non-plain formatter (see cmdutil.cat())
        for f in match.files():
            output = self._gitcommand(["show", "%s:%s" % (rev, f)])
            fp = cmdutil.makefileobj(self._ctx, fntemplate,
                                     pathname=self.wvfs.reljoin(prefix, f))
            fp.write(output)
            fp.close()
        return 0


    @annotatesubrepoerror
    def status(self, rev2, **opts):
        rev1 = self._state[1]
        if self._gitmissing() or not rev1:
            # if the repo is missing, return no results
            return scmutil.status([], [], [], [], [], [], [])
        modified, added, removed = [], [], []
        self._gitupdatestat()
        if rev2:
            command = ['diff-tree', '--no-renames', '-r', rev1, rev2]
        else:
            command = ['diff-index', '--no-renames', rev1]
        out = self._gitcommand(command)
        for line in out.split('\n'):
            tab = line.find('\t')
            if tab == -1:
                continue
            status, f = line[tab - 1:tab], line[tab + 1:]
            if status == 'M':
                modified.append(f)
            elif status == 'A':
                added.append(f)
            elif status == 'D':
                removed.append(f)

        deleted, unknown, ignored, clean = [], [], [], []

        command = ['status', '--porcelain', '-z']
        if opts.get(r'unknown'):
            command += ['--untracked-files=all']
        if opts.get(r'ignored'):
            command += ['--ignored']
        out = self._gitcommand(command)

        changedfiles = set()
        changedfiles.update(modified)
        changedfiles.update(added)
        changedfiles.update(removed)
        for line in out.split('\0'):
            if not line:
                continue
            st = line[0:2]
            #moves and copies show 2 files on one line
            if line.find('\0') >= 0:
                filename1, filename2 = line[3:].split('\0')
            else:
                filename1 = line[3:]
                filename2 = None

            changedfiles.add(filename1)
            if filename2:
                changedfiles.add(filename2)

            if st == '??':
                unknown.append(filename1)
            elif st == '!!':
                ignored.append(filename1)

        if opts.get(r'clean'):
            out = self._gitcommand(['ls-files'])
            for f in out.split('\n'):
                if not f in changedfiles:
                    clean.append(f)

        return scmutil.status(modified, added, removed, deleted,
                              unknown, ignored, clean)

    @annotatesubrepoerror
    def diff(self, ui, diffopts, node2, match, prefix, **opts):
        node1 = self._state[1]
        cmd = ['diff', '--no-renames']
        if opts[r'stat']:
            cmd.append('--stat')
        else:
            # for Git, this also implies '-p'
            cmd.append('-U%d' % diffopts.context)

        gitprefix = self.wvfs.reljoin(prefix, self._path)

        if diffopts.noprefix:
            cmd.extend(['--src-prefix=%s/' % gitprefix,
                        '--dst-prefix=%s/' % gitprefix])
        else:
            cmd.extend(['--src-prefix=a/%s/' % gitprefix,
                        '--dst-prefix=b/%s/' % gitprefix])

        if diffopts.ignorews:
            cmd.append('--ignore-all-space')
        if diffopts.ignorewsamount:
            cmd.append('--ignore-space-change')
        if self._gitversion(self._gitcommand(['--version'])) >= (1, 8, 4) \
                and diffopts.ignoreblanklines:
            cmd.append('--ignore-blank-lines')

        cmd.append(node1)
        if node2:
            cmd.append(node2)

        output = ""
        if match.always():
            output += self._gitcommand(cmd) + '\n'
        else:
            st = self.status(node2)[:3]
            files = [f for sublist in st for f in sublist]
            for f in files:
                if match(f):
                    output += self._gitcommand(cmd + ['--', f]) + '\n'

        if output.strip():
            ui.write(output)

    @annotatesubrepoerror
    def revert(self, substate, *pats, **opts):
        self.ui.status(_('reverting subrepo %s\n') % substate[0])
        if not opts.get(r'no_backup'):
            status = self.status(None)
            names = status.modified
            for name in names:
                bakname = scmutil.origpath(self.ui, self._subparent, name)
                self.ui.note(_('saving current version of %s as %s\n') %
                        (name, bakname))
                self.wvfs.rename(name, bakname)

        if not opts.get(r'dry_run'):
            self.get(substate, overwrite=True)
        return []

    def shortid(self, revid):
        return revid[:7]

types = {
    'hg': hgsubrepo,
    'svn': svnsubrepo,
    'git': gitsubrepo,
    }
