# verify.py - repository integrity checking for Mercurial
#
# Copyright 2006, 2007 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import os

from .i18n import _
from .node import (
    nullid,
    short,
)

from . import (
    error,
    pycompat,
    revlog,
    util,
)

def verify(repo):
    with repo.lock():
        return verifier(repo).verify()

def _normpath(f):
    # under hg < 2.4, convert didn't sanitize paths properly, so a
    # converted repo may contain repeated slashes
    while '//' in f:
        f = f.replace('//', '/')
    return f

class verifier(object):
    def __init__(self, repo):
        self.repo = repo.unfiltered()
        self.ui = repo.ui
        self.match = repo.narrowmatch()
        self.badrevs = set()
        self.errors = 0
        self.warnings = 0
        self.havecl = len(repo.changelog) > 0
        self.havemf = len(repo.manifestlog.getstorage(b'')) > 0
        self.revlogv1 = repo.changelog.version != revlog.REVLOGV0
        self.lrugetctx = util.lrucachefunc(repo.__getitem__)
        self.refersmf = False
        self.fncachewarned = False
        # developer config: verify.skipflags
        self.skipflags = repo.ui.configint('verify', 'skipflags')
        self.warnorphanstorefiles = True

    def warn(self, msg):
        self.ui.warn(msg + "\n")
        self.warnings += 1

    def err(self, linkrev, msg, filename=None):
        if linkrev is not None:
            self.badrevs.add(linkrev)
            linkrev = "%d" % linkrev
        else:
            linkrev = '?'
        msg = "%s: %s" % (linkrev, msg)
        if filename:
            msg = "%s@%s" % (filename, msg)
        self.ui.warn(" " + msg + "\n")
        self.errors += 1

    def exc(self, linkrev, msg, inst, filename=None):
        fmsg = pycompat.bytestr(inst)
        if not fmsg:
            fmsg = pycompat.byterepr(inst)
        self.err(linkrev, "%s: %s" % (msg, fmsg), filename)

    def checklog(self, obj, name, linkrev):
        if not len(obj) and (self.havecl or self.havemf):
            self.err(linkrev, _("empty or missing %s") % name)
            return

        d = obj.checksize()
        if d[0]:
            self.err(None, _("data length off by %d bytes") % d[0], name)
        if d[1]:
            self.err(None, _("index contains %d extra bytes") % d[1], name)

        if obj.version != revlog.REVLOGV0:
            if not self.revlogv1:
                self.warn(_("warning: `%s' uses revlog format 1") % name)
        elif self.revlogv1:
            self.warn(_("warning: `%s' uses revlog format 0") % name)

    def checkentry(self, obj, i, node, seen, linkrevs, f):
        lr = obj.linkrev(obj.rev(node))
        if lr < 0 or (self.havecl and lr not in linkrevs):
            if lr < 0 or lr >= len(self.repo.changelog):
                msg = _("rev %d points to nonexistent changeset %d")
            else:
                msg = _("rev %d points to unexpected changeset %d")
            self.err(None, msg % (i, lr), f)
            if linkrevs:
                if f and len(linkrevs) > 1:
                    try:
                        # attempt to filter down to real linkrevs
                        linkrevs = [l for l in linkrevs
                                    if self.lrugetctx(l)[f].filenode() == node]
                    except Exception:
                        pass
                self.warn(_(" (expected %s)") % " ".join
                          (map(pycompat.bytestr, linkrevs)))
            lr = None # can't be trusted

        try:
            p1, p2 = obj.parents(node)
            if p1 not in seen and p1 != nullid:
                self.err(lr, _("unknown parent 1 %s of %s") %
                    (short(p1), short(node)), f)
            if p2 not in seen and p2 != nullid:
                self.err(lr, _("unknown parent 2 %s of %s") %
                    (short(p2), short(node)), f)
        except Exception as inst:
            self.exc(lr, _("checking parents of %s") % short(node), inst, f)

        if node in seen:
            self.err(lr, _("duplicate revision %d (%d)") % (i, seen[node]), f)
        seen[node] = i
        return lr

    def verify(self):
        repo = self.repo

        ui = repo.ui

        if not repo.url().startswith('file:'):
            raise error.Abort(_("cannot verify bundle or remote repos"))

        if os.path.exists(repo.sjoin("journal")):
            ui.warn(_("abandoned transaction found - run hg recover\n"))

        if ui.verbose or not self.revlogv1:
            ui.status(_("repository uses revlog format %d\n") %
                           (self.revlogv1 and 1 or 0))

        mflinkrevs, filelinkrevs = self._verifychangelog()

        filenodes = self._verifymanifest(mflinkrevs)
        del mflinkrevs

        self._crosscheckfiles(filelinkrevs, filenodes)

        totalfiles, filerevisions = self._verifyfiles(filenodes, filelinkrevs)

        ui.status(_("checked %d changesets with %d changes to %d files\n") %
                       (len(repo.changelog), filerevisions, totalfiles))
        if self.warnings:
            ui.warn(_("%d warnings encountered!\n") % self.warnings)
        if self.fncachewarned:
            ui.warn(_('hint: run "hg debugrebuildfncache" to recover from '
                      'corrupt fncache\n'))
        if self.errors:
            ui.warn(_("%d integrity errors encountered!\n") % self.errors)
            if self.badrevs:
                ui.warn(_("(first damaged changeset appears to be %d)\n")
                        % min(self.badrevs))
            return 1

    def _verifychangelog(self):
        ui = self.ui
        repo = self.repo
        match = self.match
        cl = repo.changelog

        ui.status(_("checking changesets\n"))
        mflinkrevs = {}
        filelinkrevs = {}
        seen = {}
        self.checklog(cl, "changelog", 0)
        progress = ui.makeprogress(_('checking'), unit=_('changesets'),
                                   total=len(repo))
        for i in repo:
            progress.update(i)
            n = cl.node(i)
            self.checkentry(cl, i, n, seen, [i], "changelog")

            try:
                changes = cl.read(n)
                if changes[0] != nullid:
                    mflinkrevs.setdefault(changes[0], []).append(i)
                    self.refersmf = True
                for f in changes[3]:
                    if match(f):
                        filelinkrevs.setdefault(_normpath(f), []).append(i)
            except Exception as inst:
                self.refersmf = True
                self.exc(i, _("unpacking changeset %s") % short(n), inst)
        progress.complete()
        return mflinkrevs, filelinkrevs

    def _verifymanifest(self, mflinkrevs, dir="", storefiles=None,
                        subdirprogress=None):
        repo = self.repo
        ui = self.ui
        match = self.match
        mfl = self.repo.manifestlog
        mf = mfl.getstorage(dir)

        if not dir:
            self.ui.status(_("checking manifests\n"))

        filenodes = {}
        subdirnodes = {}
        seen = {}
        label = "manifest"
        if dir:
            label = dir
            revlogfiles = mf.files()
            storefiles.difference_update(revlogfiles)
            if subdirprogress: # should be true since we're in a subdirectory
                subdirprogress.increment()
        if self.refersmf:
            # Do not check manifest if there are only changelog entries with
            # null manifests.
            self.checklog(mf, label, 0)
        progress = ui.makeprogress(_('checking'), unit=_('manifests'),
                                   total=len(mf))
        for i in mf:
            if not dir:
                progress.update(i)
            n = mf.node(i)
            lr = self.checkentry(mf, i, n, seen, mflinkrevs.get(n, []), label)
            if n in mflinkrevs:
                del mflinkrevs[n]
            elif dir:
                self.err(lr, _("%s not in parent-directory manifest") %
                         short(n), label)
            else:
                self.err(lr, _("%s not in changesets") % short(n), label)

            try:
                mfdelta = mfl.get(dir, n).readdelta(shallow=True)
                for f, fn, fl in mfdelta.iterentries():
                    if not f:
                        self.err(lr, _("entry without name in manifest"))
                    elif f == "/dev/null":  # ignore this in very old repos
                        continue
                    fullpath = dir + _normpath(f)
                    if fl == 't':
                        if not match.visitdir(fullpath):
                            continue
                        subdirnodes.setdefault(fullpath + '/', {}).setdefault(
                            fn, []).append(lr)
                    else:
                        if not match(fullpath):
                            continue
                        filenodes.setdefault(fullpath, {}).setdefault(fn, lr)
            except Exception as inst:
                self.exc(lr, _("reading delta %s") % short(n), inst, label)
        if not dir:
            progress.complete()

        if self.havemf:
            for c, m in sorted([(c, m) for m in mflinkrevs
                        for c in mflinkrevs[m]]):
                if dir:
                    self.err(c, _("parent-directory manifest refers to unknown "
                                  "revision %s") % short(m), label)
                else:
                    self.err(c, _("changeset refers to unknown revision %s") %
                             short(m), label)

        if not dir and subdirnodes:
            self.ui.status(_("checking directory manifests\n"))
            storefiles = set()
            subdirs = set()
            revlogv1 = self.revlogv1
            for f, f2, size in repo.store.datafiles():
                if not f:
                    self.err(None, _("cannot decode filename '%s'") % f2)
                elif (size > 0 or not revlogv1) and f.startswith('meta/'):
                    storefiles.add(_normpath(f))
                    subdirs.add(os.path.dirname(f))
            subdirprogress = ui.makeprogress(_('checking'), unit=_('manifests'),
                                             total=len(subdirs))

        for subdir, linkrevs in subdirnodes.iteritems():
            subdirfilenodes = self._verifymanifest(linkrevs, subdir, storefiles,
                                                   subdirprogress)
            for f, onefilenodes in subdirfilenodes.iteritems():
                filenodes.setdefault(f, {}).update(onefilenodes)

        if not dir and subdirnodes:
            subdirprogress.complete()
            if self.warnorphanstorefiles:
                for f in sorted(storefiles):
                    self.warn(_("warning: orphan data file '%s'") % f)

        return filenodes

    def _crosscheckfiles(self, filelinkrevs, filenodes):
        repo = self.repo
        ui = self.ui
        ui.status(_("crosschecking files in changesets and manifests\n"))

        total = len(filelinkrevs) + len(filenodes)
        progress = ui.makeprogress(_('crosschecking'), unit=_('files'),
                                   total=total)
        if self.havemf:
            for f in sorted(filelinkrevs):
                progress.increment()
                if f not in filenodes:
                    lr = filelinkrevs[f][0]
                    self.err(lr, _("in changeset but not in manifest"), f)

        if self.havecl:
            for f in sorted(filenodes):
                progress.increment()
                if f not in filelinkrevs:
                    try:
                        fl = repo.file(f)
                        lr = min([fl.linkrev(fl.rev(n)) for n in filenodes[f]])
                    except Exception:
                        lr = None
                    self.err(lr, _("in manifest but not in changeset"), f)

        progress.complete()

    def _verifyfiles(self, filenodes, filelinkrevs):
        repo = self.repo
        ui = self.ui
        lrugetctx = self.lrugetctx
        revlogv1 = self.revlogv1
        havemf = self.havemf
        ui.status(_("checking files\n"))

        storefiles = set()
        for f, f2, size in repo.store.datafiles():
            if not f:
                self.err(None, _("cannot decode filename '%s'") % f2)
            elif (size > 0 or not revlogv1) and f.startswith('data/'):
                storefiles.add(_normpath(f))

        state = {
            # TODO this assumes revlog storage for changelog.
            'expectedversion': self.repo.changelog.version & 0xFFFF,
            'skipflags': self.skipflags,
            # experimental config: censor.policy
            'erroroncensored': ui.config('censor', 'policy') == 'abort',
        }

        files = sorted(set(filenodes) | set(filelinkrevs))
        revisions = 0
        progress = ui.makeprogress(_('checking'), unit=_('files'),
                                   total=len(files))
        for i, f in enumerate(files):
            progress.update(i, item=f)
            try:
                linkrevs = filelinkrevs[f]
            except KeyError:
                # in manifest but not in changelog
                linkrevs = []

            if linkrevs:
                lr = linkrevs[0]
            else:
                lr = None

            try:
                fl = repo.file(f)
            except error.StorageError as e:
                self.err(lr, _("broken revlog! (%s)") % e, f)
                continue

            for ff in fl.files():
                try:
                    storefiles.remove(ff)
                except KeyError:
                    if self.warnorphanstorefiles:
                        self.warn(_(" warning: revlog '%s' not in fncache!") %
                                  ff)
                        self.fncachewarned = True

            if not len(fl) and (self.havecl or self.havemf):
                self.err(lr, _("empty or missing %s") % f)
            else:
                # Guard against implementations not setting this.
                state['skipread'] = set()
                for problem in fl.verifyintegrity(state):
                    if problem.node is not None:
                        linkrev = fl.linkrev(fl.rev(problem.node))
                    else:
                        linkrev = None

                    if problem.warning:
                        self.warn(problem.warning)
                    elif problem.error:
                        self.err(linkrev if linkrev is not None else lr,
                                 problem.error, f)
                    else:
                        raise error.ProgrammingError(
                            'problem instance does not set warning or error '
                            'attribute: %s' % problem.msg)

            seen = {}
            for i in fl:
                revisions += 1
                n = fl.node(i)
                lr = self.checkentry(fl, i, n, seen, linkrevs, f)
                if f in filenodes:
                    if havemf and n not in filenodes[f]:
                        self.err(lr, _("%s not in manifests") % (short(n)), f)
                    else:
                        del filenodes[f][n]

                if n in state['skipread']:
                    continue

                # check renames
                try:
                    # This requires resolving fulltext (at least on revlogs). We
                    # may want ``verifyintegrity()`` to pass a set of nodes with
                    # rename metadata as an optimization.
                    rp = fl.renamed(n)
                    if rp:
                        if lr is not None and ui.verbose:
                            ctx = lrugetctx(lr)
                            if not any(rp[0] in pctx for pctx in ctx.parents()):
                                self.warn(_("warning: copy source of '%s' not"
                                            " in parents of %s") % (f, ctx))
                        fl2 = repo.file(rp[0])
                        if not len(fl2):
                            self.err(lr, _("empty or missing copy source "
                                     "revlog %s:%s") % (rp[0], short(rp[1])), f)
                        elif rp[1] == nullid:
                            ui.note(_("warning: %s@%s: copy source"
                                      " revision is nullid %s:%s\n")
                                % (f, lr, rp[0], short(rp[1])))
                        else:
                            fl2.rev(rp[1])
                except Exception as inst:
                    self.exc(lr, _("checking rename of %s") % short(n), inst, f)

            # cross-check
            if f in filenodes:
                fns = [(v, k) for k, v in filenodes[f].iteritems()]
                for lr, node in sorted(fns):
                    self.err(lr, _("manifest refers to unknown revision %s") %
                             short(node), f)
        progress.complete()

        if self.warnorphanstorefiles:
            for f in sorted(storefiles):
                self.warn(_("warning: orphan data file '%s'") % f)

        return len(files), revisions
