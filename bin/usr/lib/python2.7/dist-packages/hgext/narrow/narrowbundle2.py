# narrowbundle2.py - bundle2 extensions for narrow repository support
#
# Copyright 2017 Google, Inc.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import errno
import struct

from mercurial.i18n import _
from mercurial.node import (
    bin,
    nullid,
)
from mercurial import (
    bundle2,
    changegroup,
    error,
    exchange,
    extensions,
    narrowspec,
    repair,
    repository,
    util,
    wireprototypes,
)
from mercurial.utils import (
    stringutil,
)

NARROWCAP = 'narrow'
_NARROWACL_SECTION = 'narrowhgacl'
_CHANGESPECPART = NARROWCAP + ':changespec'
_SPECPART = NARROWCAP + ':spec'
_SPECPART_INCLUDE = 'include'
_SPECPART_EXCLUDE = 'exclude'
_KILLNODESIGNAL = 'KILL'
_DONESIGNAL = 'DONE'
_ELIDEDCSHEADER = '>20s20s20sl' # cset id, p1, p2, len(text)
_ELIDEDMFHEADER = '>20s20s20s20sl' # manifest id, p1, p2, link id, len(text)
_CSHEADERSIZE = struct.calcsize(_ELIDEDCSHEADER)
_MFHEADERSIZE = struct.calcsize(_ELIDEDMFHEADER)

# When advertising capabilities, always include narrow clone support.
def getrepocaps_narrow(orig, repo, **kwargs):
    caps = orig(repo, **kwargs)
    caps[NARROWCAP] = ['v0']
    return caps

# Serve a changegroup for a client with a narrow clone.
def getbundlechangegrouppart_narrow(bundler, repo, source,
                                    bundlecaps=None, b2caps=None, heads=None,
                                    common=None, **kwargs):
    assert repo.ui.configbool('experimental', 'narrowservebrokenellipses')

    cgversions = b2caps.get('changegroup')
    if cgversions:  # 3.1 and 3.2 ship with an empty value
        cgversions = [v for v in cgversions
                      if v in changegroup.supportedoutgoingversions(repo)]
        if not cgversions:
            raise ValueError(_('no common changegroup version'))
        version = max(cgversions)
    else:
        raise ValueError(_("server does not advertise changegroup version,"
                           " can't negotiate support for ellipsis nodes"))

    include = sorted(filter(bool, kwargs.get(r'includepats', [])))
    exclude = sorted(filter(bool, kwargs.get(r'excludepats', [])))
    newmatch = narrowspec.match(repo.root, include=include, exclude=exclude)

    depth = kwargs.get(r'depth', None)
    if depth is not None:
        depth = int(depth)
        if depth < 1:
            raise error.Abort(_('depth must be positive, got %d') % depth)

    heads = set(heads or repo.heads())
    common = set(common or [nullid])
    oldinclude = sorted(filter(bool, kwargs.get(r'oldincludepats', [])))
    oldexclude = sorted(filter(bool, kwargs.get(r'oldexcludepats', [])))
    known = {bin(n) for n in kwargs.get(r'known', [])}
    if known and (oldinclude != include or oldexclude != exclude):
        # Steps:
        # 1. Send kill for "$known & ::common"
        #
        # 2. Send changegroup for ::common
        #
        # 3. Proceed.
        #
        # In the future, we can send kills for only the specific
        # nodes we know should go away or change shape, and then
        # send a data stream that tells the client something like this:
        #
        # a) apply this changegroup
        # b) apply nodes XXX, YYY, ZZZ that you already have
        # c) goto a
        #
        # until they've built up the full new state.
        # Convert to revnums and intersect with "common". The client should
        # have made it a subset of "common" already, but let's be safe.
        known = set(repo.revs("%ln & ::%ln", known, common))
        # TODO: we could send only roots() of this set, and the
        # list of nodes in common, and the client could work out
        # what to strip, instead of us explicitly sending every
        # single node.
        deadrevs = known
        def genkills():
            for r in deadrevs:
                yield _KILLNODESIGNAL
                yield repo.changelog.node(r)
            yield _DONESIGNAL
        bundler.newpart(_CHANGESPECPART, data=genkills())
        newvisit, newfull, newellipsis = exchange._computeellipsis(
            repo, set(), common, known, newmatch)
        if newvisit:
            packer = changegroup.getbundler(version, repo,
                                            matcher=newmatch,
                                            ellipses=True,
                                            shallow=depth is not None,
                                            ellipsisroots=newellipsis,
                                            fullnodes=newfull)
            cgdata = packer.generate(common, newvisit, False, 'narrow_widen')

            part = bundler.newpart('changegroup', data=cgdata)
            part.addparam('version', version)
            if 'treemanifest' in repo.requirements:
                part.addparam('treemanifest', '1')

    visitnodes, relevant_nodes, ellipsisroots = exchange._computeellipsis(
        repo, common, heads, set(), newmatch, depth=depth)

    repo.ui.debug('Found %d relevant revs\n' % len(relevant_nodes))
    if visitnodes:
        packer = changegroup.getbundler(version, repo,
                                        matcher=newmatch,
                                        ellipses=True,
                                        shallow=depth is not None,
                                        ellipsisroots=ellipsisroots,
                                        fullnodes=relevant_nodes)
        cgdata = packer.generate(common, visitnodes, False, 'narrow_widen')

        part = bundler.newpart('changegroup', data=cgdata)
        part.addparam('version', version)
        if 'treemanifest' in repo.requirements:
            part.addparam('treemanifest', '1')

@bundle2.parthandler(_SPECPART, (_SPECPART_INCLUDE, _SPECPART_EXCLUDE))
def _handlechangespec_2(op, inpart):
    includepats = set(inpart.params.get(_SPECPART_INCLUDE, '').splitlines())
    excludepats = set(inpart.params.get(_SPECPART_EXCLUDE, '').splitlines())
    narrowspec.validatepatterns(includepats)
    narrowspec.validatepatterns(excludepats)

    if not repository.NARROW_REQUIREMENT in op.repo.requirements:
        op.repo.requirements.add(repository.NARROW_REQUIREMENT)
        op.repo._writerequirements()
    op.repo.setnarrowpats(includepats, excludepats)

@bundle2.parthandler(_CHANGESPECPART)
def _handlechangespec(op, inpart):
    repo = op.repo
    cl = repo.changelog

    # changesets which need to be stripped entirely. either they're no longer
    # needed in the new narrow spec, or the server is sending a replacement
    # in the changegroup part.
    clkills = set()

    # A changespec part contains all the updates to ellipsis nodes
    # that will happen as a result of widening or narrowing a
    # repo. All the changes that this block encounters are ellipsis
    # nodes or flags to kill an existing ellipsis.
    chunksignal = changegroup.readexactly(inpart, 4)
    while chunksignal != _DONESIGNAL:
        if chunksignal == _KILLNODESIGNAL:
            # a node used to be an ellipsis but isn't anymore
            ck = changegroup.readexactly(inpart, 20)
            if cl.hasnode(ck):
                clkills.add(ck)
        else:
            raise error.Abort(
                _('unexpected changespec node chunk type: %s') % chunksignal)
        chunksignal = changegroup.readexactly(inpart, 4)

    if clkills:
        # preserve bookmarks that repair.strip() would otherwise strip
        bmstore = repo._bookmarks
        class dummybmstore(dict):
            def applychanges(self, repo, tr, changes):
                pass
            def recordchange(self, tr): # legacy version
                pass
        repo._bookmarks = dummybmstore()
        chgrpfile = repair.strip(op.ui, repo, list(clkills), backup=True,
                                 topic='widen')
        repo._bookmarks = bmstore
        if chgrpfile:
            op._widen_uninterr = repo.ui.uninterruptable()
            op._widen_uninterr.__enter__()
            # presence of _widen_bundle attribute activates widen handler later
            op._widen_bundle = chgrpfile
    # Set the new narrowspec if we're widening. The setnewnarrowpats() method
    # will currently always be there when using the core+narrowhg server, but
    # other servers may include a changespec part even when not widening (e.g.
    # because we're deepening a shallow repo).
    if util.safehasattr(repo, 'setnewnarrowpats'):
        repo.setnewnarrowpats()

def handlechangegroup_widen(op, inpart):
    """Changegroup exchange handler which restores temporarily-stripped nodes"""
    # We saved a bundle with stripped node data we must now restore.
    # This approach is based on mercurial/repair.py@6ee26a53c111.
    repo = op.repo
    ui = op.ui

    chgrpfile = op._widen_bundle
    del op._widen_bundle
    vfs = repo.vfs

    ui.note(_("adding branch\n"))
    f = vfs.open(chgrpfile, "rb")
    try:
        gen = exchange.readbundle(ui, f, chgrpfile, vfs)
        if not ui.verbose:
            # silence internal shuffling chatter
            ui.pushbuffer()
        if isinstance(gen, bundle2.unbundle20):
            with repo.transaction('strip') as tr:
                bundle2.processbundle(repo, gen, lambda: tr)
        else:
            gen.apply(repo, 'strip', 'bundle:' + vfs.join(chgrpfile), True)
        if not ui.verbose:
            ui.popbuffer()
    finally:
        f.close()

    # remove undo files
    for undovfs, undofile in repo.undofiles():
        try:
            undovfs.unlink(undofile)
        except OSError as e:
            if e.errno != errno.ENOENT:
                ui.warn(_('error removing %s: %s\n') %
                        (undovfs.join(undofile), stringutil.forcebytestr(e)))

    # Remove partial backup only if there were no exceptions
    op._widen_uninterr.__exit__(None, None, None)
    vfs.unlink(chgrpfile)

def setup():
    """Enable narrow repo support in bundle2-related extension points."""
    extensions.wrapfunction(bundle2, 'getrepocaps', getrepocaps_narrow)

    getbundleargs = wireprototypes.GETBUNDLE_ARGUMENTS

    getbundleargs['narrow'] = 'boolean'
    getbundleargs['depth'] = 'plain'
    getbundleargs['oldincludepats'] = 'csv'
    getbundleargs['oldexcludepats'] = 'csv'
    getbundleargs['includepats'] = 'csv'
    getbundleargs['excludepats'] = 'csv'
    getbundleargs['known'] = 'csv'

    # Extend changegroup serving to handle requests from narrow clients.
    origcgfn = exchange.getbundle2partsmapping['changegroup']
    def wrappedcgfn(*args, **kwargs):
        repo = args[1]
        if repo.ui.has_section(_NARROWACL_SECTION):
            kwargs = exchange.applynarrowacl(repo, kwargs)

        if (kwargs.get(r'narrow', False) and
            repo.ui.configbool('experimental', 'narrowservebrokenellipses')):
            getbundlechangegrouppart_narrow(*args, **kwargs)
        else:
            origcgfn(*args, **kwargs)
    exchange.getbundle2partsmapping['changegroup'] = wrappedcgfn

    # Extend changegroup receiver so client can fixup after widen requests.
    origcghandler = bundle2.parthandlermapping['changegroup']
    def wrappedcghandler(op, inpart):
        origcghandler(op, inpart)
        if util.safehasattr(op, '_widen_bundle'):
            handlechangegroup_widen(op, inpart)
    wrappedcghandler.params = origcghandler.params
    bundle2.parthandlermapping['changegroup'] = wrappedcghandler
