# exchange.py - utility to exchange data between repos.
#
# Copyright 2005-2007 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import collections
import hashlib

from .i18n import _
from .node import (
    bin,
    hex,
    nullid,
    nullrev,
)
from .thirdparty import (
    attr,
)
from . import (
    bookmarks as bookmod,
    bundle2,
    changegroup,
    discovery,
    error,
    exchangev2,
    lock as lockmod,
    logexchange,
    narrowspec,
    obsolete,
    phases,
    pushkey,
    pycompat,
    repository,
    scmutil,
    sslutil,
    streamclone,
    url as urlmod,
    util,
)
from .utils import (
    stringutil,
)

urlerr = util.urlerr
urlreq = util.urlreq

_NARROWACL_SECTION = 'narrowhgacl'

# Maps bundle version human names to changegroup versions.
_bundlespeccgversions = {'v1': '01',
                         'v2': '02',
                         'packed1': 's1',
                         'bundle2': '02', #legacy
                        }

# Maps bundle version with content opts to choose which part to bundle
_bundlespeccontentopts = {
    'v1': {
        'changegroup': True,
        'cg.version': '01',
        'obsolescence': False,
        'phases': False,
        'tagsfnodescache': False,
        'revbranchcache': False
    },
    'v2': {
        'changegroup': True,
        'cg.version': '02',
        'obsolescence': False,
        'phases': False,
        'tagsfnodescache': True,
        'revbranchcache': True
    },
    'packed1' : {
        'cg.version': 's1'
    }
}
_bundlespeccontentopts['bundle2'] = _bundlespeccontentopts['v2']

_bundlespecvariants = {"streamv2": {"changegroup": False, "streamv2": True,
                                    "tagsfnodescache": False,
                                    "revbranchcache": False}}

# Compression engines allowed in version 1. THIS SHOULD NEVER CHANGE.
_bundlespecv1compengines = {'gzip', 'bzip2', 'none'}

@attr.s
class bundlespec(object):
    compression = attr.ib()
    wirecompression = attr.ib()
    version = attr.ib()
    wireversion = attr.ib()
    params = attr.ib()
    contentopts = attr.ib()

def parsebundlespec(repo, spec, strict=True):
    """Parse a bundle string specification into parts.

    Bundle specifications denote a well-defined bundle/exchange format.
    The content of a given specification should not change over time in
    order to ensure that bundles produced by a newer version of Mercurial are
    readable from an older version.

    The string currently has the form:

       <compression>-<type>[;<parameter0>[;<parameter1>]]

    Where <compression> is one of the supported compression formats
    and <type> is (currently) a version string. A ";" can follow the type and
    all text afterwards is interpreted as URI encoded, ";" delimited key=value
    pairs.

    If ``strict`` is True (the default) <compression> is required. Otherwise,
    it is optional.

    Returns a bundlespec object of (compression, version, parameters).
    Compression will be ``None`` if not in strict mode and a compression isn't
    defined.

    An ``InvalidBundleSpecification`` is raised when the specification is
    not syntactically well formed.

    An ``UnsupportedBundleSpecification`` is raised when the compression or
    bundle type/version is not recognized.

    Note: this function will likely eventually return a more complex data
    structure, including bundle2 part information.
    """
    def parseparams(s):
        if ';' not in s:
            return s, {}

        params = {}
        version, paramstr = s.split(';', 1)

        for p in paramstr.split(';'):
            if '=' not in p:
                raise error.InvalidBundleSpecification(
                    _('invalid bundle specification: '
                      'missing "=" in parameter: %s') % p)

            key, value = p.split('=', 1)
            key = urlreq.unquote(key)
            value = urlreq.unquote(value)
            params[key] = value

        return version, params


    if strict and '-' not in spec:
        raise error.InvalidBundleSpecification(
                _('invalid bundle specification; '
                  'must be prefixed with compression: %s') % spec)

    if '-' in spec:
        compression, version = spec.split('-', 1)

        if compression not in util.compengines.supportedbundlenames:
            raise error.UnsupportedBundleSpecification(
                    _('%s compression is not supported') % compression)

        version, params = parseparams(version)

        if version not in _bundlespeccgversions:
            raise error.UnsupportedBundleSpecification(
                    _('%s is not a recognized bundle version') % version)
    else:
        # Value could be just the compression or just the version, in which
        # case some defaults are assumed (but only when not in strict mode).
        assert not strict

        spec, params = parseparams(spec)

        if spec in util.compengines.supportedbundlenames:
            compression = spec
            version = 'v1'
            # Generaldelta repos require v2.
            if 'generaldelta' in repo.requirements:
                version = 'v2'
            # Modern compression engines require v2.
            if compression not in _bundlespecv1compengines:
                version = 'v2'
        elif spec in _bundlespeccgversions:
            if spec == 'packed1':
                compression = 'none'
            else:
                compression = 'bzip2'
            version = spec
        else:
            raise error.UnsupportedBundleSpecification(
                    _('%s is not a recognized bundle specification') % spec)

    # Bundle version 1 only supports a known set of compression engines.
    if version == 'v1' and compression not in _bundlespecv1compengines:
        raise error.UnsupportedBundleSpecification(
            _('compression engine %s is not supported on v1 bundles') %
            compression)

    # The specification for packed1 can optionally declare the data formats
    # required to apply it. If we see this metadata, compare against what the
    # repo supports and error if the bundle isn't compatible.
    if version == 'packed1' and 'requirements' in params:
        requirements = set(params['requirements'].split(','))
        missingreqs = requirements - repo.supportedformats
        if missingreqs:
            raise error.UnsupportedBundleSpecification(
                    _('missing support for repository features: %s') %
                      ', '.join(sorted(missingreqs)))

    # Compute contentopts based on the version
    contentopts = _bundlespeccontentopts.get(version, {}).copy()

    # Process the variants
    if "stream" in params and params["stream"] == "v2":
        variant = _bundlespecvariants["streamv2"]
        contentopts.update(variant)

    engine = util.compengines.forbundlename(compression)
    compression, wirecompression = engine.bundletype()
    wireversion = _bundlespeccgversions[version]

    return bundlespec(compression, wirecompression, version, wireversion,
                      params, contentopts)

def readbundle(ui, fh, fname, vfs=None):
    header = changegroup.readexactly(fh, 4)

    alg = None
    if not fname:
        fname = "stream"
        if not header.startswith('HG') and header.startswith('\0'):
            fh = changegroup.headerlessfixup(fh, header)
            header = "HG10"
            alg = 'UN'
    elif vfs:
        fname = vfs.join(fname)

    magic, version = header[0:2], header[2:4]

    if magic != 'HG':
        raise error.Abort(_('%s: not a Mercurial bundle') % fname)
    if version == '10':
        if alg is None:
            alg = changegroup.readexactly(fh, 2)
        return changegroup.cg1unpacker(fh, alg)
    elif version.startswith('2'):
        return bundle2.getunbundler(ui, fh, magicstring=magic + version)
    elif version == 'S1':
        return streamclone.streamcloneapplier(fh)
    else:
        raise error.Abort(_('%s: unknown bundle version %s') % (fname, version))

def getbundlespec(ui, fh):
    """Infer the bundlespec from a bundle file handle.

    The input file handle is seeked and the original seek position is not
    restored.
    """
    def speccompression(alg):
        try:
            return util.compengines.forbundletype(alg).bundletype()[0]
        except KeyError:
            return None

    b = readbundle(ui, fh, None)
    if isinstance(b, changegroup.cg1unpacker):
        alg = b._type
        if alg == '_truncatedBZ':
            alg = 'BZ'
        comp = speccompression(alg)
        if not comp:
            raise error.Abort(_('unknown compression algorithm: %s') % alg)
        return '%s-v1' % comp
    elif isinstance(b, bundle2.unbundle20):
        if 'Compression' in b.params:
            comp = speccompression(b.params['Compression'])
            if not comp:
                raise error.Abort(_('unknown compression algorithm: %s') % comp)
        else:
            comp = 'none'

        version = None
        for part in b.iterparts():
            if part.type == 'changegroup':
                version = part.params['version']
                if version in ('01', '02'):
                    version = 'v2'
                else:
                    raise error.Abort(_('changegroup version %s does not have '
                                        'a known bundlespec') % version,
                                      hint=_('try upgrading your Mercurial '
                                              'client'))
            elif part.type == 'stream2' and version is None:
                # A stream2 part requires to be part of a v2 bundle
                version = "v2"
                requirements = urlreq.unquote(part.params['requirements'])
                splitted = requirements.split()
                params = bundle2._formatrequirementsparams(splitted)
                return 'none-v2;stream=v2;%s' % params

        if not version:
            raise error.Abort(_('could not identify changegroup version in '
                                'bundle'))

        return '%s-%s' % (comp, version)
    elif isinstance(b, streamclone.streamcloneapplier):
        requirements = streamclone.readbundle1header(fh)[2]
        formatted = bundle2._formatrequirementsparams(requirements)
        return 'none-packed1;%s' % formatted
    else:
        raise error.Abort(_('unknown bundle type: %s') % b)

def _computeoutgoing(repo, heads, common):
    """Computes which revs are outgoing given a set of common
    and a set of heads.

    This is a separate function so extensions can have access to
    the logic.

    Returns a discovery.outgoing object.
    """
    cl = repo.changelog
    if common:
        hasnode = cl.hasnode
        common = [n for n in common if hasnode(n)]
    else:
        common = [nullid]
    if not heads:
        heads = cl.heads()
    return discovery.outgoing(repo, common, heads)

def _forcebundle1(op):
    """return true if a pull/push must use bundle1

    This function is used to allow testing of the older bundle version"""
    ui = op.repo.ui
    # The goal is this config is to allow developer to choose the bundle
    # version used during exchanged. This is especially handy during test.
    # Value is a list of bundle version to be picked from, highest version
    # should be used.
    #
    # developer config: devel.legacy.exchange
    exchange = ui.configlist('devel', 'legacy.exchange')
    forcebundle1 = 'bundle2' not in exchange and 'bundle1' in exchange
    return forcebundle1 or not op.remote.capable('bundle2')

class pushoperation(object):
    """A object that represent a single push operation

    Its purpose is to carry push related state and very common operations.

    A new pushoperation should be created at the beginning of each push and
    discarded afterward.
    """

    def __init__(self, repo, remote, force=False, revs=None, newbranch=False,
                 bookmarks=(), pushvars=None):
        # repo we push from
        self.repo = repo
        self.ui = repo.ui
        # repo we push to
        self.remote = remote
        # force option provided
        self.force = force
        # revs to be pushed (None is "all")
        self.revs = revs
        # bookmark explicitly pushed
        self.bookmarks = bookmarks
        # allow push of new branch
        self.newbranch = newbranch
        # step already performed
        # (used to check what steps have been already performed through bundle2)
        self.stepsdone = set()
        # Integer version of the changegroup push result
        # - None means nothing to push
        # - 0 means HTTP error
        # - 1 means we pushed and remote head count is unchanged *or*
        #   we have outgoing changesets but refused to push
        # - other values as described by addchangegroup()
        self.cgresult = None
        # Boolean value for the bookmark push
        self.bkresult = None
        # discover.outgoing object (contains common and outgoing data)
        self.outgoing = None
        # all remote topological heads before the push
        self.remoteheads = None
        # Details of the remote branch pre and post push
        #
        # mapping: {'branch': ([remoteheads],
        #                      [newheads],
        #                      [unsyncedheads],
        #                      [discardedheads])}
        # - branch: the branch name
        # - remoteheads: the list of remote heads known locally
        #                None if the branch is new
        # - newheads: the new remote heads (known locally) with outgoing pushed
        # - unsyncedheads: the list of remote heads unknown locally.
        # - discardedheads: the list of remote heads made obsolete by the push
        self.pushbranchmap = None
        # testable as a boolean indicating if any nodes are missing locally.
        self.incoming = None
        # summary of the remote phase situation
        self.remotephases = None
        # phases changes that must be pushed along side the changesets
        self.outdatedphases = None
        # phases changes that must be pushed if changeset push fails
        self.fallbackoutdatedphases = None
        # outgoing obsmarkers
        self.outobsmarkers = set()
        # outgoing bookmarks
        self.outbookmarks = []
        # transaction manager
        self.trmanager = None
        # map { pushkey partid -> callback handling failure}
        # used to handle exception from mandatory pushkey part failure
        self.pkfailcb = {}
        # an iterable of pushvars or None
        self.pushvars = pushvars

    @util.propertycache
    def futureheads(self):
        """future remote heads if the changeset push succeeds"""
        return self.outgoing.missingheads

    @util.propertycache
    def fallbackheads(self):
        """future remote heads if the changeset push fails"""
        if self.revs is None:
            # not target to push, all common are relevant
            return self.outgoing.commonheads
        unfi = self.repo.unfiltered()
        # I want cheads = heads(::missingheads and ::commonheads)
        # (missingheads is revs with secret changeset filtered out)
        #
        # This can be expressed as:
        #     cheads = ( (missingheads and ::commonheads)
        #              + (commonheads and ::missingheads))"
        #              )
        #
        # while trying to push we already computed the following:
        #     common = (::commonheads)
        #     missing = ((commonheads::missingheads) - commonheads)
        #
        # We can pick:
        # * missingheads part of common (::commonheads)
        common = self.outgoing.common
        nm = self.repo.changelog.nodemap
        cheads = [node for node in self.revs if nm[node] in common]
        # and
        # * commonheads parents on missing
        revset = unfi.set('%ln and parents(roots(%ln))',
                         self.outgoing.commonheads,
                         self.outgoing.missing)
        cheads.extend(c.node() for c in revset)
        return cheads

    @property
    def commonheads(self):
        """set of all common heads after changeset bundle push"""
        if self.cgresult:
            return self.futureheads
        else:
            return self.fallbackheads

# mapping of message used when pushing bookmark
bookmsgmap = {'update': (_("updating bookmark %s\n"),
                         _('updating bookmark %s failed!\n')),
              'export': (_("exporting bookmark %s\n"),
                         _('exporting bookmark %s failed!\n')),
              'delete': (_("deleting remote bookmark %s\n"),
                         _('deleting remote bookmark %s failed!\n')),
              }


def push(repo, remote, force=False, revs=None, newbranch=False, bookmarks=(),
         opargs=None):
    '''Push outgoing changesets (limited by revs) from a local
    repository to remote. Return an integer:
      - None means nothing to push
      - 0 means HTTP error
      - 1 means we pushed and remote head count is unchanged *or*
        we have outgoing changesets but refused to push
      - other values as described by addchangegroup()
    '''
    if opargs is None:
        opargs = {}
    pushop = pushoperation(repo, remote, force, revs, newbranch, bookmarks,
                           **pycompat.strkwargs(opargs))
    if pushop.remote.local():
        missing = (set(pushop.repo.requirements)
                   - pushop.remote.local().supported)
        if missing:
            msg = _("required features are not"
                    " supported in the destination:"
                    " %s") % (', '.join(sorted(missing)))
            raise error.Abort(msg)

    if not pushop.remote.canpush():
        raise error.Abort(_("destination does not support push"))

    if not pushop.remote.capable('unbundle'):
        raise error.Abort(_('cannot push: destination does not support the '
                            'unbundle wire protocol command'))

    # get lock as we might write phase data
    wlock = lock = None
    try:
        # bundle2 push may receive a reply bundle touching bookmarks or other
        # things requiring the wlock. Take it now to ensure proper ordering.
        maypushback = pushop.ui.configbool('experimental', 'bundle2.pushback')
        if (not _forcebundle1(pushop)) and maypushback:
            wlock = pushop.repo.wlock()
        lock = pushop.repo.lock()
        pushop.trmanager = transactionmanager(pushop.repo,
                                              'push-response',
                                              pushop.remote.url())
    except error.LockUnavailable as err:
        # source repo cannot be locked.
        # We do not abort the push, but just disable the local phase
        # synchronisation.
        msg = ('cannot lock source repository: %s\n'
               % stringutil.forcebytestr(err))
        pushop.ui.debug(msg)

    with wlock or util.nullcontextmanager(), \
            lock or util.nullcontextmanager(), \
            pushop.trmanager or util.nullcontextmanager():
        pushop.repo.checkpush(pushop)
        _pushdiscovery(pushop)
        if not _forcebundle1(pushop):
            _pushbundle2(pushop)
        _pushchangeset(pushop)
        _pushsyncphase(pushop)
        _pushobsolete(pushop)
        _pushbookmark(pushop)

    if repo.ui.configbool('experimental', 'remotenames'):
        logexchange.pullremotenames(repo, remote)

    return pushop

# list of steps to perform discovery before push
pushdiscoveryorder = []

# Mapping between step name and function
#
# This exists to help extensions wrap steps if necessary
pushdiscoverymapping = {}

def pushdiscovery(stepname):
    """decorator for function performing discovery before push

    The function is added to the step -> function mapping and appended to the
    list of steps.  Beware that decorated function will be added in order (this
    may matter).

    You can only use this decorator for a new step, if you want to wrap a step
    from an extension, change the pushdiscovery dictionary directly."""
    def dec(func):
        assert stepname not in pushdiscoverymapping
        pushdiscoverymapping[stepname] = func
        pushdiscoveryorder.append(stepname)
        return func
    return dec

def _pushdiscovery(pushop):
    """Run all discovery steps"""
    for stepname in pushdiscoveryorder:
        step = pushdiscoverymapping[stepname]
        step(pushop)

@pushdiscovery('changeset')
def _pushdiscoverychangeset(pushop):
    """discover the changeset that need to be pushed"""
    fci = discovery.findcommonincoming
    if pushop.revs:
        commoninc = fci(pushop.repo, pushop.remote, force=pushop.force,
                        ancestorsof=pushop.revs)
    else:
        commoninc = fci(pushop.repo, pushop.remote, force=pushop.force)
    common, inc, remoteheads = commoninc
    fco = discovery.findcommonoutgoing
    outgoing = fco(pushop.repo, pushop.remote, onlyheads=pushop.revs,
                   commoninc=commoninc, force=pushop.force)
    pushop.outgoing = outgoing
    pushop.remoteheads = remoteheads
    pushop.incoming = inc

@pushdiscovery('phase')
def _pushdiscoveryphase(pushop):
    """discover the phase that needs to be pushed

    (computed for both success and failure case for changesets push)"""
    outgoing = pushop.outgoing
    unfi = pushop.repo.unfiltered()
    remotephases = listkeys(pushop.remote, 'phases')

    if (pushop.ui.configbool('ui', '_usedassubrepo')
        and remotephases    # server supports phases
        and not pushop.outgoing.missing # no changesets to be pushed
        and remotephases.get('publishing', False)):
        # When:
        # - this is a subrepo push
        # - and remote support phase
        # - and no changeset are to be pushed
        # - and remote is publishing
        # We may be in issue 3781 case!
        # We drop the possible phase synchronisation done by
        # courtesy to publish changesets possibly locally draft
        # on the remote.
        pushop.outdatedphases = []
        pushop.fallbackoutdatedphases = []
        return

    pushop.remotephases = phases.remotephasessummary(pushop.repo,
                                                     pushop.fallbackheads,
                                                     remotephases)
    droots = pushop.remotephases.draftroots

    extracond = ''
    if not pushop.remotephases.publishing:
        extracond = ' and public()'
    revset = 'heads((%%ln::%%ln) %s)' % extracond
    # Get the list of all revs draft on remote by public here.
    # XXX Beware that revset break if droots is not strictly
    # XXX root we may want to ensure it is but it is costly
    fallback = list(unfi.set(revset, droots, pushop.fallbackheads))
    if not outgoing.missing:
        future = fallback
    else:
        # adds changeset we are going to push as draft
        #
        # should not be necessary for publishing server, but because of an
        # issue fixed in xxxxx we have to do it anyway.
        fdroots = list(unfi.set('roots(%ln  + %ln::)',
                       outgoing.missing, droots))
        fdroots = [f.node() for f in fdroots]
        future = list(unfi.set(revset, fdroots, pushop.futureheads))
    pushop.outdatedphases = future
    pushop.fallbackoutdatedphases = fallback

@pushdiscovery('obsmarker')
def _pushdiscoveryobsmarkers(pushop):
    if not obsolete.isenabled(pushop.repo, obsolete.exchangeopt):
        return

    if not pushop.repo.obsstore:
        return

    if 'obsolete' not in listkeys(pushop.remote, 'namespaces'):
        return

    repo = pushop.repo
    # very naive computation, that can be quite expensive on big repo.
    # However: evolution is currently slow on them anyway.
    nodes = (c.node() for c in repo.set('::%ln', pushop.futureheads))
    pushop.outobsmarkers = pushop.repo.obsstore.relevantmarkers(nodes)

@pushdiscovery('bookmarks')
def _pushdiscoverybookmarks(pushop):
    ui = pushop.ui
    repo = pushop.repo.unfiltered()
    remote = pushop.remote
    ui.debug("checking for updated bookmarks\n")
    ancestors = ()
    if pushop.revs:
        revnums = pycompat.maplist(repo.changelog.rev, pushop.revs)
        ancestors = repo.changelog.ancestors(revnums, inclusive=True)

    remotebookmark = listkeys(remote, 'bookmarks')

    explicit = set([repo._bookmarks.expandname(bookmark)
                    for bookmark in pushop.bookmarks])

    remotebookmark = bookmod.unhexlifybookmarks(remotebookmark)
    comp = bookmod.comparebookmarks(repo, repo._bookmarks, remotebookmark)

    def safehex(x):
        if x is None:
            return x
        return hex(x)

    def hexifycompbookmarks(bookmarks):
        return [(b, safehex(scid), safehex(dcid))
                for (b, scid, dcid) in bookmarks]

    comp = [hexifycompbookmarks(marks) for marks in comp]
    return _processcompared(pushop, ancestors, explicit, remotebookmark, comp)

def _processcompared(pushop, pushed, explicit, remotebms, comp):
    """take decision on bookmark to pull from the remote bookmark

    Exist to help extensions who want to alter this behavior.
    """
    addsrc, adddst, advsrc, advdst, diverge, differ, invalid, same = comp

    repo = pushop.repo

    for b, scid, dcid in advsrc:
        if b in explicit:
            explicit.remove(b)
        if not pushed or repo[scid].rev() in pushed:
            pushop.outbookmarks.append((b, dcid, scid))
    # search added bookmark
    for b, scid, dcid in addsrc:
        if b in explicit:
            explicit.remove(b)
            pushop.outbookmarks.append((b, '', scid))
    # search for overwritten bookmark
    for b, scid, dcid in list(advdst) + list(diverge) + list(differ):
        if b in explicit:
            explicit.remove(b)
            pushop.outbookmarks.append((b, dcid, scid))
    # search for bookmark to delete
    for b, scid, dcid in adddst:
        if b in explicit:
            explicit.remove(b)
            # treat as "deleted locally"
            pushop.outbookmarks.append((b, dcid, ''))
    # identical bookmarks shouldn't get reported
    for b, scid, dcid in same:
        if b in explicit:
            explicit.remove(b)

    if explicit:
        explicit = sorted(explicit)
        # we should probably list all of them
        pushop.ui.warn(_('bookmark %s does not exist on the local '
                         'or remote repository!\n') % explicit[0])
        pushop.bkresult = 2

    pushop.outbookmarks.sort()

def _pushcheckoutgoing(pushop):
    outgoing = pushop.outgoing
    unfi = pushop.repo.unfiltered()
    if not outgoing.missing:
        # nothing to push
        scmutil.nochangesfound(unfi.ui, unfi, outgoing.excluded)
        return False
    # something to push
    if not pushop.force:
        # if repo.obsstore == False --> no obsolete
        # then, save the iteration
        if unfi.obsstore:
            # this message are here for 80 char limit reason
            mso = _("push includes obsolete changeset: %s!")
            mspd = _("push includes phase-divergent changeset: %s!")
            mscd = _("push includes content-divergent changeset: %s!")
            mst = {"orphan": _("push includes orphan changeset: %s!"),
                   "phase-divergent": mspd,
                   "content-divergent": mscd}
            # If we are to push if there is at least one
            # obsolete or unstable changeset in missing, at
            # least one of the missinghead will be obsolete or
            # unstable. So checking heads only is ok
            for node in outgoing.missingheads:
                ctx = unfi[node]
                if ctx.obsolete():
                    raise error.Abort(mso % ctx)
                elif ctx.isunstable():
                    # TODO print more than one instability in the abort
                    # message
                    raise error.Abort(mst[ctx.instabilities()[0]] % ctx)

        discovery.checkheads(pushop)
    return True

# List of names of steps to perform for an outgoing bundle2, order matters.
b2partsgenorder = []

# Mapping between step name and function
#
# This exists to help extensions wrap steps if necessary
b2partsgenmapping = {}

def b2partsgenerator(stepname, idx=None):
    """decorator for function generating bundle2 part

    The function is added to the step -> function mapping and appended to the
    list of steps.  Beware that decorated functions will be added in order
    (this may matter).

    You can only use this decorator for new steps, if you want to wrap a step
    from an extension, attack the b2partsgenmapping dictionary directly."""
    def dec(func):
        assert stepname not in b2partsgenmapping
        b2partsgenmapping[stepname] = func
        if idx is None:
            b2partsgenorder.append(stepname)
        else:
            b2partsgenorder.insert(idx, stepname)
        return func
    return dec

def _pushb2ctxcheckheads(pushop, bundler):
    """Generate race condition checking parts

    Exists as an independent function to aid extensions
    """
    # * 'force' do not check for push race,
    # * if we don't push anything, there are nothing to check.
    if not pushop.force and pushop.outgoing.missingheads:
        allowunrelated = 'related' in bundler.capabilities.get('checkheads', ())
        emptyremote = pushop.pushbranchmap is None
        if not allowunrelated or emptyremote:
            bundler.newpart('check:heads', data=iter(pushop.remoteheads))
        else:
            affected = set()
            for branch, heads in pushop.pushbranchmap.iteritems():
                remoteheads, newheads, unsyncedheads, discardedheads = heads
                if remoteheads is not None:
                    remote = set(remoteheads)
                    affected |= set(discardedheads) & remote
                    affected |= remote - set(newheads)
            if affected:
                data = iter(sorted(affected))
                bundler.newpart('check:updated-heads', data=data)

def _pushing(pushop):
    """return True if we are pushing anything"""
    return bool(pushop.outgoing.missing
                or pushop.outdatedphases
                or pushop.outobsmarkers
                or pushop.outbookmarks)

@b2partsgenerator('check-bookmarks')
def _pushb2checkbookmarks(pushop, bundler):
    """insert bookmark move checking"""
    if not _pushing(pushop) or pushop.force:
        return
    b2caps = bundle2.bundle2caps(pushop.remote)
    hasbookmarkcheck = 'bookmarks' in b2caps
    if not (pushop.outbookmarks and hasbookmarkcheck):
        return
    data = []
    for book, old, new in pushop.outbookmarks:
        old = bin(old)
        data.append((book, old))
    checkdata = bookmod.binaryencode(data)
    bundler.newpart('check:bookmarks', data=checkdata)

@b2partsgenerator('check-phases')
def _pushb2checkphases(pushop, bundler):
    """insert phase move checking"""
    if not _pushing(pushop) or pushop.force:
        return
    b2caps = bundle2.bundle2caps(pushop.remote)
    hasphaseheads = 'heads' in b2caps.get('phases', ())
    if pushop.remotephases is not None and hasphaseheads:
        # check that the remote phase has not changed
        checks = [[] for p in phases.allphases]
        checks[phases.public].extend(pushop.remotephases.publicheads)
        checks[phases.draft].extend(pushop.remotephases.draftroots)
        if any(checks):
            for nodes in checks:
                nodes.sort()
            checkdata = phases.binaryencode(checks)
            bundler.newpart('check:phases', data=checkdata)

@b2partsgenerator('changeset')
def _pushb2ctx(pushop, bundler):
    """handle changegroup push through bundle2

    addchangegroup result is stored in the ``pushop.cgresult`` attribute.
    """
    if 'changesets' in pushop.stepsdone:
        return
    pushop.stepsdone.add('changesets')
    # Send known heads to the server for race detection.
    if not _pushcheckoutgoing(pushop):
        return
    pushop.repo.prepushoutgoinghooks(pushop)

    _pushb2ctxcheckheads(pushop, bundler)

    b2caps = bundle2.bundle2caps(pushop.remote)
    version = '01'
    cgversions = b2caps.get('changegroup')
    if cgversions:  # 3.1 and 3.2 ship with an empty value
        cgversions = [v for v in cgversions
                      if v in changegroup.supportedoutgoingversions(
                          pushop.repo)]
        if not cgversions:
            raise ValueError(_('no common changegroup version'))
        version = max(cgversions)
    cgstream = changegroup.makestream(pushop.repo, pushop.outgoing, version,
                                      'push')
    cgpart = bundler.newpart('changegroup', data=cgstream)
    if cgversions:
        cgpart.addparam('version', version)
    if 'treemanifest' in pushop.repo.requirements:
        cgpart.addparam('treemanifest', '1')
    def handlereply(op):
        """extract addchangegroup returns from server reply"""
        cgreplies = op.records.getreplies(cgpart.id)
        assert len(cgreplies['changegroup']) == 1
        pushop.cgresult = cgreplies['changegroup'][0]['return']
    return handlereply

@b2partsgenerator('phase')
def _pushb2phases(pushop, bundler):
    """handle phase push through bundle2"""
    if 'phases' in pushop.stepsdone:
        return
    b2caps = bundle2.bundle2caps(pushop.remote)
    ui = pushop.repo.ui

    legacyphase = 'phases' in ui.configlist('devel', 'legacy.exchange')
    haspushkey = 'pushkey' in b2caps
    hasphaseheads = 'heads' in b2caps.get('phases', ())

    if hasphaseheads and not legacyphase:
        return _pushb2phaseheads(pushop, bundler)
    elif haspushkey:
        return _pushb2phasespushkey(pushop, bundler)

def _pushb2phaseheads(pushop, bundler):
    """push phase information through a bundle2 - binary part"""
    pushop.stepsdone.add('phases')
    if pushop.outdatedphases:
        updates = [[] for p in phases.allphases]
        updates[0].extend(h.node() for h in pushop.outdatedphases)
        phasedata = phases.binaryencode(updates)
        bundler.newpart('phase-heads', data=phasedata)

def _pushb2phasespushkey(pushop, bundler):
    """push phase information through a bundle2 - pushkey part"""
    pushop.stepsdone.add('phases')
    part2node = []

    def handlefailure(pushop, exc):
        targetid = int(exc.partid)
        for partid, node in part2node:
            if partid == targetid:
                raise error.Abort(_('updating %s to public failed') % node)

    enc = pushkey.encode
    for newremotehead in pushop.outdatedphases:
        part = bundler.newpart('pushkey')
        part.addparam('namespace', enc('phases'))
        part.addparam('key', enc(newremotehead.hex()))
        part.addparam('old', enc('%d' % phases.draft))
        part.addparam('new', enc('%d' % phases.public))
        part2node.append((part.id, newremotehead))
        pushop.pkfailcb[part.id] = handlefailure

    def handlereply(op):
        for partid, node in part2node:
            partrep = op.records.getreplies(partid)
            results = partrep['pushkey']
            assert len(results) <= 1
            msg = None
            if not results:
                msg = _('server ignored update of %s to public!\n') % node
            elif not int(results[0]['return']):
                msg = _('updating %s to public failed!\n') % node
            if msg is not None:
                pushop.ui.warn(msg)
    return handlereply

@b2partsgenerator('obsmarkers')
def _pushb2obsmarkers(pushop, bundler):
    if 'obsmarkers' in pushop.stepsdone:
        return
    remoteversions = bundle2.obsmarkersversion(bundler.capabilities)
    if obsolete.commonversion(remoteversions) is None:
        return
    pushop.stepsdone.add('obsmarkers')
    if pushop.outobsmarkers:
        markers = sorted(pushop.outobsmarkers)
        bundle2.buildobsmarkerspart(bundler, markers)

@b2partsgenerator('bookmarks')
def _pushb2bookmarks(pushop, bundler):
    """handle bookmark push through bundle2"""
    if 'bookmarks' in pushop.stepsdone:
        return
    b2caps = bundle2.bundle2caps(pushop.remote)

    legacy = pushop.repo.ui.configlist('devel', 'legacy.exchange')
    legacybooks = 'bookmarks' in legacy

    if not legacybooks and 'bookmarks' in b2caps:
        return _pushb2bookmarkspart(pushop, bundler)
    elif 'pushkey' in b2caps:
        return _pushb2bookmarkspushkey(pushop, bundler)

def _bmaction(old, new):
    """small utility for bookmark pushing"""
    if not old:
        return 'export'
    elif not new:
        return 'delete'
    return 'update'

def _pushb2bookmarkspart(pushop, bundler):
    pushop.stepsdone.add('bookmarks')
    if not pushop.outbookmarks:
        return

    allactions = []
    data = []
    for book, old, new in pushop.outbookmarks:
        new = bin(new)
        data.append((book, new))
        allactions.append((book, _bmaction(old, new)))
    checkdata = bookmod.binaryencode(data)
    bundler.newpart('bookmarks', data=checkdata)

    def handlereply(op):
        ui = pushop.ui
        # if success
        for book, action in allactions:
            ui.status(bookmsgmap[action][0] % book)

    return handlereply

def _pushb2bookmarkspushkey(pushop, bundler):
    pushop.stepsdone.add('bookmarks')
    part2book = []
    enc = pushkey.encode

    def handlefailure(pushop, exc):
        targetid = int(exc.partid)
        for partid, book, action in part2book:
            if partid == targetid:
                raise error.Abort(bookmsgmap[action][1].rstrip() % book)
        # we should not be called for part we did not generated
        assert False

    for book, old, new in pushop.outbookmarks:
        part = bundler.newpart('pushkey')
        part.addparam('namespace', enc('bookmarks'))
        part.addparam('key', enc(book))
        part.addparam('old', enc(old))
        part.addparam('new', enc(new))
        action = 'update'
        if not old:
            action = 'export'
        elif not new:
            action = 'delete'
        part2book.append((part.id, book, action))
        pushop.pkfailcb[part.id] = handlefailure

    def handlereply(op):
        ui = pushop.ui
        for partid, book, action in part2book:
            partrep = op.records.getreplies(partid)
            results = partrep['pushkey']
            assert len(results) <= 1
            if not results:
                pushop.ui.warn(_('server ignored bookmark %s update\n') % book)
            else:
                ret = int(results[0]['return'])
                if ret:
                    ui.status(bookmsgmap[action][0] % book)
                else:
                    ui.warn(bookmsgmap[action][1] % book)
                    if pushop.bkresult is not None:
                        pushop.bkresult = 1
    return handlereply

@b2partsgenerator('pushvars', idx=0)
def _getbundlesendvars(pushop, bundler):
    '''send shellvars via bundle2'''
    pushvars = pushop.pushvars
    if pushvars:
        shellvars = {}
        for raw in pushvars:
            if '=' not in raw:
                msg = ("unable to parse variable '%s', should follow "
                        "'KEY=VALUE' or 'KEY=' format")
                raise error.Abort(msg % raw)
            k, v = raw.split('=', 1)
            shellvars[k] = v

        part = bundler.newpart('pushvars')

        for key, value in shellvars.iteritems():
            part.addparam(key, value, mandatory=False)

def _pushbundle2(pushop):
    """push data to the remote using bundle2

    The only currently supported type of data is changegroup but this will
    evolve in the future."""
    bundler = bundle2.bundle20(pushop.ui, bundle2.bundle2caps(pushop.remote))
    pushback = (pushop.trmanager
                and pushop.ui.configbool('experimental', 'bundle2.pushback'))

    # create reply capability
    capsblob = bundle2.encodecaps(bundle2.getrepocaps(pushop.repo,
                                                      allowpushback=pushback,
                                                      role='client'))
    bundler.newpart('replycaps', data=capsblob)
    replyhandlers = []
    for partgenname in b2partsgenorder:
        partgen = b2partsgenmapping[partgenname]
        ret = partgen(pushop, bundler)
        if callable(ret):
            replyhandlers.append(ret)
    # do not push if nothing to push
    if bundler.nbparts <= 1:
        return
    stream = util.chunkbuffer(bundler.getchunks())
    try:
        try:
            with pushop.remote.commandexecutor() as e:
                reply = e.callcommand('unbundle', {
                    'bundle': stream,
                    'heads': ['force'],
                    'url': pushop.remote.url(),
                }).result()
        except error.BundleValueError as exc:
            raise error.Abort(_('missing support for %s') % exc)
        try:
            trgetter = None
            if pushback:
                trgetter = pushop.trmanager.transaction
            op = bundle2.processbundle(pushop.repo, reply, trgetter)
        except error.BundleValueError as exc:
            raise error.Abort(_('missing support for %s') % exc)
        except bundle2.AbortFromPart as exc:
            pushop.ui.status(_('remote: %s\n') % exc)
            if exc.hint is not None:
                pushop.ui.status(_('remote: %s\n') % ('(%s)' % exc.hint))
            raise error.Abort(_('push failed on remote'))
    except error.PushkeyFailed as exc:
        partid = int(exc.partid)
        if partid not in pushop.pkfailcb:
            raise
        pushop.pkfailcb[partid](pushop, exc)
    for rephand in replyhandlers:
        rephand(op)

def _pushchangeset(pushop):
    """Make the actual push of changeset bundle to remote repo"""
    if 'changesets' in pushop.stepsdone:
        return
    pushop.stepsdone.add('changesets')
    if not _pushcheckoutgoing(pushop):
        return

    # Should have verified this in push().
    assert pushop.remote.capable('unbundle')

    pushop.repo.prepushoutgoinghooks(pushop)
    outgoing = pushop.outgoing
    # TODO: get bundlecaps from remote
    bundlecaps = None
    # create a changegroup from local
    if pushop.revs is None and not (outgoing.excluded
                            or pushop.repo.changelog.filteredrevs):
        # push everything,
        # use the fast path, no race possible on push
        cg = changegroup.makechangegroup(pushop.repo, outgoing, '01', 'push',
                fastpath=True, bundlecaps=bundlecaps)
    else:
        cg = changegroup.makechangegroup(pushop.repo, outgoing, '01',
                                        'push', bundlecaps=bundlecaps)

    # apply changegroup to remote
    # local repo finds heads on server, finds out what
    # revs it must push. once revs transferred, if server
    # finds it has different heads (someone else won
    # commit/push race), server aborts.
    if pushop.force:
        remoteheads = ['force']
    else:
        remoteheads = pushop.remoteheads
    # ssh: return remote's addchangegroup()
    # http: return remote's addchangegroup() or 0 for error
    pushop.cgresult = pushop.remote.unbundle(cg, remoteheads,
                                        pushop.repo.url())

def _pushsyncphase(pushop):
    """synchronise phase information locally and remotely"""
    cheads = pushop.commonheads
    # even when we don't push, exchanging phase data is useful
    remotephases = listkeys(pushop.remote, 'phases')
    if (pushop.ui.configbool('ui', '_usedassubrepo')
        and remotephases    # server supports phases
        and pushop.cgresult is None # nothing was pushed
        and remotephases.get('publishing', False)):
        # When:
        # - this is a subrepo push
        # - and remote support phase
        # - and no changeset was pushed
        # - and remote is publishing
        # We may be in issue 3871 case!
        # We drop the possible phase synchronisation done by
        # courtesy to publish changesets possibly locally draft
        # on the remote.
        remotephases = {'publishing': 'True'}
    if not remotephases: # old server or public only reply from non-publishing
        _localphasemove(pushop, cheads)
        # don't push any phase data as there is nothing to push
    else:
        ana = phases.analyzeremotephases(pushop.repo, cheads,
                                         remotephases)
        pheads, droots = ana
        ### Apply remote phase on local
        if remotephases.get('publishing', False):
            _localphasemove(pushop, cheads)
        else: # publish = False
            _localphasemove(pushop, pheads)
            _localphasemove(pushop, cheads, phases.draft)
        ### Apply local phase on remote

        if pushop.cgresult:
            if 'phases' in pushop.stepsdone:
                # phases already pushed though bundle2
                return
            outdated = pushop.outdatedphases
        else:
            outdated = pushop.fallbackoutdatedphases

        pushop.stepsdone.add('phases')

        # filter heads already turned public by the push
        outdated = [c for c in outdated if c.node() not in pheads]
        # fallback to independent pushkey command
        for newremotehead in outdated:
            with pushop.remote.commandexecutor() as e:
                r = e.callcommand('pushkey', {
                    'namespace': 'phases',
                    'key': newremotehead.hex(),
                    'old': '%d' % phases.draft,
                    'new': '%d' % phases.public
                }).result()

            if not r:
                pushop.ui.warn(_('updating %s to public failed!\n')
                               % newremotehead)

def _localphasemove(pushop, nodes, phase=phases.public):
    """move <nodes> to <phase> in the local source repo"""
    if pushop.trmanager:
        phases.advanceboundary(pushop.repo,
                               pushop.trmanager.transaction(),
                               phase,
                               nodes)
    else:
        # repo is not locked, do not change any phases!
        # Informs the user that phases should have been moved when
        # applicable.
        actualmoves = [n for n in nodes if phase < pushop.repo[n].phase()]
        phasestr = phases.phasenames[phase]
        if actualmoves:
            pushop.ui.status(_('cannot lock source repo, skipping '
                               'local %s phase update\n') % phasestr)

def _pushobsolete(pushop):
    """utility function to push obsolete markers to a remote"""
    if 'obsmarkers' in pushop.stepsdone:
        return
    repo = pushop.repo
    remote = pushop.remote
    pushop.stepsdone.add('obsmarkers')
    if pushop.outobsmarkers:
        pushop.ui.debug('try to push obsolete markers to remote\n')
        rslts = []
        remotedata = obsolete._pushkeyescape(sorted(pushop.outobsmarkers))
        for key in sorted(remotedata, reverse=True):
            # reverse sort to ensure we end with dump0
            data = remotedata[key]
            rslts.append(remote.pushkey('obsolete', key, '', data))
        if [r for r in rslts if not r]:
            msg = _('failed to push some obsolete markers!\n')
            repo.ui.warn(msg)

def _pushbookmark(pushop):
    """Update bookmark position on remote"""
    if pushop.cgresult == 0 or 'bookmarks' in pushop.stepsdone:
        return
    pushop.stepsdone.add('bookmarks')
    ui = pushop.ui
    remote = pushop.remote

    for b, old, new in pushop.outbookmarks:
        action = 'update'
        if not old:
            action = 'export'
        elif not new:
            action = 'delete'

        with remote.commandexecutor() as e:
            r = e.callcommand('pushkey', {
                'namespace': 'bookmarks',
                'key': b,
                'old': old,
                'new': new,
            }).result()

        if r:
            ui.status(bookmsgmap[action][0] % b)
        else:
            ui.warn(bookmsgmap[action][1] % b)
            # discovery can have set the value form invalid entry
            if pushop.bkresult is not None:
                pushop.bkresult = 1

class pulloperation(object):
    """A object that represent a single pull operation

    It purpose is to carry pull related state and very common operation.

    A new should be created at the beginning of each pull and discarded
    afterward.
    """

    def __init__(self, repo, remote, heads=None, force=False, bookmarks=(),
                 remotebookmarks=None, streamclonerequested=None,
                 includepats=None, excludepats=None, depth=None):
        # repo we pull into
        self.repo = repo
        # repo we pull from
        self.remote = remote
        # revision we try to pull (None is "all")
        self.heads = heads
        # bookmark pulled explicitly
        self.explicitbookmarks = [repo._bookmarks.expandname(bookmark)
                                  for bookmark in bookmarks]
        # do we force pull?
        self.force = force
        # whether a streaming clone was requested
        self.streamclonerequested = streamclonerequested
        # transaction manager
        self.trmanager = None
        # set of common changeset between local and remote before pull
        self.common = None
        # set of pulled head
        self.rheads = None
        # list of missing changeset to fetch remotely
        self.fetch = None
        # remote bookmarks data
        self.remotebookmarks = remotebookmarks
        # result of changegroup pulling (used as return code by pull)
        self.cgresult = None
        # list of step already done
        self.stepsdone = set()
        # Whether we attempted a clone from pre-generated bundles.
        self.clonebundleattempted = False
        # Set of file patterns to include.
        self.includepats = includepats
        # Set of file patterns to exclude.
        self.excludepats = excludepats
        # Number of ancestor changesets to pull from each pulled head.
        self.depth = depth

    @util.propertycache
    def pulledsubset(self):
        """heads of the set of changeset target by the pull"""
        # compute target subset
        if self.heads is None:
            # We pulled every thing possible
            # sync on everything common
            c = set(self.common)
            ret = list(self.common)
            for n in self.rheads:
                if n not in c:
                    ret.append(n)
            return ret
        else:
            # We pulled a specific subset
            # sync on this subset
            return self.heads

    @util.propertycache
    def canusebundle2(self):
        return not _forcebundle1(self)

    @util.propertycache
    def remotebundle2caps(self):
        return bundle2.bundle2caps(self.remote)

    def gettransaction(self):
        # deprecated; talk to trmanager directly
        return self.trmanager.transaction()

class transactionmanager(util.transactional):
    """An object to manage the life cycle of a transaction

    It creates the transaction on demand and calls the appropriate hooks when
    closing the transaction."""
    def __init__(self, repo, source, url):
        self.repo = repo
        self.source = source
        self.url = url
        self._tr = None

    def transaction(self):
        """Return an open transaction object, constructing if necessary"""
        if not self._tr:
            trname = '%s\n%s' % (self.source, util.hidepassword(self.url))
            self._tr = self.repo.transaction(trname)
            self._tr.hookargs['source'] = self.source
            self._tr.hookargs['url'] = self.url
        return self._tr

    def close(self):
        """close transaction if created"""
        if self._tr is not None:
            self._tr.close()

    def release(self):
        """release transaction if created"""
        if self._tr is not None:
            self._tr.release()

def listkeys(remote, namespace):
    with remote.commandexecutor() as e:
        return e.callcommand('listkeys', {'namespace': namespace}).result()

def _fullpullbundle2(repo, pullop):
    # The server may send a partial reply, i.e. when inlining
    # pre-computed bundles. In that case, update the common
    # set based on the results and pull another bundle.
    #
    # There are two indicators that the process is finished:
    # - no changeset has been added, or
    # - all remote heads are known locally.
    # The head check must use the unfiltered view as obsoletion
    # markers can hide heads.
    unfi = repo.unfiltered()
    unficl = unfi.changelog
    def headsofdiff(h1, h2):
        """Returns heads(h1 % h2)"""
        res = unfi.set('heads(%ln %% %ln)', h1, h2)
        return set(ctx.node() for ctx in res)
    def headsofunion(h1, h2):
        """Returns heads((h1 + h2) - null)"""
        res = unfi.set('heads((%ln + %ln - null))', h1, h2)
        return set(ctx.node() for ctx in res)
    while True:
        old_heads = unficl.heads()
        clstart = len(unficl)
        _pullbundle2(pullop)
        if repository.NARROW_REQUIREMENT in repo.requirements:
            # XXX narrow clones filter the heads on the server side during
            # XXX getbundle and result in partial replies as well.
            # XXX Disable pull bundles in this case as band aid to avoid
            # XXX extra round trips.
            break
        if clstart == len(unficl):
            break
        if all(unficl.hasnode(n) for n in pullop.rheads):
            break
        new_heads = headsofdiff(unficl.heads(), old_heads)
        pullop.common = headsofunion(new_heads, pullop.common)
        pullop.rheads = set(pullop.rheads) - pullop.common

def pull(repo, remote, heads=None, force=False, bookmarks=(), opargs=None,
         streamclonerequested=None, includepats=None, excludepats=None,
         depth=None):
    """Fetch repository data from a remote.

    This is the main function used to retrieve data from a remote repository.

    ``repo`` is the local repository to clone into.
    ``remote`` is a peer instance.
    ``heads`` is an iterable of revisions we want to pull. ``None`` (the
    default) means to pull everything from the remote.
    ``bookmarks`` is an iterable of bookmarks requesting to be pulled. By
    default, all remote bookmarks are pulled.
    ``opargs`` are additional keyword arguments to pass to ``pulloperation``
    initialization.
    ``streamclonerequested`` is a boolean indicating whether a "streaming
    clone" is requested. A "streaming clone" is essentially a raw file copy
    of revlogs from the server. This only works when the local repository is
    empty. The default value of ``None`` means to respect the server
    configuration for preferring stream clones.
    ``includepats`` and ``excludepats`` define explicit file patterns to
    include and exclude in storage, respectively. If not defined, narrow
    patterns from the repo instance are used, if available.
    ``depth`` is an integer indicating the DAG depth of history we're
    interested in. If defined, for each revision specified in ``heads``, we
    will fetch up to this many of its ancestors and data associated with them.

    Returns the ``pulloperation`` created for this pull.
    """
    if opargs is None:
        opargs = {}

    # We allow the narrow patterns to be passed in explicitly to provide more
    # flexibility for API consumers.
    if includepats or excludepats:
        includepats = includepats or set()
        excludepats = excludepats or set()
    else:
        includepats, excludepats = repo.narrowpats

    narrowspec.validatepatterns(includepats)
    narrowspec.validatepatterns(excludepats)

    pullop = pulloperation(repo, remote, heads, force, bookmarks=bookmarks,
                           streamclonerequested=streamclonerequested,
                           includepats=includepats, excludepats=excludepats,
                           depth=depth,
                           **pycompat.strkwargs(opargs))

    peerlocal = pullop.remote.local()
    if peerlocal:
        missing = set(peerlocal.requirements) - pullop.repo.supported
        if missing:
            msg = _("required features are not"
                    " supported in the destination:"
                    " %s") % (', '.join(sorted(missing)))
            raise error.Abort(msg)

    pullop.trmanager = transactionmanager(repo, 'pull', remote.url())
    with repo.wlock(), repo.lock(), pullop.trmanager:
        # Use the modern wire protocol, if available.
        if remote.capable('command-changesetdata'):
            exchangev2.pull(pullop)
        else:
            # This should ideally be in _pullbundle2(). However, it needs to run
            # before discovery to avoid extra work.
            _maybeapplyclonebundle(pullop)
            streamclone.maybeperformlegacystreamclone(pullop)
            _pulldiscovery(pullop)
            if pullop.canusebundle2:
                _fullpullbundle2(repo, pullop)
            _pullchangeset(pullop)
            _pullphase(pullop)
            _pullbookmarks(pullop)
            _pullobsolete(pullop)

    # storing remotenames
    if repo.ui.configbool('experimental', 'remotenames'):
        logexchange.pullremotenames(repo, remote)

    return pullop

# list of steps to perform discovery before pull
pulldiscoveryorder = []

# Mapping between step name and function
#
# This exists to help extensions wrap steps if necessary
pulldiscoverymapping = {}

def pulldiscovery(stepname):
    """decorator for function performing discovery before pull

    The function is added to the step -> function mapping and appended to the
    list of steps.  Beware that decorated function will be added in order (this
    may matter).

    You can only use this decorator for a new step, if you want to wrap a step
    from an extension, change the pulldiscovery dictionary directly."""
    def dec(func):
        assert stepname not in pulldiscoverymapping
        pulldiscoverymapping[stepname] = func
        pulldiscoveryorder.append(stepname)
        return func
    return dec

def _pulldiscovery(pullop):
    """Run all discovery steps"""
    for stepname in pulldiscoveryorder:
        step = pulldiscoverymapping[stepname]
        step(pullop)

@pulldiscovery('b1:bookmarks')
def _pullbookmarkbundle1(pullop):
    """fetch bookmark data in bundle1 case

    If not using bundle2, we have to fetch bookmarks before changeset
    discovery to reduce the chance and impact of race conditions."""
    if pullop.remotebookmarks is not None:
        return
    if pullop.canusebundle2 and 'listkeys' in pullop.remotebundle2caps:
        # all known bundle2 servers now support listkeys, but lets be nice with
        # new implementation.
        return
    books = listkeys(pullop.remote, 'bookmarks')
    pullop.remotebookmarks = bookmod.unhexlifybookmarks(books)


@pulldiscovery('changegroup')
def _pulldiscoverychangegroup(pullop):
    """discovery phase for the pull

    Current handle changeset discovery only, will change handle all discovery
    at some point."""
    tmp = discovery.findcommonincoming(pullop.repo,
                                       pullop.remote,
                                       heads=pullop.heads,
                                       force=pullop.force)
    common, fetch, rheads = tmp
    nm = pullop.repo.unfiltered().changelog.nodemap
    if fetch and rheads:
        # If a remote heads is filtered locally, put in back in common.
        #
        # This is a hackish solution to catch most of "common but locally
        # hidden situation".  We do not performs discovery on unfiltered
        # repository because it end up doing a pathological amount of round
        # trip for w huge amount of changeset we do not care about.
        #
        # If a set of such "common but filtered" changeset exist on the server
        # but are not including a remote heads, we'll not be able to detect it,
        scommon = set(common)
        for n in rheads:
            if n in nm:
                if n not in scommon:
                    common.append(n)
        if set(rheads).issubset(set(common)):
            fetch = []
    pullop.common = common
    pullop.fetch = fetch
    pullop.rheads = rheads

def _pullbundle2(pullop):
    """pull data using bundle2

    For now, the only supported data are changegroup."""
    kwargs = {'bundlecaps': caps20to10(pullop.repo, role='client')}

    # make ui easier to access
    ui = pullop.repo.ui

    # At the moment we don't do stream clones over bundle2. If that is
    # implemented then here's where the check for that will go.
    streaming = streamclone.canperformstreamclone(pullop, bundle2=True)[0]

    # declare pull perimeters
    kwargs['common'] = pullop.common
    kwargs['heads'] = pullop.heads or pullop.rheads

    if streaming:
        kwargs['cg'] = False
        kwargs['stream'] = True
        pullop.stepsdone.add('changegroup')
        pullop.stepsdone.add('phases')

    else:
        # pulling changegroup
        pullop.stepsdone.add('changegroup')

        kwargs['cg'] = pullop.fetch

        legacyphase = 'phases' in ui.configlist('devel', 'legacy.exchange')
        hasbinaryphase = 'heads' in pullop.remotebundle2caps.get('phases', ())
        if (not legacyphase and hasbinaryphase):
            kwargs['phases'] = True
            pullop.stepsdone.add('phases')

        if 'listkeys' in pullop.remotebundle2caps:
            if 'phases' not in pullop.stepsdone:
                kwargs['listkeys'] = ['phases']

    bookmarksrequested = False
    legacybookmark = 'bookmarks' in ui.configlist('devel', 'legacy.exchange')
    hasbinarybook = 'bookmarks' in pullop.remotebundle2caps

    if pullop.remotebookmarks is not None:
        pullop.stepsdone.add('request-bookmarks')

    if ('request-bookmarks' not in pullop.stepsdone
        and pullop.remotebookmarks is None
        and not legacybookmark and hasbinarybook):
        kwargs['bookmarks'] = True
        bookmarksrequested = True

    if 'listkeys' in pullop.remotebundle2caps:
        if 'request-bookmarks' not in pullop.stepsdone:
            # make sure to always includes bookmark data when migrating
            # `hg incoming --bundle` to using this function.
            pullop.stepsdone.add('request-bookmarks')
            kwargs.setdefault('listkeys', []).append('bookmarks')

    # If this is a full pull / clone and the server supports the clone bundles
    # feature, tell the server whether we attempted a clone bundle. The
    # presence of this flag indicates the client supports clone bundles. This
    # will enable the server to treat clients that support clone bundles
    # differently from those that don't.
    if (pullop.remote.capable('clonebundles')
        and pullop.heads is None and list(pullop.common) == [nullid]):
        kwargs['cbattempted'] = pullop.clonebundleattempted

    if streaming:
        pullop.repo.ui.status(_('streaming all changes\n'))
    elif not pullop.fetch:
        pullop.repo.ui.status(_("no changes found\n"))
        pullop.cgresult = 0
    else:
        if pullop.heads is None and list(pullop.common) == [nullid]:
            pullop.repo.ui.status(_("requesting all changes\n"))
    if obsolete.isenabled(pullop.repo, obsolete.exchangeopt):
        remoteversions = bundle2.obsmarkersversion(pullop.remotebundle2caps)
        if obsolete.commonversion(remoteversions) is not None:
            kwargs['obsmarkers'] = True
            pullop.stepsdone.add('obsmarkers')
    _pullbundle2extraprepare(pullop, kwargs)

    with pullop.remote.commandexecutor() as e:
        args = dict(kwargs)
        args['source'] = 'pull'
        bundle = e.callcommand('getbundle', args).result()

        try:
            op = bundle2.bundleoperation(pullop.repo, pullop.gettransaction,
                                         source='pull')
            op.modes['bookmarks'] = 'records'
            bundle2.processbundle(pullop.repo, bundle, op=op)
        except bundle2.AbortFromPart as exc:
            pullop.repo.ui.status(_('remote: abort: %s\n') % exc)
            raise error.Abort(_('pull failed on remote'), hint=exc.hint)
        except error.BundleValueError as exc:
            raise error.Abort(_('missing support for %s') % exc)

    if pullop.fetch:
        pullop.cgresult = bundle2.combinechangegroupresults(op)

    # processing phases change
    for namespace, value in op.records['listkeys']:
        if namespace == 'phases':
            _pullapplyphases(pullop, value)

    # processing bookmark update
    if bookmarksrequested:
        books = {}
        for record in op.records['bookmarks']:
            books[record['bookmark']] = record["node"]
        pullop.remotebookmarks = books
    else:
        for namespace, value in op.records['listkeys']:
            if namespace == 'bookmarks':
                pullop.remotebookmarks = bookmod.unhexlifybookmarks(value)

    # bookmark data were either already there or pulled in the bundle
    if pullop.remotebookmarks is not None:
        _pullbookmarks(pullop)

def _pullbundle2extraprepare(pullop, kwargs):
    """hook function so that extensions can extend the getbundle call"""

def _pullchangeset(pullop):
    """pull changeset from unbundle into the local repo"""
    # We delay the open of the transaction as late as possible so we
    # don't open transaction for nothing or you break future useful
    # rollback call
    if 'changegroup' in pullop.stepsdone:
        return
    pullop.stepsdone.add('changegroup')
    if not pullop.fetch:
        pullop.repo.ui.status(_("no changes found\n"))
        pullop.cgresult = 0
        return
    tr = pullop.gettransaction()
    if pullop.heads is None and list(pullop.common) == [nullid]:
        pullop.repo.ui.status(_("requesting all changes\n"))
    elif pullop.heads is None and pullop.remote.capable('changegroupsubset'):
        # issue1320, avoid a race if remote changed after discovery
        pullop.heads = pullop.rheads

    if pullop.remote.capable('getbundle'):
        # TODO: get bundlecaps from remote
        cg = pullop.remote.getbundle('pull', common=pullop.common,
                                     heads=pullop.heads or pullop.rheads)
    elif pullop.heads is None:
        with pullop.remote.commandexecutor() as e:
            cg = e.callcommand('changegroup', {
                'nodes': pullop.fetch,
                'source': 'pull',
            }).result()

    elif not pullop.remote.capable('changegroupsubset'):
        raise error.Abort(_("partial pull cannot be done because "
                           "other repository doesn't support "
                           "changegroupsubset."))
    else:
        with pullop.remote.commandexecutor() as e:
            cg = e.callcommand('changegroupsubset', {
                'bases': pullop.fetch,
                'heads': pullop.heads,
                'source': 'pull',
            }).result()

    bundleop = bundle2.applybundle(pullop.repo, cg, tr, 'pull',
                                   pullop.remote.url())
    pullop.cgresult = bundle2.combinechangegroupresults(bundleop)

def _pullphase(pullop):
    # Get remote phases data from remote
    if 'phases' in pullop.stepsdone:
        return
    remotephases = listkeys(pullop.remote, 'phases')
    _pullapplyphases(pullop, remotephases)

def _pullapplyphases(pullop, remotephases):
    """apply phase movement from observed remote state"""
    if 'phases' in pullop.stepsdone:
        return
    pullop.stepsdone.add('phases')
    publishing = bool(remotephases.get('publishing', False))
    if remotephases and not publishing:
        # remote is new and non-publishing
        pheads, _dr = phases.analyzeremotephases(pullop.repo,
                                                 pullop.pulledsubset,
                                                 remotephases)
        dheads = pullop.pulledsubset
    else:
        # Remote is old or publishing all common changesets
        # should be seen as public
        pheads = pullop.pulledsubset
        dheads = []
    unfi = pullop.repo.unfiltered()
    phase = unfi._phasecache.phase
    rev = unfi.changelog.nodemap.get
    public = phases.public
    draft = phases.draft

    # exclude changesets already public locally and update the others
    pheads = [pn for pn in pheads if phase(unfi, rev(pn)) > public]
    if pheads:
        tr = pullop.gettransaction()
        phases.advanceboundary(pullop.repo, tr, public, pheads)

    # exclude changesets already draft locally and update the others
    dheads = [pn for pn in dheads if phase(unfi, rev(pn)) > draft]
    if dheads:
        tr = pullop.gettransaction()
        phases.advanceboundary(pullop.repo, tr, draft, dheads)

def _pullbookmarks(pullop):
    """process the remote bookmark information to update the local one"""
    if 'bookmarks' in pullop.stepsdone:
        return
    pullop.stepsdone.add('bookmarks')
    repo = pullop.repo
    remotebookmarks = pullop.remotebookmarks
    bookmod.updatefromremote(repo.ui, repo, remotebookmarks,
                             pullop.remote.url(),
                             pullop.gettransaction,
                             explicit=pullop.explicitbookmarks)

def _pullobsolete(pullop):
    """utility function to pull obsolete markers from a remote

    The `gettransaction` is function that return the pull transaction, creating
    one if necessary. We return the transaction to inform the calling code that
    a new transaction have been created (when applicable).

    Exists mostly to allow overriding for experimentation purpose"""
    if 'obsmarkers' in pullop.stepsdone:
        return
    pullop.stepsdone.add('obsmarkers')
    tr = None
    if obsolete.isenabled(pullop.repo, obsolete.exchangeopt):
        pullop.repo.ui.debug('fetching remote obsolete markers\n')
        remoteobs = listkeys(pullop.remote, 'obsolete')
        if 'dump0' in remoteobs:
            tr = pullop.gettransaction()
            markers = []
            for key in sorted(remoteobs, reverse=True):
                if key.startswith('dump'):
                    data = util.b85decode(remoteobs[key])
                    version, newmarks = obsolete._readmarkers(data)
                    markers += newmarks
            if markers:
                pullop.repo.obsstore.add(tr, markers)
            pullop.repo.invalidatevolatilesets()
    return tr

def applynarrowacl(repo, kwargs):
    """Apply narrow fetch access control.

    This massages the named arguments for getbundle wire protocol commands
    so requested data is filtered through access control rules.
    """
    ui = repo.ui
    # TODO this assumes existence of HTTP and is a layering violation.
    username = ui.shortuser(ui.environ.get('REMOTE_USER') or ui.username())
    user_includes = ui.configlist(
        _NARROWACL_SECTION, username + '.includes',
        ui.configlist(_NARROWACL_SECTION, 'default.includes'))
    user_excludes = ui.configlist(
        _NARROWACL_SECTION, username + '.excludes',
        ui.configlist(_NARROWACL_SECTION, 'default.excludes'))
    if not user_includes:
        raise error.Abort(_("{} configuration for user {} is empty")
                          .format(_NARROWACL_SECTION, username))

    user_includes = [
        'path:.' if p == '*' else 'path:' + p for p in user_includes]
    user_excludes = [
        'path:.' if p == '*' else 'path:' + p for p in user_excludes]

    req_includes = set(kwargs.get(r'includepats', []))
    req_excludes = set(kwargs.get(r'excludepats', []))

    req_includes, req_excludes, invalid_includes = narrowspec.restrictpatterns(
        req_includes, req_excludes, user_includes, user_excludes)

    if invalid_includes:
        raise error.Abort(
            _("The following includes are not accessible for {}: {}")
            .format(username, invalid_includes))

    new_args = {}
    new_args.update(kwargs)
    new_args[r'narrow'] = True
    new_args[r'narrow_acl'] = True
    new_args[r'includepats'] = req_includes
    if req_excludes:
        new_args[r'excludepats'] = req_excludes

    return new_args

def _computeellipsis(repo, common, heads, known, match, depth=None):
    """Compute the shape of a narrowed DAG.

    Args:
      repo: The repository we're transferring.
      common: The roots of the DAG range we're transferring.
              May be just [nullid], which means all ancestors of heads.
      heads: The heads of the DAG range we're transferring.
      match: The narrowmatcher that allows us to identify relevant changes.
      depth: If not None, only consider nodes to be full nodes if they are at
             most depth changesets away from one of heads.

    Returns:
      A tuple of (visitnodes, relevant_nodes, ellipsisroots) where:

        visitnodes: The list of nodes (either full or ellipsis) which
                    need to be sent to the client.
        relevant_nodes: The set of changelog nodes which change a file inside
                 the narrowspec. The client needs these as non-ellipsis nodes.
        ellipsisroots: A dict of {rev: parents} that is used in
                       narrowchangegroup to produce ellipsis nodes with the
                       correct parents.
    """
    cl = repo.changelog
    mfl = repo.manifestlog

    clrev = cl.rev

    commonrevs = {clrev(n) for n in common} | {nullrev}
    headsrevs = {clrev(n) for n in heads}

    if depth:
        revdepth = {h: 0 for h in headsrevs}

    ellipsisheads = collections.defaultdict(set)
    ellipsisroots = collections.defaultdict(set)

    def addroot(head, curchange):
        """Add a root to an ellipsis head, splitting heads with 3 roots."""
        ellipsisroots[head].add(curchange)
        # Recursively split ellipsis heads with 3 roots by finding the
        # roots' youngest common descendant which is an elided merge commit.
        # That descendant takes 2 of the 3 roots as its own, and becomes a
        # root of the head.
        while len(ellipsisroots[head]) > 2:
            child, roots = splithead(head)
            splitroots(head, child, roots)
            head = child  # Recurse in case we just added a 3rd root

    def splitroots(head, child, roots):
        ellipsisroots[head].difference_update(roots)
        ellipsisroots[head].add(child)
        ellipsisroots[child].update(roots)
        ellipsisroots[child].discard(child)

    def splithead(head):
        r1, r2, r3 = sorted(ellipsisroots[head])
        for nr1, nr2 in ((r2, r3), (r1, r3), (r1, r2)):
            mid = repo.revs('sort(merge() & %d::%d & %d::%d, -rev)',
                            nr1, head, nr2, head)
            for j in mid:
                if j == nr2:
                    return nr2, (nr1, nr2)
                if j not in ellipsisroots or len(ellipsisroots[j]) < 2:
                    return j, (nr1, nr2)
        raise error.Abort(_('Failed to split up ellipsis node! head: %d, '
                            'roots: %d %d %d') % (head, r1, r2, r3))

    missing = list(cl.findmissingrevs(common=commonrevs, heads=headsrevs))
    visit = reversed(missing)
    relevant_nodes = set()
    visitnodes = [cl.node(m) for m in missing]
    required = set(headsrevs) | known
    for rev in visit:
        clrev = cl.changelogrevision(rev)
        ps = [prev for prev in cl.parentrevs(rev) if prev != nullrev]
        if depth is not None:
            curdepth = revdepth[rev]
            for p in ps:
                revdepth[p] = min(curdepth + 1, revdepth.get(p, depth + 1))
        needed = False
        shallow_enough = depth is None or revdepth[rev] <= depth
        if shallow_enough:
            curmf = mfl[clrev.manifest].read()
            if ps:
                # We choose to not trust the changed files list in
                # changesets because it's not always correct. TODO: could
                # we trust it for the non-merge case?
                p1mf = mfl[cl.changelogrevision(ps[0]).manifest].read()
                needed = bool(curmf.diff(p1mf, match))
                if not needed and len(ps) > 1:
                    # For merge changes, the list of changed files is not
                    # helpful, since we need to emit the merge if a file
                    # in the narrow spec has changed on either side of the
                    # merge. As a result, we do a manifest diff to check.
                    p2mf = mfl[cl.changelogrevision(ps[1]).manifest].read()
                    needed = bool(curmf.diff(p2mf, match))
            else:
                # For a root node, we need to include the node if any
                # files in the node match the narrowspec.
                needed = any(curmf.walk(match))

        if needed:
            for head in ellipsisheads[rev]:
                addroot(head, rev)
            for p in ps:
                required.add(p)
            relevant_nodes.add(cl.node(rev))
        else:
            if not ps:
                ps = [nullrev]
            if rev in required:
                for head in ellipsisheads[rev]:
                    addroot(head, rev)
                for p in ps:
                    ellipsisheads[p].add(rev)
            else:
                for p in ps:
                    ellipsisheads[p] |= ellipsisheads[rev]

    # add common changesets as roots of their reachable ellipsis heads
    for c in commonrevs:
        for head in ellipsisheads[c]:
            addroot(head, c)
    return visitnodes, relevant_nodes, ellipsisroots

def caps20to10(repo, role):
    """return a set with appropriate options to use bundle20 during getbundle"""
    caps = {'HG20'}
    capsblob = bundle2.encodecaps(bundle2.getrepocaps(repo, role=role))
    caps.add('bundle2=' + urlreq.quote(capsblob))
    return caps

# List of names of steps to perform for a bundle2 for getbundle, order matters.
getbundle2partsorder = []

# Mapping between step name and function
#
# This exists to help extensions wrap steps if necessary
getbundle2partsmapping = {}

def getbundle2partsgenerator(stepname, idx=None):
    """decorator for function generating bundle2 part for getbundle

    The function is added to the step -> function mapping and appended to the
    list of steps.  Beware that decorated functions will be added in order
    (this may matter).

    You can only use this decorator for new steps, if you want to wrap a step
    from an extension, attack the getbundle2partsmapping dictionary directly."""
    def dec(func):
        assert stepname not in getbundle2partsmapping
        getbundle2partsmapping[stepname] = func
        if idx is None:
            getbundle2partsorder.append(stepname)
        else:
            getbundle2partsorder.insert(idx, stepname)
        return func
    return dec

def bundle2requested(bundlecaps):
    if bundlecaps is not None:
        return any(cap.startswith('HG2') for cap in bundlecaps)
    return False

def getbundlechunks(repo, source, heads=None, common=None, bundlecaps=None,
                    **kwargs):
    """Return chunks constituting a bundle's raw data.

    Could be a bundle HG10 or a bundle HG20 depending on bundlecaps
    passed.

    Returns a 2-tuple of a dict with metadata about the generated bundle
    and an iterator over raw chunks (of varying sizes).
    """
    kwargs = pycompat.byteskwargs(kwargs)
    info = {}
    usebundle2 = bundle2requested(bundlecaps)
    # bundle10 case
    if not usebundle2:
        if bundlecaps and not kwargs.get('cg', True):
            raise ValueError(_('request for bundle10 must include changegroup'))

        if kwargs:
            raise ValueError(_('unsupported getbundle arguments: %s')
                             % ', '.join(sorted(kwargs.keys())))
        outgoing = _computeoutgoing(repo, heads, common)
        info['bundleversion'] = 1
        return info, changegroup.makestream(repo, outgoing, '01', source,
                                            bundlecaps=bundlecaps)

    # bundle20 case
    info['bundleversion'] = 2
    b2caps = {}
    for bcaps in bundlecaps:
        if bcaps.startswith('bundle2='):
            blob = urlreq.unquote(bcaps[len('bundle2='):])
            b2caps.update(bundle2.decodecaps(blob))
    bundler = bundle2.bundle20(repo.ui, b2caps)

    kwargs['heads'] = heads
    kwargs['common'] = common

    for name in getbundle2partsorder:
        func = getbundle2partsmapping[name]
        func(bundler, repo, source, bundlecaps=bundlecaps, b2caps=b2caps,
             **pycompat.strkwargs(kwargs))

    info['prefercompressed'] = bundler.prefercompressed

    return info, bundler.getchunks()

@getbundle2partsgenerator('stream2')
def _getbundlestream2(bundler, repo, *args, **kwargs):
    return bundle2.addpartbundlestream2(bundler, repo, **kwargs)

@getbundle2partsgenerator('changegroup')
def _getbundlechangegrouppart(bundler, repo, source, bundlecaps=None,
                              b2caps=None, heads=None, common=None, **kwargs):
    """add a changegroup part to the requested bundle"""
    if not kwargs.get(r'cg', True):
        return

    version = '01'
    cgversions = b2caps.get('changegroup')
    if cgversions:  # 3.1 and 3.2 ship with an empty value
        cgversions = [v for v in cgversions
                      if v in changegroup.supportedoutgoingversions(repo)]
        if not cgversions:
            raise ValueError(_('no common changegroup version'))
        version = max(cgversions)

    outgoing = _computeoutgoing(repo, heads, common)
    if not outgoing.missing:
        return

    if kwargs.get(r'narrow', False):
        include = sorted(filter(bool, kwargs.get(r'includepats', [])))
        exclude = sorted(filter(bool, kwargs.get(r'excludepats', [])))
        matcher = narrowspec.match(repo.root, include=include, exclude=exclude)
    else:
        matcher = None

    cgstream = changegroup.makestream(repo, outgoing, version, source,
                                      bundlecaps=bundlecaps, matcher=matcher)

    part = bundler.newpart('changegroup', data=cgstream)
    if cgversions:
        part.addparam('version', version)

    part.addparam('nbchanges', '%d' % len(outgoing.missing),
                  mandatory=False)

    if 'treemanifest' in repo.requirements:
        part.addparam('treemanifest', '1')

    if (kwargs.get(r'narrow', False) and kwargs.get(r'narrow_acl', False)
        and (include or exclude)):
        narrowspecpart = bundler.newpart('narrow:spec')
        if include:
            narrowspecpart.addparam(
                'include', '\n'.join(include), mandatory=True)
        if exclude:
            narrowspecpart.addparam(
                'exclude', '\n'.join(exclude), mandatory=True)

@getbundle2partsgenerator('bookmarks')
def _getbundlebookmarkpart(bundler, repo, source, bundlecaps=None,
                              b2caps=None, **kwargs):
    """add a bookmark part to the requested bundle"""
    if not kwargs.get(r'bookmarks', False):
        return
    if 'bookmarks' not in b2caps:
        raise ValueError(_('no common bookmarks exchange method'))
    books  = bookmod.listbinbookmarks(repo)
    data = bookmod.binaryencode(books)
    if data:
        bundler.newpart('bookmarks', data=data)

@getbundle2partsgenerator('listkeys')
def _getbundlelistkeysparts(bundler, repo, source, bundlecaps=None,
                            b2caps=None, **kwargs):
    """add parts containing listkeys namespaces to the requested bundle"""
    listkeys = kwargs.get(r'listkeys', ())
    for namespace in listkeys:
        part = bundler.newpart('listkeys')
        part.addparam('namespace', namespace)
        keys = repo.listkeys(namespace).items()
        part.data = pushkey.encodekeys(keys)

@getbundle2partsgenerator('obsmarkers')
def _getbundleobsmarkerpart(bundler, repo, source, bundlecaps=None,
                            b2caps=None, heads=None, **kwargs):
    """add an obsolescence markers part to the requested bundle"""
    if kwargs.get(r'obsmarkers', False):
        if heads is None:
            heads = repo.heads()
        subset = [c.node() for c in repo.set('::%ln', heads)]
        markers = repo.obsstore.relevantmarkers(subset)
        markers = sorted(markers)
        bundle2.buildobsmarkerspart(bundler, markers)

@getbundle2partsgenerator('phases')
def _getbundlephasespart(bundler, repo, source, bundlecaps=None,
                            b2caps=None, heads=None, **kwargs):
    """add phase heads part to the requested bundle"""
    if kwargs.get(r'phases', False):
        if not 'heads' in b2caps.get('phases'):
            raise ValueError(_('no common phases exchange method'))
        if heads is None:
            heads = repo.heads()

        headsbyphase = collections.defaultdict(set)
        if repo.publishing():
            headsbyphase[phases.public] = heads
        else:
            # find the appropriate heads to move

            phase = repo._phasecache.phase
            node = repo.changelog.node
            rev = repo.changelog.rev
            for h in heads:
                headsbyphase[phase(repo, rev(h))].add(h)
            seenphases = list(headsbyphase.keys())

            # We do not handle anything but public and draft phase for now)
            if seenphases:
                assert max(seenphases) <= phases.draft

            # if client is pulling non-public changesets, we need to find
            # intermediate public heads.
            draftheads = headsbyphase.get(phases.draft, set())
            if draftheads:
                publicheads = headsbyphase.get(phases.public, set())

                revset = 'heads(only(%ln, %ln) and public())'
                extraheads = repo.revs(revset, draftheads, publicheads)
                for r in extraheads:
                    headsbyphase[phases.public].add(node(r))

        # transform data in a format used by the encoding function
        phasemapping = []
        for phase in phases.allphases:
            phasemapping.append(sorted(headsbyphase[phase]))

        # generate the actual part
        phasedata = phases.binaryencode(phasemapping)
        bundler.newpart('phase-heads', data=phasedata)

@getbundle2partsgenerator('hgtagsfnodes')
def _getbundletagsfnodes(bundler, repo, source, bundlecaps=None,
                         b2caps=None, heads=None, common=None,
                         **kwargs):
    """Transfer the .hgtags filenodes mapping.

    Only values for heads in this bundle will be transferred.

    The part data consists of pairs of 20 byte changeset node and .hgtags
    filenodes raw values.
    """
    # Don't send unless:
    # - changeset are being exchanged,
    # - the client supports it.
    if not (kwargs.get(r'cg', True) and 'hgtagsfnodes' in b2caps):
        return

    outgoing = _computeoutgoing(repo, heads, common)
    bundle2.addparttagsfnodescache(repo, bundler, outgoing)

@getbundle2partsgenerator('cache:rev-branch-cache')
def _getbundlerevbranchcache(bundler, repo, source, bundlecaps=None,
                             b2caps=None, heads=None, common=None,
                             **kwargs):
    """Transfer the rev-branch-cache mapping

    The payload is a series of data related to each branch

    1) branch name length
    2) number of open heads
    3) number of closed heads
    4) open heads nodes
    5) closed heads nodes
    """
    # Don't send unless:
    # - changeset are being exchanged,
    # - the client supports it.
    # - narrow bundle isn't in play (not currently compatible).
    if (not kwargs.get(r'cg', True)
        or 'rev-branch-cache' not in b2caps
        or kwargs.get(r'narrow', False)
        or repo.ui.has_section(_NARROWACL_SECTION)):
        return

    outgoing = _computeoutgoing(repo, heads, common)
    bundle2.addpartrevbranchcache(repo, bundler, outgoing)

def check_heads(repo, their_heads, context):
    """check if the heads of a repo have been modified

    Used by peer for unbundling.
    """
    heads = repo.heads()
    heads_hash = hashlib.sha1(''.join(sorted(heads))).digest()
    if not (their_heads == ['force'] or their_heads == heads or
            their_heads == ['hashed', heads_hash]):
        # someone else committed/pushed/unbundled while we
        # were transferring data
        raise error.PushRaced('repository changed while %s - '
                              'please try again' % context)

def unbundle(repo, cg, heads, source, url):
    """Apply a bundle to a repo.

    this function makes sure the repo is locked during the application and have
    mechanism to check that no push race occurred between the creation of the
    bundle and its application.

    If the push was raced as PushRaced exception is raised."""
    r = 0
    # need a transaction when processing a bundle2 stream
    # [wlock, lock, tr] - needs to be an array so nested functions can modify it
    lockandtr = [None, None, None]
    recordout = None
    # quick fix for output mismatch with bundle2 in 3.4
    captureoutput = repo.ui.configbool('experimental', 'bundle2-output-capture')
    if url.startswith('remote:http:') or url.startswith('remote:https:'):
        captureoutput = True
    try:
        # note: outside bundle1, 'heads' is expected to be empty and this
        # 'check_heads' call wil be a no-op
        check_heads(repo, heads, 'uploading changes')
        # push can proceed
        if not isinstance(cg, bundle2.unbundle20):
            # legacy case: bundle1 (changegroup 01)
            txnname = "\n".join([source, util.hidepassword(url)])
            with repo.lock(), repo.transaction(txnname) as tr:
                op = bundle2.applybundle(repo, cg, tr, source, url)
                r = bundle2.combinechangegroupresults(op)
        else:
            r = None
            try:
                def gettransaction():
                    if not lockandtr[2]:
                        lockandtr[0] = repo.wlock()
                        lockandtr[1] = repo.lock()
                        lockandtr[2] = repo.transaction(source)
                        lockandtr[2].hookargs['source'] = source
                        lockandtr[2].hookargs['url'] = url
                        lockandtr[2].hookargs['bundle2'] = '1'
                    return lockandtr[2]

                # Do greedy locking by default until we're satisfied with lazy
                # locking.
                if not repo.ui.configbool('experimental', 'bundle2lazylocking'):
                    gettransaction()

                op = bundle2.bundleoperation(repo, gettransaction,
                                             captureoutput=captureoutput,
                                             source='push')
                try:
                    op = bundle2.processbundle(repo, cg, op=op)
                finally:
                    r = op.reply
                    if captureoutput and r is not None:
                        repo.ui.pushbuffer(error=True, subproc=True)
                        def recordout(output):
                            r.newpart('output', data=output, mandatory=False)
                if lockandtr[2] is not None:
                    lockandtr[2].close()
            except BaseException as exc:
                exc.duringunbundle2 = True
                if captureoutput and r is not None:
                    parts = exc._bundle2salvagedoutput = r.salvageoutput()
                    def recordout(output):
                        part = bundle2.bundlepart('output', data=output,
                                                  mandatory=False)
                        parts.append(part)
                raise
    finally:
        lockmod.release(lockandtr[2], lockandtr[1], lockandtr[0])
        if recordout is not None:
            recordout(repo.ui.popbuffer())
    return r

def _maybeapplyclonebundle(pullop):
    """Apply a clone bundle from a remote, if possible."""

    repo = pullop.repo
    remote = pullop.remote

    if not repo.ui.configbool('ui', 'clonebundles'):
        return

    # Only run if local repo is empty.
    if len(repo):
        return

    if pullop.heads:
        return

    if not remote.capable('clonebundles'):
        return

    with remote.commandexecutor() as e:
        res = e.callcommand('clonebundles', {}).result()

    # If we call the wire protocol command, that's good enough to record the
    # attempt.
    pullop.clonebundleattempted = True

    entries = parseclonebundlesmanifest(repo, res)
    if not entries:
        repo.ui.note(_('no clone bundles available on remote; '
                       'falling back to regular clone\n'))
        return

    entries = filterclonebundleentries(
        repo, entries, streamclonerequested=pullop.streamclonerequested)

    if not entries:
        # There is a thundering herd concern here. However, if a server
        # operator doesn't advertise bundles appropriate for its clients,
        # they deserve what's coming. Furthermore, from a client's
        # perspective, no automatic fallback would mean not being able to
        # clone!
        repo.ui.warn(_('no compatible clone bundles available on server; '
                       'falling back to regular clone\n'))
        repo.ui.warn(_('(you may want to report this to the server '
                       'operator)\n'))
        return

    entries = sortclonebundleentries(repo.ui, entries)

    url = entries[0]['URL']
    repo.ui.status(_('applying clone bundle from %s\n') % url)
    if trypullbundlefromurl(repo.ui, repo, url):
        repo.ui.status(_('finished applying clone bundle\n'))
    # Bundle failed.
    #
    # We abort by default to avoid the thundering herd of
    # clients flooding a server that was expecting expensive
    # clone load to be offloaded.
    elif repo.ui.configbool('ui', 'clonebundlefallback'):
        repo.ui.warn(_('falling back to normal clone\n'))
    else:
        raise error.Abort(_('error applying bundle'),
                          hint=_('if this error persists, consider contacting '
                                 'the server operator or disable clone '
                                 'bundles via '
                                 '"--config ui.clonebundles=false"'))

def parseclonebundlesmanifest(repo, s):
    """Parses the raw text of a clone bundles manifest.

    Returns a list of dicts. The dicts have a ``URL`` key corresponding
    to the URL and other keys are the attributes for the entry.
    """
    m = []
    for line in s.splitlines():
        fields = line.split()
        if not fields:
            continue
        attrs = {'URL': fields[0]}
        for rawattr in fields[1:]:
            key, value = rawattr.split('=', 1)
            key = urlreq.unquote(key)
            value = urlreq.unquote(value)
            attrs[key] = value

            # Parse BUNDLESPEC into components. This makes client-side
            # preferences easier to specify since you can prefer a single
            # component of the BUNDLESPEC.
            if key == 'BUNDLESPEC':
                try:
                    bundlespec = parsebundlespec(repo, value)
                    attrs['COMPRESSION'] = bundlespec.compression
                    attrs['VERSION'] = bundlespec.version
                except error.InvalidBundleSpecification:
                    pass
                except error.UnsupportedBundleSpecification:
                    pass

        m.append(attrs)

    return m

def isstreamclonespec(bundlespec):
    # Stream clone v1
    if (bundlespec.wirecompression == 'UN' and bundlespec.wireversion == 's1'):
        return True

    # Stream clone v2
    if (bundlespec.wirecompression == 'UN' and \
        bundlespec.wireversion == '02' and \
        bundlespec.contentopts.get('streamv2')):
        return True

    return False

def filterclonebundleentries(repo, entries, streamclonerequested=False):
    """Remove incompatible clone bundle manifest entries.

    Accepts a list of entries parsed with ``parseclonebundlesmanifest``
    and returns a new list consisting of only the entries that this client
    should be able to apply.

    There is no guarantee we'll be able to apply all returned entries because
    the metadata we use to filter on may be missing or wrong.
    """
    newentries = []
    for entry in entries:
        spec = entry.get('BUNDLESPEC')
        if spec:
            try:
                bundlespec = parsebundlespec(repo, spec, strict=True)

                # If a stream clone was requested, filter out non-streamclone
                # entries.
                if streamclonerequested and not isstreamclonespec(bundlespec):
                    repo.ui.debug('filtering %s because not a stream clone\n' %
                                  entry['URL'])
                    continue

            except error.InvalidBundleSpecification as e:
                repo.ui.debug(stringutil.forcebytestr(e) + '\n')
                continue
            except error.UnsupportedBundleSpecification as e:
                repo.ui.debug('filtering %s because unsupported bundle '
                              'spec: %s\n' % (
                                  entry['URL'], stringutil.forcebytestr(e)))
                continue
        # If we don't have a spec and requested a stream clone, we don't know
        # what the entry is so don't attempt to apply it.
        elif streamclonerequested:
            repo.ui.debug('filtering %s because cannot determine if a stream '
                          'clone bundle\n' % entry['URL'])
            continue

        if 'REQUIRESNI' in entry and not sslutil.hassni:
            repo.ui.debug('filtering %s because SNI not supported\n' %
                          entry['URL'])
            continue

        newentries.append(entry)

    return newentries

class clonebundleentry(object):
    """Represents an item in a clone bundles manifest.

    This rich class is needed to support sorting since sorted() in Python 3
    doesn't support ``cmp`` and our comparison is complex enough that ``key=``
    won't work.
    """

    def __init__(self, value, prefers):
        self.value = value
        self.prefers = prefers

    def _cmp(self, other):
        for prefkey, prefvalue in self.prefers:
            avalue = self.value.get(prefkey)
            bvalue = other.value.get(prefkey)

            # Special case for b missing attribute and a matches exactly.
            if avalue is not None and bvalue is None and avalue == prefvalue:
                return -1

            # Special case for a missing attribute and b matches exactly.
            if bvalue is not None and avalue is None and bvalue == prefvalue:
                return 1

            # We can't compare unless attribute present on both.
            if avalue is None or bvalue is None:
                continue

            # Same values should fall back to next attribute.
            if avalue == bvalue:
                continue

            # Exact matches come first.
            if avalue == prefvalue:
                return -1
            if bvalue == prefvalue:
                return 1

            # Fall back to next attribute.
            continue

        # If we got here we couldn't sort by attributes and prefers. Fall
        # back to index order.
        return 0

    def __lt__(self, other):
        return self._cmp(other) < 0

    def __gt__(self, other):
        return self._cmp(other) > 0

    def __eq__(self, other):
        return self._cmp(other) == 0

    def __le__(self, other):
        return self._cmp(other) <= 0

    def __ge__(self, other):
        return self._cmp(other) >= 0

    def __ne__(self, other):
        return self._cmp(other) != 0

def sortclonebundleentries(ui, entries):
    prefers = ui.configlist('ui', 'clonebundleprefers')
    if not prefers:
        return list(entries)

    prefers = [p.split('=', 1) for p in prefers]

    items = sorted(clonebundleentry(v, prefers) for v in entries)
    return [i.value for i in items]

def trypullbundlefromurl(ui, repo, url):
    """Attempt to apply a bundle from a URL."""
    with repo.lock(), repo.transaction('bundleurl') as tr:
        try:
            fh = urlmod.open(ui, url)
            cg = readbundle(ui, fh, 'stream')

            if isinstance(cg, streamclone.streamcloneapplier):
                cg.apply(repo)
            else:
                bundle2.applybundle(repo, cg, tr, 'clonebundles', url)
            return True
        except urlerr.httperror as e:
            ui.warn(_('HTTP error fetching bundle: %s\n') %
                    stringutil.forcebytestr(e))
        except urlerr.urlerror as e:
            ui.warn(_('error fetching bundle: %s\n') %
                    stringutil.forcebytestr(e.reason))

        return False
