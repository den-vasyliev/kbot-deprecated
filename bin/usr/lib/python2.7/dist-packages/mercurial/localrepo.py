# localrepo.py - read/write repository class for mercurial
#
# Copyright 2005-2007 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import errno
import hashlib
import os
import random
import sys
import time
import weakref

from .i18n import _
from .node import (
    bin,
    hex,
    nullid,
    nullrev,
    short,
)
from . import (
    bookmarks,
    branchmap,
    bundle2,
    changegroup,
    changelog,
    color,
    context,
    dirstate,
    dirstateguard,
    discovery,
    encoding,
    error,
    exchange,
    extensions,
    filelog,
    hook,
    lock as lockmod,
    manifest,
    match as matchmod,
    merge as mergemod,
    mergeutil,
    namespaces,
    narrowspec,
    obsolete,
    pathutil,
    phases,
    pushkey,
    pycompat,
    repository,
    repoview,
    revset,
    revsetlang,
    scmutil,
    sparse,
    store as storemod,
    subrepoutil,
    tags as tagsmod,
    transaction,
    txnutil,
    util,
    vfs as vfsmod,
)
from .utils import (
    interfaceutil,
    procutil,
    stringutil,
)

from .revlogutils import (
    constants as revlogconst,
)

release = lockmod.release
urlerr = util.urlerr
urlreq = util.urlreq

# set of (path, vfs-location) tuples. vfs-location is:
# - 'plain for vfs relative paths
# - '' for svfs relative paths
_cachedfiles = set()

class _basefilecache(scmutil.filecache):
    """All filecache usage on repo are done for logic that should be unfiltered
    """
    def __get__(self, repo, type=None):
        if repo is None:
            return self
        return super(_basefilecache, self).__get__(repo.unfiltered(), type)
    def __set__(self, repo, value):
        return super(_basefilecache, self).__set__(repo.unfiltered(), value)
    def __delete__(self, repo):
        return super(_basefilecache, self).__delete__(repo.unfiltered())

class repofilecache(_basefilecache):
    """filecache for files in .hg but outside of .hg/store"""
    def __init__(self, *paths):
        super(repofilecache, self).__init__(*paths)
        for path in paths:
            _cachedfiles.add((path, 'plain'))

    def join(self, obj, fname):
        return obj.vfs.join(fname)

class storecache(_basefilecache):
    """filecache for files in the store"""
    def __init__(self, *paths):
        super(storecache, self).__init__(*paths)
        for path in paths:
            _cachedfiles.add((path, ''))

    def join(self, obj, fname):
        return obj.sjoin(fname)

def isfilecached(repo, name):
    """check if a repo has already cached "name" filecache-ed property

    This returns (cachedobj-or-None, iscached) tuple.
    """
    cacheentry = repo.unfiltered()._filecache.get(name, None)
    if not cacheentry:
        return None, False
    return cacheentry.obj, True

class unfilteredpropertycache(util.propertycache):
    """propertycache that apply to unfiltered repo only"""

    def __get__(self, repo, type=None):
        unfi = repo.unfiltered()
        if unfi is repo:
            return super(unfilteredpropertycache, self).__get__(unfi)
        return getattr(unfi, self.name)

class filteredpropertycache(util.propertycache):
    """propertycache that must take filtering in account"""

    def cachevalue(self, obj, value):
        object.__setattr__(obj, self.name, value)


def hasunfilteredcache(repo, name):
    """check if a repo has an unfilteredpropertycache value for <name>"""
    return name in vars(repo.unfiltered())

def unfilteredmethod(orig):
    """decorate method that always need to be run on unfiltered version"""
    def wrapper(repo, *args, **kwargs):
        return orig(repo.unfiltered(), *args, **kwargs)
    return wrapper

moderncaps = {'lookup', 'branchmap', 'pushkey', 'known', 'getbundle',
              'unbundle'}
legacycaps = moderncaps.union({'changegroupsubset'})

@interfaceutil.implementer(repository.ipeercommandexecutor)
class localcommandexecutor(object):
    def __init__(self, peer):
        self._peer = peer
        self._sent = False
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, exctype, excvalue, exctb):
        self.close()

    def callcommand(self, command, args):
        if self._sent:
            raise error.ProgrammingError('callcommand() cannot be used after '
                                         'sendcommands()')

        if self._closed:
            raise error.ProgrammingError('callcommand() cannot be used after '
                                         'close()')

        # We don't need to support anything fancy. Just call the named
        # method on the peer and return a resolved future.
        fn = getattr(self._peer, pycompat.sysstr(command))

        f = pycompat.futures.Future()

        try:
            result = fn(**pycompat.strkwargs(args))
        except Exception:
            pycompat.future_set_exception_info(f, sys.exc_info()[1:])
        else:
            f.set_result(result)

        return f

    def sendcommands(self):
        self._sent = True

    def close(self):
        self._closed = True

@interfaceutil.implementer(repository.ipeercommands)
class localpeer(repository.peer):
    '''peer for a local repo; reflects only the most recent API'''

    def __init__(self, repo, caps=None):
        super(localpeer, self).__init__()

        if caps is None:
            caps = moderncaps.copy()
        self._repo = repo.filtered('served')
        self.ui = repo.ui
        self._caps = repo._restrictcapabilities(caps)

    # Begin of _basepeer interface.

    def url(self):
        return self._repo.url()

    def local(self):
        return self._repo

    def peer(self):
        return self

    def canpush(self):
        return True

    def close(self):
        self._repo.close()

    # End of _basepeer interface.

    # Begin of _basewirecommands interface.

    def branchmap(self):
        return self._repo.branchmap()

    def capabilities(self):
        return self._caps

    def clonebundles(self):
        return self._repo.tryread('clonebundles.manifest')

    def debugwireargs(self, one, two, three=None, four=None, five=None):
        """Used to test argument passing over the wire"""
        return "%s %s %s %s %s" % (one, two, pycompat.bytestr(three),
                                   pycompat.bytestr(four),
                                   pycompat.bytestr(five))

    def getbundle(self, source, heads=None, common=None, bundlecaps=None,
                  **kwargs):
        chunks = exchange.getbundlechunks(self._repo, source, heads=heads,
                                          common=common, bundlecaps=bundlecaps,
                                          **kwargs)[1]
        cb = util.chunkbuffer(chunks)

        if exchange.bundle2requested(bundlecaps):
            # When requesting a bundle2, getbundle returns a stream to make the
            # wire level function happier. We need to build a proper object
            # from it in local peer.
            return bundle2.getunbundler(self.ui, cb)
        else:
            return changegroup.getunbundler('01', cb, None)

    def heads(self):
        return self._repo.heads()

    def known(self, nodes):
        return self._repo.known(nodes)

    def listkeys(self, namespace):
        return self._repo.listkeys(namespace)

    def lookup(self, key):
        return self._repo.lookup(key)

    def pushkey(self, namespace, key, old, new):
        return self._repo.pushkey(namespace, key, old, new)

    def stream_out(self):
        raise error.Abort(_('cannot perform stream clone against local '
                            'peer'))

    def unbundle(self, bundle, heads, url):
        """apply a bundle on a repo

        This function handles the repo locking itself."""
        try:
            try:
                bundle = exchange.readbundle(self.ui, bundle, None)
                ret = exchange.unbundle(self._repo, bundle, heads, 'push', url)
                if util.safehasattr(ret, 'getchunks'):
                    # This is a bundle20 object, turn it into an unbundler.
                    # This little dance should be dropped eventually when the
                    # API is finally improved.
                    stream = util.chunkbuffer(ret.getchunks())
                    ret = bundle2.getunbundler(self.ui, stream)
                return ret
            except Exception as exc:
                # If the exception contains output salvaged from a bundle2
                # reply, we need to make sure it is printed before continuing
                # to fail. So we build a bundle2 with such output and consume
                # it directly.
                #
                # This is not very elegant but allows a "simple" solution for
                # issue4594
                output = getattr(exc, '_bundle2salvagedoutput', ())
                if output:
                    bundler = bundle2.bundle20(self._repo.ui)
                    for out in output:
                        bundler.addpart(out)
                    stream = util.chunkbuffer(bundler.getchunks())
                    b = bundle2.getunbundler(self.ui, stream)
                    bundle2.processbundle(self._repo, b)
                raise
        except error.PushRaced as exc:
            raise error.ResponseError(_('push failed:'),
                                      stringutil.forcebytestr(exc))

    # End of _basewirecommands interface.

    # Begin of peer interface.

    def commandexecutor(self):
        return localcommandexecutor(self)

    # End of peer interface.

@interfaceutil.implementer(repository.ipeerlegacycommands)
class locallegacypeer(localpeer):
    '''peer extension which implements legacy methods too; used for tests with
    restricted capabilities'''

    def __init__(self, repo):
        super(locallegacypeer, self).__init__(repo, caps=legacycaps)

    # Begin of baselegacywirecommands interface.

    def between(self, pairs):
        return self._repo.between(pairs)

    def branches(self, nodes):
        return self._repo.branches(nodes)

    def changegroup(self, nodes, source):
        outgoing = discovery.outgoing(self._repo, missingroots=nodes,
                                      missingheads=self._repo.heads())
        return changegroup.makechangegroup(self._repo, outgoing, '01', source)

    def changegroupsubset(self, bases, heads, source):
        outgoing = discovery.outgoing(self._repo, missingroots=bases,
                                      missingheads=heads)
        return changegroup.makechangegroup(self._repo, outgoing, '01', source)

    # End of baselegacywirecommands interface.

# Increment the sub-version when the revlog v2 format changes to lock out old
# clients.
REVLOGV2_REQUIREMENT = 'exp-revlogv2.0'

# A repository with the sparserevlog feature will have delta chains that
# can spread over a larger span. Sparse reading cuts these large spans into
# pieces, so that each piece isn't too big.
# Without the sparserevlog capability, reading from the repository could use
# huge amounts of memory, because the whole span would be read at once,
# including all the intermediate revisions that aren't pertinent for the chain.
# This is why once a repository has enabled sparse-read, it becomes required.
SPARSEREVLOG_REQUIREMENT = 'sparserevlog'

# Functions receiving (ui, features) that extensions can register to impact
# the ability to load repositories with custom requirements. Only
# functions defined in loaded extensions are called.
#
# The function receives a set of requirement strings that the repository
# is capable of opening. Functions will typically add elements to the
# set to reflect that the extension knows how to handle that requirements.
featuresetupfuncs = set()

def makelocalrepository(baseui, path, intents=None):
    """Create a local repository object.

    Given arguments needed to construct a local repository, this function
    performs various early repository loading functionality (such as
    reading the ``.hg/requires`` and ``.hg/hgrc`` files), validates that
    the repository can be opened, derives a type suitable for representing
    that repository, and returns an instance of it.

    The returned object conforms to the ``repository.completelocalrepository``
    interface.

    The repository type is derived by calling a series of factory functions
    for each aspect/interface of the final repository. These are defined by
    ``REPO_INTERFACES``.

    Each factory function is called to produce a type implementing a specific
    interface. The cumulative list of returned types will be combined into a
    new type and that type will be instantiated to represent the local
    repository.

    The factory functions each receive various state that may be consulted
    as part of deriving a type.

    Extensions should wrap these factory functions to customize repository type
    creation. Note that an extension's wrapped function may be called even if
    that extension is not loaded for the repo being constructed. Extensions
    should check if their ``__name__`` appears in the
    ``extensionmodulenames`` set passed to the factory function and no-op if
    not.
    """
    ui = baseui.copy()
    # Prevent copying repo configuration.
    ui.copy = baseui.copy

    # Working directory VFS rooted at repository root.
    wdirvfs = vfsmod.vfs(path, expandpath=True, realpath=True)

    # Main VFS for .hg/ directory.
    hgpath = wdirvfs.join(b'.hg')
    hgvfs = vfsmod.vfs(hgpath, cacheaudited=True)

    # The .hg/ path should exist and should be a directory. All other
    # cases are errors.
    if not hgvfs.isdir():
        try:
            hgvfs.stat()
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

        raise error.RepoError(_(b'repository %s not found') % path)

    # .hg/requires file contains a newline-delimited list of
    # features/capabilities the opener (us) must have in order to use
    # the repository. This file was introduced in Mercurial 0.9.2,
    # which means very old repositories may not have one. We assume
    # a missing file translates to no requirements.
    try:
        requirements = set(hgvfs.read(b'requires').splitlines())
    except IOError as e:
        if e.errno != errno.ENOENT:
            raise
        requirements = set()

    # The .hg/hgrc file may load extensions or contain config options
    # that influence repository construction. Attempt to load it and
    # process any new extensions that it may have pulled in.
    try:
        ui.readconfig(hgvfs.join(b'hgrc'), root=wdirvfs.base)
        # Run this before extensions.loadall() so extensions can be
        # automatically enabled.
        afterhgrcload(ui, wdirvfs, hgvfs, requirements)
    except IOError:
        pass
    else:
        extensions.loadall(ui)

    # Set of module names of extensions loaded for this repository.
    extensionmodulenames = {m.__name__ for n, m in extensions.extensions(ui)}

    supportedrequirements = gathersupportedrequirements(ui)

    # We first validate the requirements are known.
    ensurerequirementsrecognized(requirements, supportedrequirements)

    # Then we validate that the known set is reasonable to use together.
    ensurerequirementscompatible(ui, requirements)

    # TODO there are unhandled edge cases related to opening repositories with
    # shared storage. If storage is shared, we should also test for requirements
    # compatibility in the pointed-to repo. This entails loading the .hg/hgrc in
    # that repo, as that repo may load extensions needed to open it. This is a
    # bit complicated because we don't want the other hgrc to overwrite settings
    # in this hgrc.
    #
    # This bug is somewhat mitigated by the fact that we copy the .hg/requires
    # file when sharing repos. But if a requirement is added after the share is
    # performed, thereby introducing a new requirement for the opener, we may
    # will not see that and could encounter a run-time error interacting with
    # that shared store since it has an unknown-to-us requirement.

    # At this point, we know we should be capable of opening the repository.
    # Now get on with doing that.

    features = set()

    # The "store" part of the repository holds versioned data. How it is
    # accessed is determined by various requirements. The ``shared`` or
    # ``relshared`` requirements indicate the store lives in the path contained
    # in the ``.hg/sharedpath`` file. This is an absolute path for
    # ``shared`` and relative to ``.hg/`` for ``relshared``.
    if b'shared' in requirements or b'relshared' in requirements:
        sharedpath = hgvfs.read(b'sharedpath').rstrip(b'\n')
        if b'relshared' in requirements:
            sharedpath = hgvfs.join(sharedpath)

        sharedvfs = vfsmod.vfs(sharedpath, realpath=True)

        if not sharedvfs.exists():
            raise error.RepoError(_(b'.hg/sharedpath points to nonexistent '
                                    b'directory %s') % sharedvfs.base)

        features.add(repository.REPO_FEATURE_SHARED_STORAGE)

        storebasepath = sharedvfs.base
        cachepath = sharedvfs.join(b'cache')
    else:
        storebasepath = hgvfs.base
        cachepath = hgvfs.join(b'cache')

    # The store has changed over time and the exact layout is dictated by
    # requirements. The store interface abstracts differences across all
    # of them.
    store = makestore(requirements, storebasepath,
                      lambda base: vfsmod.vfs(base, cacheaudited=True))
    hgvfs.createmode = store.createmode

    storevfs = store.vfs
    storevfs.options = resolvestorevfsoptions(ui, requirements, features)

    # The cache vfs is used to manage cache files.
    cachevfs = vfsmod.vfs(cachepath, cacheaudited=True)
    cachevfs.createmode = store.createmode

    # Now resolve the type for the repository object. We do this by repeatedly
    # calling a factory function to produces types for specific aspects of the
    # repo's operation. The aggregate returned types are used as base classes
    # for a dynamically-derived type, which will represent our new repository.

    bases = []
    extrastate = {}

    for iface, fn in REPO_INTERFACES:
        # We pass all potentially useful state to give extensions tons of
        # flexibility.
        typ = fn()(ui=ui,
                 intents=intents,
                 requirements=requirements,
                 features=features,
                 wdirvfs=wdirvfs,
                 hgvfs=hgvfs,
                 store=store,
                 storevfs=storevfs,
                 storeoptions=storevfs.options,
                 cachevfs=cachevfs,
                 extensionmodulenames=extensionmodulenames,
                 extrastate=extrastate,
                 baseclasses=bases)

        if not isinstance(typ, type):
            raise error.ProgrammingError('unable to construct type for %s' %
                                         iface)

        bases.append(typ)

    # type() allows you to use characters in type names that wouldn't be
    # recognized as Python symbols in source code. We abuse that to add
    # rich information about our constructed repo.
    name = pycompat.sysstr(b'derivedrepo:%s<%s>' % (
        wdirvfs.base,
        b','.join(sorted(requirements))))

    cls = type(name, tuple(bases), {})

    return cls(
        baseui=baseui,
        ui=ui,
        origroot=path,
        wdirvfs=wdirvfs,
        hgvfs=hgvfs,
        requirements=requirements,
        supportedrequirements=supportedrequirements,
        sharedpath=storebasepath,
        store=store,
        cachevfs=cachevfs,
        features=features,
        intents=intents)

def afterhgrcload(ui, wdirvfs, hgvfs, requirements):
    """Perform additional actions after .hg/hgrc is loaded.

    This function is called during repository loading immediately after
    the .hg/hgrc file is loaded and before per-repo extensions are loaded.

    The function can be used to validate configs, automatically add
    options (including extensions) based on requirements, etc.
    """

    # Map of requirements to list of extensions to load automatically when
    # requirement is present.
    autoextensions = {
        b'largefiles': [b'largefiles'],
        b'lfs': [b'lfs'],
    }

    for requirement, names in sorted(autoextensions.items()):
        if requirement not in requirements:
            continue

        for name in names:
            if not ui.hasconfig(b'extensions', name):
                ui.setconfig(b'extensions', name, b'', source='autoload')

def gathersupportedrequirements(ui):
    """Determine the complete set of recognized requirements."""
    # Start with all requirements supported by this file.
    supported = set(localrepository._basesupported)

    # Execute ``featuresetupfuncs`` entries if they belong to an extension
    # relevant to this ui instance.
    modules = {m.__name__ for n, m in extensions.extensions(ui)}

    for fn in featuresetupfuncs:
        if fn.__module__ in modules:
            fn(ui, supported)

    # Add derived requirements from registered compression engines.
    for name in util.compengines:
        engine = util.compengines[name]
        if engine.revlogheader():
            supported.add(b'exp-compression-%s' % name)

    return supported

def ensurerequirementsrecognized(requirements, supported):
    """Validate that a set of local requirements is recognized.

    Receives a set of requirements. Raises an ``error.RepoError`` if there
    exists any requirement in that set that currently loaded code doesn't
    recognize.

    Returns a set of supported requirements.
    """
    missing = set()

    for requirement in requirements:
        if requirement in supported:
            continue

        if not requirement or not requirement[0:1].isalnum():
            raise error.RequirementError(_(b'.hg/requires file is corrupt'))

        missing.add(requirement)

    if missing:
        raise error.RequirementError(
            _(b'repository requires features unknown to this Mercurial: %s') %
            b' '.join(sorted(missing)),
            hint=_(b'see https://mercurial-scm.org/wiki/MissingRequirement '
                   b'for more information'))

def ensurerequirementscompatible(ui, requirements):
    """Validates that a set of recognized requirements is mutually compatible.

    Some requirements may not be compatible with others or require
    config options that aren't enabled. This function is called during
    repository opening to ensure that the set of requirements needed
    to open a repository is sane and compatible with config options.

    Extensions can monkeypatch this function to perform additional
    checking.

    ``error.RepoError`` should be raised on failure.
    """
    if b'exp-sparse' in requirements and not sparse.enabled:
        raise error.RepoError(_(b'repository is using sparse feature but '
                                b'sparse is not enabled; enable the '
                                b'"sparse" extensions to access'))

def makestore(requirements, path, vfstype):
    """Construct a storage object for a repository."""
    if b'store' in requirements:
        if b'fncache' in requirements:
            return storemod.fncachestore(path, vfstype,
                                         b'dotencode' in requirements)

        return storemod.encodedstore(path, vfstype)

    return storemod.basicstore(path, vfstype)

def resolvestorevfsoptions(ui, requirements, features):
    """Resolve the options to pass to the store vfs opener.

    The returned dict is used to influence behavior of the storage layer.
    """
    options = {}

    if b'treemanifest' in requirements:
        options[b'treemanifest'] = True

    # experimental config: format.manifestcachesize
    manifestcachesize = ui.configint(b'format', b'manifestcachesize')
    if manifestcachesize is not None:
        options[b'manifestcachesize'] = manifestcachesize

    # In the absence of another requirement superseding a revlog-related
    # requirement, we have to assume the repo is using revlog version 0.
    # This revlog format is super old and we don't bother trying to parse
    # opener options for it because those options wouldn't do anything
    # meaningful on such old repos.
    if b'revlogv1' in requirements or REVLOGV2_REQUIREMENT in requirements:
        options.update(resolverevlogstorevfsoptions(ui, requirements, features))

    return options

def resolverevlogstorevfsoptions(ui, requirements, features):
    """Resolve opener options specific to revlogs."""

    options = {}
    options[b'flagprocessors'] = {}

    if b'revlogv1' in requirements:
        options[b'revlogv1'] = True
    if REVLOGV2_REQUIREMENT in requirements:
        options[b'revlogv2'] = True

    if b'generaldelta' in requirements:
        options[b'generaldelta'] = True

    # experimental config: format.chunkcachesize
    chunkcachesize = ui.configint(b'format', b'chunkcachesize')
    if chunkcachesize is not None:
        options[b'chunkcachesize'] = chunkcachesize

    deltabothparents = ui.configbool(b'storage',
                                     b'revlog.optimize-delta-parent-choice')
    options[b'deltabothparents'] = deltabothparents

    options[b'lazydeltabase'] = not scmutil.gddeltaconfig(ui)

    chainspan = ui.configbytes(b'experimental', b'maxdeltachainspan')
    if 0 <= chainspan:
        options[b'maxdeltachainspan'] = chainspan

    mmapindexthreshold = ui.configbytes(b'experimental',
                                        b'mmapindexthreshold')
    if mmapindexthreshold is not None:
        options[b'mmapindexthreshold'] = mmapindexthreshold

    withsparseread = ui.configbool(b'experimental', b'sparse-read')
    srdensitythres = float(ui.config(b'experimental',
                                     b'sparse-read.density-threshold'))
    srmingapsize = ui.configbytes(b'experimental',
                                  b'sparse-read.min-gap-size')
    options[b'with-sparse-read'] = withsparseread
    options[b'sparse-read-density-threshold'] = srdensitythres
    options[b'sparse-read-min-gap-size'] = srmingapsize

    sparserevlog = SPARSEREVLOG_REQUIREMENT in requirements
    options[b'sparse-revlog'] = sparserevlog
    if sparserevlog:
        options[b'generaldelta'] = True

    maxchainlen = None
    if sparserevlog:
        maxchainlen = revlogconst.SPARSE_REVLOG_MAX_CHAIN_LENGTH
    # experimental config: format.maxchainlen
    maxchainlen = ui.configint(b'format', b'maxchainlen', maxchainlen)
    if maxchainlen is not None:
        options[b'maxchainlen'] = maxchainlen

    for r in requirements:
        if r.startswith(b'exp-compression-'):
            options[b'compengine'] = r[len(b'exp-compression-'):]

    if repository.NARROW_REQUIREMENT in requirements:
        options[b'enableellipsis'] = True

    return options

def makemain(**kwargs):
    """Produce a type conforming to ``ilocalrepositorymain``."""
    return localrepository

@interfaceutil.implementer(repository.ilocalrepositoryfilestorage)
class revlogfilestorage(object):
    """File storage when using revlogs."""

    def file(self, path):
        if path[0] == b'/':
            path = path[1:]

        return filelog.filelog(self.svfs, path)

@interfaceutil.implementer(repository.ilocalrepositoryfilestorage)
class revlognarrowfilestorage(object):
    """File storage when using revlogs and narrow files."""

    def file(self, path):
        if path[0] == b'/':
            path = path[1:]

        return filelog.narrowfilelog(self.svfs, path, self.narrowmatch())

def makefilestorage(requirements, features, **kwargs):
    """Produce a type conforming to ``ilocalrepositoryfilestorage``."""
    features.add(repository.REPO_FEATURE_REVLOG_FILE_STORAGE)
    features.add(repository.REPO_FEATURE_STREAM_CLONE)

    if repository.NARROW_REQUIREMENT in requirements:
        return revlognarrowfilestorage
    else:
        return revlogfilestorage

# List of repository interfaces and factory functions for them. Each
# will be called in order during ``makelocalrepository()`` to iteratively
# derive the final type for a local repository instance. We capture the
# function as a lambda so we don't hold a reference and the module-level
# functions can be wrapped.
REPO_INTERFACES = [
    (repository.ilocalrepositorymain, lambda: makemain),
    (repository.ilocalrepositoryfilestorage, lambda: makefilestorage),
]

@interfaceutil.implementer(repository.ilocalrepositorymain)
class localrepository(object):
    """Main class for representing local repositories.

    All local repositories are instances of this class.

    Constructed on its own, instances of this class are not usable as
    repository objects. To obtain a usable repository object, call
    ``hg.repository()``, ``localrepo.instance()``, or
    ``localrepo.makelocalrepository()``. The latter is the lowest-level.
    ``instance()`` adds support for creating new repositories.
    ``hg.repository()`` adds more extension integration, including calling
    ``reposetup()``. Generally speaking, ``hg.repository()`` should be
    used.
    """

    # obsolete experimental requirements:
    #  - manifestv2: An experimental new manifest format that allowed
    #    for stem compression of long paths. Experiment ended up not
    #    being successful (repository sizes went up due to worse delta
    #    chains), and the code was deleted in 4.6.
    supportedformats = {
        'revlogv1',
        'generaldelta',
        'treemanifest',
        REVLOGV2_REQUIREMENT,
        SPARSEREVLOG_REQUIREMENT,
    }
    _basesupported = supportedformats | {
        'store',
        'fncache',
        'shared',
        'relshared',
        'dotencode',
        'exp-sparse',
        'internal-phase'
    }

    # list of prefix for file which can be written without 'wlock'
    # Extensions should extend this list when needed
    _wlockfreeprefix = {
        # We migh consider requiring 'wlock' for the next
        # two, but pretty much all the existing code assume
        # wlock is not needed so we keep them excluded for
        # now.
        'hgrc',
        'requires',
        # XXX cache is a complicatged business someone
        # should investigate this in depth at some point
        'cache/',
        # XXX shouldn't be dirstate covered by the wlock?
        'dirstate',
        # XXX bisect was still a bit too messy at the time
        # this changeset was introduced. Someone should fix
        # the remainig bit and drop this line
        'bisect.state',
    }

    def __init__(self, baseui, ui, origroot, wdirvfs, hgvfs, requirements,
                 supportedrequirements, sharedpath, store, cachevfs,
                 features, intents=None):
        """Create a new local repository instance.

        Most callers should use ``hg.repository()``, ``localrepo.instance()``,
        or ``localrepo.makelocalrepository()`` for obtaining a new repository
        object.

        Arguments:

        baseui
           ``ui.ui`` instance that ``ui`` argument was based off of.

        ui
           ``ui.ui`` instance for use by the repository.

        origroot
           ``bytes`` path to working directory root of this repository.

        wdirvfs
           ``vfs.vfs`` rooted at the working directory.

        hgvfs
           ``vfs.vfs`` rooted at .hg/

        requirements
           ``set`` of bytestrings representing repository opening requirements.

        supportedrequirements
           ``set`` of bytestrings representing repository requirements that we
           know how to open. May be a supetset of ``requirements``.

        sharedpath
           ``bytes`` Defining path to storage base directory. Points to a
           ``.hg/`` directory somewhere.

        store
           ``store.basicstore`` (or derived) instance providing access to
           versioned storage.

        cachevfs
           ``vfs.vfs`` used for cache files.

        features
           ``set`` of bytestrings defining features/capabilities of this
           instance.

        intents
           ``set`` of system strings indicating what this repo will be used
           for.
        """
        self.baseui = baseui
        self.ui = ui
        self.origroot = origroot
        # vfs rooted at working directory.
        self.wvfs = wdirvfs
        self.root = wdirvfs.base
        # vfs rooted at .hg/. Used to access most non-store paths.
        self.vfs = hgvfs
        self.path = hgvfs.base
        self.requirements = requirements
        self.supported = supportedrequirements
        self.sharedpath = sharedpath
        self.store = store
        self.cachevfs = cachevfs
        self.features = features

        self.filtername = None

        if (self.ui.configbool('devel', 'all-warnings') or
            self.ui.configbool('devel', 'check-locks')):
            self.vfs.audit = self._getvfsward(self.vfs.audit)
        # A list of callback to shape the phase if no data were found.
        # Callback are in the form: func(repo, roots) --> processed root.
        # This list it to be filled by extension during repo setup
        self._phasedefaults = []

        color.setup(self.ui)

        self.spath = self.store.path
        self.svfs = self.store.vfs
        self.sjoin = self.store.join
        if (self.ui.configbool('devel', 'all-warnings') or
            self.ui.configbool('devel', 'check-locks')):
            if util.safehasattr(self.svfs, 'vfs'): # this is filtervfs
                self.svfs.vfs.audit = self._getsvfsward(self.svfs.vfs.audit)
            else: # standard vfs
                self.svfs.audit = self._getsvfsward(self.svfs.audit)

        self._dirstatevalidatewarned = False

        self._branchcaches = {}
        self._revbranchcache = None
        self._filterpats = {}
        self._datafilters = {}
        self._transref = self._lockref = self._wlockref = None

        # A cache for various files under .hg/ that tracks file changes,
        # (used by the filecache decorator)
        #
        # Maps a property name to its util.filecacheentry
        self._filecache = {}

        # hold sets of revision to be filtered
        # should be cleared when something might have changed the filter value:
        # - new changesets,
        # - phase change,
        # - new obsolescence marker,
        # - working directory parent change,
        # - bookmark changes
        self.filteredrevcache = {}

        # post-dirstate-status hooks
        self._postdsstatus = []

        # generic mapping between names and nodes
        self.names = namespaces.namespaces()

        # Key to signature value.
        self._sparsesignaturecache = {}
        # Signature to cached matcher instance.
        self._sparsematchercache = {}

    def _getvfsward(self, origfunc):
        """build a ward for self.vfs"""
        rref = weakref.ref(self)
        def checkvfs(path, mode=None):
            ret = origfunc(path, mode=mode)
            repo = rref()
            if (repo is None
                or not util.safehasattr(repo, '_wlockref')
                or not util.safehasattr(repo, '_lockref')):
                return
            if mode in (None, 'r', 'rb'):
                return
            if path.startswith(repo.path):
                # truncate name relative to the repository (.hg)
                path = path[len(repo.path) + 1:]
            if path.startswith('cache/'):
                msg = 'accessing cache with vfs instead of cachevfs: "%s"'
                repo.ui.develwarn(msg % path, stacklevel=2, config="cache-vfs")
            if path.startswith('journal.'):
                # journal is covered by 'lock'
                if repo._currentlock(repo._lockref) is None:
                    repo.ui.develwarn('write with no lock: "%s"' % path,
                                      stacklevel=2, config='check-locks')
            elif repo._currentlock(repo._wlockref) is None:
                # rest of vfs files are covered by 'wlock'
                #
                # exclude special files
                for prefix in self._wlockfreeprefix:
                    if path.startswith(prefix):
                        return
                repo.ui.develwarn('write with no wlock: "%s"' % path,
                                  stacklevel=2, config='check-locks')
            return ret
        return checkvfs

    def _getsvfsward(self, origfunc):
        """build a ward for self.svfs"""
        rref = weakref.ref(self)
        def checksvfs(path, mode=None):
            ret = origfunc(path, mode=mode)
            repo = rref()
            if repo is None or not util.safehasattr(repo, '_lockref'):
                return
            if mode in (None, 'r', 'rb'):
                return
            if path.startswith(repo.sharedpath):
                # truncate name relative to the repository (.hg)
                path = path[len(repo.sharedpath) + 1:]
            if repo._currentlock(repo._lockref) is None:
                repo.ui.develwarn('write with no lock: "%s"' % path,
                                  stacklevel=3)
            return ret
        return checksvfs

    def close(self):
        self._writecaches()

    def _writecaches(self):
        if self._revbranchcache:
            self._revbranchcache.write()

    def _restrictcapabilities(self, caps):
        if self.ui.configbool('experimental', 'bundle2-advertise'):
            caps = set(caps)
            capsblob = bundle2.encodecaps(bundle2.getrepocaps(self,
                                                              role='client'))
            caps.add('bundle2=' + urlreq.quote(capsblob))
        return caps

    def _writerequirements(self):
        scmutil.writerequires(self.vfs, self.requirements)

    # Don't cache auditor/nofsauditor, or you'll end up with reference cycle:
    # self -> auditor -> self._checknested -> self

    @property
    def auditor(self):
        # This is only used by context.workingctx.match in order to
        # detect files in subrepos.
        return pathutil.pathauditor(self.root, callback=self._checknested)

    @property
    def nofsauditor(self):
        # This is only used by context.basectx.match in order to detect
        # files in subrepos.
        return pathutil.pathauditor(self.root, callback=self._checknested,
                                    realfs=False, cached=True)

    def _checknested(self, path):
        """Determine if path is a legal nested repository."""
        if not path.startswith(self.root):
            return False
        subpath = path[len(self.root) + 1:]
        normsubpath = util.pconvert(subpath)

        # XXX: Checking against the current working copy is wrong in
        # the sense that it can reject things like
        #
        #   $ hg cat -r 10 sub/x.txt
        #
        # if sub/ is no longer a subrepository in the working copy
        # parent revision.
        #
        # However, it can of course also allow things that would have
        # been rejected before, such as the above cat command if sub/
        # is a subrepository now, but was a normal directory before.
        # The old path auditor would have rejected by mistake since it
        # panics when it sees sub/.hg/.
        #
        # All in all, checking against the working copy seems sensible
        # since we want to prevent access to nested repositories on
        # the filesystem *now*.
        ctx = self[None]
        parts = util.splitpath(subpath)
        while parts:
            prefix = '/'.join(parts)
            if prefix in ctx.substate:
                if prefix == normsubpath:
                    return True
                else:
                    sub = ctx.sub(prefix)
                    return sub.checknested(subpath[len(prefix) + 1:])
            else:
                parts.pop()
        return False

    def peer(self):
        return localpeer(self) # not cached to avoid reference cycle

    def unfiltered(self):
        """Return unfiltered version of the repository

        Intended to be overwritten by filtered repo."""
        return self

    def filtered(self, name, visibilityexceptions=None):
        """Return a filtered version of a repository"""
        cls = repoview.newtype(self.unfiltered().__class__)
        return cls(self, name, visibilityexceptions)

    @repofilecache('bookmarks', 'bookmarks.current')
    def _bookmarks(self):
        return bookmarks.bmstore(self)

    @property
    def _activebookmark(self):
        return self._bookmarks.active

    # _phasesets depend on changelog. what we need is to call
    # _phasecache.invalidate() if '00changelog.i' was changed, but it
    # can't be easily expressed in filecache mechanism.
    @storecache('phaseroots', '00changelog.i')
    def _phasecache(self):
        return phases.phasecache(self, self._phasedefaults)

    @storecache('obsstore')
    def obsstore(self):
        return obsolete.makestore(self.ui, self)

    @storecache('00changelog.i')
    def changelog(self):
        return changelog.changelog(self.svfs,
                                   trypending=txnutil.mayhavepending(self.root))

    @storecache('00manifest.i')
    def manifestlog(self):
        rootstore = manifest.manifestrevlog(self.svfs)
        return manifest.manifestlog(self.svfs, self, rootstore)

    @repofilecache('dirstate')
    def dirstate(self):
        return self._makedirstate()

    def _makedirstate(self):
        """Extension point for wrapping the dirstate per-repo."""
        sparsematchfn = lambda: sparse.matcher(self)

        return dirstate.dirstate(self.vfs, self.ui, self.root,
                                 self._dirstatevalidate, sparsematchfn)

    def _dirstatevalidate(self, node):
        try:
            self.changelog.rev(node)
            return node
        except error.LookupError:
            if not self._dirstatevalidatewarned:
                self._dirstatevalidatewarned = True
                self.ui.warn(_("warning: ignoring unknown"
                               " working parent %s!\n") % short(node))
            return nullid

    @storecache(narrowspec.FILENAME)
    def narrowpats(self):
        """matcher patterns for this repository's narrowspec

        A tuple of (includes, excludes).
        """
        return narrowspec.load(self)

    @storecache(narrowspec.FILENAME)
    def _narrowmatch(self):
        if repository.NARROW_REQUIREMENT not in self.requirements:
            return matchmod.always(self.root, '')
        include, exclude = self.narrowpats
        return narrowspec.match(self.root, include=include, exclude=exclude)

    def narrowmatch(self, match=None, includeexact=False):
        """matcher corresponding the the repo's narrowspec

        If `match` is given, then that will be intersected with the narrow
        matcher.

        If `includeexact` is True, then any exact matches from `match` will
        be included even if they're outside the narrowspec.
        """
        if match:
            if includeexact and not self._narrowmatch.always():
                # do not exclude explicitly-specified paths so that they can
                # be warned later on
                em = matchmod.exact(match._root, match._cwd, match.files())
                nm = matchmod.unionmatcher([self._narrowmatch, em])
                return matchmod.intersectmatchers(match, nm)
            return matchmod.intersectmatchers(match, self._narrowmatch)
        return self._narrowmatch

    def setnarrowpats(self, newincludes, newexcludes):
        narrowspec.save(self, newincludes, newexcludes)
        self.invalidate(clearfilecache=True)

    def __getitem__(self, changeid):
        if changeid is None:
            return context.workingctx(self)
        if isinstance(changeid, context.basectx):
            return changeid
        if isinstance(changeid, slice):
            # wdirrev isn't contiguous so the slice shouldn't include it
            return [self[i]
                    for i in pycompat.xrange(*changeid.indices(len(self)))
                    if i not in self.changelog.filteredrevs]
        try:
            if isinstance(changeid, int):
                node = self.changelog.node(changeid)
                rev = changeid
            elif changeid == 'null':
                node = nullid
                rev = nullrev
            elif changeid == 'tip':
                node = self.changelog.tip()
                rev = self.changelog.rev(node)
            elif changeid == '.':
                # this is a hack to delay/avoid loading obsmarkers
                # when we know that '.' won't be hidden
                node = self.dirstate.p1()
                rev = self.unfiltered().changelog.rev(node)
            elif len(changeid) == 20:
                try:
                    node = changeid
                    rev = self.changelog.rev(changeid)
                except error.FilteredLookupError:
                    changeid = hex(changeid) # for the error message
                    raise
                except LookupError:
                    # check if it might have come from damaged dirstate
                    #
                    # XXX we could avoid the unfiltered if we had a recognizable
                    # exception for filtered changeset access
                    if (self.local()
                        and changeid in self.unfiltered().dirstate.parents()):
                        msg = _("working directory has unknown parent '%s'!")
                        raise error.Abort(msg % short(changeid))
                    changeid = hex(changeid) # for the error message
                    raise

            elif len(changeid) == 40:
                node = bin(changeid)
                rev = self.changelog.rev(node)
            else:
                raise error.ProgrammingError(
                        "unsupported changeid '%s' of type %s" %
                        (changeid, type(changeid)))

            return context.changectx(self, rev, node)

        except (error.FilteredIndexError, error.FilteredLookupError):
            raise error.FilteredRepoLookupError(_("filtered revision '%s'")
                                                % pycompat.bytestr(changeid))
        except (IndexError, LookupError):
            raise error.RepoLookupError(
                _("unknown revision '%s'") % pycompat.bytestr(changeid))
        except error.WdirUnsupported:
            return context.workingctx(self)

    def __contains__(self, changeid):
        """True if the given changeid exists

        error.AmbiguousPrefixLookupError is raised if an ambiguous node
        specified.
        """
        try:
            self[changeid]
            return True
        except error.RepoLookupError:
            return False

    def __nonzero__(self):
        return True

    __bool__ = __nonzero__

    def __len__(self):
        # no need to pay the cost of repoview.changelog
        unfi = self.unfiltered()
        return len(unfi.changelog)

    def __iter__(self):
        return iter(self.changelog)

    def revs(self, expr, *args):
        '''Find revisions matching a revset.

        The revset is specified as a string ``expr`` that may contain
        %-formatting to escape certain types. See ``revsetlang.formatspec``.

        Revset aliases from the configuration are not expanded. To expand
        user aliases, consider calling ``scmutil.revrange()`` or
        ``repo.anyrevs([expr], user=True)``.

        Returns a revset.abstractsmartset, which is a list-like interface
        that contains integer revisions.
        '''
        expr = revsetlang.formatspec(expr, *args)
        m = revset.match(None, expr)
        return m(self)

    def set(self, expr, *args):
        '''Find revisions matching a revset and emit changectx instances.

        This is a convenience wrapper around ``revs()`` that iterates the
        result and is a generator of changectx instances.

        Revset aliases from the configuration are not expanded. To expand
        user aliases, consider calling ``scmutil.revrange()``.
        '''
        for r in self.revs(expr, *args):
            yield self[r]

    def anyrevs(self, specs, user=False, localalias=None):
        '''Find revisions matching one of the given revsets.

        Revset aliases from the configuration are not expanded by default. To
        expand user aliases, specify ``user=True``. To provide some local
        definitions overriding user aliases, set ``localalias`` to
        ``{name: definitionstring}``.
        '''
        if user:
            m = revset.matchany(self.ui, specs,
                                lookup=revset.lookupfn(self),
                                localalias=localalias)
        else:
            m = revset.matchany(None, specs, localalias=localalias)
        return m(self)

    def url(self):
        return 'file:' + self.root

    def hook(self, name, throw=False, **args):
        """Call a hook, passing this repo instance.

        This a convenience method to aid invoking hooks. Extensions likely
        won't call this unless they have registered a custom hook or are
        replacing code that is expected to call a hook.
        """
        return hook.hook(self.ui, self, name, throw, **args)

    @filteredpropertycache
    def _tagscache(self):
        '''Returns a tagscache object that contains various tags related
        caches.'''

        # This simplifies its cache management by having one decorated
        # function (this one) and the rest simply fetch things from it.
        class tagscache(object):
            def __init__(self):
                # These two define the set of tags for this repository. tags
                # maps tag name to node; tagtypes maps tag name to 'global' or
                # 'local'. (Global tags are defined by .hgtags across all
                # heads, and local tags are defined in .hg/localtags.)
                # They constitute the in-memory cache of tags.
                self.tags = self.tagtypes = None

                self.nodetagscache = self.tagslist = None

        cache = tagscache()
        cache.tags, cache.tagtypes = self._findtags()

        return cache

    def tags(self):
        '''return a mapping of tag to node'''
        t = {}
        if self.changelog.filteredrevs:
            tags, tt = self._findtags()
        else:
            tags = self._tagscache.tags
        for k, v in tags.iteritems():
            try:
                # ignore tags to unknown nodes
                self.changelog.rev(v)
                t[k] = v
            except (error.LookupError, ValueError):
                pass
        return t

    def _findtags(self):
        '''Do the hard work of finding tags.  Return a pair of dicts
        (tags, tagtypes) where tags maps tag name to node, and tagtypes
        maps tag name to a string like \'global\' or \'local\'.
        Subclasses or extensions are free to add their own tags, but
        should be aware that the returned dicts will be retained for the
        duration of the localrepo object.'''

        # XXX what tagtype should subclasses/extensions use?  Currently
        # mq and bookmarks add tags, but do not set the tagtype at all.
        # Should each extension invent its own tag type?  Should there
        # be one tagtype for all such "virtual" tags?  Or is the status
        # quo fine?


        # map tag name to (node, hist)
        alltags = tagsmod.findglobaltags(self.ui, self)
        # map tag name to tag type
        tagtypes = dict((tag, 'global') for tag in alltags)

        tagsmod.readlocaltags(self.ui, self, alltags, tagtypes)

        # Build the return dicts.  Have to re-encode tag names because
        # the tags module always uses UTF-8 (in order not to lose info
        # writing to the cache), but the rest of Mercurial wants them in
        # local encoding.
        tags = {}
        for (name, (node, hist)) in alltags.iteritems():
            if node != nullid:
                tags[encoding.tolocal(name)] = node
        tags['tip'] = self.changelog.tip()
        tagtypes = dict([(encoding.tolocal(name), value)
                         for (name, value) in tagtypes.iteritems()])
        return (tags, tagtypes)

    def tagtype(self, tagname):
        '''
        return the type of the given tag. result can be:

        'local'  : a local tag
        'global' : a global tag
        None     : tag does not exist
        '''

        return self._tagscache.tagtypes.get(tagname)

    def tagslist(self):
        '''return a list of tags ordered by revision'''
        if not self._tagscache.tagslist:
            l = []
            for t, n in self.tags().iteritems():
                l.append((self.changelog.rev(n), t, n))
            self._tagscache.tagslist = [(t, n) for r, t, n in sorted(l)]

        return self._tagscache.tagslist

    def nodetags(self, node):
        '''return the tags associated with a node'''
        if not self._tagscache.nodetagscache:
            nodetagscache = {}
            for t, n in self._tagscache.tags.iteritems():
                nodetagscache.setdefault(n, []).append(t)
            for tags in nodetagscache.itervalues():
                tags.sort()
            self._tagscache.nodetagscache = nodetagscache
        return self._tagscache.nodetagscache.get(node, [])

    def nodebookmarks(self, node):
        """return the list of bookmarks pointing to the specified node"""
        return self._bookmarks.names(node)

    def branchmap(self):
        '''returns a dictionary {branch: [branchheads]} with branchheads
        ordered by increasing revision number'''
        branchmap.updatecache(self)
        return self._branchcaches[self.filtername]

    @unfilteredmethod
    def revbranchcache(self):
        if not self._revbranchcache:
            self._revbranchcache = branchmap.revbranchcache(self.unfiltered())
        return self._revbranchcache

    def branchtip(self, branch, ignoremissing=False):
        '''return the tip node for a given branch

        If ignoremissing is True, then this method will not raise an error.
        This is helpful for callers that only expect None for a missing branch
        (e.g. namespace).

        '''
        try:
            return self.branchmap().branchtip(branch)
        except KeyError:
            if not ignoremissing:
                raise error.RepoLookupError(_("unknown branch '%s'") % branch)
            else:
                pass

    def lookup(self, key):
        return scmutil.revsymbol(self, key).node()

    def lookupbranch(self, key):
        if key in self.branchmap():
            return key

        return scmutil.revsymbol(self, key).branch()

    def known(self, nodes):
        cl = self.changelog
        nm = cl.nodemap
        filtered = cl.filteredrevs
        result = []
        for n in nodes:
            r = nm.get(n)
            resp = not (r is None or r in filtered)
            result.append(resp)
        return result

    def local(self):
        return self

    def publishing(self):
        # it's safe (and desirable) to trust the publish flag unconditionally
        # so that we don't finalize changes shared between users via ssh or nfs
        return self.ui.configbool('phases', 'publish', untrusted=True)

    def cancopy(self):
        # so statichttprepo's override of local() works
        if not self.local():
            return False
        if not self.publishing():
            return True
        # if publishing we can't copy if there is filtered content
        return not self.filtered('visible').changelog.filteredrevs

    def shared(self):
        '''the type of shared repository (None if not shared)'''
        if self.sharedpath != self.path:
            return 'store'
        return None

    def wjoin(self, f, *insidef):
        return self.vfs.reljoin(self.root, f, *insidef)

    def setparents(self, p1, p2=nullid):
        with self.dirstate.parentchange():
            copies = self.dirstate.setparents(p1, p2)
            pctx = self[p1]
            if copies:
                # Adjust copy records, the dirstate cannot do it, it
                # requires access to parents manifests. Preserve them
                # only for entries added to first parent.
                for f in copies:
                    if f not in pctx and copies[f] in pctx:
                        self.dirstate.copy(copies[f], f)
            if p2 == nullid:
                for f, s in sorted(self.dirstate.copies().items()):
                    if f not in pctx and s not in pctx:
                        self.dirstate.copy(None, f)

    def filectx(self, path, changeid=None, fileid=None, changectx=None):
        """changeid can be a changeset revision, node, or tag.
           fileid can be a file revision or node."""
        return context.filectx(self, path, changeid, fileid,
                               changectx=changectx)

    def getcwd(self):
        return self.dirstate.getcwd()

    def pathto(self, f, cwd=None):
        return self.dirstate.pathto(f, cwd)

    def _loadfilter(self, filter):
        if filter not in self._filterpats:
            l = []
            for pat, cmd in self.ui.configitems(filter):
                if cmd == '!':
                    continue
                mf = matchmod.match(self.root, '', [pat])
                fn = None
                params = cmd
                for name, filterfn in self._datafilters.iteritems():
                    if cmd.startswith(name):
                        fn = filterfn
                        params = cmd[len(name):].lstrip()
                        break
                if not fn:
                    fn = lambda s, c, **kwargs: procutil.filter(s, c)
                # Wrap old filters not supporting keyword arguments
                if not pycompat.getargspec(fn)[2]:
                    oldfn = fn
                    fn = lambda s, c, **kwargs: oldfn(s, c)
                l.append((mf, fn, params))
            self._filterpats[filter] = l
        return self._filterpats[filter]

    def _filter(self, filterpats, filename, data):
        for mf, fn, cmd in filterpats:
            if mf(filename):
                self.ui.debug("filtering %s through %s\n" % (filename, cmd))
                data = fn(data, cmd, ui=self.ui, repo=self, filename=filename)
                break

        return data

    @unfilteredpropertycache
    def _encodefilterpats(self):
        return self._loadfilter('encode')

    @unfilteredpropertycache
    def _decodefilterpats(self):
        return self._loadfilter('decode')

    def adddatafilter(self, name, filter):
        self._datafilters[name] = filter

    def wread(self, filename):
        if self.wvfs.islink(filename):
            data = self.wvfs.readlink(filename)
        else:
            data = self.wvfs.read(filename)
        return self._filter(self._encodefilterpats, filename, data)

    def wwrite(self, filename, data, flags, backgroundclose=False, **kwargs):
        """write ``data`` into ``filename`` in the working directory

        This returns length of written (maybe decoded) data.
        """
        data = self._filter(self._decodefilterpats, filename, data)
        if 'l' in flags:
            self.wvfs.symlink(data, filename)
        else:
            self.wvfs.write(filename, data, backgroundclose=backgroundclose,
                            **kwargs)
            if 'x' in flags:
                self.wvfs.setflags(filename, False, True)
            else:
                self.wvfs.setflags(filename, False, False)
        return len(data)

    def wwritedata(self, filename, data):
        return self._filter(self._decodefilterpats, filename, data)

    def currenttransaction(self):
        """return the current transaction or None if non exists"""
        if self._transref:
            tr = self._transref()
        else:
            tr = None

        if tr and tr.running():
            return tr
        return None

    def transaction(self, desc, report=None):
        if (self.ui.configbool('devel', 'all-warnings')
                or self.ui.configbool('devel', 'check-locks')):
            if self._currentlock(self._lockref) is None:
                raise error.ProgrammingError('transaction requires locking')
        tr = self.currenttransaction()
        if tr is not None:
            return tr.nest(name=desc)

        # abort here if the journal already exists
        if self.svfs.exists("journal"):
            raise error.RepoError(
                _("abandoned transaction found"),
                hint=_("run 'hg recover' to clean up transaction"))

        idbase = "%.40f#%f" % (random.random(), time.time())
        ha = hex(hashlib.sha1(idbase).digest())
        txnid = 'TXN:' + ha
        self.hook('pretxnopen', throw=True, txnname=desc, txnid=txnid)

        self._writejournal(desc)
        renames = [(vfs, x, undoname(x)) for vfs, x in self._journalfiles()]
        if report:
            rp = report
        else:
            rp = self.ui.warn
        vfsmap = {'plain': self.vfs, 'store': self.svfs} # root of .hg/
        # we must avoid cyclic reference between repo and transaction.
        reporef = weakref.ref(self)
        # Code to track tag movement
        #
        # Since tags are all handled as file content, it is actually quite hard
        # to track these movement from a code perspective. So we fallback to a
        # tracking at the repository level. One could envision to track changes
        # to the '.hgtags' file through changegroup apply but that fails to
        # cope with case where transaction expose new heads without changegroup
        # being involved (eg: phase movement).
        #
        # For now, We gate the feature behind a flag since this likely comes
        # with performance impacts. The current code run more often than needed
        # and do not use caches as much as it could.  The current focus is on
        # the behavior of the feature so we disable it by default. The flag
        # will be removed when we are happy with the performance impact.
        #
        # Once this feature is no longer experimental move the following
        # documentation to the appropriate help section:
        #
        # The ``HG_TAG_MOVED`` variable will be set if the transaction touched
        # tags (new or changed or deleted tags). In addition the details of
        # these changes are made available in a file at:
        #     ``REPOROOT/.hg/changes/tags.changes``.
        # Make sure you check for HG_TAG_MOVED before reading that file as it
        # might exist from a previous transaction even if no tag were touched
        # in this one. Changes are recorded in a line base format::
        #
        #     <action> <hex-node> <tag-name>\n
        #
        # Actions are defined as follow:
        #   "-R": tag is removed,
        #   "+A": tag is added,
        #   "-M": tag is moved (old value),
        #   "+M": tag is moved (new value),
        tracktags = lambda x: None
        # experimental config: experimental.hook-track-tags
        shouldtracktags = self.ui.configbool('experimental', 'hook-track-tags')
        if desc != 'strip' and shouldtracktags:
            oldheads = self.changelog.headrevs()
            def tracktags(tr2):
                repo = reporef()
                oldfnodes = tagsmod.fnoderevs(repo.ui, repo, oldheads)
                newheads = repo.changelog.headrevs()
                newfnodes = tagsmod.fnoderevs(repo.ui, repo, newheads)
                # notes: we compare lists here.
                # As we do it only once buiding set would not be cheaper
                changes = tagsmod.difftags(repo.ui, repo, oldfnodes, newfnodes)
                if changes:
                    tr2.hookargs['tag_moved'] = '1'
                    with repo.vfs('changes/tags.changes', 'w',
                                  atomictemp=True) as changesfile:
                        # note: we do not register the file to the transaction
                        # because we needs it to still exist on the transaction
                        # is close (for txnclose hooks)
                        tagsmod.writediff(changesfile, changes)
        def validate(tr2):
            """will run pre-closing hooks"""
            # XXX the transaction API is a bit lacking here so we take a hacky
            # path for now
            #
            # We cannot add this as a "pending" hooks since the 'tr.hookargs'
            # dict is copied before these run. In addition we needs the data
            # available to in memory hooks too.
            #
            # Moreover, we also need to make sure this runs before txnclose
            # hooks and there is no "pending" mechanism that would execute
            # logic only if hooks are about to run.
            #
            # Fixing this limitation of the transaction is also needed to track
            # other families of changes (bookmarks, phases, obsolescence).
            #
            # This will have to be fixed before we remove the experimental
            # gating.
            tracktags(tr2)
            repo = reporef()
            if repo.ui.configbool('experimental', 'single-head-per-branch'):
                scmutil.enforcesinglehead(repo, tr2, desc)
            if hook.hashook(repo.ui, 'pretxnclose-bookmark'):
                for name, (old, new) in sorted(tr.changes['bookmarks'].items()):
                    args = tr.hookargs.copy()
                    args.update(bookmarks.preparehookargs(name, old, new))
                    repo.hook('pretxnclose-bookmark', throw=True,
                              txnname=desc,
                              **pycompat.strkwargs(args))
            if hook.hashook(repo.ui, 'pretxnclose-phase'):
                cl = repo.unfiltered().changelog
                for rev, (old, new) in tr.changes['phases'].items():
                    args = tr.hookargs.copy()
                    node = hex(cl.node(rev))
                    args.update(phases.preparehookargs(node, old, new))
                    repo.hook('pretxnclose-phase', throw=True, txnname=desc,
                              **pycompat.strkwargs(args))

            repo.hook('pretxnclose', throw=True,
                      txnname=desc, **pycompat.strkwargs(tr.hookargs))
        def releasefn(tr, success):
            repo = reporef()
            if success:
                # this should be explicitly invoked here, because
                # in-memory changes aren't written out at closing
                # transaction, if tr.addfilegenerator (via
                # dirstate.write or so) isn't invoked while
                # transaction running
                repo.dirstate.write(None)
            else:
                # discard all changes (including ones already written
                # out) in this transaction
                narrowspec.restorebackup(self, 'journal.narrowspec')
                repo.dirstate.restorebackup(None, 'journal.dirstate')

                repo.invalidate(clearfilecache=True)

        tr = transaction.transaction(rp, self.svfs, vfsmap,
                                     "journal",
                                     "undo",
                                     aftertrans(renames),
                                     self.store.createmode,
                                     validator=validate,
                                     releasefn=releasefn,
                                     checkambigfiles=_cachedfiles,
                                     name=desc)
        tr.changes['origrepolen'] = len(self)
        tr.changes['obsmarkers'] = set()
        tr.changes['phases'] = {}
        tr.changes['bookmarks'] = {}

        tr.hookargs['txnid'] = txnid
        # note: writing the fncache only during finalize mean that the file is
        # outdated when running hooks. As fncache is used for streaming clone,
        # this is not expected to break anything that happen during the hooks.
        tr.addfinalize('flush-fncache', self.store.write)
        def txnclosehook(tr2):
            """To be run if transaction is successful, will schedule a hook run
            """
            # Don't reference tr2 in hook() so we don't hold a reference.
            # This reduces memory consumption when there are multiple
            # transactions per lock. This can likely go away if issue5045
            # fixes the function accumulation.
            hookargs = tr2.hookargs

            def hookfunc():
                repo = reporef()
                if hook.hashook(repo.ui, 'txnclose-bookmark'):
                    bmchanges = sorted(tr.changes['bookmarks'].items())
                    for name, (old, new) in bmchanges:
                        args = tr.hookargs.copy()
                        args.update(bookmarks.preparehookargs(name, old, new))
                        repo.hook('txnclose-bookmark', throw=False,
                                  txnname=desc, **pycompat.strkwargs(args))

                if hook.hashook(repo.ui, 'txnclose-phase'):
                    cl = repo.unfiltered().changelog
                    phasemv = sorted(tr.changes['phases'].items())
                    for rev, (old, new) in phasemv:
                        args = tr.hookargs.copy()
                        node = hex(cl.node(rev))
                        args.update(phases.preparehookargs(node, old, new))
                        repo.hook('txnclose-phase', throw=False, txnname=desc,
                                  **pycompat.strkwargs(args))

                repo.hook('txnclose', throw=False, txnname=desc,
                          **pycompat.strkwargs(hookargs))
            reporef()._afterlock(hookfunc)
        tr.addfinalize('txnclose-hook', txnclosehook)
        # Include a leading "-" to make it happen before the transaction summary
        # reports registered via scmutil.registersummarycallback() whose names
        # are 00-txnreport etc. That way, the caches will be warm when the
        # callbacks run.
        tr.addpostclose('-warm-cache', self._buildcacheupdater(tr))
        def txnaborthook(tr2):
            """To be run if transaction is aborted
            """
            reporef().hook('txnabort', throw=False, txnname=desc,
                           **pycompat.strkwargs(tr2.hookargs))
        tr.addabort('txnabort-hook', txnaborthook)
        # avoid eager cache invalidation. in-memory data should be identical
        # to stored data if transaction has no error.
        tr.addpostclose('refresh-filecachestats', self._refreshfilecachestats)
        self._transref = weakref.ref(tr)
        scmutil.registersummarycallback(self, tr, desc)
        return tr

    def _journalfiles(self):
        return ((self.svfs, 'journal'),
                (self.vfs, 'journal.dirstate'),
                (self.vfs, 'journal.branch'),
                (self.vfs, 'journal.desc'),
                (self.vfs, 'journal.bookmarks'),
                (self.svfs, 'journal.phaseroots'))

    def undofiles(self):
        return [(vfs, undoname(x)) for vfs, x in self._journalfiles()]

    @unfilteredmethod
    def _writejournal(self, desc):
        self.dirstate.savebackup(None, 'journal.dirstate')
        narrowspec.savebackup(self, 'journal.narrowspec')
        self.vfs.write("journal.branch",
                          encoding.fromlocal(self.dirstate.branch()))
        self.vfs.write("journal.desc",
                          "%d\n%s\n" % (len(self), desc))
        self.vfs.write("journal.bookmarks",
                          self.vfs.tryread("bookmarks"))
        self.svfs.write("journal.phaseroots",
                           self.svfs.tryread("phaseroots"))

    def recover(self):
        with self.lock():
            if self.svfs.exists("journal"):
                self.ui.status(_("rolling back interrupted transaction\n"))
                vfsmap = {'': self.svfs,
                          'plain': self.vfs,}
                transaction.rollback(self.svfs, vfsmap, "journal",
                                     self.ui.warn,
                                     checkambigfiles=_cachedfiles)
                self.invalidate()
                return True
            else:
                self.ui.warn(_("no interrupted transaction available\n"))
                return False

    def rollback(self, dryrun=False, force=False):
        wlock = lock = dsguard = None
        try:
            wlock = self.wlock()
            lock = self.lock()
            if self.svfs.exists("undo"):
                dsguard = dirstateguard.dirstateguard(self, 'rollback')

                return self._rollback(dryrun, force, dsguard)
            else:
                self.ui.warn(_("no rollback information available\n"))
                return 1
        finally:
            release(dsguard, lock, wlock)

    @unfilteredmethod # Until we get smarter cache management
    def _rollback(self, dryrun, force, dsguard):
        ui = self.ui
        try:
            args = self.vfs.read('undo.desc').splitlines()
            (oldlen, desc, detail) = (int(args[0]), args[1], None)
            if len(args) >= 3:
                detail = args[2]
            oldtip = oldlen - 1

            if detail and ui.verbose:
                msg = (_('repository tip rolled back to revision %d'
                         ' (undo %s: %s)\n')
                       % (oldtip, desc, detail))
            else:
                msg = (_('repository tip rolled back to revision %d'
                         ' (undo %s)\n')
                       % (oldtip, desc))
        except IOError:
            msg = _('rolling back unknown transaction\n')
            desc = None

        if not force and self['.'] != self['tip'] and desc == 'commit':
            raise error.Abort(
                _('rollback of last commit while not checked out '
                  'may lose data'), hint=_('use -f to force'))

        ui.status(msg)
        if dryrun:
            return 0

        parents = self.dirstate.parents()
        self.destroying()
        vfsmap = {'plain': self.vfs, '': self.svfs}
        transaction.rollback(self.svfs, vfsmap, 'undo', ui.warn,
                             checkambigfiles=_cachedfiles)
        if self.vfs.exists('undo.bookmarks'):
            self.vfs.rename('undo.bookmarks', 'bookmarks', checkambig=True)
        if self.svfs.exists('undo.phaseroots'):
            self.svfs.rename('undo.phaseroots', 'phaseroots', checkambig=True)
        self.invalidate()

        parentgone = (parents[0] not in self.changelog.nodemap or
                      parents[1] not in self.changelog.nodemap)
        if parentgone:
            # prevent dirstateguard from overwriting already restored one
            dsguard.close()

            narrowspec.restorebackup(self, 'undo.narrowspec')
            self.dirstate.restorebackup(None, 'undo.dirstate')
            try:
                branch = self.vfs.read('undo.branch')
                self.dirstate.setbranch(encoding.tolocal(branch))
            except IOError:
                ui.warn(_('named branch could not be reset: '
                          'current branch is still \'%s\'\n')
                        % self.dirstate.branch())

            parents = tuple([p.rev() for p in self[None].parents()])
            if len(parents) > 1:
                ui.status(_('working directory now based on '
                            'revisions %d and %d\n') % parents)
            else:
                ui.status(_('working directory now based on '
                            'revision %d\n') % parents)
            mergemod.mergestate.clean(self, self['.'].node())

        # TODO: if we know which new heads may result from this rollback, pass
        # them to destroy(), which will prevent the branchhead cache from being
        # invalidated.
        self.destroyed()
        return 0

    def _buildcacheupdater(self, newtransaction):
        """called during transaction to build the callback updating cache

        Lives on the repository to help extension who might want to augment
        this logic. For this purpose, the created transaction is passed to the
        method.
        """
        # we must avoid cyclic reference between repo and transaction.
        reporef = weakref.ref(self)
        def updater(tr):
            repo = reporef()
            repo.updatecaches(tr)
        return updater

    @unfilteredmethod
    def updatecaches(self, tr=None, full=False):
        """warm appropriate caches

        If this function is called after a transaction closed. The transaction
        will be available in the 'tr' argument. This can be used to selectively
        update caches relevant to the changes in that transaction.

        If 'full' is set, make sure all caches the function knows about have
        up-to-date data. Even the ones usually loaded more lazily.
        """
        if tr is not None and tr.hookargs.get('source') == 'strip':
            # During strip, many caches are invalid but
            # later call to `destroyed` will refresh them.
            return

        if tr is None or tr.changes['origrepolen'] < len(self):
            # updating the unfiltered branchmap should refresh all the others,
            self.ui.debug('updating the branch cache\n')
            branchmap.updatecache(self.filtered('served'))

        if full:
            rbc = self.revbranchcache()
            for r in self.changelog:
                rbc.branchinfo(r)
            rbc.write()

            # ensure the working copy parents are in the manifestfulltextcache
            for ctx in self['.'].parents():
                ctx.manifest()  # accessing the manifest is enough

    def invalidatecaches(self):

        if r'_tagscache' in vars(self):
            # can't use delattr on proxy
            del self.__dict__[r'_tagscache']

        self.unfiltered()._branchcaches.clear()
        self.invalidatevolatilesets()
        self._sparsesignaturecache.clear()

    def invalidatevolatilesets(self):
        self.filteredrevcache.clear()
        obsolete.clearobscaches(self)

    def invalidatedirstate(self):
        '''Invalidates the dirstate, causing the next call to dirstate
        to check if it was modified since the last time it was read,
        rereading it if it has.

        This is different to dirstate.invalidate() that it doesn't always
        rereads the dirstate. Use dirstate.invalidate() if you want to
        explicitly read the dirstate again (i.e. restoring it to a previous
        known good state).'''
        if hasunfilteredcache(self, r'dirstate'):
            for k in self.dirstate._filecache:
                try:
                    delattr(self.dirstate, k)
                except AttributeError:
                    pass
            delattr(self.unfiltered(), r'dirstate')

    def invalidate(self, clearfilecache=False):
        '''Invalidates both store and non-store parts other than dirstate

        If a transaction is running, invalidation of store is omitted,
        because discarding in-memory changes might cause inconsistency
        (e.g. incomplete fncache causes unintentional failure, but
        redundant one doesn't).
        '''
        unfiltered = self.unfiltered() # all file caches are stored unfiltered
        for k in list(self._filecache.keys()):
            # dirstate is invalidated separately in invalidatedirstate()
            if k == 'dirstate':
                continue
            if (k == 'changelog' and
                self.currenttransaction() and
                self.changelog._delayed):
                # The changelog object may store unwritten revisions. We don't
                # want to lose them.
                # TODO: Solve the problem instead of working around it.
                continue

            if clearfilecache:
                del self._filecache[k]
            try:
                delattr(unfiltered, k)
            except AttributeError:
                pass
        self.invalidatecaches()
        if not self.currenttransaction():
            # TODO: Changing contents of store outside transaction
            # causes inconsistency. We should make in-memory store
            # changes detectable, and abort if changed.
            self.store.invalidatecaches()

    def invalidateall(self):
        '''Fully invalidates both store and non-store parts, causing the
        subsequent operation to reread any outside changes.'''
        # extension should hook this to invalidate its caches
        self.invalidate()
        self.invalidatedirstate()

    @unfilteredmethod
    def _refreshfilecachestats(self, tr):
        """Reload stats of cached files so that they are flagged as valid"""
        for k, ce in self._filecache.items():
            k = pycompat.sysstr(k)
            if k == r'dirstate' or k not in self.__dict__:
                continue
            ce.refresh()

    def _lock(self, vfs, lockname, wait, releasefn, acquirefn, desc,
              inheritchecker=None, parentenvvar=None):
        parentlock = None
        # the contents of parentenvvar are used by the underlying lock to
        # determine whether it can be inherited
        if parentenvvar is not None:
            parentlock = encoding.environ.get(parentenvvar)

        timeout = 0
        warntimeout = 0
        if wait:
            timeout = self.ui.configint("ui", "timeout")
            warntimeout = self.ui.configint("ui", "timeout.warn")
        # internal config: ui.signal-safe-lock
        signalsafe = self.ui.configbool('ui', 'signal-safe-lock')

        l = lockmod.trylock(self.ui, vfs, lockname, timeout, warntimeout,
                            releasefn=releasefn,
                            acquirefn=acquirefn, desc=desc,
                            inheritchecker=inheritchecker,
                            parentlock=parentlock,
                            signalsafe=signalsafe)
        return l

    def _afterlock(self, callback):
        """add a callback to be run when the repository is fully unlocked

        The callback will be executed when the outermost lock is released
        (with wlock being higher level than 'lock')."""
        for ref in (self._wlockref, self._lockref):
            l = ref and ref()
            if l and l.held:
                l.postrelease.append(callback)
                break
        else: # no lock have been found.
            callback()

    def lock(self, wait=True):
        '''Lock the repository store (.hg/store) and return a weak reference
        to the lock. Use this before modifying the store (e.g. committing or
        stripping). If you are opening a transaction, get a lock as well.)

        If both 'lock' and 'wlock' must be acquired, ensure you always acquires
        'wlock' first to avoid a dead-lock hazard.'''
        l = self._currentlock(self._lockref)
        if l is not None:
            l.lock()
            return l

        l = self._lock(self.svfs, "lock", wait, None,
                       self.invalidate, _('repository %s') % self.origroot)
        self._lockref = weakref.ref(l)
        return l

    def _wlockchecktransaction(self):
        if self.currenttransaction() is not None:
            raise error.LockInheritanceContractViolation(
                'wlock cannot be inherited in the middle of a transaction')

    def wlock(self, wait=True):
        '''Lock the non-store parts of the repository (everything under
        .hg except .hg/store) and return a weak reference to the lock.

        Use this before modifying files in .hg.

        If both 'lock' and 'wlock' must be acquired, ensure you always acquires
        'wlock' first to avoid a dead-lock hazard.'''
        l = self._wlockref and self._wlockref()
        if l is not None and l.held:
            l.lock()
            return l

        # We do not need to check for non-waiting lock acquisition.  Such
        # acquisition would not cause dead-lock as they would just fail.
        if wait and (self.ui.configbool('devel', 'all-warnings')
                     or self.ui.configbool('devel', 'check-locks')):
            if self._currentlock(self._lockref) is not None:
                self.ui.develwarn('"wlock" acquired after "lock"')

        def unlock():
            if self.dirstate.pendingparentchange():
                self.dirstate.invalidate()
            else:
                self.dirstate.write(None)

            self._filecache['dirstate'].refresh()

        l = self._lock(self.vfs, "wlock", wait, unlock,
                       self.invalidatedirstate, _('working directory of %s') %
                       self.origroot,
                       inheritchecker=self._wlockchecktransaction,
                       parentenvvar='HG_WLOCK_LOCKER')
        self._wlockref = weakref.ref(l)
        return l

    def _currentlock(self, lockref):
        """Returns the lock if it's held, or None if it's not."""
        if lockref is None:
            return None
        l = lockref()
        if l is None or not l.held:
            return None
        return l

    def currentwlock(self):
        """Returns the wlock if it's held, or None if it's not."""
        return self._currentlock(self._wlockref)

    def _filecommit(self, fctx, manifest1, manifest2, linkrev, tr, changelist):
        """
        commit an individual file as part of a larger transaction
        """

        fname = fctx.path()
        fparent1 = manifest1.get(fname, nullid)
        fparent2 = manifest2.get(fname, nullid)
        if isinstance(fctx, context.filectx):
            node = fctx.filenode()
            if node in [fparent1, fparent2]:
                self.ui.debug('reusing %s filelog entry\n' % fname)
                if manifest1.flags(fname) != fctx.flags():
                    changelist.append(fname)
                return node

        flog = self.file(fname)
        meta = {}
        copy = fctx.renamed()
        if copy and copy[0] != fname:
            # Mark the new revision of this file as a copy of another
            # file.  This copy data will effectively act as a parent
            # of this new revision.  If this is a merge, the first
            # parent will be the nullid (meaning "look up the copy data")
            # and the second one will be the other parent.  For example:
            #
            # 0 --- 1 --- 3   rev1 changes file foo
            #   \       /     rev2 renames foo to bar and changes it
            #    \- 2 -/      rev3 should have bar with all changes and
            #                      should record that bar descends from
            #                      bar in rev2 and foo in rev1
            #
            # this allows this merge to succeed:
            #
            # 0 --- 1 --- 3   rev4 reverts the content change from rev2
            #   \       /     merging rev3 and rev4 should use bar@rev2
            #    \- 2 --- 4        as the merge base
            #

            cfname = copy[0]
            crev = manifest1.get(cfname)
            newfparent = fparent2

            if manifest2: # branch merge
                if fparent2 == nullid or crev is None: # copied on remote side
                    if cfname in manifest2:
                        crev = manifest2[cfname]
                        newfparent = fparent1

            # Here, we used to search backwards through history to try to find
            # where the file copy came from if the source of a copy was not in
            # the parent directory. However, this doesn't actually make sense to
            # do (what does a copy from something not in your working copy even
            # mean?) and it causes bugs (eg, issue4476). Instead, we will warn
            # the user that copy information was dropped, so if they didn't
            # expect this outcome it can be fixed, but this is the correct
            # behavior in this circumstance.

            if crev:
                self.ui.debug(" %s: copy %s:%s\n" % (fname, cfname, hex(crev)))
                meta["copy"] = cfname
                meta["copyrev"] = hex(crev)
                fparent1, fparent2 = nullid, newfparent
            else:
                self.ui.warn(_("warning: can't find ancestor for '%s' "
                               "copied from '%s'!\n") % (fname, cfname))

        elif fparent1 == nullid:
            fparent1, fparent2 = fparent2, nullid
        elif fparent2 != nullid:
            # is one parent an ancestor of the other?
            fparentancestors = flog.commonancestorsheads(fparent1, fparent2)
            if fparent1 in fparentancestors:
                fparent1, fparent2 = fparent2, nullid
            elif fparent2 in fparentancestors:
                fparent2 = nullid

        # is the file changed?
        text = fctx.data()
        if fparent2 != nullid or flog.cmp(fparent1, text) or meta:
            changelist.append(fname)
            return flog.add(text, meta, tr, linkrev, fparent1, fparent2)
        # are just the flags changed during merge?
        elif fname in manifest1 and manifest1.flags(fname) != fctx.flags():
            changelist.append(fname)

        return fparent1

    def checkcommitpatterns(self, wctx, vdirs, match, status, fail):
        """check for commit arguments that aren't committable"""
        if match.isexact() or match.prefix():
            matched = set(status.modified + status.added + status.removed)

            for f in match.files():
                f = self.dirstate.normalize(f)
                if f == '.' or f in matched or f in wctx.substate:
                    continue
                if f in status.deleted:
                    fail(f, _('file not found!'))
                if f in vdirs: # visited directory
                    d = f + '/'
                    for mf in matched:
                        if mf.startswith(d):
                            break
                    else:
                        fail(f, _("no match under directory!"))
                elif f not in self.dirstate:
                    fail(f, _("file not tracked!"))

    @unfilteredmethod
    def commit(self, text="", user=None, date=None, match=None, force=False,
               editor=False, extra=None):
        """Add a new revision to current repository.

        Revision information is gathered from the working directory,
        match can be used to filter the committed files. If editor is
        supplied, it is called to get a commit message.
        """
        if extra is None:
            extra = {}

        def fail(f, msg):
            raise error.Abort('%s: %s' % (f, msg))

        if not match:
            match = matchmod.always(self.root, '')

        if not force:
            vdirs = []
            match.explicitdir = vdirs.append
            match.bad = fail

        wlock = lock = tr = None
        try:
            wlock = self.wlock()
            lock = self.lock() # for recent changelog (see issue4368)

            wctx = self[None]
            merge = len(wctx.parents()) > 1

            if not force and merge and not match.always():
                raise error.Abort(_('cannot partially commit a merge '
                                   '(do not specify files or patterns)'))

            status = self.status(match=match, clean=force)
            if force:
                status.modified.extend(status.clean) # mq may commit clean files

            # check subrepos
            subs, commitsubs, newstate = subrepoutil.precommit(
                self.ui, wctx, status, match, force=force)

            # make sure all explicit patterns are matched
            if not force:
                self.checkcommitpatterns(wctx, vdirs, match, status, fail)

            cctx = context.workingcommitctx(self, status,
                                            text, user, date, extra)

            # internal config: ui.allowemptycommit
            allowemptycommit = (wctx.branch() != wctx.p1().branch()
                                or extra.get('close') or merge or cctx.files()
                                or self.ui.configbool('ui', 'allowemptycommit'))
            if not allowemptycommit:
                return None

            if merge and cctx.deleted():
                raise error.Abort(_("cannot commit merge with missing files"))

            ms = mergemod.mergestate.read(self)
            mergeutil.checkunresolved(ms)

            if editor:
                cctx._text = editor(self, cctx, subs)
            edited = (text != cctx._text)

            # Save commit message in case this transaction gets rolled back
            # (e.g. by a pretxncommit hook).  Leave the content alone on
            # the assumption that the user will use the same editor again.
            msgfn = self.savecommitmessage(cctx._text)

            # commit subs and write new state
            if subs:
                for s in sorted(commitsubs):
                    sub = wctx.sub(s)
                    self.ui.status(_('committing subrepository %s\n') %
                                   subrepoutil.subrelpath(sub))
                    sr = sub.commit(cctx._text, user, date)
                    newstate[s] = (newstate[s][0], sr)
                subrepoutil.writestate(self, newstate)

            p1, p2 = self.dirstate.parents()
            hookp1, hookp2 = hex(p1), (p2 != nullid and hex(p2) or '')
            try:
                self.hook("precommit", throw=True, parent1=hookp1,
                          parent2=hookp2)
                tr = self.transaction('commit')
                ret = self.commitctx(cctx, True)
            except: # re-raises
                if edited:
                    self.ui.write(
                        _('note: commit message saved in %s\n') % msgfn)
                raise
            # update bookmarks, dirstate and mergestate
            bookmarks.update(self, [p1, p2], ret)
            cctx.markcommitted(ret)
            ms.reset()
            tr.close()

        finally:
            lockmod.release(tr, lock, wlock)

        def commithook(node=hex(ret), parent1=hookp1, parent2=hookp2):
            # hack for command that use a temporary commit (eg: histedit)
            # temporary commit got stripped before hook release
            if self.changelog.hasnode(ret):
                self.hook("commit", node=node, parent1=parent1,
                          parent2=parent2)
        self._afterlock(commithook)
        return ret

    @unfilteredmethod
    def commitctx(self, ctx, error=False):
        """Add a new revision to current repository.
        Revision information is passed via the context argument.

        ctx.files() should list all files involved in this commit, i.e.
        modified/added/removed files. On merge, it may be wider than the
        ctx.files() to be committed, since any file nodes derived directly
        from p1 or p2 are excluded from the committed ctx.files().
        """

        tr = None
        p1, p2 = ctx.p1(), ctx.p2()
        user = ctx.user()

        lock = self.lock()
        try:
            tr = self.transaction("commit")
            trp = weakref.proxy(tr)

            if ctx.manifestnode():
                # reuse an existing manifest revision
                self.ui.debug('reusing known manifest\n')
                mn = ctx.manifestnode()
                files = ctx.files()
            elif ctx.files():
                m1ctx = p1.manifestctx()
                m2ctx = p2.manifestctx()
                mctx = m1ctx.copy()

                m = mctx.read()
                m1 = m1ctx.read()
                m2 = m2ctx.read()

                # check in files
                added = []
                changed = []
                removed = list(ctx.removed())
                linkrev = len(self)
                self.ui.note(_("committing files:\n"))
                for f in sorted(ctx.modified() + ctx.added()):
                    self.ui.note(f + "\n")
                    try:
                        fctx = ctx[f]
                        if fctx is None:
                            removed.append(f)
                        else:
                            added.append(f)
                            m[f] = self._filecommit(fctx, m1, m2, linkrev,
                                                    trp, changed)
                            m.setflag(f, fctx.flags())
                    except OSError as inst:
                        self.ui.warn(_("trouble committing %s!\n") % f)
                        raise
                    except IOError as inst:
                        errcode = getattr(inst, 'errno', errno.ENOENT)
                        if error or errcode and errcode != errno.ENOENT:
                            self.ui.warn(_("trouble committing %s!\n") % f)
                        raise

                # update manifest
                removed = [f for f in sorted(removed) if f in m1 or f in m2]
                drop = [f for f in removed if f in m]
                for f in drop:
                    del m[f]
                files = changed + removed
                md = None
                if not files:
                    # if no "files" actually changed in terms of the changelog,
                    # try hard to detect unmodified manifest entry so that the
                    # exact same commit can be reproduced later on convert.
                    md = m1.diff(m, scmutil.matchfiles(self, ctx.files()))
                if not files and md:
                    self.ui.debug('not reusing manifest (no file change in '
                                  'changelog, but manifest differs)\n')
                if files or md:
                    self.ui.note(_("committing manifest\n"))
                    # we're using narrowmatch here since it's already applied at
                    # other stages (such as dirstate.walk), so we're already
                    # ignoring things outside of narrowspec in most cases. The
                    # one case where we might have files outside the narrowspec
                    # at this point is merges, and we already error out in the
                    # case where the merge has files outside of the narrowspec,
                    # so this is safe.
                    mn = mctx.write(trp, linkrev,
                                    p1.manifestnode(), p2.manifestnode(),
                                    added, drop, match=self.narrowmatch())
                else:
                    self.ui.debug('reusing manifest form p1 (listed files '
                                  'actually unchanged)\n')
                    mn = p1.manifestnode()
            else:
                self.ui.debug('reusing manifest from p1 (no file change)\n')
                mn = p1.manifestnode()
                files = []

            # update changelog
            self.ui.note(_("committing changelog\n"))
            self.changelog.delayupdate(tr)
            n = self.changelog.add(mn, files, ctx.description(),
                                   trp, p1.node(), p2.node(),
                                   user, ctx.date(), ctx.extra().copy())
            xp1, xp2 = p1.hex(), p2 and p2.hex() or ''
            self.hook('pretxncommit', throw=True, node=hex(n), parent1=xp1,
                      parent2=xp2)
            # set the new commit is proper phase
            targetphase = subrepoutil.newcommitphase(self.ui, ctx)
            if targetphase:
                # retract boundary do not alter parent changeset.
                # if a parent have higher the resulting phase will
                # be compliant anyway
                #
                # if minimal phase was 0 we don't need to retract anything
                phases.registernew(self, tr, targetphase, [n])
            tr.close()
            return n
        finally:
            if tr:
                tr.release()
            lock.release()

    @unfilteredmethod
    def destroying(self):
        '''Inform the repository that nodes are about to be destroyed.
        Intended for use by strip and rollback, so there's a common
        place for anything that has to be done before destroying history.

        This is mostly useful for saving state that is in memory and waiting
        to be flushed when the current lock is released. Because a call to
        destroyed is imminent, the repo will be invalidated causing those
        changes to stay in memory (waiting for the next unlock), or vanish
        completely.
        '''
        # When using the same lock to commit and strip, the phasecache is left
        # dirty after committing. Then when we strip, the repo is invalidated,
        # causing those changes to disappear.
        if '_phasecache' in vars(self):
            self._phasecache.write()

    @unfilteredmethod
    def destroyed(self):
        '''Inform the repository that nodes have been destroyed.
        Intended for use by strip and rollback, so there's a common
        place for anything that has to be done after destroying history.
        '''
        # When one tries to:
        # 1) destroy nodes thus calling this method (e.g. strip)
        # 2) use phasecache somewhere (e.g. commit)
        #
        # then 2) will fail because the phasecache contains nodes that were
        # removed. We can either remove phasecache from the filecache,
        # causing it to reload next time it is accessed, or simply filter
        # the removed nodes now and write the updated cache.
        self._phasecache.filterunknown(self)
        self._phasecache.write()

        # refresh all repository caches
        self.updatecaches()

        # Ensure the persistent tag cache is updated.  Doing it now
        # means that the tag cache only has to worry about destroyed
        # heads immediately after a strip/rollback.  That in turn
        # guarantees that "cachetip == currenttip" (comparing both rev
        # and node) always means no nodes have been added or destroyed.

        # XXX this is suboptimal when qrefresh'ing: we strip the current
        # head, refresh the tag cache, then immediately add a new head.
        # But I think doing it this way is necessary for the "instant
        # tag cache retrieval" case to work.
        self.invalidate()

    def status(self, node1='.', node2=None, match=None,
               ignored=False, clean=False, unknown=False,
               listsubrepos=False):
        '''a convenience method that calls node1.status(node2)'''
        return self[node1].status(node2, match, ignored, clean, unknown,
                                  listsubrepos)

    def addpostdsstatus(self, ps):
        """Add a callback to run within the wlock, at the point at which status
        fixups happen.

        On status completion, callback(wctx, status) will be called with the
        wlock held, unless the dirstate has changed from underneath or the wlock
        couldn't be grabbed.

        Callbacks should not capture and use a cached copy of the dirstate --
        it might change in the meanwhile. Instead, they should access the
        dirstate via wctx.repo().dirstate.

        This list is emptied out after each status run -- extensions should
        make sure it adds to this list each time dirstate.status is called.
        Extensions should also make sure they don't call this for statuses
        that don't involve the dirstate.
        """

        # The list is located here for uniqueness reasons -- it is actually
        # managed by the workingctx, but that isn't unique per-repo.
        self._postdsstatus.append(ps)

    def postdsstatus(self):
        """Used by workingctx to get the list of post-dirstate-status hooks."""
        return self._postdsstatus

    def clearpostdsstatus(self):
        """Used by workingctx to clear post-dirstate-status hooks."""
        del self._postdsstatus[:]

    def heads(self, start=None):
        if start is None:
            cl = self.changelog
            headrevs = reversed(cl.headrevs())
            return [cl.node(rev) for rev in headrevs]

        heads = self.changelog.heads(start)
        # sort the output in rev descending order
        return sorted(heads, key=self.changelog.rev, reverse=True)

    def branchheads(self, branch=None, start=None, closed=False):
        '''return a (possibly filtered) list of heads for the given branch

        Heads are returned in topological order, from newest to oldest.
        If branch is None, use the dirstate branch.
        If start is not None, return only heads reachable from start.
        If closed is True, return heads that are marked as closed as well.
        '''
        if branch is None:
            branch = self[None].branch()
        branches = self.branchmap()
        if branch not in branches:
            return []
        # the cache returns heads ordered lowest to highest
        bheads = list(reversed(branches.branchheads(branch, closed=closed)))
        if start is not None:
            # filter out the heads that cannot be reached from startrev
            fbheads = set(self.changelog.nodesbetween([start], bheads)[2])
            bheads = [h for h in bheads if h in fbheads]
        return bheads

    def branches(self, nodes):
        if not nodes:
            nodes = [self.changelog.tip()]
        b = []
        for n in nodes:
            t = n
            while True:
                p = self.changelog.parents(n)
                if p[1] != nullid or p[0] == nullid:
                    b.append((t, n, p[0], p[1]))
                    break
                n = p[0]
        return b

    def between(self, pairs):
        r = []

        for top, bottom in pairs:
            n, l, i = top, [], 0
            f = 1

            while n != bottom and n != nullid:
                p = self.changelog.parents(n)[0]
                if i == f:
                    l.append(n)
                    f = f * 2
                n = p
                i += 1

            r.append(l)

        return r

    def checkpush(self, pushop):
        """Extensions can override this function if additional checks have
        to be performed before pushing, or call it if they override push
        command.
        """

    @unfilteredpropertycache
    def prepushoutgoinghooks(self):
        """Return util.hooks consists of a pushop with repo, remote, outgoing
        methods, which are called before pushing changesets.
        """
        return util.hooks()

    def pushkey(self, namespace, key, old, new):
        try:
            tr = self.currenttransaction()
            hookargs = {}
            if tr is not None:
                hookargs.update(tr.hookargs)
            hookargs = pycompat.strkwargs(hookargs)
            hookargs[r'namespace'] = namespace
            hookargs[r'key'] = key
            hookargs[r'old'] = old
            hookargs[r'new'] = new
            self.hook('prepushkey', throw=True, **hookargs)
        except error.HookAbort as exc:
            self.ui.write_err(_("pushkey-abort: %s\n") % exc)
            if exc.hint:
                self.ui.write_err(_("(%s)\n") % exc.hint)
            return False
        self.ui.debug('pushing key for "%s:%s"\n' % (namespace, key))
        ret = pushkey.push(self, namespace, key, old, new)
        def runhook():
            self.hook('pushkey', namespace=namespace, key=key, old=old, new=new,
                      ret=ret)
        self._afterlock(runhook)
        return ret

    def listkeys(self, namespace):
        self.hook('prelistkeys', throw=True, namespace=namespace)
        self.ui.debug('listing keys for "%s"\n' % namespace)
        values = pushkey.list(self, namespace)
        self.hook('listkeys', namespace=namespace, values=values)
        return values

    def debugwireargs(self, one, two, three=None, four=None, five=None):
        '''used to test argument passing over the wire'''
        return "%s %s %s %s %s" % (one, two, pycompat.bytestr(three),
                                   pycompat.bytestr(four),
                                   pycompat.bytestr(five))

    def savecommitmessage(self, text):
        fp = self.vfs('last-message.txt', 'wb')
        try:
            fp.write(text)
        finally:
            fp.close()
        return self.pathto(fp.name[len(self.root) + 1:])

# used to avoid circular references so destructors work
def aftertrans(files):
    renamefiles = [tuple(t) for t in files]
    def a():
        for vfs, src, dest in renamefiles:
            # if src and dest refer to a same file, vfs.rename is a no-op,
            # leaving both src and dest on disk. delete dest to make sure
            # the rename couldn't be such a no-op.
            vfs.tryunlink(dest)
            try:
                vfs.rename(src, dest)
            except OSError: # journal file does not yet exist
                pass
    return a

def undoname(fn):
    base, name = os.path.split(fn)
    assert name.startswith('journal')
    return os.path.join(base, name.replace('journal', 'undo', 1))

def instance(ui, path, create, intents=None, createopts=None):
    localpath = util.urllocalpath(path)
    if create:
        createrepository(ui, localpath, createopts=createopts)

    return makelocalrepository(ui, localpath, intents=intents)

def islocal(path):
    return True

def defaultcreateopts(ui, createopts=None):
    """Populate the default creation options for a repository.

    A dictionary of explicitly requested creation options can be passed
    in. Missing keys will be populated.
    """
    createopts = dict(createopts or {})

    if 'backend' not in createopts:
        # experimental config: storage.new-repo-backend
        createopts['backend'] = ui.config('storage', 'new-repo-backend')

    return createopts

def newreporequirements(ui, createopts):
    """Determine the set of requirements for a new local repository.

    Extensions can wrap this function to specify custom requirements for
    new repositories.
    """
    # If the repo is being created from a shared repository, we copy
    # its requirements.
    if 'sharedrepo' in createopts:
        requirements = set(createopts['sharedrepo'].requirements)
        if createopts.get('sharedrelative'):
            requirements.add('relshared')
        else:
            requirements.add('shared')

        return requirements

    if 'backend' not in createopts:
        raise error.ProgrammingError('backend key not present in createopts; '
                                     'was defaultcreateopts() called?')

    if createopts['backend'] != 'revlogv1':
        raise error.Abort(_('unable to determine repository requirements for '
                            'storage backend: %s') % createopts['backend'])

    requirements = {'revlogv1'}
    if ui.configbool('format', 'usestore'):
        requirements.add('store')
        if ui.configbool('format', 'usefncache'):
            requirements.add('fncache')
            if ui.configbool('format', 'dotencode'):
                requirements.add('dotencode')

    compengine = ui.config('experimental', 'format.compression')
    if compengine not in util.compengines:
        raise error.Abort(_('compression engine %s defined by '
                            'experimental.format.compression not available') %
                          compengine,
                          hint=_('run "hg debuginstall" to list available '
                                 'compression engines'))

    # zlib is the historical default and doesn't need an explicit requirement.
    if compengine != 'zlib':
        requirements.add('exp-compression-%s' % compengine)

    if scmutil.gdinitconfig(ui):
        requirements.add('generaldelta')
    if ui.configbool('experimental', 'treemanifest'):
        requirements.add('treemanifest')
    # experimental config: format.sparse-revlog
    if ui.configbool('format', 'sparse-revlog'):
        requirements.add(SPARSEREVLOG_REQUIREMENT)

    revlogv2 = ui.config('experimental', 'revlogv2')
    if revlogv2 == 'enable-unstable-format-and-corrupt-my-data':
        requirements.remove('revlogv1')
        # generaldelta is implied by revlogv2.
        requirements.discard('generaldelta')
        requirements.add(REVLOGV2_REQUIREMENT)
    # experimental config: format.internal-phase
    if ui.configbool('format', 'internal-phase'):
        requirements.add('internal-phase')

    if createopts.get('narrowfiles'):
        requirements.add(repository.NARROW_REQUIREMENT)

    if createopts.get('lfs'):
        requirements.add('lfs')

    return requirements

def filterknowncreateopts(ui, createopts):
    """Filters a dict of repo creation options against options that are known.

    Receives a dict of repo creation options and returns a dict of those
    options that we don't know how to handle.

    This function is called as part of repository creation. If the
    returned dict contains any items, repository creation will not
    be allowed, as it means there was a request to create a repository
    with options not recognized by loaded code.

    Extensions can wrap this function to filter out creation options
    they know how to handle.
    """
    known = {
        'backend',
        'lfs',
        'narrowfiles',
        'sharedrepo',
        'sharedrelative',
        'shareditems',
        'shallowfilestore',
    }

    return {k: v for k, v in createopts.items() if k not in known}

def createrepository(ui, path, createopts=None):
    """Create a new repository in a vfs.

    ``path`` path to the new repo's working directory.
    ``createopts`` options for the new repository.

    The following keys for ``createopts`` are recognized:

    backend
       The storage backend to use.
    lfs
       Repository will be created with ``lfs`` requirement. The lfs extension
       will automatically be loaded when the repository is accessed.
    narrowfiles
       Set up repository to support narrow file storage.
    sharedrepo
       Repository object from which storage should be shared.
    sharedrelative
       Boolean indicating if the path to the shared repo should be
       stored as relative. By default, the pointer to the "parent" repo
       is stored as an absolute path.
    shareditems
       Set of items to share to the new repository (in addition to storage).
    shallowfilestore
       Indicates that storage for files should be shallow (not all ancestor
       revisions are known).
    """
    createopts = defaultcreateopts(ui, createopts=createopts)

    unknownopts = filterknowncreateopts(ui, createopts)

    if not isinstance(unknownopts, dict):
        raise error.ProgrammingError('filterknowncreateopts() did not return '
                                     'a dict')

    if unknownopts:
        raise error.Abort(_('unable to create repository because of unknown '
                            'creation option: %s') %
                          ', '.join(sorted(unknownopts)),
                          hint=_('is a required extension not loaded?'))

    requirements = newreporequirements(ui, createopts=createopts)

    wdirvfs = vfsmod.vfs(path, expandpath=True, realpath=True)

    hgvfs = vfsmod.vfs(wdirvfs.join(b'.hg'))
    if hgvfs.exists():
        raise error.RepoError(_('repository %s already exists') % path)

    if 'sharedrepo' in createopts:
        sharedpath = createopts['sharedrepo'].sharedpath

        if createopts.get('sharedrelative'):
            try:
                sharedpath = os.path.relpath(sharedpath, hgvfs.base)
            except (IOError, ValueError) as e:
                # ValueError is raised on Windows if the drive letters differ
                # on each path.
                raise error.Abort(_('cannot calculate relative path'),
                                  hint=stringutil.forcebytestr(e))

    if not wdirvfs.exists():
        wdirvfs.makedirs()

    hgvfs.makedir(notindexed=True)

    if b'store' in requirements and 'sharedrepo' not in createopts:
        hgvfs.mkdir(b'store')

        # We create an invalid changelog outside the store so very old
        # Mercurial versions (which didn't know about the requirements
        # file) encounter an error on reading the changelog. This
        # effectively locks out old clients and prevents them from
        # mucking with a repo in an unknown format.
        #
        # The revlog header has version 2, which won't be recognized by
        # such old clients.
        hgvfs.append(b'00changelog.i',
                     b'\0\0\0\2 dummy changelog to prevent using the old repo '
                     b'layout')

    scmutil.writerequires(hgvfs, requirements)

    # Write out file telling readers where to find the shared store.
    if 'sharedrepo' in createopts:
        hgvfs.write(b'sharedpath', sharedpath)

    if createopts.get('shareditems'):
        shared = b'\n'.join(sorted(createopts['shareditems'])) + b'\n'
        hgvfs.write(b'shared', shared)

def poisonrepository(repo):
    """Poison a repository instance so it can no longer be used."""
    # Perform any cleanup on the instance.
    repo.close()

    # Our strategy is to replace the type of the object with one that
    # has all attribute lookups result in error.
    #
    # But we have to allow the close() method because some constructors
    # of repos call close() on repo references.
    class poisonedrepository(object):
        def __getattribute__(self, item):
            if item == r'close':
                return object.__getattribute__(self, item)

            raise error.ProgrammingError('repo instances should not be used '
                                         'after unshare')

        def close(self):
            pass

    # We may have a repoview, which intercepts __setattr__. So be sure
    # we operate at the lowest level possible.
    object.__setattr__(repo, r'__class__', poisonedrepository)
