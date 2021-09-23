# repoview.py - Filtered view of a localrepo object
#
# Copyright 2012 Pierre-Yves David <pierre-yves.david@ens-lyon.org>
#                Logilab SA        <contact@logilab.fr>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import copy
import weakref

from .node import nullrev
from . import (
    obsolete,
    phases,
    pycompat,
    tags as tagsmod,
)

def hideablerevs(repo):
    """Revision candidates to be hidden

    This is a standalone function to allow extensions to wrap it.

    Because we use the set of immutable changesets as a fallback subset in
    branchmap (see mercurial.branchmap.subsettable), you cannot set "public"
    changesets as "hideable". Doing so would break multiple code assertions and
    lead to crashes."""
    obsoletes = obsolete.getrevs(repo, 'obsolete')
    internals = repo._phasecache.getrevset(repo, phases.localhiddenphases)
    internals = frozenset(internals)
    return obsoletes | internals

def pinnedrevs(repo):
    """revisions blocking hidden changesets from being filtered
    """

    cl = repo.changelog
    pinned = set()
    pinned.update([par.rev() for par in repo[None].parents()])
    pinned.update([cl.rev(bm) for bm in repo._bookmarks.values()])

    tags = {}
    tagsmod.readlocaltags(repo.ui, repo, tags, {})
    if tags:
        rev, nodemap = cl.rev, cl.nodemap
        pinned.update(rev(t[0]) for t in tags.values() if t[0] in nodemap)
    return pinned


def _revealancestors(pfunc, hidden, revs):
    """reveals contiguous chains of hidden ancestors of 'revs' by removing them
    from 'hidden'

    - pfunc(r): a funtion returning parent of 'r',
    - hidden: the (preliminary) hidden revisions, to be updated
    - revs: iterable of revnum,

    (Ancestors are revealed exclusively, i.e. the elements in 'revs' are
    *not* revealed)
    """
    stack = list(revs)
    while stack:
        for p in pfunc(stack.pop()):
            if p != nullrev and p in hidden:
                hidden.remove(p)
                stack.append(p)

def computehidden(repo, visibilityexceptions=None):
    """compute the set of hidden revision to filter

    During most operation hidden should be filtered."""
    assert not repo.changelog.filteredrevs

    hidden = hideablerevs(repo)
    if hidden:
        hidden = set(hidden - pinnedrevs(repo))
        if visibilityexceptions:
            hidden -= visibilityexceptions
        pfunc = repo.changelog.parentrevs
        mutable = repo._phasecache.getrevset(repo, phases.mutablephases)

        visible = mutable - hidden
        _revealancestors(pfunc, hidden, visible)
    return frozenset(hidden)

def computeunserved(repo, visibilityexceptions=None):
    """compute the set of revision that should be filtered when used a server

    Secret and hidden changeset should not pretend to be here."""
    assert not repo.changelog.filteredrevs
    # fast path in simple case to avoid impact of non optimised code
    hiddens = filterrevs(repo, 'visible')
    if phases.hassecret(repo):
        secrets = repo._phasecache.getrevset(repo, phases.remotehiddenphases)
        return frozenset(hiddens | frozenset(secrets))
    else:
        return hiddens

def computemutable(repo, visibilityexceptions=None):
    assert not repo.changelog.filteredrevs
    # fast check to avoid revset call on huge repo
    if any(repo._phasecache.phaseroots[1:]):
        getphase = repo._phasecache.phase
        maymutable = filterrevs(repo, 'base')
        return frozenset(r for r in maymutable if getphase(repo, r))
    return frozenset()

def computeimpactable(repo, visibilityexceptions=None):
    """Everything impactable by mutable revision

    The immutable filter still have some chance to get invalidated. This will
    happen when:

    - you garbage collect hidden changeset,
    - public phase is moved backward,
    - something is changed in the filtering (this could be fixed)

    This filter out any mutable changeset and any public changeset that may be
    impacted by something happening to a mutable revision.

    This is achieved by filtered everything with a revision number egal or
    higher than the first mutable changeset is filtered."""
    assert not repo.changelog.filteredrevs
    cl = repo.changelog
    firstmutable = len(cl)
    for roots in repo._phasecache.phaseroots[1:]:
        if roots:
            firstmutable = min(firstmutable, min(cl.rev(r) for r in roots))
    # protect from nullrev root
    firstmutable = max(0, firstmutable)
    return frozenset(pycompat.xrange(firstmutable, len(cl)))

# function to compute filtered set
#
# When adding a new filter you MUST update the table at:
#     mercurial.branchmap.subsettable
# Otherwise your filter will have to recompute all its branches cache
# from scratch (very slow).
filtertable = {'visible': computehidden,
               'visible-hidden': computehidden,
               'served': computeunserved,
               'immutable':  computemutable,
               'base':  computeimpactable}

def filterrevs(repo, filtername, visibilityexceptions=None):
    """returns set of filtered revision for this filter name

    visibilityexceptions is a set of revs which must are exceptions for
    hidden-state and must be visible. They are dynamic and hence we should not
    cache it's result"""
    if filtername not in repo.filteredrevcache:
        func = filtertable[filtername]
        if visibilityexceptions:
            return func(repo.unfiltered, visibilityexceptions)
        repo.filteredrevcache[filtername] = func(repo.unfiltered())
    return repo.filteredrevcache[filtername]

class repoview(object):
    """Provide a read/write view of a repo through a filtered changelog

    This object is used to access a filtered version of a repository without
    altering the original repository object itself. We can not alter the
    original object for two main reasons:
    - It prevents the use of a repo with multiple filters at the same time. In
      particular when multiple threads are involved.
    - It makes scope of the filtering harder to control.

    This object behaves very closely to the original repository. All attribute
    operations are done on the original repository:
    - An access to `repoview.someattr` actually returns `repo.someattr`,
    - A write to `repoview.someattr` actually sets value of `repo.someattr`,
    - A deletion of `repoview.someattr` actually drops `someattr`
      from `repo.__dict__`.

    The only exception is the `changelog` property. It is overridden to return
    a (surface) copy of `repo.changelog` with some revisions filtered. The
    `filtername` attribute of the view control the revisions that need to be
    filtered.  (the fact the changelog is copied is an implementation detail).

    Unlike attributes, this object intercepts all method calls. This means that
    all methods are run on the `repoview` object with the filtered `changelog`
    property. For this purpose the simple `repoview` class must be mixed with
    the actual class of the repository. This ensures that the resulting
    `repoview` object have the very same methods than the repo object. This
    leads to the property below.

        repoview.method() --> repo.__class__.method(repoview)

    The inheritance has to be done dynamically because `repo` can be of any
    subclasses of `localrepo`. Eg: `bundlerepo` or `statichttprepo`.
    """

    def __init__(self, repo, filtername, visibilityexceptions=None):
        object.__setattr__(self, r'_unfilteredrepo', repo)
        object.__setattr__(self, r'filtername', filtername)
        object.__setattr__(self, r'_clcachekey', None)
        object.__setattr__(self, r'_clcache', None)
        # revs which are exceptions and must not be hidden
        object.__setattr__(self, r'_visibilityexceptions',
                           visibilityexceptions)

    # not a propertycache on purpose we shall implement a proper cache later
    @property
    def changelog(self):
        """return a filtered version of the changeset

        this changelog must not be used for writing"""
        # some cache may be implemented later
        unfi = self._unfilteredrepo
        unfichangelog = unfi.changelog
        # bypass call to changelog.method
        unfiindex = unfichangelog.index
        unfilen = len(unfiindex)
        unfinode = unfiindex[unfilen - 1][7]

        revs = filterrevs(unfi, self.filtername, self._visibilityexceptions)
        cl = self._clcache
        newkey = (unfilen, unfinode, hash(revs), unfichangelog._delayed)
        # if cl.index is not unfiindex, unfi.changelog would be
        # recreated, and our clcache refers to garbage object
        if (cl is not None and
            (cl.index is not unfiindex or newkey != self._clcachekey)):
            cl = None
        # could have been made None by the previous if
        if cl is None:
            cl = copy.copy(unfichangelog)
            cl.filteredrevs = revs
            object.__setattr__(self, r'_clcache', cl)
            object.__setattr__(self, r'_clcachekey', newkey)
        return cl

    def unfiltered(self):
        """Return an unfiltered version of a repo"""
        return self._unfilteredrepo

    def filtered(self, name, visibilityexceptions=None):
        """Return a filtered version of a repository"""
        if name == self.filtername and not visibilityexceptions:
            return self
        return self.unfiltered().filtered(name, visibilityexceptions)

    def __repr__(self):
        return r'<%s:%s %r>' % (self.__class__.__name__,
                                pycompat.sysstr(self.filtername),
                                self.unfiltered())

    # everything access are forwarded to the proxied repo
    def __getattr__(self, attr):
        return getattr(self._unfilteredrepo, attr)

    def __setattr__(self, attr, value):
        return setattr(self._unfilteredrepo, attr, value)

    def __delattr__(self, attr):
        return delattr(self._unfilteredrepo, attr)

# Python <3.4 easily leaks types via __mro__. See
# https://bugs.python.org/issue17950. We cache dynamically created types
# so they won't be leaked on every invocation of repo.filtered().
_filteredrepotypes = weakref.WeakKeyDictionary()

def newtype(base):
    """Create a new type with the repoview mixin and the given base class"""
    if base not in _filteredrepotypes:
        class filteredrepo(repoview, base):
            pass
        _filteredrepotypes[base] = filteredrepo
    return _filteredrepotypes[base]
