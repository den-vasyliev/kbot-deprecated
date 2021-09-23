# discovery.py - protocol changeset discovery functions
#
# Copyright 2010 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import collections

from .i18n import _
from .node import (
    nullid,
    short,
)
from . import (
    error,
    pycompat,
)

def findcommonincoming(repo, remote, heads=None, force=False):
    """Return a tuple (common, fetch, heads) used to identify the common
    subset of nodes between repo and remote.

    "common" is a list of (at least) the heads of the common subset.
    "fetch" is a list of roots of the nodes that would be incoming, to be
      supplied to changegroupsubset.
    "heads" is either the supplied heads, or else the remote's heads.
    """

    knownnode = repo.changelog.hasnode
    search = []
    fetch = set()
    seen = set()
    seenbranch = set()
    base = set()

    if not heads:
        with remote.commandexecutor() as e:
            heads = e.callcommand('heads', {}).result()

    if repo.changelog.tip() == nullid:
        base.add(nullid)
        if heads != [nullid]:
            return [nullid], [nullid], list(heads)
        return [nullid], [], heads

    # assume we're closer to the tip than the root
    # and start by examining the heads
    repo.ui.status(_("searching for changes\n"))

    unknown = []
    for h in heads:
        if not knownnode(h):
            unknown.append(h)
        else:
            base.add(h)

    if not unknown:
        return list(base), [], list(heads)

    req = set(unknown)
    reqcnt = 0
    progress = repo.ui.makeprogress(_('searching'), unit=_('queries'))

    # search through remote branches
    # a 'branch' here is a linear segment of history, with four parts:
    # head, root, first parent, second parent
    # (a branch always has two parents (or none) by definition)
    with remote.commandexecutor() as e:
        branches = e.callcommand('branches', {'nodes': unknown}).result()

    unknown = collections.deque(branches)
    while unknown:
        r = []
        while unknown:
            n = unknown.popleft()
            if n[0] in seen:
                continue

            repo.ui.debug("examining %s:%s\n"
                          % (short(n[0]), short(n[1])))
            if n[0] == nullid: # found the end of the branch
                pass
            elif n in seenbranch:
                repo.ui.debug("branch already found\n")
                continue
            elif n[1] and knownnode(n[1]): # do we know the base?
                repo.ui.debug("found incomplete branch %s:%s\n"
                              % (short(n[0]), short(n[1])))
                search.append(n[0:2]) # schedule branch range for scanning
                seenbranch.add(n)
            else:
                if n[1] not in seen and n[1] not in fetch:
                    if knownnode(n[2]) and knownnode(n[3]):
                        repo.ui.debug("found new changeset %s\n" %
                                      short(n[1]))
                        fetch.add(n[1]) # earliest unknown
                    for p in n[2:4]:
                        if knownnode(p):
                            base.add(p) # latest known

                for p in n[2:4]:
                    if p not in req and not knownnode(p):
                        r.append(p)
                        req.add(p)
            seen.add(n[0])

        if r:
            reqcnt += 1
            progress.increment()
            repo.ui.debug("request %d: %s\n" %
                        (reqcnt, " ".join(map(short, r))))
            for p in pycompat.xrange(0, len(r), 10):
                with remote.commandexecutor() as e:
                    branches = e.callcommand('branches', {
                        'nodes': r[p:p + 10],
                    }).result()

                for b in branches:
                    repo.ui.debug("received %s:%s\n" %
                                  (short(b[0]), short(b[1])))
                    unknown.append(b)

    # do binary search on the branches we found
    while search:
        newsearch = []
        reqcnt += 1
        progress.increment()

        with remote.commandexecutor() as e:
            between = e.callcommand('between', {'pairs': search}).result()

        for n, l in zip(search, between):
            l.append(n[1])
            p = n[0]
            f = 1
            for i in l:
                repo.ui.debug("narrowing %d:%d %s\n" % (f, len(l), short(i)))
                if knownnode(i):
                    if f <= 2:
                        repo.ui.debug("found new branch changeset %s\n" %
                                          short(p))
                        fetch.add(p)
                        base.add(i)
                    else:
                        repo.ui.debug("narrowed branch search to %s:%s\n"
                                      % (short(p), short(i)))
                        newsearch.append((p, i))
                    break
                p, f = i, f * 2
            search = newsearch

    # sanity check our fetch list
    for f in fetch:
        if knownnode(f):
            raise error.RepoError(_("already have changeset ")
                                  + short(f[:4]))

    base = list(base)
    if base == [nullid]:
        if force:
            repo.ui.warn(_("warning: repository is unrelated\n"))
        else:
            raise error.Abort(_("repository is unrelated"))

    repo.ui.debug("found new changesets starting at " +
                 " ".join([short(f) for f in fetch]) + "\n")

    progress.complete()
    repo.ui.debug("%d total queries\n" % reqcnt)

    return base, list(fetch), heads
