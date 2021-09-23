# stack.py - Mercurial functions for stack definition
#
#  Copyright Matt Mackall <mpm@selenic.com> and other
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

from . import (
    revsetlang,
    scmutil,
)

def getstack(repo, rev=None):
    """return a sorted smartrev of the stack containing either rev if it is
    not None or the current working directory parent.

    The stack will always contain all drafts changesets which are ancestors to
    the revision and are not merges.
    """
    if rev is None:
        rev = '.'

    revspec = 'reverse(only(%s) and not public() and not ::merge())'
    revset = revsetlang.formatspec(revspec, rev)
    revisions = scmutil.revrange(repo, [revset])
    revisions.sort()
    return revisions
