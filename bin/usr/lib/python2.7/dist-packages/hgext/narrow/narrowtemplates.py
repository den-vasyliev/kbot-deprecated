# narrowtemplates.py - added template keywords for narrow clones
#
# Copyright 2017 Google, Inc.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

from mercurial import (
    registrar,
    revlog,
)

keywords = {}
templatekeyword = registrar.templatekeyword(keywords)
revsetpredicate = registrar.revsetpredicate()

def _isellipsis(repo, rev):
    if repo.changelog.flags(rev) & revlog.REVIDX_ELLIPSIS:
        return True
    return False

@templatekeyword('ellipsis', requires={'repo', 'ctx'})
def ellipsis(context, mapping):
    """String. 'ellipsis' if the change is an ellipsis node, else ''."""
    repo = context.resource(mapping, 'repo')
    ctx = context.resource(mapping, 'ctx')
    if _isellipsis(repo, ctx.rev()):
        return 'ellipsis'
    return ''

@templatekeyword('outsidenarrow', requires={'repo', 'ctx'})
def outsidenarrow(context, mapping):
    """String. 'outsidenarrow' if the change affects no tracked files,
    else ''."""
    repo = context.resource(mapping, 'repo')
    ctx = context.resource(mapping, 'ctx')
    m = repo.narrowmatch()
    if not m.always():
        if not any(m(f) for f in ctx.files()):
            return 'outsidenarrow'
    return ''

@revsetpredicate('ellipsis()')
def ellipsisrevset(repo, subset, x):
    """Changesets that are ellipsis nodes."""
    return subset.filter(lambda r: _isellipsis(repo, r))
