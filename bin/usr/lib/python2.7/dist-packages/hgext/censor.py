# Copyright (C) 2015 - Mike Edgar <adgar@google.com>
#
# This extension enables removal of file content at a given revision,
# rewriting the data/metadata of successive revisions to preserve revision log
# integrity.

"""erase file content at a given revision

The censor command instructs Mercurial to erase all content of a file at a given
revision *without updating the changeset hash.* This allows existing history to
remain valid while preventing future clones/pulls from receiving the erased
data.

Typical uses for censor are due to security or legal requirements, including::

 * Passwords, private keys, cryptographic material
 * Licensed data/code/libraries for which the license has expired
 * Personally Identifiable Information or other private data

Censored nodes can interrupt mercurial's typical operation whenever the excised
data needs to be materialized. Some commands, like ``hg cat``/``hg revert``,
simply fail when asked to produce censored data. Others, like ``hg verify`` and
``hg update``, must be capable of tolerating censored data to continue to
function in a meaningful way. Such commands only tolerate censored file
revisions if they are allowed by the "censor.policy=ignore" config option.
"""

from __future__ import absolute_import

from mercurial.i18n import _
from mercurial.node import short

from mercurial import (
    error,
    registrar,
    scmutil,
)

cmdtable = {}
command = registrar.command(cmdtable)
# Note for extension authors: ONLY specify testedwith = 'ships-with-hg-core' for
# extensions which SHIP WITH MERCURIAL. Non-mainline extensions should
# be specifying the version(s) of Mercurial they are tested with, or
# leave the attribute unspecified.
testedwith = 'ships-with-hg-core'

@command('censor',
    [('r', 'rev', '', _('censor file from specified revision'), _('REV')),
     ('t', 'tombstone', '', _('replacement tombstone data'), _('TEXT'))],
    _('-r REV [-t TEXT] [FILE]'),
    helpcategory=command.CATEGORY_MAINTENANCE)
def censor(ui, repo, path, rev='', tombstone='', **opts):
    with repo.wlock(), repo.lock():
        return _docensor(ui, repo, path, rev, tombstone, **opts)

def _docensor(ui, repo, path, rev='', tombstone='', **opts):
    if not path:
        raise error.Abort(_('must specify file path to censor'))
    if not rev:
        raise error.Abort(_('must specify revision to censor'))

    wctx = repo[None]

    m = scmutil.match(wctx, (path,))
    if m.anypats() or len(m.files()) != 1:
        raise error.Abort(_('can only specify an explicit filename'))
    path = m.files()[0]
    flog = repo.file(path)
    if not len(flog):
        raise error.Abort(_('cannot censor file with no history'))

    rev = scmutil.revsingle(repo, rev, rev).rev()
    try:
        ctx = repo[rev]
    except KeyError:
        raise error.Abort(_('invalid revision identifier %s') % rev)

    try:
        fctx = ctx.filectx(path)
    except error.LookupError:
        raise error.Abort(_('file does not exist at revision %s') % rev)

    fnode = fctx.filenode()
    heads = []
    for headnode in repo.heads():
        hc = repo[headnode]
        if path in hc and hc.filenode(path) == fnode:
            heads.append(hc)
    if heads:
        headlist = ', '.join([short(c.node()) for c in heads])
        raise error.Abort(_('cannot censor file in heads (%s)') % headlist,
            hint=_('clean/delete and commit first'))

    wp = wctx.parents()
    if ctx.node() in [p.node() for p in wp]:
        raise error.Abort(_('cannot censor working directory'),
            hint=_('clean/delete/update first'))

    with repo.transaction(b'censor') as tr:
        flog.censorrevision(tr, fnode, tombstone=tombstone)
