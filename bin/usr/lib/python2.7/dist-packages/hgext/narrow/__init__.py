# __init__.py - narrowhg extension
#
# Copyright 2017 Google, Inc.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.
'''create clones which fetch history data for subset of files (EXPERIMENTAL)'''

from __future__ import absolute_import

# Note for extension authors: ONLY specify testedwith = 'ships-with-hg-core' for
# extensions which SHIP WITH MERCURIAL. Non-mainline extensions should
# be specifying the version(s) of Mercurial they are tested with, or
# leave the attribute unspecified.
testedwith = 'ships-with-hg-core'

from mercurial import (
    localrepo,
    registrar,
    repository,
)

from . import (
    narrowbundle2,
    narrowcommands,
    narrowrepo,
    narrowtemplates,
    narrowwirepeer,
)

configtable = {}
configitem = registrar.configitem(configtable)
# Narrowhg *has* support for serving ellipsis nodes (which are used at
# least by Google's internal server), but that support is pretty
# fragile and has a lot of problems on real-world repositories that
# have complex graph topologies. This could probably be corrected, but
# absent someone needing the full support for ellipsis nodes in
# repositories with merges, it's unlikely this work will get done. As
# of this writining in late 2017, all repositories large enough for
# ellipsis nodes to be a hard requirement also enforce strictly linear
# history for other scaling reasons.
configitem('experimental', 'narrowservebrokenellipses',
           default=False,
           alias=[('narrow', 'serveellipses')],
)

# Export the commands table for Mercurial to see.
cmdtable = narrowcommands.table

def featuresetup(ui, features):
    features.add(repository.NARROW_REQUIREMENT)

def uisetup(ui):
    """Wraps user-facing mercurial commands with narrow-aware versions."""
    localrepo.featuresetupfuncs.add(featuresetup)
    narrowbundle2.setup()
    narrowcommands.setup()
    narrowwirepeer.uisetup()

def reposetup(ui, repo):
    """Wraps local repositories with narrow repo support."""
    if not repo.local():
        return

    repo.ui.setconfig('experimental', 'narrow', True, 'narrow-ext')
    if repository.NARROW_REQUIREMENT in repo.requirements:
        narrowrepo.wraprepo(repo)
        narrowwirepeer.reposetup(repo)

templatekeyword = narrowtemplates.templatekeyword
revsetpredicate = narrowtemplates.revsetpredicate
