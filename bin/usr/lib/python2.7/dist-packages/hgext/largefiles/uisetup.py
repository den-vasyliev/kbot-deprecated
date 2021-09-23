# Copyright 2009-2010 Gregory P. Ward
# Copyright 2009-2010 Intelerad Medical Systems Incorporated
# Copyright 2010-2011 Fog Creek Software
# Copyright 2010-2011 Unity Technologies
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

'''setup for largefiles extension: uisetup'''
from __future__ import absolute_import

from mercurial.i18n import _

from mercurial.hgweb import (
    webcommands,
)

from mercurial import (
    archival,
    cmdutil,
    commands,
    copies,
    exchange,
    extensions,
    filemerge,
    hg,
    httppeer,
    merge,
    scmutil,
    sshpeer,
    subrepo,
    upgrade,
    url,
    wireprotov1server,
)

from . import (
    overrides,
    proto,
)

def uisetup(ui):
    # Disable auto-status for some commands which assume that all
    # files in the result are under Mercurial's control

    entry = extensions.wrapcommand(commands.table, 'add',
                                   overrides.overrideadd)
    addopt = [('', 'large', None, _('add as largefile')),
              ('', 'normal', None, _('add as normal file')),
              ('', 'lfsize', '', _('add all files above this size '
                                   '(in megabytes) as largefiles '
                                   '(default: 10)'))]
    entry[1].extend(addopt)

    # The scmutil function is called both by the (trivial) addremove command,
    # and in the process of handling commit -A (issue3542)
    extensions.wrapfunction(scmutil, 'addremove', overrides.scmutiladdremove)
    extensions.wrapfunction(cmdutil, 'add', overrides.cmdutiladd)
    extensions.wrapfunction(cmdutil, 'remove', overrides.cmdutilremove)
    extensions.wrapfunction(cmdutil, 'forget', overrides.cmdutilforget)

    extensions.wrapfunction(copies, 'pathcopies', overrides.copiespathcopies)

    extensions.wrapfunction(upgrade, 'preservedrequirements',
                            overrides.upgraderequirements)

    extensions.wrapfunction(upgrade, 'supporteddestrequirements',
                            overrides.upgraderequirements)

    # Subrepos call status function
    entry = extensions.wrapcommand(commands.table, 'status',
                                   overrides.overridestatus)
    extensions.wrapfunction(subrepo.hgsubrepo, 'status',
                            overrides.overridestatusfn)

    entry = extensions.wrapcommand(commands.table, 'log',
                                   overrides.overridelog)
    entry = extensions.wrapcommand(commands.table, 'rollback',
                                   overrides.overriderollback)
    entry = extensions.wrapcommand(commands.table, 'verify',
                                   overrides.overrideverify)

    verifyopt = [('', 'large', None,
                  _('verify that all largefiles in current revision exists')),
                 ('', 'lfa', None,
                  _('verify largefiles in all revisions, not just current')),
                 ('', 'lfc', None,
                  _('verify local largefile contents, not just existence'))]
    entry[1].extend(verifyopt)

    entry = extensions.wrapcommand(commands.table, 'debugstate',
                                   overrides.overridedebugstate)
    debugstateopt = [('', 'large', None, _('display largefiles dirstate'))]
    entry[1].extend(debugstateopt)

    outgoing = lambda orgfunc, *arg, **kwargs: orgfunc(*arg, **kwargs)
    entry = extensions.wrapcommand(commands.table, 'outgoing', outgoing)
    outgoingopt = [('', 'large', None, _('display outgoing largefiles'))]
    entry[1].extend(outgoingopt)
    cmdutil.outgoinghooks.add('largefiles', overrides.outgoinghook)
    entry = extensions.wrapcommand(commands.table, 'summary',
                                   overrides.overridesummary)
    summaryopt = [('', 'large', None, _('display outgoing largefiles'))]
    entry[1].extend(summaryopt)
    cmdutil.summaryremotehooks.add('largefiles', overrides.summaryremotehook)

    entry = extensions.wrapcommand(commands.table, 'pull',
                                   overrides.overridepull)
    pullopt = [('', 'all-largefiles', None,
                 _('download all pulled versions of largefiles (DEPRECATED)')),
               ('', 'lfrev', [],
                _('download largefiles for these revisions'), _('REV'))]
    entry[1].extend(pullopt)

    entry = extensions.wrapcommand(commands.table, 'push',
                                   overrides.overridepush)
    pushopt = [('', 'lfrev', [],
                _('upload largefiles for these revisions'), _('REV'))]
    entry[1].extend(pushopt)
    extensions.wrapfunction(exchange, 'pushoperation',
                            overrides.exchangepushoperation)

    entry = extensions.wrapcommand(commands.table, 'clone',
                                   overrides.overrideclone)
    cloneopt = [('', 'all-largefiles', None,
                 _('download all versions of all largefiles'))]
    entry[1].extend(cloneopt)
    extensions.wrapfunction(hg, 'clone', overrides.hgclone)

    entry = extensions.wrapcommand(commands.table, 'cat',
                                   overrides.overridecat)
    extensions.wrapfunction(merge, '_checkunknownfile',
                            overrides.overridecheckunknownfile)
    extensions.wrapfunction(merge, 'calculateupdates',
                            overrides.overridecalculateupdates)
    extensions.wrapfunction(merge, 'recordupdates',
                            overrides.mergerecordupdates)
    extensions.wrapfunction(merge, 'update', overrides.mergeupdate)
    extensions.wrapfunction(filemerge, '_filemerge',
                            overrides.overridefilemerge)
    extensions.wrapfunction(cmdutil, 'copy', overrides.overridecopy)

    # Summary calls dirty on the subrepos
    extensions.wrapfunction(subrepo.hgsubrepo, 'dirty', overrides.overridedirty)

    extensions.wrapfunction(cmdutil, 'revert', overrides.overriderevert)

    extensions.wrapcommand(commands.table, 'archive',
                           overrides.overridearchivecmd)
    extensions.wrapfunction(archival, 'archive', overrides.overridearchive)
    extensions.wrapfunction(subrepo.hgsubrepo, 'archive',
                            overrides.hgsubrepoarchive)
    extensions.wrapfunction(webcommands, 'archive', overrides.hgwebarchive)
    extensions.wrapfunction(cmdutil, 'bailifchanged',
                            overrides.overridebailifchanged)

    extensions.wrapfunction(cmdutil, 'postcommitstatus',
                            overrides.postcommitstatus)
    extensions.wrapfunction(scmutil, 'marktouched',
                            overrides.scmutilmarktouched)

    extensions.wrapfunction(url, 'open',
                            overrides.openlargefile)

    # create the new wireproto commands ...
    wireprotov1server.wireprotocommand('putlfile', 'sha', permission='push')(
        proto.putlfile)
    wireprotov1server.wireprotocommand('getlfile', 'sha', permission='pull')(
        proto.getlfile)
    wireprotov1server.wireprotocommand('statlfile', 'sha', permission='pull')(
        proto.statlfile)
    wireprotov1server.wireprotocommand('lheads', '', permission='pull')(
        wireprotov1server.heads)

    # ... and wrap some existing ones
    extensions.wrapfunction(wireprotov1server.commands['heads'], 'func',
                            proto.heads)
    # TODO also wrap wireproto.commandsv2 once heads is implemented there.

    extensions.wrapfunction(webcommands, 'decodepath', overrides.decodepath)

    extensions.wrapfunction(wireprotov1server, '_capabilities',
                            proto._capabilities)

    # can't do this in reposetup because it needs to have happened before
    # wirerepo.__init__ is called
    proto.ssholdcallstream = sshpeer.sshv1peer._callstream
    proto.httpoldcallstream = httppeer.httppeer._callstream
    sshpeer.sshv1peer._callstream = proto.sshrepocallstream
    httppeer.httppeer._callstream = proto.httprepocallstream

    # override some extensions' stuff as well
    for name, module in extensions.extensions():
        if name == 'purge':
            extensions.wrapcommand(getattr(module, 'cmdtable'), 'purge',
                overrides.overridepurge)
        if name == 'rebase':
            extensions.wrapcommand(getattr(module, 'cmdtable'), 'rebase',
                overrides.overriderebase)
            extensions.wrapfunction(module, 'rebase',
                                    overrides.overriderebase)
        if name == 'transplant':
            extensions.wrapcommand(getattr(module, 'cmdtable'), 'transplant',
                overrides.overridetransplant)
