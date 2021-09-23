# filemerge.py - file-level merge handling for Mercurial
#
# Copyright 2006, 2007, 2008 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import contextlib
import os
import re
import shutil

from .i18n import _
from .node import nullid, short

from . import (
    encoding,
    error,
    formatter,
    match,
    pycompat,
    registrar,
    scmutil,
    simplemerge,
    tagmerge,
    templatekw,
    templater,
    util,
)

from .utils import (
    procutil,
    stringutil,
)

def _toolstr(ui, tool, part, *args):
    return ui.config("merge-tools", tool + "." + part, *args)

def _toolbool(ui, tool, part,*args):
    return ui.configbool("merge-tools", tool + "." + part, *args)

def _toollist(ui, tool, part):
    return ui.configlist("merge-tools", tool + "." + part)

internals = {}
# Merge tools to document.
internalsdoc = {}

internaltool = registrar.internalmerge()

# internal tool merge types
nomerge = internaltool.nomerge
mergeonly = internaltool.mergeonly # just the full merge, no premerge
fullmerge = internaltool.fullmerge # both premerge and merge

_localchangedotherdeletedmsg = _(
    "file '%(fd)s' was deleted in other%(o)s but was modified in local%(l)s.\n"
    "What do you want to do?\n"
    "use (c)hanged version, (d)elete, or leave (u)nresolved?"
    "$$ &Changed $$ &Delete $$ &Unresolved")

_otherchangedlocaldeletedmsg = _(
    "file '%(fd)s' was deleted in local%(l)s but was modified in other%(o)s.\n"
    "What do you want to do?\n"
    "use (c)hanged version, leave (d)eleted, or "
    "leave (u)nresolved?"
    "$$ &Changed $$ &Deleted $$ &Unresolved")

class absentfilectx(object):
    """Represents a file that's ostensibly in a context but is actually not
    present in it.

    This is here because it's very specific to the filemerge code for now --
    other code is likely going to break with the values this returns."""
    def __init__(self, ctx, f):
        self._ctx = ctx
        self._f = f

    def path(self):
        return self._f

    def size(self):
        return None

    def data(self):
        return None

    def filenode(self):
        return nullid

    _customcmp = True
    def cmp(self, fctx):
        """compare with other file context

        returns True if different from fctx.
        """
        return not (fctx.isabsent() and
                    fctx.ctx() == self.ctx() and
                    fctx.path() == self.path())

    def flags(self):
        return ''

    def changectx(self):
        return self._ctx

    def isbinary(self):
        return False

    def isabsent(self):
        return True

def _findtool(ui, tool):
    if tool in internals:
        return tool
    cmd = _toolstr(ui, tool, "executable", tool)
    if cmd.startswith('python:'):
        return cmd
    return findexternaltool(ui, tool)

def _quotetoolpath(cmd):
    if cmd.startswith('python:'):
        return cmd
    return procutil.shellquote(cmd)

def findexternaltool(ui, tool):
    for kn in ("regkey", "regkeyalt"):
        k = _toolstr(ui, tool, kn)
        if not k:
            continue
        p = util.lookupreg(k, _toolstr(ui, tool, "regname"))
        if p:
            p = procutil.findexe(p + _toolstr(ui, tool, "regappend", ""))
            if p:
                return p
    exe = _toolstr(ui, tool, "executable", tool)
    return procutil.findexe(util.expandpath(exe))

def _picktool(repo, ui, path, binary, symlink, changedelete):
    strictcheck = ui.configbool('merge', 'strict-capability-check')

    def hascapability(tool, capability, strict=False):
        if tool in internals:
            return strict and internals[tool].capabilities.get(capability)
        return _toolbool(ui, tool, capability)

    def supportscd(tool):
        return tool in internals and internals[tool].mergetype == nomerge

    def check(tool, pat, symlink, binary, changedelete):
        tmsg = tool
        if pat:
            tmsg = _("%s (for pattern %s)") % (tool, pat)
        if not _findtool(ui, tool):
            if pat: # explicitly requested tool deserves a warning
                ui.warn(_("couldn't find merge tool %s\n") % tmsg)
            else: # configured but non-existing tools are more silent
                ui.note(_("couldn't find merge tool %s\n") % tmsg)
        elif symlink and not hascapability(tool, "symlink", strictcheck):
            ui.warn(_("tool %s can't handle symlinks\n") % tmsg)
        elif binary and not hascapability(tool, "binary", strictcheck):
            ui.warn(_("tool %s can't handle binary\n") % tmsg)
        elif changedelete and not supportscd(tool):
            # the nomerge tools are the only tools that support change/delete
            # conflicts
            pass
        elif not procutil.gui() and _toolbool(ui, tool, "gui"):
            ui.warn(_("tool %s requires a GUI\n") % tmsg)
        else:
            return True
        return False

    # internal config: ui.forcemerge
    # forcemerge comes from command line arguments, highest priority
    force = ui.config('ui', 'forcemerge')
    if force:
        toolpath = _findtool(ui, force)
        if changedelete and not supportscd(toolpath):
            return ":prompt", None
        else:
            if toolpath:
                return (force, _quotetoolpath(toolpath))
            else:
                # mimic HGMERGE if given tool not found
                return (force, force)

    # HGMERGE takes next precedence
    hgmerge = encoding.environ.get("HGMERGE")
    if hgmerge:
        if changedelete and not supportscd(hgmerge):
            return ":prompt", None
        else:
            return (hgmerge, hgmerge)

    # then patterns

    # whether binary capability should be checked strictly
    binarycap = binary and strictcheck

    for pat, tool in ui.configitems("merge-patterns"):
        mf = match.match(repo.root, '', [pat])
        if mf(path) and check(tool, pat, symlink, binarycap, changedelete):
            if binary and not hascapability(tool, "binary", strict=True):
                ui.warn(_("warning: check merge-patterns configurations,"
                          " if %r for binary file %r is unintentional\n"
                          "(see 'hg help merge-tools'"
                          " for binary files capability)\n")
                        % (pycompat.bytestr(tool), pycompat.bytestr(path)))
            toolpath = _findtool(ui, tool)
            return (tool, _quotetoolpath(toolpath))

    # then merge tools
    tools = {}
    disabled = set()
    for k, v in ui.configitems("merge-tools"):
        t = k.split('.')[0]
        if t not in tools:
            tools[t] = int(_toolstr(ui, t, "priority"))
        if _toolbool(ui, t, "disabled"):
            disabled.add(t)
    names = tools.keys()
    tools = sorted([(-p, tool) for tool, p in tools.items()
                    if tool not in disabled])
    uimerge = ui.config("ui", "merge")
    if uimerge:
        # external tools defined in uimerge won't be able to handle
        # change/delete conflicts
        if check(uimerge, path, symlink, binary, changedelete):
            if uimerge not in names and not changedelete:
                return (uimerge, uimerge)
            tools.insert(0, (None, uimerge)) # highest priority
    tools.append((None, "hgmerge")) # the old default, if found
    for p, t in tools:
        if check(t, None, symlink, binary, changedelete):
            toolpath = _findtool(ui, t)
            return (t, _quotetoolpath(toolpath))

    # internal merge or prompt as last resort
    if symlink or binary or changedelete:
        if not changedelete and len(tools):
            # any tool is rejected by capability for symlink or binary
            ui.warn(_("no tool found to merge %s\n") % path)
        return ":prompt", None
    return ":merge", None

def _eoltype(data):
    "Guess the EOL type of a file"
    if '\0' in data: # binary
        return None
    if '\r\n' in data: # Windows
        return '\r\n'
    if '\r' in data: # Old Mac
        return '\r'
    if '\n' in data: # UNIX
        return '\n'
    return None # unknown

def _matcheol(file, back):
    "Convert EOL markers in a file to match origfile"
    tostyle = _eoltype(back.data()) # No repo.wread filters?
    if tostyle:
        data = util.readfile(file)
        style = _eoltype(data)
        if style:
            newdata = data.replace(style, tostyle)
            if newdata != data:
                util.writefile(file, newdata)

@internaltool('prompt', nomerge)
def _iprompt(repo, mynode, orig, fcd, fco, fca, toolconf, labels=None):
    """Asks the user which of the local `p1()` or the other `p2()` version to
    keep as the merged version."""
    ui = repo.ui
    fd = fcd.path()

    # Avoid prompting during an in-memory merge since it doesn't support merge
    # conflicts.
    if fcd.changectx().isinmemory():
        raise error.InMemoryMergeConflictsError('in-memory merge does not '
                                                'support file conflicts')

    prompts = partextras(labels)
    prompts['fd'] = fd
    try:
        if fco.isabsent():
            index = ui.promptchoice(
                _localchangedotherdeletedmsg % prompts, 2)
            choice = ['local', 'other', 'unresolved'][index]
        elif fcd.isabsent():
            index = ui.promptchoice(
                _otherchangedlocaldeletedmsg % prompts, 2)
            choice = ['other', 'local', 'unresolved'][index]
        else:
            index = ui.promptchoice(
                _("keep (l)ocal%(l)s, take (o)ther%(o)s, or leave (u)nresolved"
                  " for %(fd)s?"
                  "$$ &Local $$ &Other $$ &Unresolved") % prompts, 2)
            choice = ['local', 'other', 'unresolved'][index]

        if choice == 'other':
            return _iother(repo, mynode, orig, fcd, fco, fca, toolconf,
                           labels)
        elif choice == 'local':
            return _ilocal(repo, mynode, orig, fcd, fco, fca, toolconf,
                           labels)
        elif choice == 'unresolved':
            return _ifail(repo, mynode, orig, fcd, fco, fca, toolconf,
                          labels)
    except error.ResponseExpected:
        ui.write("\n")
        return _ifail(repo, mynode, orig, fcd, fco, fca, toolconf,
                      labels)

@internaltool('local', nomerge)
def _ilocal(repo, mynode, orig, fcd, fco, fca, toolconf, labels=None):
    """Uses the local `p1()` version of files as the merged version."""
    return 0, fcd.isabsent()

@internaltool('other', nomerge)
def _iother(repo, mynode, orig, fcd, fco, fca, toolconf, labels=None):
    """Uses the other `p2()` version of files as the merged version."""
    if fco.isabsent():
        # local changed, remote deleted -- 'deleted' picked
        _underlyingfctxifabsent(fcd).remove()
        deleted = True
    else:
        _underlyingfctxifabsent(fcd).write(fco.data(), fco.flags())
        deleted = False
    return 0, deleted

@internaltool('fail', nomerge)
def _ifail(repo, mynode, orig, fcd, fco, fca, toolconf, labels=None):
    """
    Rather than attempting to merge files that were modified on both
    branches, it marks them as unresolved. The resolve command must be
    used to resolve these conflicts."""
    # for change/delete conflicts write out the changed version, then fail
    if fcd.isabsent():
        _underlyingfctxifabsent(fcd).write(fco.data(), fco.flags())
    return 1, False

def _underlyingfctxifabsent(filectx):
    """Sometimes when resolving, our fcd is actually an absentfilectx, but
    we want to write to it (to do the resolve). This helper returns the
    underyling workingfilectx in that case.
    """
    if filectx.isabsent():
        return filectx.changectx()[filectx.path()]
    else:
        return filectx

def _premerge(repo, fcd, fco, fca, toolconf, files, labels=None):
    tool, toolpath, binary, symlink, scriptfn = toolconf
    if symlink or fcd.isabsent() or fco.isabsent():
        return 1
    unused, unused, unused, back = files

    ui = repo.ui

    validkeep = ['keep', 'keep-merge3']

    # do we attempt to simplemerge first?
    try:
        premerge = _toolbool(ui, tool, "premerge", not binary)
    except error.ConfigError:
        premerge = _toolstr(ui, tool, "premerge", "").lower()
        if premerge not in validkeep:
            _valid = ', '.join(["'" + v + "'" for v in validkeep])
            raise error.ConfigError(_("%s.premerge not valid "
                                      "('%s' is neither boolean nor %s)") %
                                    (tool, premerge, _valid))

    if premerge:
        if premerge == 'keep-merge3':
            if not labels:
                labels = _defaultconflictlabels
            if len(labels) < 3:
                labels.append('base')
        r = simplemerge.simplemerge(ui, fcd, fca, fco, quiet=True, label=labels)
        if not r:
            ui.debug(" premerge successful\n")
            return 0
        if premerge not in validkeep:
            # restore from backup and try again
            _restorebackup(fcd, back)
    return 1 # continue merging

def _mergecheck(repo, mynode, orig, fcd, fco, fca, toolconf):
    tool, toolpath, binary, symlink, scriptfn = toolconf
    if symlink:
        repo.ui.warn(_('warning: internal %s cannot merge symlinks '
                       'for %s\n') % (tool, fcd.path()))
        return False
    if fcd.isabsent() or fco.isabsent():
        repo.ui.warn(_('warning: internal %s cannot merge change/delete '
                       'conflict for %s\n') % (tool, fcd.path()))
        return False
    return True

def _merge(repo, mynode, orig, fcd, fco, fca, toolconf, files, labels, mode):
    """
    Uses the internal non-interactive simple merge algorithm for merging
    files. It will fail if there are any conflicts and leave markers in
    the partially merged file. Markers will have two sections, one for each side
    of merge, unless mode equals 'union' which suppresses the markers."""
    ui = repo.ui

    r = simplemerge.simplemerge(ui, fcd, fca, fco, label=labels, mode=mode)
    return True, r, False

@internaltool('union', fullmerge,
              _("warning: conflicts while merging %s! "
                "(edit, then use 'hg resolve --mark')\n"),
              precheck=_mergecheck)
def _iunion(repo, mynode, orig, fcd, fco, fca, toolconf, files, labels=None):
    """
    Uses the internal non-interactive simple merge algorithm for merging
    files. It will use both left and right sides for conflict regions.
    No markers are inserted."""
    return _merge(repo, mynode, orig, fcd, fco, fca, toolconf,
                  files, labels, 'union')

@internaltool('merge', fullmerge,
              _("warning: conflicts while merging %s! "
                "(edit, then use 'hg resolve --mark')\n"),
              precheck=_mergecheck)
def _imerge(repo, mynode, orig, fcd, fco, fca, toolconf, files, labels=None):
    """
    Uses the internal non-interactive simple merge algorithm for merging
    files. It will fail if there are any conflicts and leave markers in
    the partially merged file. Markers will have two sections, one for each side
    of merge."""
    return _merge(repo, mynode, orig, fcd, fco, fca, toolconf,
                  files, labels, 'merge')

@internaltool('merge3', fullmerge,
              _("warning: conflicts while merging %s! "
                "(edit, then use 'hg resolve --mark')\n"),
              precheck=_mergecheck)
def _imerge3(repo, mynode, orig, fcd, fco, fca, toolconf, files, labels=None):
    """
    Uses the internal non-interactive simple merge algorithm for merging
    files. It will fail if there are any conflicts and leave markers in
    the partially merged file. Marker will have three sections, one from each
    side of the merge and one for the base content."""
    if not labels:
        labels = _defaultconflictlabels
    if len(labels) < 3:
        labels.append('base')
    return _imerge(repo, mynode, orig, fcd, fco, fca, toolconf, files, labels)

def _imergeauto(repo, mynode, orig, fcd, fco, fca, toolconf, files,
                labels=None, localorother=None):
    """
    Generic driver for _imergelocal and _imergeother
    """
    assert localorother is not None
    tool, toolpath, binary, symlink, scriptfn = toolconf
    r = simplemerge.simplemerge(repo.ui, fcd, fca, fco, label=labels,
                                localorother=localorother)
    return True, r

@internaltool('merge-local', mergeonly, precheck=_mergecheck)
def _imergelocal(*args, **kwargs):
    """
    Like :merge, but resolve all conflicts non-interactively in favor
    of the local `p1()` changes."""
    success, status = _imergeauto(localorother='local', *args, **kwargs)
    return success, status, False

@internaltool('merge-other', mergeonly, precheck=_mergecheck)
def _imergeother(*args, **kwargs):
    """
    Like :merge, but resolve all conflicts non-interactively in favor
    of the other `p2()` changes."""
    success, status = _imergeauto(localorother='other', *args, **kwargs)
    return success, status, False

@internaltool('tagmerge', mergeonly,
              _("automatic tag merging of %s failed! "
                "(use 'hg resolve --tool :merge' or another merge "
                "tool of your choice)\n"))
def _itagmerge(repo, mynode, orig, fcd, fco, fca, toolconf, files, labels=None):
    """
    Uses the internal tag merge algorithm (experimental).
    """
    success, status = tagmerge.merge(repo, fcd, fco, fca)
    return success, status, False

@internaltool('dump', fullmerge, binary=True, symlink=True)
def _idump(repo, mynode, orig, fcd, fco, fca, toolconf, files, labels=None):
    """
    Creates three versions of the files to merge, containing the
    contents of local, other and base. These files can then be used to
    perform a merge manually. If the file to be merged is named
    ``a.txt``, these files will accordingly be named ``a.txt.local``,
    ``a.txt.other`` and ``a.txt.base`` and they will be placed in the
    same directory as ``a.txt``.

    This implies premerge. Therefore, files aren't dumped, if premerge
    runs successfully. Use :forcedump to forcibly write files out.
    """
    a = _workingpath(repo, fcd)
    fd = fcd.path()

    from . import context
    if isinstance(fcd, context.overlayworkingfilectx):
        raise error.InMemoryMergeConflictsError('in-memory merge does not '
                                                'support the :dump tool.')

    util.writefile(a + ".local", fcd.decodeddata())
    repo.wwrite(fd + ".other", fco.data(), fco.flags())
    repo.wwrite(fd + ".base", fca.data(), fca.flags())
    return False, 1, False

@internaltool('forcedump', mergeonly, binary=True, symlink=True)
def _forcedump(repo, mynode, orig, fcd, fco, fca, toolconf, files,
                labels=None):
    """
    Creates three versions of the files as same as :dump, but omits premerge.
    """
    return _idump(repo, mynode, orig, fcd, fco, fca, toolconf, files,
                labels=labels)

def _xmergeimm(repo, mynode, orig, fcd, fco, fca, toolconf, files, labels=None):
    # In-memory merge simply raises an exception on all external merge tools,
    # for now.
    #
    # It would be possible to run most tools with temporary files, but this
    # raises the question of what to do if the user only partially resolves the
    # file -- we can't leave a merge state. (Copy to somewhere in the .hg/
    # directory and tell the user how to get it is my best idea, but it's
    # clunky.)
    raise error.InMemoryMergeConflictsError('in-memory merge does not support '
                                            'external merge tools')

def _xmerge(repo, mynode, orig, fcd, fco, fca, toolconf, files, labels=None):
    tool, toolpath, binary, symlink, scriptfn = toolconf
    if fcd.isabsent() or fco.isabsent():
        repo.ui.warn(_('warning: %s cannot merge change/delete conflict '
                       'for %s\n') % (tool, fcd.path()))
        return False, 1, None
    unused, unused, unused, back = files
    localpath = _workingpath(repo, fcd)
    args = _toolstr(repo.ui, tool, "args")

    with _maketempfiles(repo, fco, fca, repo.wvfs.join(back.path()),
                        "$output" in args) as temppaths:
        basepath, otherpath, localoutputpath = temppaths
        outpath = ""
        mylabel, otherlabel = labels[:2]
        if len(labels) >= 3:
            baselabel = labels[2]
        else:
            baselabel = 'base'
        env = {'HG_FILE': fcd.path(),
               'HG_MY_NODE': short(mynode),
               'HG_OTHER_NODE': short(fco.changectx().node()),
               'HG_BASE_NODE': short(fca.changectx().node()),
               'HG_MY_ISLINK': 'l' in fcd.flags(),
               'HG_OTHER_ISLINK': 'l' in fco.flags(),
               'HG_BASE_ISLINK': 'l' in fca.flags(),
               'HG_MY_LABEL': mylabel,
               'HG_OTHER_LABEL': otherlabel,
               'HG_BASE_LABEL': baselabel,
               }
        ui = repo.ui

        if "$output" in args:
            # read input from backup, write to original
            outpath = localpath
            localpath = localoutputpath
        replace = {'local': localpath, 'base': basepath, 'other': otherpath,
                   'output': outpath, 'labellocal': mylabel,
                   'labelother': otherlabel, 'labelbase': baselabel}
        args = util.interpolate(
            br'\$', replace, args,
            lambda s: procutil.shellquote(util.localpath(s)))
        if _toolbool(ui, tool, "gui"):
            repo.ui.status(_('running merge tool %s for file %s\n') %
                           (tool, fcd.path()))
        if scriptfn is None:
            cmd = toolpath + ' ' + args
            repo.ui.debug('launching merge tool: %s\n' % cmd)
            r = ui.system(cmd, cwd=repo.root, environ=env,
                          blockedtag='mergetool')
        else:
            repo.ui.debug('launching python merge script: %s:%s\n' %
                          (toolpath, scriptfn))
            r = 0
            try:
                # avoid cycle cmdutil->merge->filemerge->extensions->cmdutil
                from . import extensions
                mod = extensions.loadpath(toolpath, 'hgmerge.%s' % tool)
            except Exception:
                raise error.Abort(_("loading python merge script failed: %s") %
                                  toolpath)
            mergefn = getattr(mod, scriptfn, None)
            if mergefn is None:
                raise error.Abort(_("%s does not have function: %s") %
                                  (toolpath, scriptfn))
            argslist = procutil.shellsplit(args)
            # avoid cycle cmdutil->merge->filemerge->hook->extensions->cmdutil
            from . import hook
            ret, raised = hook.pythonhook(ui, repo, "merge", toolpath,
                                          mergefn, {'args': argslist}, True)
            if raised:
                r = 1
        repo.ui.debug('merge tool returned: %d\n' % r)
        return True, r, False

def _formatconflictmarker(ctx, template, label, pad):
    """Applies the given template to the ctx, prefixed by the label.

    Pad is the minimum width of the label prefix, so that multiple markers
    can have aligned templated parts.
    """
    if ctx.node() is None:
        ctx = ctx.p1()

    props = {'ctx': ctx}
    templateresult = template.renderdefault(props)

    label = ('%s:' % label).ljust(pad + 1)
    mark = '%s %s' % (label, templateresult)

    if mark:
        mark = mark.splitlines()[0] # split for safety

    # 8 for the prefix of conflict marker lines (e.g. '<<<<<<< ')
    return stringutil.ellipsis(mark, 80 - 8)

_defaultconflictlabels = ['local', 'other']

def _formatlabels(repo, fcd, fco, fca, labels, tool=None):
    """Formats the given labels using the conflict marker template.

    Returns a list of formatted labels.
    """
    cd = fcd.changectx()
    co = fco.changectx()
    ca = fca.changectx()

    ui = repo.ui
    template = ui.config('ui', 'mergemarkertemplate')
    if tool is not None:
        template = _toolstr(ui, tool, 'mergemarkertemplate', template)
    template = templater.unquotestring(template)
    tres = formatter.templateresources(ui, repo)
    tmpl = formatter.maketemplater(ui, template, defaults=templatekw.keywords,
                                   resources=tres)

    pad = max(len(l) for l in labels)

    newlabels = [_formatconflictmarker(cd, tmpl, labels[0], pad),
                 _formatconflictmarker(co, tmpl, labels[1], pad)]
    if len(labels) > 2:
        newlabels.append(_formatconflictmarker(ca, tmpl, labels[2], pad))
    return newlabels

def partextras(labels):
    """Return a dictionary of extra labels for use in prompts to the user

    Intended use is in strings of the form "(l)ocal%(l)s".
    """
    if labels is None:
        return {
            "l": "",
            "o": "",
        }

    return {
        "l": " [%s]" % labels[0],
        "o": " [%s]" % labels[1],
    }

def _restorebackup(fcd, back):
    # TODO: Add a workingfilectx.write(otherfilectx) path so we can use
    # util.copy here instead.
    fcd.write(back.data(), fcd.flags())

def _makebackup(repo, ui, wctx, fcd, premerge):
    """Makes and returns a filectx-like object for ``fcd``'s backup file.

    In addition to preserving the user's pre-existing modifications to `fcd`
    (if any), the backup is used to undo certain premerges, confirm whether a
    merge changed anything, and determine what line endings the new file should
    have.

    Backups only need to be written once (right before the premerge) since their
    content doesn't change afterwards.
    """
    if fcd.isabsent():
        return None
    # TODO: Break this import cycle somehow. (filectx -> ctx -> fileset ->
    # merge -> filemerge). (I suspect the fileset import is the weakest link)
    from . import context
    a = _workingpath(repo, fcd)
    back = scmutil.origpath(ui, repo, a)
    inworkingdir = (back.startswith(repo.wvfs.base) and not
        back.startswith(repo.vfs.base))
    if isinstance(fcd, context.overlayworkingfilectx) and inworkingdir:
        # If the backup file is to be in the working directory, and we're
        # merging in-memory, we must redirect the backup to the memory context
        # so we don't disturb the working directory.
        relpath = back[len(repo.wvfs.base) + 1:]
        if premerge:
            wctx[relpath].write(fcd.data(), fcd.flags())
        return wctx[relpath]
    else:
        if premerge:
            # Otherwise, write to wherever path the user specified the backups
            # should go. We still need to switch based on whether the source is
            # in-memory so we can use the fast path of ``util.copy`` if both are
            # on disk.
            if isinstance(fcd, context.overlayworkingfilectx):
                util.writefile(back, fcd.data())
            else:
                util.copyfile(a, back)
        # A arbitraryfilectx is returned, so we can run the same functions on
        # the backup context regardless of where it lives.
        return context.arbitraryfilectx(back, repo=repo)

@contextlib.contextmanager
def _maketempfiles(repo, fco, fca, localpath, uselocalpath):
    """Writes out `fco` and `fca` as temporary files, and (if uselocalpath)
    copies `localpath` to another temporary file, so an external merge tool may
    use them.
    """
    tmproot = None
    tmprootprefix = repo.ui.config('experimental', 'mergetempdirprefix')
    if tmprootprefix:
        tmproot = pycompat.mkdtemp(prefix=tmprootprefix)

    def maketempfrompath(prefix, path):
        fullbase, ext = os.path.splitext(path)
        pre = "%s~%s" % (os.path.basename(fullbase), prefix)
        if tmproot:
            name = os.path.join(tmproot, pre)
            if ext:
                name += ext
            f = open(name, r"wb")
        else:
            fd, name = pycompat.mkstemp(prefix=pre + '.', suffix=ext)
            f = os.fdopen(fd, r"wb")
        return f, name

    def tempfromcontext(prefix, ctx):
        f, name = maketempfrompath(prefix, ctx.path())
        data = repo.wwritedata(ctx.path(), ctx.data())
        f.write(data)
        f.close()
        return name

    b = tempfromcontext("base", fca)
    c = tempfromcontext("other", fco)
    d = localpath
    if uselocalpath:
        # We start off with this being the backup filename, so remove the .orig
        # to make syntax-highlighting more likely.
        if d.endswith('.orig'):
            d, _ = os.path.splitext(d)
        f, d = maketempfrompath("local", d)
        with open(localpath, 'rb') as src:
            f.write(src.read())
        f.close()

    try:
        yield b, c, d
    finally:
        if tmproot:
            shutil.rmtree(tmproot)
        else:
            util.unlink(b)
            util.unlink(c)
            # if not uselocalpath, d is the 'orig'/backup file which we
            # shouldn't delete.
            if d and uselocalpath:
                util.unlink(d)

def _filemerge(premerge, repo, wctx, mynode, orig, fcd, fco, fca, labels=None):
    """perform a 3-way merge in the working directory

    premerge = whether this is a premerge
    mynode = parent node before merge
    orig = original local filename before merge
    fco = other file context
    fca = ancestor file context
    fcd = local file context for current/destination file

    Returns whether the merge is complete, the return value of the merge, and
    a boolean indicating whether the file was deleted from disk."""

    if not fco.cmp(fcd): # files identical?
        return True, None, False

    ui = repo.ui
    fd = fcd.path()
    binary = fcd.isbinary() or fco.isbinary() or fca.isbinary()
    symlink = 'l' in fcd.flags() + fco.flags()
    changedelete = fcd.isabsent() or fco.isabsent()
    tool, toolpath = _picktool(repo, ui, fd, binary, symlink, changedelete)
    scriptfn = None
    if tool in internals and tool.startswith('internal:'):
        # normalize to new-style names (':merge' etc)
        tool = tool[len('internal'):]
    if toolpath and toolpath.startswith('python:'):
        invalidsyntax = False
        if toolpath.count(':') >= 2:
            script, scriptfn = toolpath[7:].rsplit(':', 1)
            if not scriptfn:
                invalidsyntax = True
            # missing :callable can lead to spliting on windows drive letter
            if '\\' in scriptfn or '/' in scriptfn:
                invalidsyntax = True
        else:
            invalidsyntax = True
        if invalidsyntax:
            raise error.Abort(_("invalid 'python:' syntax: %s") % toolpath)
        toolpath = script
    ui.debug("picked tool '%s' for %s (binary %s symlink %s changedelete %s)\n"
             % (tool, fd, pycompat.bytestr(binary), pycompat.bytestr(symlink),
                    pycompat.bytestr(changedelete)))

    if tool in internals:
        func = internals[tool]
        mergetype = func.mergetype
        onfailure = func.onfailure
        precheck = func.precheck
        isexternal = False
    else:
        if wctx.isinmemory():
            func = _xmergeimm
        else:
            func = _xmerge
        mergetype = fullmerge
        onfailure = _("merging %s failed!\n")
        precheck = None
        isexternal = True

    toolconf = tool, toolpath, binary, symlink, scriptfn

    if mergetype == nomerge:
        r, deleted = func(repo, mynode, orig, fcd, fco, fca, toolconf, labels)
        return True, r, deleted

    if premerge:
        if orig != fco.path():
            ui.status(_("merging %s and %s to %s\n") % (orig, fco.path(), fd))
        else:
            ui.status(_("merging %s\n") % fd)

    ui.debug("my %s other %s ancestor %s\n" % (fcd, fco, fca))

    if precheck and not precheck(repo, mynode, orig, fcd, fco, fca,
                                 toolconf):
        if onfailure:
            if wctx.isinmemory():
                raise error.InMemoryMergeConflictsError('in-memory merge does '
                                                        'not support merge '
                                                        'conflicts')
            ui.warn(onfailure % fd)
        return True, 1, False

    back = _makebackup(repo, ui, wctx, fcd, premerge)
    files = (None, None, None, back)
    r = 1
    try:
        internalmarkerstyle = ui.config('ui', 'mergemarkers')
        if isexternal:
            markerstyle = _toolstr(ui, tool, 'mergemarkers')
        else:
            markerstyle = internalmarkerstyle

        if not labels:
            labels = _defaultconflictlabels
        formattedlabels = labels
        if markerstyle != 'basic':
            formattedlabels = _formatlabels(repo, fcd, fco, fca, labels,
                                            tool=tool)

        if premerge and mergetype == fullmerge:
            # conflict markers generated by premerge will use 'detailed'
            # settings if either ui.mergemarkers or the tool's mergemarkers
            # setting is 'detailed'. This way tools can have basic labels in
            # space-constrained areas of the UI, but still get full information
            # in conflict markers if premerge is 'keep' or 'keep-merge3'.
            premergelabels = labels
            labeltool = None
            if markerstyle != 'basic':
                # respect 'tool's mergemarkertemplate (which defaults to
                # ui.mergemarkertemplate)
                labeltool = tool
            if internalmarkerstyle != 'basic' or markerstyle != 'basic':
                premergelabels = _formatlabels(repo, fcd, fco, fca,
                                               premergelabels, tool=labeltool)

            r = _premerge(repo, fcd, fco, fca, toolconf, files,
                          labels=premergelabels)
            # complete if premerge successful (r is 0)
            return not r, r, False

        needcheck, r, deleted = func(repo, mynode, orig, fcd, fco, fca,
                                     toolconf, files, labels=formattedlabels)

        if needcheck:
            r = _check(repo, r, ui, tool, fcd, files)

        if r:
            if onfailure:
                if wctx.isinmemory():
                    raise error.InMemoryMergeConflictsError('in-memory merge '
                                                            'does not support '
                                                            'merge conflicts')
                ui.warn(onfailure % fd)
            _onfilemergefailure(ui)

        return True, r, deleted
    finally:
        if not r and back is not None:
            back.remove()

def _haltmerge():
    msg = _('merge halted after failed merge (see hg resolve)')
    raise error.InterventionRequired(msg)

def _onfilemergefailure(ui):
    action = ui.config('merge', 'on-failure')
    if action == 'prompt':
        msg = _('continue merge operation (yn)?' '$$ &Yes $$ &No')
        if ui.promptchoice(msg, 0) == 1:
            _haltmerge()
    if action == 'halt':
        _haltmerge()
    # default action is 'continue', in which case we neither prompt nor halt

def hasconflictmarkers(data):
    return bool(re.search("^(<<<<<<< .*|=======|>>>>>>> .*)$", data,
                          re.MULTILINE))

def _check(repo, r, ui, tool, fcd, files):
    fd = fcd.path()
    unused, unused, unused, back = files

    if not r and (_toolbool(ui, tool, "checkconflicts") or
                  'conflicts' in _toollist(ui, tool, "check")):
        if hasconflictmarkers(fcd.data()):
            r = 1

    checked = False
    if 'prompt' in _toollist(ui, tool, "check"):
        checked = True
        if ui.promptchoice(_("was merge of '%s' successful (yn)?"
                             "$$ &Yes $$ &No") % fd, 1):
            r = 1

    if not r and not checked and (_toolbool(ui, tool, "checkchanged") or
                                  'changed' in
                                  _toollist(ui, tool, "check")):
        if back is not None and not fcd.cmp(back):
            if ui.promptchoice(_(" output file %s appears unchanged\n"
                                 "was merge successful (yn)?"
                                 "$$ &Yes $$ &No") % fd, 1):
                r = 1

    if back is not None and _toolbool(ui, tool, "fixeol"):
        _matcheol(_workingpath(repo, fcd), back)

    return r

def _workingpath(repo, ctx):
    return repo.wjoin(ctx.path())

def premerge(repo, wctx, mynode, orig, fcd, fco, fca, labels=None):
    return _filemerge(True, repo, wctx, mynode, orig, fcd, fco, fca,
                      labels=labels)

def filemerge(repo, wctx, mynode, orig, fcd, fco, fca, labels=None):
    return _filemerge(False, repo, wctx, mynode, orig, fcd, fco, fca,
                      labels=labels)

def loadinternalmerge(ui, extname, registrarobj):
    """Load internal merge tool from specified registrarobj
    """
    for name, func in registrarobj._table.iteritems():
        fullname = ':' + name
        internals[fullname] = func
        internals['internal:' + name] = func
        internalsdoc[fullname] = func

        capabilities = sorted([k for k, v in func.capabilities.items() if v])
        if capabilities:
            capdesc = "    (actual capabilities: %s)" % ', '.join(capabilities)
            func.__doc__ = (func.__doc__ +
                            pycompat.sysstr("\n\n%s" % capdesc))

    # to put i18n comments into hg.pot for automatically generated texts

    # i18n: "binary" and "symlink" are keywords
    # i18n: this text is added automatically
    _("    (actual capabilities: binary, symlink)")
    # i18n: "binary" is keyword
    # i18n: this text is added automatically
    _("    (actual capabilities: binary)")
    # i18n: "symlink" is keyword
    # i18n: this text is added automatically
    _("    (actual capabilities: symlink)")

# load built-in merge tools explicitly to setup internalsdoc
loadinternalmerge(None, None, internaltool)

# tell hggettext to extract docstrings from these functions:
i18nfunctions = internals.values()
