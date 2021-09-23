# hook.py - hook support for mercurial
#
# Copyright 2007 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import os
import sys

from .i18n import _
from . import (
    demandimport,
    encoding,
    error,
    extensions,
    pycompat,
    util,
)
from .utils import (
    procutil,
    stringutil,
)

def pythonhook(ui, repo, htype, hname, funcname, args, throw):
    '''call python hook. hook is callable object, looked up as
    name in python module. if callable returns "true", hook
    fails, else passes. if hook raises exception, treated as
    hook failure. exception propagates if throw is "true".

    reason for "true" meaning "hook failed" is so that
    unmodified commands (e.g. mercurial.commands.update) can
    be run as hooks without wrappers to convert return values.'''

    if callable(funcname):
        obj = funcname
        funcname = pycompat.sysbytes(obj.__module__ + r"." + obj.__name__)
    else:
        d = funcname.rfind('.')
        if d == -1:
            raise error.HookLoadError(
                _('%s hook is invalid: "%s" not in a module')
                % (hname, funcname))
        modname = funcname[:d]
        oldpaths = sys.path
        if procutil.mainfrozen():
            # binary installs require sys.path manipulation
            modpath, modfile = os.path.split(modname)
            if modpath and modfile:
                sys.path = sys.path[:] + [modpath]
                modname = modfile
        with demandimport.deactivated():
            try:
                obj = __import__(pycompat.sysstr(modname))
            except (ImportError, SyntaxError):
                e1 = sys.exc_info()
                try:
                    # extensions are loaded with hgext_ prefix
                    obj = __import__(r"hgext_%s" % pycompat.sysstr(modname))
                except (ImportError, SyntaxError):
                    e2 = sys.exc_info()
                    if ui.tracebackflag:
                        ui.warn(_('exception from first failed import '
                                  'attempt:\n'))
                    ui.traceback(e1)
                    if ui.tracebackflag:
                        ui.warn(_('exception from second failed import '
                                  'attempt:\n'))
                    ui.traceback(e2)

                    if not ui.tracebackflag:
                        tracebackhint = _(
                            'run with --traceback for stack trace')
                    else:
                        tracebackhint = None
                    raise error.HookLoadError(
                        _('%s hook is invalid: import of "%s" failed') %
                        (hname, modname), hint=tracebackhint)
        sys.path = oldpaths
        try:
            for p in funcname.split('.')[1:]:
                obj = getattr(obj, p)
        except AttributeError:
            raise error.HookLoadError(
                _('%s hook is invalid: "%s" is not defined')
                % (hname, funcname))
        if not callable(obj):
            raise error.HookLoadError(
                _('%s hook is invalid: "%s" is not callable')
                % (hname, funcname))

    ui.note(_("calling hook %s: %s\n") % (hname, funcname))
    starttime = util.timer()

    try:
        r = obj(ui=ui, repo=repo, hooktype=htype, **pycompat.strkwargs(args))
    except Exception as exc:
        if isinstance(exc, error.Abort):
            ui.warn(_('error: %s hook failed: %s\n') %
                         (hname, exc.args[0]))
        else:
            ui.warn(_('error: %s hook raised an exception: '
                      '%s\n') % (hname, encoding.strtolocal(str(exc))))
        if throw:
            raise
        if not ui.tracebackflag:
            ui.warn(_('(run with --traceback for stack trace)\n'))
        ui.traceback()
        return True, True
    finally:
        duration = util.timer() - starttime
        ui.log('pythonhook', 'pythonhook-%s: %s finished in %0.2f seconds\n',
               htype, funcname, duration)
    if r:
        if throw:
            raise error.HookAbort(_('%s hook failed') % hname)
        ui.warn(_('warning: %s hook failed\n') % hname)
    return r, False

def _exthook(ui, repo, htype, name, cmd, args, throw):
    starttime = util.timer()
    env = {}

    # make in-memory changes visible to external process
    if repo is not None:
        tr = repo.currenttransaction()
        repo.dirstate.write(tr)
        if tr and tr.writepending():
            env['HG_PENDING'] = repo.root
    env['HG_HOOKTYPE'] = htype
    env['HG_HOOKNAME'] = name

    for k, v in args.iteritems():
        if callable(v):
            v = v()
        if isinstance(v, (dict, list)):
            v = stringutil.pprint(v)
        env['HG_' + k.upper()] = v

    if ui.configbool('hooks', 'tonative.%s' % name, False):
        oldcmd = cmd
        cmd = procutil.shelltonative(cmd, env)
        if cmd != oldcmd:
            ui.note(_('converting hook "%s" to native\n') % name)

    ui.note(_("running hook %s: %s\n") % (name, cmd))

    if repo:
        cwd = repo.root
    else:
        cwd = encoding.getcwd()
    r = ui.system(cmd, environ=env, cwd=cwd, blockedtag='exthook-%s' % (name,))

    duration = util.timer() - starttime
    ui.log('exthook', 'exthook-%s: %s finished in %0.2f seconds\n',
           name, cmd, duration)
    if r:
        desc = procutil.explainexit(r)
        if throw:
            raise error.HookAbort(_('%s hook %s') % (name, desc))
        ui.warn(_('warning: %s hook %s\n') % (name, desc))
    return r

# represent an untrusted hook command
_fromuntrusted = object()

def _allhooks(ui):
    """return a list of (hook-id, cmd) pairs sorted by priority"""
    hooks = _hookitems(ui)
    # Be careful in this section, propagating the real commands from untrusted
    # sources would create a security vulnerability, make sure anything altered
    # in that section uses "_fromuntrusted" as its command.
    untrustedhooks = _hookitems(ui, _untrusted=True)
    for name, value in untrustedhooks.items():
        trustedvalue = hooks.get(name, (None, None, name, _fromuntrusted))
        if value != trustedvalue:
            (lp, lo, lk, lv) = trustedvalue
            hooks[name] = (lp, lo, lk, _fromuntrusted)
    # (end of the security sensitive section)
    return [(k, v) for p, o, k, v in sorted(hooks.values())]

def _hookitems(ui, _untrusted=False):
    """return all hooks items ready to be sorted"""
    hooks = {}
    for name, cmd in ui.configitems('hooks', untrusted=_untrusted):
        if name.startswith('priority.') or name.startswith('tonative.'):
            continue

        priority = ui.configint('hooks', 'priority.%s' % name, 0)
        hooks[name] = (-priority, len(hooks), name, cmd)
    return hooks

_redirect = False
def redirect(state):
    global _redirect
    _redirect = state

def hashook(ui, htype):
    """return True if a hook is configured for 'htype'"""
    if not ui.callhooks:
        return False
    for hname, cmd in _allhooks(ui):
        if hname.split('.')[0] == htype and cmd:
            return True
    return False

def hook(ui, repo, htype, throw=False, **args):
    if not ui.callhooks:
        return False

    hooks = []
    for hname, cmd in _allhooks(ui):
        if hname.split('.')[0] == htype and cmd:
            hooks.append((hname, cmd))

    res = runhooks(ui, repo, htype, hooks, throw=throw, **args)
    r = False
    for hname, cmd in hooks:
        r = res[hname][0] or r
    return r

def runhooks(ui, repo, htype, hooks, throw=False, **args):
    args = pycompat.byteskwargs(args)
    res = {}
    oldstdout = -1

    try:
        for hname, cmd in hooks:
            if oldstdout == -1 and _redirect:
                try:
                    stdoutno = procutil.stdout.fileno()
                    stderrno = procutil.stderr.fileno()
                    # temporarily redirect stdout to stderr, if possible
                    if stdoutno >= 0 and stderrno >= 0:
                        procutil.stdout.flush()
                        oldstdout = os.dup(stdoutno)
                        os.dup2(stderrno, stdoutno)
                except (OSError, AttributeError):
                    # files seem to be bogus, give up on redirecting (WSGI, etc)
                    pass

            if cmd is _fromuntrusted:
                if throw:
                    raise error.HookAbort(
                        _('untrusted hook %s not executed') % hname,
                        hint = _("see 'hg help config.trusted'"))
                ui.warn(_('warning: untrusted hook %s not executed\n') % hname)
                r = 1
                raised = False
            elif callable(cmd):
                r, raised = pythonhook(ui, repo, htype, hname, cmd, args,
                                        throw)
            elif cmd.startswith('python:'):
                if cmd.count(':') >= 2:
                    path, cmd = cmd[7:].rsplit(':', 1)
                    path = util.expandpath(path)
                    if repo:
                        path = os.path.join(repo.root, path)
                    try:
                        mod = extensions.loadpath(path, 'hghook.%s' % hname)
                    except Exception:
                        ui.write(_("loading %s hook failed:\n") % hname)
                        raise
                    hookfn = getattr(mod, cmd)
                else:
                    hookfn = cmd[7:].strip()
                r, raised = pythonhook(ui, repo, htype, hname, hookfn, args,
                                        throw)
            else:
                r = _exthook(ui, repo, htype, hname, cmd, args, throw)
                raised = False

            res[hname] = r, raised
    finally:
        # The stderr is fully buffered on Windows when connected to a pipe.
        # A forcible flush is required to make small stderr data in the
        # remote side available to the client immediately.
        procutil.stderr.flush()

        if _redirect and oldstdout >= 0:
            procutil.stdout.flush()  # write hook output to stderr fd
            os.dup2(oldstdout, stdoutno)
            os.close(oldstdout)

    return res
