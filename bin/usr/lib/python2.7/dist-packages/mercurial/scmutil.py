# scmutil.py - Mercurial core utility functions
#
#  Copyright Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import errno
import glob
import hashlib
import os
import re
import socket
import subprocess
import weakref

from .i18n import _
from .node import (
    bin,
    hex,
    nullid,
    nullrev,
    short,
    wdirid,
    wdirrev,
)

from . import (
    encoding,
    error,
    match as matchmod,
    obsolete,
    obsutil,
    pathutil,
    phases,
    policy,
    pycompat,
    revsetlang,
    similar,
    smartset,
    url,
    util,
    vfs,
)

from .utils import (
    procutil,
    stringutil,
)

if pycompat.iswindows:
    from . import scmwindows as scmplatform
else:
    from . import scmposix as scmplatform

parsers = policy.importmod(r'parsers')

termsize = scmplatform.termsize

class status(tuple):
    '''Named tuple with a list of files per status. The 'deleted', 'unknown'
       and 'ignored' properties are only relevant to the working copy.
    '''

    __slots__ = ()

    def __new__(cls, modified, added, removed, deleted, unknown, ignored,
                clean):
        return tuple.__new__(cls, (modified, added, removed, deleted, unknown,
                                   ignored, clean))

    @property
    def modified(self):
        '''files that have been modified'''
        return self[0]

    @property
    def added(self):
        '''files that have been added'''
        return self[1]

    @property
    def removed(self):
        '''files that have been removed'''
        return self[2]

    @property
    def deleted(self):
        '''files that are in the dirstate, but have been deleted from the
           working copy (aka "missing")
        '''
        return self[3]

    @property
    def unknown(self):
        '''files not in the dirstate that are not ignored'''
        return self[4]

    @property
    def ignored(self):
        '''files not in the dirstate that are ignored (by _dirignore())'''
        return self[5]

    @property
    def clean(self):
        '''files that have not been modified'''
        return self[6]

    def __repr__(self, *args, **kwargs):
        return ((r'<status modified=%s, added=%s, removed=%s, deleted=%s, '
                 r'unknown=%s, ignored=%s, clean=%s>') %
                tuple(pycompat.sysstr(stringutil.pprint(v)) for v in self))

def itersubrepos(ctx1, ctx2):
    """find subrepos in ctx1 or ctx2"""
    # Create a (subpath, ctx) mapping where we prefer subpaths from
    # ctx1. The subpaths from ctx2 are important when the .hgsub file
    # has been modified (in ctx2) but not yet committed (in ctx1).
    subpaths = dict.fromkeys(ctx2.substate, ctx2)
    subpaths.update(dict.fromkeys(ctx1.substate, ctx1))

    missing = set()

    for subpath in ctx2.substate:
        if subpath not in ctx1.substate:
            del subpaths[subpath]
            missing.add(subpath)

    for subpath, ctx in sorted(subpaths.iteritems()):
        yield subpath, ctx.sub(subpath)

    # Yield an empty subrepo based on ctx1 for anything only in ctx2.  That way,
    # status and diff will have an accurate result when it does
    # 'sub.{status|diff}(rev2)'.  Otherwise, the ctx2 subrepo is compared
    # against itself.
    for subpath in missing:
        yield subpath, ctx2.nullsub(subpath, ctx1)

def nochangesfound(ui, repo, excluded=None):
    '''Report no changes for push/pull, excluded is None or a list of
    nodes excluded from the push/pull.
    '''
    secretlist = []
    if excluded:
        for n in excluded:
            ctx = repo[n]
            if ctx.phase() >= phases.secret and not ctx.extinct():
                secretlist.append(n)

    if secretlist:
        ui.status(_("no changes found (ignored %d secret changesets)\n")
                  % len(secretlist))
    else:
        ui.status(_("no changes found\n"))

def callcatch(ui, func):
    """call func() with global exception handling

    return func() if no exception happens. otherwise do some error handling
    and return an exit code accordingly. does not handle all exceptions.
    """
    try:
        try:
            return func()
        except: # re-raises
            ui.traceback()
            raise
    # Global exception handling, alphabetically
    # Mercurial-specific first, followed by built-in and library exceptions
    except error.LockHeld as inst:
        if inst.errno == errno.ETIMEDOUT:
            reason = _('timed out waiting for lock held by %r') % (
                pycompat.bytestr(inst.locker))
        else:
            reason = _('lock held by %r') % inst.locker
        ui.error(_("abort: %s: %s\n") % (
            inst.desc or stringutil.forcebytestr(inst.filename), reason))
        if not inst.locker:
            ui.error(_("(lock might be very busy)\n"))
    except error.LockUnavailable as inst:
        ui.error(_("abort: could not lock %s: %s\n") %
                 (inst.desc or stringutil.forcebytestr(inst.filename),
                  encoding.strtolocal(inst.strerror)))
    except error.OutOfBandError as inst:
        if inst.args:
            msg = _("abort: remote error:\n")
        else:
            msg = _("abort: remote error\n")
        ui.error(msg)
        if inst.args:
            ui.error(''.join(inst.args))
        if inst.hint:
            ui.error('(%s)\n' % inst.hint)
    except error.RepoError as inst:
        ui.error(_("abort: %s!\n") % inst)
        if inst.hint:
            ui.error(_("(%s)\n") % inst.hint)
    except error.ResponseError as inst:
        ui.error(_("abort: %s") % inst.args[0])
        msg = inst.args[1]
        if isinstance(msg, type(u'')):
            msg = pycompat.sysbytes(msg)
        if not isinstance(msg, bytes):
            ui.error(" %r\n" % (msg,))
        elif not msg:
            ui.error(_(" empty string\n"))
        else:
            ui.error("\n%r\n" % pycompat.bytestr(stringutil.ellipsis(msg)))
    except error.CensoredNodeError as inst:
        ui.error(_("abort: file censored %s!\n") % inst)
    except error.StorageError as inst:
        ui.error(_("abort: %s!\n") % inst)
    except error.InterventionRequired as inst:
        ui.error("%s\n" % inst)
        if inst.hint:
            ui.error(_("(%s)\n") % inst.hint)
        return 1
    except error.WdirUnsupported:
        ui.error(_("abort: working directory revision cannot be specified\n"))
    except error.Abort as inst:
        ui.error(_("abort: %s\n") % inst)
        if inst.hint:
            ui.error(_("(%s)\n") % inst.hint)
    except ImportError as inst:
        ui.error(_("abort: %s!\n") % stringutil.forcebytestr(inst))
        m = stringutil.forcebytestr(inst).split()[-1]
        if m in "mpatch bdiff".split():
            ui.error(_("(did you forget to compile extensions?)\n"))
        elif m in "zlib".split():
            ui.error(_("(is your Python install correct?)\n"))
    except IOError as inst:
        if util.safehasattr(inst, "code"):
            ui.error(_("abort: %s\n") % stringutil.forcebytestr(inst))
        elif util.safehasattr(inst, "reason"):
            try: # usually it is in the form (errno, strerror)
                reason = inst.reason.args[1]
            except (AttributeError, IndexError):
                # it might be anything, for example a string
                reason = inst.reason
            if isinstance(reason, pycompat.unicode):
                # SSLError of Python 2.7.9 contains a unicode
                reason = encoding.unitolocal(reason)
            ui.error(_("abort: error: %s\n") % reason)
        elif (util.safehasattr(inst, "args")
              and inst.args and inst.args[0] == errno.EPIPE):
            pass
        elif getattr(inst, "strerror", None):
            if getattr(inst, "filename", None):
                ui.error(_("abort: %s: %s\n") % (
                    encoding.strtolocal(inst.strerror),
                    stringutil.forcebytestr(inst.filename)))
            else:
                ui.error(_("abort: %s\n") % encoding.strtolocal(inst.strerror))
        else:
            raise
    except OSError as inst:
        if getattr(inst, "filename", None) is not None:
            ui.error(_("abort: %s: '%s'\n") % (
                encoding.strtolocal(inst.strerror),
                stringutil.forcebytestr(inst.filename)))
        else:
            ui.error(_("abort: %s\n") % encoding.strtolocal(inst.strerror))
    except MemoryError:
        ui.error(_("abort: out of memory\n"))
    except SystemExit as inst:
        # Commands shouldn't sys.exit directly, but give a return code.
        # Just in case catch this and and pass exit code to caller.
        return inst.code
    except socket.error as inst:
        ui.error(_("abort: %s\n") % stringutil.forcebytestr(inst.args[-1]))

    return -1

def checknewlabel(repo, lbl, kind):
    # Do not use the "kind" parameter in ui output.
    # It makes strings difficult to translate.
    if lbl in ['tip', '.', 'null']:
        raise error.Abort(_("the name '%s' is reserved") % lbl)
    for c in (':', '\0', '\n', '\r'):
        if c in lbl:
            raise error.Abort(
                _("%r cannot be used in a name") % pycompat.bytestr(c))
    try:
        int(lbl)
        raise error.Abort(_("cannot use an integer as a name"))
    except ValueError:
        pass
    if lbl.strip() != lbl:
        raise error.Abort(_("leading or trailing whitespace in name %r") % lbl)

def checkfilename(f):
    '''Check that the filename f is an acceptable filename for a tracked file'''
    if '\r' in f or '\n' in f:
        raise error.Abort(_("'\\n' and '\\r' disallowed in filenames: %r")
                          % pycompat.bytestr(f))

def checkportable(ui, f):
    '''Check if filename f is portable and warn or abort depending on config'''
    checkfilename(f)
    abort, warn = checkportabilityalert(ui)
    if abort or warn:
        msg = util.checkwinfilename(f)
        if msg:
            msg = "%s: %s" % (msg, procutil.shellquote(f))
            if abort:
                raise error.Abort(msg)
            ui.warn(_("warning: %s\n") % msg)

def checkportabilityalert(ui):
    '''check if the user's config requests nothing, a warning, or abort for
    non-portable filenames'''
    val = ui.config('ui', 'portablefilenames')
    lval = val.lower()
    bval = stringutil.parsebool(val)
    abort = pycompat.iswindows or lval == 'abort'
    warn = bval or lval == 'warn'
    if bval is None and not (warn or abort or lval == 'ignore'):
        raise error.ConfigError(
            _("ui.portablefilenames value is invalid ('%s')") % val)
    return abort, warn

class casecollisionauditor(object):
    def __init__(self, ui, abort, dirstate):
        self._ui = ui
        self._abort = abort
        allfiles = '\0'.join(dirstate._map)
        self._loweredfiles = set(encoding.lower(allfiles).split('\0'))
        self._dirstate = dirstate
        # The purpose of _newfiles is so that we don't complain about
        # case collisions if someone were to call this object with the
        # same filename twice.
        self._newfiles = set()

    def __call__(self, f):
        if f in self._newfiles:
            return
        fl = encoding.lower(f)
        if fl in self._loweredfiles and f not in self._dirstate:
            msg = _('possible case-folding collision for %s') % f
            if self._abort:
                raise error.Abort(msg)
            self._ui.warn(_("warning: %s\n") % msg)
        self._loweredfiles.add(fl)
        self._newfiles.add(f)

def filteredhash(repo, maxrev):
    """build hash of filtered revisions in the current repoview.

    Multiple caches perform up-to-date validation by checking that the
    tiprev and tipnode stored in the cache file match the current repository.
    However, this is not sufficient for validating repoviews because the set
    of revisions in the view may change without the repository tiprev and
    tipnode changing.

    This function hashes all the revs filtered from the view and returns
    that SHA-1 digest.
    """
    cl = repo.changelog
    if not cl.filteredrevs:
        return None
    key = None
    revs = sorted(r for r in cl.filteredrevs if r <= maxrev)
    if revs:
        s = hashlib.sha1()
        for rev in revs:
            s.update('%d;' % rev)
        key = s.digest()
    return key

def walkrepos(path, followsym=False, seen_dirs=None, recurse=False):
    '''yield every hg repository under path, always recursively.
    The recurse flag will only control recursion into repo working dirs'''
    def errhandler(err):
        if err.filename == path:
            raise err
    samestat = getattr(os.path, 'samestat', None)
    if followsym and samestat is not None:
        def adddir(dirlst, dirname):
            dirstat = os.stat(dirname)
            match = any(samestat(dirstat, lstdirstat) for lstdirstat in dirlst)
            if not match:
                dirlst.append(dirstat)
            return not match
    else:
        followsym = False

    if (seen_dirs is None) and followsym:
        seen_dirs = []
        adddir(seen_dirs, path)
    for root, dirs, files in os.walk(path, topdown=True, onerror=errhandler):
        dirs.sort()
        if '.hg' in dirs:
            yield root # found a repository
            qroot = os.path.join(root, '.hg', 'patches')
            if os.path.isdir(os.path.join(qroot, '.hg')):
                yield qroot # we have a patch queue repo here
            if recurse:
                # avoid recursing inside the .hg directory
                dirs.remove('.hg')
            else:
                dirs[:] = [] # don't descend further
        elif followsym:
            newdirs = []
            for d in dirs:
                fname = os.path.join(root, d)
                if adddir(seen_dirs, fname):
                    if os.path.islink(fname):
                        for hgname in walkrepos(fname, True, seen_dirs):
                            yield hgname
                    else:
                        newdirs.append(d)
            dirs[:] = newdirs

def binnode(ctx):
    """Return binary node id for a given basectx"""
    node = ctx.node()
    if node is None:
        return wdirid
    return node

def intrev(ctx):
    """Return integer for a given basectx that can be used in comparison or
    arithmetic operation"""
    rev = ctx.rev()
    if rev is None:
        return wdirrev
    return rev

def formatchangeid(ctx):
    """Format changectx as '{rev}:{node|formatnode}', which is the default
    template provided by logcmdutil.changesettemplater"""
    repo = ctx.repo()
    return formatrevnode(repo.ui, intrev(ctx), binnode(ctx))

def formatrevnode(ui, rev, node):
    """Format given revision and node depending on the current verbosity"""
    if ui.debugflag:
        hexfunc = hex
    else:
        hexfunc = short
    return '%d:%s' % (rev, hexfunc(node))

def resolvehexnodeidprefix(repo, prefix):
    if (prefix.startswith('x') and
        repo.ui.configbool('experimental', 'revisions.prefixhexnode')):
        prefix = prefix[1:]
    try:
        # Uses unfiltered repo because it's faster when prefix is ambiguous/
        # This matches the shortesthexnodeidprefix() function below.
        node = repo.unfiltered().changelog._partialmatch(prefix)
    except error.AmbiguousPrefixLookupError:
        revset = repo.ui.config('experimental', 'revisions.disambiguatewithin')
        if revset:
            # Clear config to avoid infinite recursion
            configoverrides = {('experimental',
                                'revisions.disambiguatewithin'): None}
            with repo.ui.configoverride(configoverrides):
                revs = repo.anyrevs([revset], user=True)
                matches = []
                for rev in revs:
                    node = repo.changelog.node(rev)
                    if hex(node).startswith(prefix):
                        matches.append(node)
                if len(matches) == 1:
                    return matches[0]
        raise
    if node is None:
        return
    repo.changelog.rev(node)  # make sure node isn't filtered
    return node

def mayberevnum(repo, prefix):
    """Checks if the given prefix may be mistaken for a revision number"""
    try:
        i = int(prefix)
        # if we are a pure int, then starting with zero will not be
        # confused as a rev; or, obviously, if the int is larger
        # than the value of the tip rev. We still need to disambiguate if
        # prefix == '0', since that *is* a valid revnum.
        if (prefix != b'0' and prefix[0:1] == b'0') or i >= len(repo):
            return False
        return True
    except ValueError:
        return False

def shortesthexnodeidprefix(repo, node, minlength=1, cache=None):
    """Find the shortest unambiguous prefix that matches hexnode.

    If "cache" is not None, it must be a dictionary that can be used for
    caching between calls to this method.
    """
    # _partialmatch() of filtered changelog could take O(len(repo)) time,
    # which would be unacceptably slow. so we look for hash collision in
    # unfiltered space, which means some hashes may be slightly longer.

    minlength=max(minlength, 1)

    def disambiguate(prefix):
        """Disambiguate against revnums."""
        if repo.ui.configbool('experimental', 'revisions.prefixhexnode'):
            if mayberevnum(repo, prefix):
                return 'x' + prefix
            else:
                return prefix

        hexnode = hex(node)
        for length in range(len(prefix), len(hexnode) + 1):
            prefix = hexnode[:length]
            if not mayberevnum(repo, prefix):
                return prefix

    cl = repo.unfiltered().changelog
    revset = repo.ui.config('experimental', 'revisions.disambiguatewithin')
    if revset:
        revs = None
        if cache is not None:
            revs = cache.get('disambiguationrevset')
        if revs is None:
            revs = repo.anyrevs([revset], user=True)
            if cache is not None:
                cache['disambiguationrevset'] = revs
        if cl.rev(node) in revs:
            hexnode = hex(node)
            nodetree = None
            if cache is not None:
                nodetree = cache.get('disambiguationnodetree')
            if not nodetree:
                try:
                    nodetree = parsers.nodetree(cl.index, len(revs))
                except AttributeError:
                    # no native nodetree
                    pass
                else:
                    for r in revs:
                        nodetree.insert(r)
                    if cache is not None:
                        cache['disambiguationnodetree'] = nodetree
            if nodetree is not None:
                length = max(nodetree.shortest(node), minlength)
                prefix = hexnode[:length]
                return disambiguate(prefix)
            for length in range(minlength, len(hexnode) + 1):
                matches = []
                prefix = hexnode[:length]
                for rev in revs:
                    otherhexnode = repo[rev].hex()
                    if prefix == otherhexnode[:length]:
                        matches.append(otherhexnode)
                if len(matches) == 1:
                    return disambiguate(prefix)

    try:
        return disambiguate(cl.shortest(node, minlength))
    except error.LookupError:
        raise error.RepoLookupError()

def isrevsymbol(repo, symbol):
    """Checks if a symbol exists in the repo.

    See revsymbol() for details. Raises error.AmbiguousPrefixLookupError if the
    symbol is an ambiguous nodeid prefix.
    """
    try:
        revsymbol(repo, symbol)
        return True
    except error.RepoLookupError:
        return False

def revsymbol(repo, symbol):
    """Returns a context given a single revision symbol (as string).

    This is similar to revsingle(), but accepts only a single revision symbol,
    i.e. things like ".", "tip", "1234", "deadbeef", "my-bookmark" work, but
    not "max(public())".
    """
    if not isinstance(symbol, bytes):
        msg = ("symbol (%s of type %s) was not a string, did you mean "
               "repo[symbol]?" % (symbol, type(symbol)))
        raise error.ProgrammingError(msg)
    try:
        if symbol in ('.', 'tip', 'null'):
            return repo[symbol]

        try:
            r = int(symbol)
            if '%d' % r != symbol:
                raise ValueError
            l = len(repo.changelog)
            if r < 0:
                r += l
            if r < 0 or r >= l and r != wdirrev:
                raise ValueError
            return repo[r]
        except error.FilteredIndexError:
            raise
        except (ValueError, OverflowError, IndexError):
            pass

        if len(symbol) == 40:
            try:
                node = bin(symbol)
                rev = repo.changelog.rev(node)
                return repo[rev]
            except error.FilteredLookupError:
                raise
            except (TypeError, LookupError):
                pass

        # look up bookmarks through the name interface
        try:
            node = repo.names.singlenode(repo, symbol)
            rev = repo.changelog.rev(node)
            return repo[rev]
        except KeyError:
            pass

        node = resolvehexnodeidprefix(repo, symbol)
        if node is not None:
            rev = repo.changelog.rev(node)
            return repo[rev]

        raise error.RepoLookupError(_("unknown revision '%s'") % symbol)

    except error.WdirUnsupported:
        return repo[None]
    except (error.FilteredIndexError, error.FilteredLookupError,
            error.FilteredRepoLookupError):
        raise _filterederror(repo, symbol)

def _filterederror(repo, changeid):
    """build an exception to be raised about a filtered changeid

    This is extracted in a function to help extensions (eg: evolve) to
    experiment with various message variants."""
    if repo.filtername.startswith('visible'):

        # Check if the changeset is obsolete
        unfilteredrepo = repo.unfiltered()
        ctx = revsymbol(unfilteredrepo, changeid)

        # If the changeset is obsolete, enrich the message with the reason
        # that made this changeset not visible
        if ctx.obsolete():
            msg = obsutil._getfilteredreason(repo, changeid, ctx)
        else:
            msg = _("hidden revision '%s'") % changeid

        hint = _('use --hidden to access hidden revisions')

        return error.FilteredRepoLookupError(msg, hint=hint)
    msg = _("filtered revision '%s' (not in '%s' subset)")
    msg %= (changeid, repo.filtername)
    return error.FilteredRepoLookupError(msg)

def revsingle(repo, revspec, default='.', localalias=None):
    if not revspec and revspec != 0:
        return repo[default]

    l = revrange(repo, [revspec], localalias=localalias)
    if not l:
        raise error.Abort(_('empty revision set'))
    return repo[l.last()]

def _pairspec(revspec):
    tree = revsetlang.parse(revspec)
    return tree and tree[0] in ('range', 'rangepre', 'rangepost', 'rangeall')

def revpair(repo, revs):
    if not revs:
        return repo['.'], repo[None]

    l = revrange(repo, revs)

    if not l:
        first = second = None
    elif l.isascending():
        first = l.min()
        second = l.max()
    elif l.isdescending():
        first = l.max()
        second = l.min()
    else:
        first = l.first()
        second = l.last()

    if first is None:
        raise error.Abort(_('empty revision range'))
    if (first == second and len(revs) >= 2
        and not all(revrange(repo, [r]) for r in revs)):
        raise error.Abort(_('empty revision on one side of range'))

    # if top-level is range expression, the result must always be a pair
    if first == second and len(revs) == 1 and not _pairspec(revs[0]):
        return repo[first], repo[None]

    return repo[first], repo[second]

def revrange(repo, specs, localalias=None):
    """Execute 1 to many revsets and return the union.

    This is the preferred mechanism for executing revsets using user-specified
    config options, such as revset aliases.

    The revsets specified by ``specs`` will be executed via a chained ``OR``
    expression. If ``specs`` is empty, an empty result is returned.

    ``specs`` can contain integers, in which case they are assumed to be
    revision numbers.

    It is assumed the revsets are already formatted. If you have arguments
    that need to be expanded in the revset, call ``revsetlang.formatspec()``
    and pass the result as an element of ``specs``.

    Specifying a single revset is allowed.

    Returns a ``revset.abstractsmartset`` which is a list-like interface over
    integer revisions.
    """
    allspecs = []
    for spec in specs:
        if isinstance(spec, int):
            spec = revsetlang.formatspec('rev(%d)', spec)
        allspecs.append(spec)
    return repo.anyrevs(allspecs, user=True, localalias=localalias)

def meaningfulparents(repo, ctx):
    """Return list of meaningful (or all if debug) parentrevs for rev.

    For merges (two non-nullrev revisions) both parents are meaningful.
    Otherwise the first parent revision is considered meaningful if it
    is not the preceding revision.
    """
    parents = ctx.parents()
    if len(parents) > 1:
        return parents
    if repo.ui.debugflag:
        return [parents[0], repo[nullrev]]
    if parents[0].rev() >= intrev(ctx) - 1:
        return []
    return parents

def expandpats(pats):
    '''Expand bare globs when running on windows.
    On posix we assume it already has already been done by sh.'''
    if not util.expandglobs:
        return list(pats)
    ret = []
    for kindpat in pats:
        kind, pat = matchmod._patsplit(kindpat, None)
        if kind is None:
            try:
                globbed = glob.glob(pat)
            except re.error:
                globbed = [pat]
            if globbed:
                ret.extend(globbed)
                continue
        ret.append(kindpat)
    return ret

def matchandpats(ctx, pats=(), opts=None, globbed=False, default='relpath',
                 badfn=None):
    '''Return a matcher and the patterns that were used.
    The matcher will warn about bad matches, unless an alternate badfn callback
    is provided.'''
    if pats == ("",):
        pats = []
    if opts is None:
        opts = {}
    if not globbed and default == 'relpath':
        pats = expandpats(pats or [])

    def bad(f, msg):
        ctx.repo().ui.warn("%s: %s\n" % (m.rel(f), msg))

    if badfn is None:
        badfn = bad

    m = ctx.match(pats, opts.get('include'), opts.get('exclude'),
                  default, listsubrepos=opts.get('subrepos'), badfn=badfn)

    if m.always():
        pats = []
    return m, pats

def match(ctx, pats=(), opts=None, globbed=False, default='relpath',
          badfn=None):
    '''Return a matcher that will warn about bad matches.'''
    return matchandpats(ctx, pats, opts, globbed, default, badfn=badfn)[0]

def matchall(repo):
    '''Return a matcher that will efficiently match everything.'''
    return matchmod.always(repo.root, repo.getcwd())

def matchfiles(repo, files, badfn=None):
    '''Return a matcher that will efficiently match exactly these files.'''
    return matchmod.exact(repo.root, repo.getcwd(), files, badfn=badfn)

def parsefollowlinespattern(repo, rev, pat, msg):
    """Return a file name from `pat` pattern suitable for usage in followlines
    logic.
    """
    if not matchmod.patkind(pat):
        return pathutil.canonpath(repo.root, repo.getcwd(), pat)
    else:
        ctx = repo[rev]
        m = matchmod.match(repo.root, repo.getcwd(), [pat], ctx=ctx)
        files = [f for f in ctx if m(f)]
        if len(files) != 1:
            raise error.ParseError(msg)
        return files[0]

def origpath(ui, repo, filepath):
    '''customize where .orig files are created

    Fetch user defined path from config file: [ui] origbackuppath = <path>
    Fall back to default (filepath with .orig suffix) if not specified
    '''
    origbackuppath = ui.config('ui', 'origbackuppath')
    if not origbackuppath:
        return filepath + ".orig"

    # Convert filepath from an absolute path into a path inside the repo.
    filepathfromroot = util.normpath(os.path.relpath(filepath,
                                                     start=repo.root))

    origvfs = vfs.vfs(repo.wjoin(origbackuppath))
    origbackupdir = origvfs.dirname(filepathfromroot)
    if not origvfs.isdir(origbackupdir) or origvfs.islink(origbackupdir):
        ui.note(_('creating directory: %s\n') % origvfs.join(origbackupdir))

        # Remove any files that conflict with the backup file's path
        for f in reversed(list(util.finddirs(filepathfromroot))):
            if origvfs.isfileorlink(f):
                ui.note(_('removing conflicting file: %s\n')
                        % origvfs.join(f))
                origvfs.unlink(f)
                break

        origvfs.makedirs(origbackupdir)

    if origvfs.isdir(filepathfromroot) and not origvfs.islink(filepathfromroot):
        ui.note(_('removing conflicting directory: %s\n')
                % origvfs.join(filepathfromroot))
        origvfs.rmtree(filepathfromroot, forcibly=True)

    return origvfs.join(filepathfromroot)

class _containsnode(object):
    """proxy __contains__(node) to container.__contains__ which accepts revs"""

    def __init__(self, repo, revcontainer):
        self._torev = repo.changelog.rev
        self._revcontains = revcontainer.__contains__

    def __contains__(self, node):
        return self._revcontains(self._torev(node))

def cleanupnodes(repo, replacements, operation, moves=None, metadata=None,
                 fixphase=False, targetphase=None, backup=True):
    """do common cleanups when old nodes are replaced by new nodes

    That includes writing obsmarkers or stripping nodes, and moving bookmarks.
    (we might also want to move working directory parent in the future)

    By default, bookmark moves are calculated automatically from 'replacements',
    but 'moves' can be used to override that. Also, 'moves' may include
    additional bookmark moves that should not have associated obsmarkers.

    replacements is {oldnode: [newnode]} or a iterable of nodes if they do not
    have replacements. operation is a string, like "rebase".

    metadata is dictionary containing metadata to be stored in obsmarker if
    obsolescence is enabled.
    """
    assert fixphase or targetphase is None
    if not replacements and not moves:
        return

    # translate mapping's other forms
    if not util.safehasattr(replacements, 'items'):
        replacements = {(n,): () for n in replacements}
    else:
        # upgrading non tuple "source" to tuple ones for BC
        repls = {}
        for key, value in replacements.items():
            if not isinstance(key, tuple):
                key = (key,)
            repls[key] = value
        replacements = repls

    # Calculate bookmark movements
    if moves is None:
        moves = {}
    # Unfiltered repo is needed since nodes in replacements might be hidden.
    unfi = repo.unfiltered()
    for oldnodes, newnodes in replacements.items():
        for oldnode in oldnodes:
            if oldnode in moves:
                continue
            if len(newnodes) > 1:
                # usually a split, take the one with biggest rev number
                newnode = next(unfi.set('max(%ln)', newnodes)).node()
            elif len(newnodes) == 0:
                # move bookmark backwards
                allreplaced = []
                for rep in replacements:
                    allreplaced.extend(rep)
                roots = list(unfi.set('max((::%n) - %ln)', oldnode,
                                      allreplaced))
                if roots:
                    newnode = roots[0].node()
                else:
                    newnode = nullid
            else:
                newnode = newnodes[0]
            moves[oldnode] = newnode

    allnewnodes = [n for ns in replacements.values() for n in ns]
    toretract = {}
    toadvance = {}
    if fixphase:
        precursors = {}
        for oldnodes, newnodes in replacements.items():
            for oldnode in oldnodes:
                for newnode in newnodes:
                    precursors.setdefault(newnode, []).append(oldnode)

        allnewnodes.sort(key=lambda n: unfi[n].rev())
        newphases = {}
        def phase(ctx):
            return newphases.get(ctx.node(), ctx.phase())
        for newnode in allnewnodes:
            ctx = unfi[newnode]
            parentphase = max(phase(p) for p in ctx.parents())
            if targetphase is None:
                oldphase = max(unfi[oldnode].phase()
                               for oldnode in precursors[newnode])
                newphase = max(oldphase, parentphase)
            else:
                newphase = max(targetphase, parentphase)
            newphases[newnode] = newphase
            if newphase > ctx.phase():
                toretract.setdefault(newphase, []).append(newnode)
            elif newphase < ctx.phase():
                toadvance.setdefault(newphase, []).append(newnode)

    with repo.transaction('cleanup') as tr:
        # Move bookmarks
        bmarks = repo._bookmarks
        bmarkchanges = []
        for oldnode, newnode in moves.items():
            oldbmarks = repo.nodebookmarks(oldnode)
            if not oldbmarks:
                continue
            from . import bookmarks # avoid import cycle
            repo.ui.debug('moving bookmarks %r from %s to %s\n' %
                          (pycompat.rapply(pycompat.maybebytestr, oldbmarks),
                           hex(oldnode), hex(newnode)))
            # Delete divergent bookmarks being parents of related newnodes
            deleterevs = repo.revs('parents(roots(%ln & (::%n))) - parents(%n)',
                                   allnewnodes, newnode, oldnode)
            deletenodes = _containsnode(repo, deleterevs)
            for name in oldbmarks:
                bmarkchanges.append((name, newnode))
                for b in bookmarks.divergent2delete(repo, deletenodes, name):
                    bmarkchanges.append((b, None))

        if bmarkchanges:
            bmarks.applychanges(repo, tr, bmarkchanges)

        for phase, nodes in toretract.items():
            phases.retractboundary(repo, tr, phase, nodes)
        for phase, nodes in toadvance.items():
            phases.advanceboundary(repo, tr, phase, nodes)

        # Obsolete or strip nodes
        if obsolete.isenabled(repo, obsolete.createmarkersopt):
            # If a node is already obsoleted, and we want to obsolete it
            # without a successor, skip that obssolete request since it's
            # unnecessary. That's the "if s or not isobs(n)" check below.
            # Also sort the node in topology order, that might be useful for
            # some obsstore logic.
            # NOTE: the sorting might belong to createmarkers.
            torev = unfi.changelog.rev
            sortfunc = lambda ns: torev(ns[0][0])
            rels = []
            for ns, s in sorted(replacements.items(), key=sortfunc):
                rel = (tuple(unfi[n] for n in ns), tuple(unfi[m] for m in s))
                rels.append(rel)
            if rels:
                obsolete.createmarkers(repo, rels, operation=operation,
                                       metadata=metadata)
        else:
            from . import repair # avoid import cycle
            tostrip = list(n for ns in replacements for n in ns)
            if tostrip:
                repair.delayedstrip(repo.ui, repo, tostrip, operation,
                                    backup=backup)

def addremove(repo, matcher, prefix, opts=None):
    if opts is None:
        opts = {}
    m = matcher
    dry_run = opts.get('dry_run')
    try:
        similarity = float(opts.get('similarity') or 0)
    except ValueError:
        raise error.Abort(_('similarity must be a number'))
    if similarity < 0 or similarity > 100:
        raise error.Abort(_('similarity must be between 0 and 100'))
    similarity /= 100.0

    ret = 0
    join = lambda f: os.path.join(prefix, f)

    wctx = repo[None]
    for subpath in sorted(wctx.substate):
        submatch = matchmod.subdirmatcher(subpath, m)
        if opts.get('subrepos') or m.exact(subpath) or any(submatch.files()):
            sub = wctx.sub(subpath)
            try:
                if sub.addremove(submatch, prefix, opts):
                    ret = 1
            except error.LookupError:
                repo.ui.status(_("skipping missing subrepository: %s\n")
                                 % join(subpath))

    rejected = []
    def badfn(f, msg):
        if f in m.files():
            m.bad(f, msg)
        rejected.append(f)

    badmatch = matchmod.badmatch(m, badfn)
    added, unknown, deleted, removed, forgotten = _interestingfiles(repo,
                                                                    badmatch)

    unknownset = set(unknown + forgotten)
    toprint = unknownset.copy()
    toprint.update(deleted)
    for abs in sorted(toprint):
        if repo.ui.verbose or not m.exact(abs):
            if abs in unknownset:
                status = _('adding %s\n') % m.uipath(abs)
                label = 'ui.addremove.added'
            else:
                status = _('removing %s\n') % m.uipath(abs)
                label = 'ui.addremove.removed'
            repo.ui.status(status, label=label)

    renames = _findrenames(repo, m, added + unknown, removed + deleted,
                           similarity)

    if not dry_run:
        _markchanges(repo, unknown + forgotten, deleted, renames)

    for f in rejected:
        if f in m.files():
            return 1
    return ret

def marktouched(repo, files, similarity=0.0):
    '''Assert that files have somehow been operated upon. files are relative to
    the repo root.'''
    m = matchfiles(repo, files, badfn=lambda x, y: rejected.append(x))
    rejected = []

    added, unknown, deleted, removed, forgotten = _interestingfiles(repo, m)

    if repo.ui.verbose:
        unknownset = set(unknown + forgotten)
        toprint = unknownset.copy()
        toprint.update(deleted)
        for abs in sorted(toprint):
            if abs in unknownset:
                status = _('adding %s\n') % abs
            else:
                status = _('removing %s\n') % abs
            repo.ui.status(status)

    renames = _findrenames(repo, m, added + unknown, removed + deleted,
                           similarity)

    _markchanges(repo, unknown + forgotten, deleted, renames)

    for f in rejected:
        if f in m.files():
            return 1
    return 0

def _interestingfiles(repo, matcher):
    '''Walk dirstate with matcher, looking for files that addremove would care
    about.

    This is different from dirstate.status because it doesn't care about
    whether files are modified or clean.'''
    added, unknown, deleted, removed, forgotten = [], [], [], [], []
    audit_path = pathutil.pathauditor(repo.root, cached=True)

    ctx = repo[None]
    dirstate = repo.dirstate
    matcher = repo.narrowmatch(matcher, includeexact=True)
    walkresults = dirstate.walk(matcher, subrepos=sorted(ctx.substate),
                                unknown=True, ignored=False, full=False)
    for abs, st in walkresults.iteritems():
        dstate = dirstate[abs]
        if dstate == '?' and audit_path.check(abs):
            unknown.append(abs)
        elif dstate != 'r' and not st:
            deleted.append(abs)
        elif dstate == 'r' and st:
            forgotten.append(abs)
        # for finding renames
        elif dstate == 'r' and not st:
            removed.append(abs)
        elif dstate == 'a':
            added.append(abs)

    return added, unknown, deleted, removed, forgotten

def _findrenames(repo, matcher, added, removed, similarity):
    '''Find renames from removed files to added ones.'''
    renames = {}
    if similarity > 0:
        for old, new, score in similar.findrenames(repo, added, removed,
                                                   similarity):
            if (repo.ui.verbose or not matcher.exact(old)
                or not matcher.exact(new)):
                repo.ui.status(_('recording removal of %s as rename to %s '
                                 '(%d%% similar)\n') %
                               (matcher.rel(old), matcher.rel(new),
                                score * 100))
            renames[new] = old
    return renames

def _markchanges(repo, unknown, deleted, renames):
    '''Marks the files in unknown as added, the files in deleted as removed,
    and the files in renames as copied.'''
    wctx = repo[None]
    with repo.wlock():
        wctx.forget(deleted)
        wctx.add(unknown)
        for new, old in renames.iteritems():
            wctx.copy(old, new)

def dirstatecopy(ui, repo, wctx, src, dst, dryrun=False, cwd=None):
    """Update the dirstate to reflect the intent of copying src to dst. For
    different reasons it might not end with dst being marked as copied from src.
    """
    origsrc = repo.dirstate.copied(src) or src
    if dst == origsrc: # copying back a copy?
        if repo.dirstate[dst] not in 'mn' and not dryrun:
            repo.dirstate.normallookup(dst)
    else:
        if repo.dirstate[origsrc] == 'a' and origsrc == src:
            if not ui.quiet:
                ui.warn(_("%s has not been committed yet, so no copy "
                          "data will be stored for %s.\n")
                        % (repo.pathto(origsrc, cwd), repo.pathto(dst, cwd)))
            if repo.dirstate[dst] in '?r' and not dryrun:
                wctx.add([dst])
        elif not dryrun:
            wctx.copy(origsrc, dst)

def writerequires(opener, requirements):
    with opener('requires', 'w') as fp:
        for r in sorted(requirements):
            fp.write("%s\n" % r)

class filecachesubentry(object):
    def __init__(self, path, stat):
        self.path = path
        self.cachestat = None
        self._cacheable = None

        if stat:
            self.cachestat = filecachesubentry.stat(self.path)

            if self.cachestat:
                self._cacheable = self.cachestat.cacheable()
            else:
                # None means we don't know yet
                self._cacheable = None

    def refresh(self):
        if self.cacheable():
            self.cachestat = filecachesubentry.stat(self.path)

    def cacheable(self):
        if self._cacheable is not None:
            return self._cacheable

        # we don't know yet, assume it is for now
        return True

    def changed(self):
        # no point in going further if we can't cache it
        if not self.cacheable():
            return True

        newstat = filecachesubentry.stat(self.path)

        # we may not know if it's cacheable yet, check again now
        if newstat and self._cacheable is None:
            self._cacheable = newstat.cacheable()

            # check again
            if not self._cacheable:
                return True

        if self.cachestat != newstat:
            self.cachestat = newstat
            return True
        else:
            return False

    @staticmethod
    def stat(path):
        try:
            return util.cachestat(path)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

class filecacheentry(object):
    def __init__(self, paths, stat=True):
        self._entries = []
        for path in paths:
            self._entries.append(filecachesubentry(path, stat))

    def changed(self):
        '''true if any entry has changed'''
        for entry in self._entries:
            if entry.changed():
                return True
        return False

    def refresh(self):
        for entry in self._entries:
            entry.refresh()

class filecache(object):
    """A property like decorator that tracks files under .hg/ for updates.

    On first access, the files defined as arguments are stat()ed and the
    results cached. The decorated function is called. The results are stashed
    away in a ``_filecache`` dict on the object whose method is decorated.

    On subsequent access, the cached result is returned.

    On external property set operations, stat() calls are performed and the new
    value is cached.

    On property delete operations, cached data is removed.

    When using the property API, cached data is always returned, if available:
    no stat() is performed to check if the file has changed and if the function
    needs to be called to reflect file changes.

    Others can muck about with the state of the ``_filecache`` dict. e.g. they
    can populate an entry before the property's getter is called. In this case,
    entries in ``_filecache`` will be used during property operations,
    if available. If the underlying file changes, it is up to external callers
    to reflect this by e.g. calling ``delattr(obj, attr)`` to remove the cached
    method result as well as possibly calling ``del obj._filecache[attr]`` to
    remove the ``filecacheentry``.
    """

    def __init__(self, *paths):
        self.paths = paths

    def join(self, obj, fname):
        """Used to compute the runtime path of a cached file.

        Users should subclass filecache and provide their own version of this
        function to call the appropriate join function on 'obj' (an instance
        of the class that its member function was decorated).
        """
        raise NotImplementedError

    def __call__(self, func):
        self.func = func
        self.sname = func.__name__
        self.name = pycompat.sysbytes(self.sname)
        return self

    def __get__(self, obj, type=None):
        # if accessed on the class, return the descriptor itself.
        if obj is None:
            return self
        # do we need to check if the file changed?
        if self.sname in obj.__dict__:
            assert self.name in obj._filecache, self.name
            return obj.__dict__[self.sname]

        entry = obj._filecache.get(self.name)

        if entry:
            if entry.changed():
                entry.obj = self.func(obj)
        else:
            paths = [self.join(obj, path) for path in self.paths]

            # We stat -before- creating the object so our cache doesn't lie if
            # a writer modified between the time we read and stat
            entry = filecacheentry(paths, True)
            entry.obj = self.func(obj)

            obj._filecache[self.name] = entry

        obj.__dict__[self.sname] = entry.obj
        return entry.obj

    def __set__(self, obj, value):
        if self.name not in obj._filecache:
            # we add an entry for the missing value because X in __dict__
            # implies X in _filecache
            paths = [self.join(obj, path) for path in self.paths]
            ce = filecacheentry(paths, False)
            obj._filecache[self.name] = ce
        else:
            ce = obj._filecache[self.name]

        ce.obj = value # update cached copy
        obj.__dict__[self.sname] = value # update copy returned by obj.x

    def __delete__(self, obj):
        try:
            del obj.__dict__[self.sname]
        except KeyError:
            raise AttributeError(self.sname)

def extdatasource(repo, source):
    """Gather a map of rev -> value dict from the specified source

    A source spec is treated as a URL, with a special case shell: type
    for parsing the output from a shell command.

    The data is parsed as a series of newline-separated records where
    each record is a revision specifier optionally followed by a space
    and a freeform string value. If the revision is known locally, it
    is converted to a rev, otherwise the record is skipped.

    Note that both key and value are treated as UTF-8 and converted to
    the local encoding. This allows uniformity between local and
    remote data sources.
    """

    spec = repo.ui.config("extdata", source)
    if not spec:
        raise error.Abort(_("unknown extdata source '%s'") % source)

    data = {}
    src = proc = None
    try:
        if spec.startswith("shell:"):
            # external commands should be run relative to the repo root
            cmd = spec[6:]
            proc = subprocess.Popen(procutil.tonativestr(cmd),
                                    shell=True, bufsize=-1,
                                    close_fds=procutil.closefds,
                                    stdout=subprocess.PIPE,
                                    cwd=procutil.tonativestr(repo.root))
            src = proc.stdout
        else:
            # treat as a URL or file
            src = url.open(repo.ui, spec)
        for l in src:
            if " " in l:
                k, v = l.strip().split(" ", 1)
            else:
                k, v = l.strip(), ""

            k = encoding.tolocal(k)
            try:
                data[revsingle(repo, k).rev()] = encoding.tolocal(v)
            except (error.LookupError, error.RepoLookupError):
                pass # we ignore data for nodes that don't exist locally
    finally:
        if proc:
            proc.communicate()
        if src:
            src.close()
    if proc and proc.returncode != 0:
        raise error.Abort(_("extdata command '%s' failed: %s")
                          % (cmd, procutil.explainexit(proc.returncode)))

    return data

def _locksub(repo, lock, envvar, cmd, environ=None, *args, **kwargs):
    if lock is None:
        raise error.LockInheritanceContractViolation(
            'lock can only be inherited while held')
    if environ is None:
        environ = {}
    with lock.inherit() as locker:
        environ[envvar] = locker
        return repo.ui.system(cmd, environ=environ, *args, **kwargs)

def wlocksub(repo, cmd, *args, **kwargs):
    """run cmd as a subprocess that allows inheriting repo's wlock

    This can only be called while the wlock is held. This takes all the
    arguments that ui.system does, and returns the exit code of the
    subprocess."""
    return _locksub(repo, repo.currentwlock(), 'HG_WLOCK_LOCKER', cmd, *args,
                    **kwargs)

class progress(object):
    def __init__(self, ui, topic, unit="", total=None):
        self.ui = ui
        self.pos = 0
        self.topic = topic
        self.unit = unit
        self.total = total

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.complete()

    def update(self, pos, item="", total=None):
        assert pos is not None
        if total:
            self.total = total
        self.pos = pos
        self._print(item)

    def increment(self, step=1, item="", total=None):
        self.update(self.pos + step, item, total)

    def complete(self):
        self.ui.progress(self.topic, None)

    def _print(self, item):
        self.ui.progress(self.topic, self.pos, item, self.unit,
                         self.total)

def gdinitconfig(ui):
    """helper function to know if a repo should be created as general delta
    """
    # experimental config: format.generaldelta
    return (ui.configbool('format', 'generaldelta')
            or ui.configbool('format', 'usegeneraldelta')
            or ui.configbool('format', 'sparse-revlog'))

def gddeltaconfig(ui):
    """helper function to know if incoming delta should be optimised
    """
    # experimental config: format.generaldelta
    return ui.configbool('format', 'generaldelta')

class simplekeyvaluefile(object):
    """A simple file with key=value lines

    Keys must be alphanumerics and start with a letter, values must not
    contain '\n' characters"""
    firstlinekey = '__firstline'

    def __init__(self, vfs, path, keys=None):
        self.vfs = vfs
        self.path = path

    def read(self, firstlinenonkeyval=False):
        """Read the contents of a simple key-value file

        'firstlinenonkeyval' indicates whether the first line of file should
        be treated as a key-value pair or reuturned fully under the
        __firstline key."""
        lines = self.vfs.readlines(self.path)
        d = {}
        if firstlinenonkeyval:
            if not lines:
                e = _("empty simplekeyvalue file")
                raise error.CorruptedState(e)
            # we don't want to include '\n' in the __firstline
            d[self.firstlinekey] = lines[0][:-1]
            del lines[0]

        try:
            # the 'if line.strip()' part prevents us from failing on empty
            # lines which only contain '\n' therefore are not skipped
            # by 'if line'
            updatedict = dict(line[:-1].split('=', 1) for line in lines
                                                      if line.strip())
            if self.firstlinekey in updatedict:
                e = _("%r can't be used as a key")
                raise error.CorruptedState(e % self.firstlinekey)
            d.update(updatedict)
        except ValueError as e:
            raise error.CorruptedState(str(e))
        return d

    def write(self, data, firstline=None):
        """Write key=>value mapping to a file
        data is a dict. Keys must be alphanumerical and start with a letter.
        Values must not contain newline characters.

        If 'firstline' is not None, it is written to file before
        everything else, as it is, not in a key=value form"""
        lines = []
        if firstline is not None:
            lines.append('%s\n' % firstline)

        for k, v in data.items():
            if k == self.firstlinekey:
                e = "key name '%s' is reserved" % self.firstlinekey
                raise error.ProgrammingError(e)
            if not k[0:1].isalpha():
                e = "keys must start with a letter in a key-value file"
                raise error.ProgrammingError(e)
            if not k.isalnum():
                e = "invalid key name in a simple key-value file"
                raise error.ProgrammingError(e)
            if '\n' in v:
                e = "invalid value in a simple key-value file"
                raise error.ProgrammingError(e)
            lines.append("%s=%s\n" % (k, v))
        with self.vfs(self.path, mode='wb', atomictemp=True) as fp:
            fp.write(''.join(lines))

_reportobsoletedsource = [
    'debugobsolete',
    'pull',
    'push',
    'serve',
    'unbundle',
]

_reportnewcssource = [
    'pull',
    'unbundle',
]

def prefetchfiles(repo, revs, match):
    """Invokes the registered file prefetch functions, allowing extensions to
    ensure the corresponding files are available locally, before the command
    uses them."""
    if match:
        # The command itself will complain about files that don't exist, so
        # don't duplicate the message.
        match = matchmod.badmatch(match, lambda fn, msg: None)
    else:
        match = matchall(repo)

    fileprefetchhooks(repo, revs, match)

# a list of (repo, revs, match) prefetch functions
fileprefetchhooks = util.hooks()

# A marker that tells the evolve extension to suppress its own reporting
_reportstroubledchangesets = True

def registersummarycallback(repo, otr, txnname=''):
    """register a callback to issue a summary after the transaction is closed
    """
    def txmatch(sources):
        return any(txnname.startswith(source) for source in sources)

    categories = []

    def reportsummary(func):
        """decorator for report callbacks."""
        # The repoview life cycle is shorter than the one of the actual
        # underlying repository. So the filtered object can die before the
        # weakref is used leading to troubles. We keep a reference to the
        # unfiltered object and restore the filtering when retrieving the
        # repository through the weakref.
        filtername = repo.filtername
        reporef = weakref.ref(repo.unfiltered())
        def wrapped(tr):
            repo = reporef()
            if filtername:
                repo = repo.filtered(filtername)
            func(repo, tr)
        newcat = '%02i-txnreport' % len(categories)
        otr.addpostclose(newcat, wrapped)
        categories.append(newcat)
        return wrapped

    if txmatch(_reportobsoletedsource):
        @reportsummary
        def reportobsoleted(repo, tr):
            obsoleted = obsutil.getobsoleted(repo, tr)
            if obsoleted:
                repo.ui.status(_('obsoleted %i changesets\n')
                               % len(obsoleted))

    if (obsolete.isenabled(repo, obsolete.createmarkersopt) and
        repo.ui.configbool('experimental', 'evolution.report-instabilities')):
        instabilitytypes = [
            ('orphan', 'orphan'),
            ('phase-divergent', 'phasedivergent'),
            ('content-divergent', 'contentdivergent'),
        ]

        def getinstabilitycounts(repo):
            filtered = repo.changelog.filteredrevs
            counts = {}
            for instability, revset in instabilitytypes:
                counts[instability] = len(set(obsolete.getrevs(repo, revset)) -
                                          filtered)
            return counts

        oldinstabilitycounts = getinstabilitycounts(repo)
        @reportsummary
        def reportnewinstabilities(repo, tr):
            newinstabilitycounts = getinstabilitycounts(repo)
            for instability, revset in instabilitytypes:
                delta = (newinstabilitycounts[instability] -
                         oldinstabilitycounts[instability])
                msg = getinstabilitymessage(delta, instability)
                if msg:
                    repo.ui.warn(msg)

    if txmatch(_reportnewcssource):
        @reportsummary
        def reportnewcs(repo, tr):
            """Report the range of new revisions pulled/unbundled."""
            origrepolen = tr.changes.get('origrepolen', len(repo))
            unfi = repo.unfiltered()
            if origrepolen >= len(unfi):
                return

            # Compute the bounds of new visible revisions' range.
            revs = smartset.spanset(repo, start=origrepolen)
            if revs:
                minrev, maxrev = repo[revs.min()], repo[revs.max()]

                if minrev == maxrev:
                    revrange = minrev
                else:
                    revrange = '%s:%s' % (minrev, maxrev)
                draft = len(repo.revs('%ld and draft()', revs))
                secret = len(repo.revs('%ld and secret()', revs))
                if not (draft or secret):
                    msg = _('new changesets %s\n') % revrange
                elif draft and secret:
                    msg = _('new changesets %s (%d drafts, %d secrets)\n')
                    msg %= (revrange, draft, secret)
                elif draft:
                    msg = _('new changesets %s (%d drafts)\n')
                    msg %= (revrange, draft)
                elif secret:
                    msg = _('new changesets %s (%d secrets)\n')
                    msg %= (revrange, secret)
                else:
                    errormsg = 'entered unreachable condition'
                    raise error.ProgrammingError(errormsg)
                repo.ui.status(msg)

            # search new changesets directly pulled as obsolete
            duplicates = tr.changes.get('revduplicates', ())
            obsadded = unfi.revs('(%d: + %ld) and obsolete()',
                                 origrepolen, duplicates)
            cl = repo.changelog
            extinctadded = [r for r in obsadded if r not in cl]
            if extinctadded:
                # They are not just obsolete, but obsolete and invisible
                # we call them "extinct" internally but the terms have not been
                # exposed to users.
                msg = '(%d other changesets obsolete on arrival)\n'
                repo.ui.status(msg % len(extinctadded))

        @reportsummary
        def reportphasechanges(repo, tr):
            """Report statistics of phase changes for changesets pre-existing
            pull/unbundle.
            """
            origrepolen = tr.changes.get('origrepolen', len(repo))
            phasetracking = tr.changes.get('phases', {})
            if not phasetracking:
                return
            published = [
                rev for rev, (old, new) in phasetracking.iteritems()
                if new == phases.public and rev < origrepolen
            ]
            if not published:
                return
            repo.ui.status(_('%d local changesets published\n')
                           % len(published))

def getinstabilitymessage(delta, instability):
    """function to return the message to show warning about new instabilities

    exists as a separate function so that extension can wrap to show more
    information like how to fix instabilities"""
    if delta > 0:
        return _('%i new %s changesets\n') % (delta, instability)

def nodesummaries(repo, nodes, maxnumnodes=4):
    if len(nodes) <= maxnumnodes or repo.ui.verbose:
        return ' '.join(short(h) for h in nodes)
    first = ' '.join(short(h) for h in nodes[:maxnumnodes])
    return _("%s and %d others") % (first, len(nodes) - maxnumnodes)

def enforcesinglehead(repo, tr, desc):
    """check that no named branch has multiple heads"""
    if desc in ('strip', 'repair'):
        # skip the logic during strip
        return
    visible = repo.filtered('visible')
    # possible improvement: we could restrict the check to affected branch
    for name, heads in visible.branchmap().iteritems():
        if len(heads) > 1:
            msg = _('rejecting multiple heads on branch "%s"')
            msg %= name
            hint = _('%d heads: %s')
            hint %= (len(heads), nodesummaries(repo, heads))
            raise error.Abort(msg, hint=hint)

def wrapconvertsink(sink):
    """Allow extensions to wrap the sink returned by convcmd.convertsink()
    before it is used, whether or not the convert extension was formally loaded.
    """
    return sink

def unhidehashlikerevs(repo, specs, hiddentype):
    """parse the user specs and unhide changesets whose hash or revision number
    is passed.

    hiddentype can be: 1) 'warn': warn while unhiding changesets
                       2) 'nowarn': don't warn while unhiding changesets

    returns a repo object with the required changesets unhidden
    """
    if not repo.filtername or not repo.ui.configbool('experimental',
                                                     'directaccess'):
        return repo

    if repo.filtername not in ('visible', 'visible-hidden'):
        return repo

    symbols = set()
    for spec in specs:
        try:
            tree = revsetlang.parse(spec)
        except error.ParseError: # will be reported by scmutil.revrange()
            continue

        symbols.update(revsetlang.gethashlikesymbols(tree))

    if not symbols:
        return repo

    revs = _getrevsfromsymbols(repo, symbols)

    if not revs:
        return repo

    if hiddentype == 'warn':
        unfi = repo.unfiltered()
        revstr = ", ".join([pycompat.bytestr(unfi[l]) for l in revs])
        repo.ui.warn(_("warning: accessing hidden changesets for write "
                       "operation: %s\n") % revstr)

    # we have to use new filtername to separate branch/tags cache until we can
    # disbale these cache when revisions are dynamically pinned.
    return repo.filtered('visible-hidden', revs)

def _getrevsfromsymbols(repo, symbols):
    """parse the list of symbols and returns a set of revision numbers of hidden
    changesets present in symbols"""
    revs = set()
    unfi = repo.unfiltered()
    unficl = unfi.changelog
    cl = repo.changelog
    tiprev = len(unficl)
    allowrevnums = repo.ui.configbool('experimental', 'directaccess.revnums')
    for s in symbols:
        try:
            n = int(s)
            if n <= tiprev:
                if not allowrevnums:
                    continue
                else:
                    if n not in cl:
                        revs.add(n)
                    continue
        except ValueError:
            pass

        try:
            s = resolvehexnodeidprefix(unfi, s)
        except (error.LookupError, error.WdirUnsupported):
            s = None

        if s is not None:
            rev = unficl.rev(s)
            if rev not in cl:
                revs.add(rev)

    return revs

def bookmarkrevs(repo, mark):
    """
    Select revisions reachable by a given bookmark
    """
    return repo.revs("ancestors(bookmark(%s)) - "
                     "ancestors(head() and not bookmark(%s)) - "
                     "ancestors(bookmark() and not bookmark(%s))",
                     mark, mark, mark)
