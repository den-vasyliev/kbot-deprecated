# cmdutil.py - help for command processing in mercurial
#
# Copyright 2005-2007 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import errno
import os
import re

from .i18n import _
from .node import (
    hex,
    nullid,
    nullrev,
    short,
)

from . import (
    bookmarks,
    changelog,
    copies,
    crecord as crecordmod,
    dirstateguard,
    encoding,
    error,
    formatter,
    logcmdutil,
    match as matchmod,
    merge as mergemod,
    mergeutil,
    obsolete,
    patch,
    pathutil,
    phases,
    pycompat,
    revlog,
    rewriteutil,
    scmutil,
    smartset,
    subrepoutil,
    templatekw,
    templater,
    util,
    vfs as vfsmod,
)

from .utils import (
    dateutil,
    stringutil,
)

stringio = util.stringio

# templates of common command options

dryrunopts = [
    ('n', 'dry-run', None,
     _('do not perform actions, just print output')),
]

confirmopts = [
    ('', 'confirm', None,
     _('ask before applying actions')),
]

remoteopts = [
    ('e', 'ssh', '',
     _('specify ssh command to use'), _('CMD')),
    ('', 'remotecmd', '',
     _('specify hg command to run on the remote side'), _('CMD')),
    ('', 'insecure', None,
     _('do not verify server certificate (ignoring web.cacerts config)')),
]

walkopts = [
    ('I', 'include', [],
     _('include names matching the given patterns'), _('PATTERN')),
    ('X', 'exclude', [],
     _('exclude names matching the given patterns'), _('PATTERN')),
]

commitopts = [
    ('m', 'message', '',
     _('use text as commit message'), _('TEXT')),
    ('l', 'logfile', '',
     _('read commit message from file'), _('FILE')),
]

commitopts2 = [
    ('d', 'date', '',
     _('record the specified date as commit date'), _('DATE')),
    ('u', 'user', '',
     _('record the specified user as committer'), _('USER')),
]

formatteropts = [
    ('T', 'template', '',
     _('display with template'), _('TEMPLATE')),
]

templateopts = [
    ('', 'style', '',
     _('display using template map file (DEPRECATED)'), _('STYLE')),
    ('T', 'template', '',
     _('display with template'), _('TEMPLATE')),
]

logopts = [
    ('p', 'patch', None, _('show patch')),
    ('g', 'git', None, _('use git extended diff format')),
    ('l', 'limit', '',
     _('limit number of changes displayed'), _('NUM')),
    ('M', 'no-merges', None, _('do not show merges')),
    ('', 'stat', None, _('output diffstat-style summary of changes')),
    ('G', 'graph', None, _("show the revision DAG")),
] + templateopts

diffopts = [
    ('a', 'text', None, _('treat all files as text')),
    ('g', 'git', None, _('use git extended diff format')),
    ('', 'binary', None, _('generate binary diffs in git mode (default)')),
    ('', 'nodates', None, _('omit dates from diff headers'))
]

diffwsopts = [
    ('w', 'ignore-all-space', None,
     _('ignore white space when comparing lines')),
    ('b', 'ignore-space-change', None,
     _('ignore changes in the amount of white space')),
    ('B', 'ignore-blank-lines', None,
     _('ignore changes whose lines are all blank')),
    ('Z', 'ignore-space-at-eol', None,
     _('ignore changes in whitespace at EOL')),
]

diffopts2 = [
    ('', 'noprefix', None, _('omit a/ and b/ prefixes from filenames')),
    ('p', 'show-function', None, _('show which function each change is in')),
    ('', 'reverse', None, _('produce a diff that undoes the changes')),
] + diffwsopts + [
    ('U', 'unified', '',
     _('number of lines of context to show'), _('NUM')),
    ('', 'stat', None, _('output diffstat-style summary of changes')),
    ('', 'root', '', _('produce diffs relative to subdirectory'), _('DIR')),
]

mergetoolopts = [
    ('t', 'tool', '', _('specify merge tool'), _('TOOL')),
]

similarityopts = [
    ('s', 'similarity', '',
     _('guess renamed files by similarity (0<=s<=100)'), _('SIMILARITY'))
]

subrepoopts = [
    ('S', 'subrepos', None,
     _('recurse into subrepositories'))
]

debugrevlogopts = [
    ('c', 'changelog', False, _('open changelog')),
    ('m', 'manifest', False, _('open manifest')),
    ('', 'dir', '', _('open directory manifest')),
]

# special string such that everything below this line will be ingored in the
# editor text
_linebelow = "^HG: ------------------------ >8 ------------------------$"

def ishunk(x):
    hunkclasses = (crecordmod.uihunk, patch.recordhunk)
    return isinstance(x, hunkclasses)

def newandmodified(chunks, originalchunks):
    newlyaddedandmodifiedfiles = set()
    for chunk in chunks:
        if ishunk(chunk) and chunk.header.isnewfile() and chunk not in \
            originalchunks:
            newlyaddedandmodifiedfiles.add(chunk.header.filename())
    return newlyaddedandmodifiedfiles

def parsealiases(cmd):
    return cmd.split("|")

def setupwrapcolorwrite(ui):
    # wrap ui.write so diff output can be labeled/colorized
    def wrapwrite(orig, *args, **kw):
        label = kw.pop(r'label', '')
        for chunk, l in patch.difflabel(lambda: args):
            orig(chunk, label=label + l)

    oldwrite = ui.write
    def wrap(*args, **kwargs):
        return wrapwrite(oldwrite, *args, **kwargs)
    setattr(ui, 'write', wrap)
    return oldwrite

def filterchunks(ui, originalhunks, usecurses, testfile, operation=None):
    try:
        if usecurses:
            if testfile:
                recordfn = crecordmod.testdecorator(
                    testfile, crecordmod.testchunkselector)
            else:
                recordfn = crecordmod.chunkselector

            return crecordmod.filterpatch(ui, originalhunks, recordfn,
                                          operation)
    except crecordmod.fallbackerror as e:
        ui.warn('%s\n' % e.message)
        ui.warn(_('falling back to text mode\n'))

    return patch.filterpatch(ui, originalhunks, operation)

def recordfilter(ui, originalhunks, operation=None):
    """ Prompts the user to filter the originalhunks and return a list of
    selected hunks.
    *operation* is used for to build ui messages to indicate the user what
    kind of filtering they are doing: reverting, committing, shelving, etc.
    (see patch.filterpatch).
    """
    usecurses = crecordmod.checkcurses(ui)
    testfile = ui.config('experimental', 'crecordtest')
    oldwrite = setupwrapcolorwrite(ui)
    try:
        newchunks, newopts = filterchunks(ui, originalhunks, usecurses,
                                          testfile, operation)
    finally:
        ui.write = oldwrite
    return newchunks, newopts

def dorecord(ui, repo, commitfunc, cmdsuggest, backupall,
            filterfn, *pats, **opts):
    opts = pycompat.byteskwargs(opts)
    if not ui.interactive():
        if cmdsuggest:
            msg = _('running non-interactively, use %s instead') % cmdsuggest
        else:
            msg = _('running non-interactively')
        raise error.Abort(msg)

    # make sure username is set before going interactive
    if not opts.get('user'):
        ui.username() # raise exception, username not provided

    def recordfunc(ui, repo, message, match, opts):
        """This is generic record driver.

        Its job is to interactively filter local changes, and
        accordingly prepare working directory into a state in which the
        job can be delegated to a non-interactive commit command such as
        'commit' or 'qrefresh'.

        After the actual job is done by non-interactive command, the
        working directory is restored to its original state.

        In the end we'll record interesting changes, and everything else
        will be left in place, so the user can continue working.
        """

        checkunfinished(repo, commit=True)
        wctx = repo[None]
        merge = len(wctx.parents()) > 1
        if merge:
            raise error.Abort(_('cannot partially commit a merge '
                               '(use "hg commit" instead)'))

        def fail(f, msg):
            raise error.Abort('%s: %s' % (f, msg))

        force = opts.get('force')
        if not force:
            vdirs = []
            match.explicitdir = vdirs.append
            match.bad = fail

        status = repo.status(match=match)
        if not force:
            repo.checkcommitpatterns(wctx, vdirs, match, status, fail)
        diffopts = patch.difffeatureopts(ui, opts=opts, whitespace=True)
        diffopts.nodates = True
        diffopts.git = True
        diffopts.showfunc = True
        originaldiff = patch.diff(repo, changes=status, opts=diffopts)
        originalchunks = patch.parsepatch(originaldiff)

        # 1. filter patch, since we are intending to apply subset of it
        try:
            chunks, newopts = filterfn(ui, originalchunks)
        except error.PatchError as err:
            raise error.Abort(_('error parsing patch: %s') % err)
        opts.update(newopts)

        # We need to keep a backup of files that have been newly added and
        # modified during the recording process because there is a previous
        # version without the edit in the workdir
        newlyaddedandmodifiedfiles = newandmodified(chunks, originalchunks)
        contenders = set()
        for h in chunks:
            try:
                contenders.update(set(h.files()))
            except AttributeError:
                pass

        changed = status.modified + status.added + status.removed
        newfiles = [f for f in changed if f in contenders]
        if not newfiles:
            ui.status(_('no changes to record\n'))
            return 0

        modified = set(status.modified)

        # 2. backup changed files, so we can restore them in the end

        if backupall:
            tobackup = changed
        else:
            tobackup = [f for f in newfiles if f in modified or f in \
                    newlyaddedandmodifiedfiles]
        backups = {}
        if tobackup:
            backupdir = repo.vfs.join('record-backups')
            try:
                os.mkdir(backupdir)
            except OSError as err:
                if err.errno != errno.EEXIST:
                    raise
        try:
            # backup continues
            for f in tobackup:
                fd, tmpname = pycompat.mkstemp(prefix=f.replace('/', '_') + '.',
                                               dir=backupdir)
                os.close(fd)
                ui.debug('backup %r as %r\n' % (f, tmpname))
                util.copyfile(repo.wjoin(f), tmpname, copystat=True)
                backups[f] = tmpname

            fp = stringio()
            for c in chunks:
                fname = c.filename()
                if fname in backups:
                    c.write(fp)
            dopatch = fp.tell()
            fp.seek(0)

            # 2.5 optionally review / modify patch in text editor
            if opts.get('review', False):
                patchtext = (crecordmod.diffhelptext
                             + crecordmod.patchhelptext
                             + fp.read())
                reviewedpatch = ui.edit(patchtext, "",
                                        action="diff",
                                        repopath=repo.path)
                fp.truncate(0)
                fp.write(reviewedpatch)
                fp.seek(0)

            [os.unlink(repo.wjoin(c)) for c in newlyaddedandmodifiedfiles]
            # 3a. apply filtered patch to clean repo  (clean)
            if backups:
                # Equivalent to hg.revert
                m = scmutil.matchfiles(repo, backups.keys())
                mergemod.update(repo, repo.dirstate.p1(), branchmerge=False,
                                force=True, matcher=m)

            # 3b. (apply)
            if dopatch:
                try:
                    ui.debug('applying patch\n')
                    ui.debug(fp.getvalue())
                    patch.internalpatch(ui, repo, fp, 1, eolmode=None)
                except error.PatchError as err:
                    raise error.Abort(pycompat.bytestr(err))
            del fp

            # 4. We prepared working directory according to filtered
            #    patch. Now is the time to delegate the job to
            #    commit/qrefresh or the like!

            # Make all of the pathnames absolute.
            newfiles = [repo.wjoin(nf) for nf in newfiles]
            return commitfunc(ui, repo, *newfiles, **pycompat.strkwargs(opts))
        finally:
            # 5. finally restore backed-up files
            try:
                dirstate = repo.dirstate
                for realname, tmpname in backups.iteritems():
                    ui.debug('restoring %r to %r\n' % (tmpname, realname))

                    if dirstate[realname] == 'n':
                        # without normallookup, restoring timestamp
                        # may cause partially committed files
                        # to be treated as unmodified
                        dirstate.normallookup(realname)

                    # copystat=True here and above are a hack to trick any
                    # editors that have f open that we haven't modified them.
                    #
                    # Also note that this racy as an editor could notice the
                    # file's mtime before we've finished writing it.
                    util.copyfile(tmpname, repo.wjoin(realname), copystat=True)
                    os.unlink(tmpname)
                if tobackup:
                    os.rmdir(backupdir)
            except OSError:
                pass

    def recordinwlock(ui, repo, message, match, opts):
        with repo.wlock():
            return recordfunc(ui, repo, message, match, opts)

    return commit(ui, repo, recordinwlock, pats, opts)

class dirnode(object):
    """
    Represent a directory in user working copy with information required for
    the purpose of tersing its status.

    path is the path to the directory, without a trailing '/'

    statuses is a set of statuses of all files in this directory (this includes
    all the files in all the subdirectories too)

    files is a list of files which are direct child of this directory

    subdirs is a dictionary of sub-directory name as the key and it's own
    dirnode object as the value
    """

    def __init__(self, dirpath):
        self.path = dirpath
        self.statuses = set([])
        self.files = []
        self.subdirs = {}

    def _addfileindir(self, filename, status):
        """Add a file in this directory as a direct child."""
        self.files.append((filename, status))

    def addfile(self, filename, status):
        """
        Add a file to this directory or to its direct parent directory.

        If the file is not direct child of this directory, we traverse to the
        directory of which this file is a direct child of and add the file
        there.
        """

        # the filename contains a path separator, it means it's not the direct
        # child of this directory
        if '/' in filename:
            subdir, filep = filename.split('/', 1)

            # does the dirnode object for subdir exists
            if subdir not in self.subdirs:
                subdirpath = pathutil.join(self.path, subdir)
                self.subdirs[subdir] = dirnode(subdirpath)

            # try adding the file in subdir
            self.subdirs[subdir].addfile(filep, status)

        else:
            self._addfileindir(filename, status)

        if status not in self.statuses:
            self.statuses.add(status)

    def iterfilepaths(self):
        """Yield (status, path) for files directly under this directory."""
        for f, st in self.files:
            yield st, pathutil.join(self.path, f)

    def tersewalk(self, terseargs):
        """
        Yield (status, path) obtained by processing the status of this
        dirnode.

        terseargs is the string of arguments passed by the user with `--terse`
        flag.

        Following are the cases which can happen:

        1) All the files in the directory (including all the files in its
        subdirectories) share the same status and the user has asked us to terse
        that status. -> yield (status, dirpath).  dirpath will end in '/'.

        2) Otherwise, we do following:

                a) Yield (status, filepath)  for all the files which are in this
                    directory (only the ones in this directory, not the subdirs)

                b) Recurse the function on all the subdirectories of this
                   directory
        """

        if len(self.statuses) == 1:
            onlyst = self.statuses.pop()

            # Making sure we terse only when the status abbreviation is
            # passed as terse argument
            if onlyst in terseargs:
                yield onlyst, self.path + '/'
                return

        # add the files to status list
        for st, fpath in self.iterfilepaths():
            yield st, fpath

        #recurse on the subdirs
        for dirobj in self.subdirs.values():
            for st, fpath in dirobj.tersewalk(terseargs):
                yield st, fpath

def tersedir(statuslist, terseargs):
    """
    Terse the status if all the files in a directory shares the same status.

    statuslist is scmutil.status() object which contains a list of files for
    each status.
    terseargs is string which is passed by the user as the argument to `--terse`
    flag.

    The function makes a tree of objects of dirnode class, and at each node it
    stores the information required to know whether we can terse a certain
    directory or not.
    """
    # the order matters here as that is used to produce final list
    allst = ('m', 'a', 'r', 'd', 'u', 'i', 'c')

    # checking the argument validity
    for s in pycompat.bytestr(terseargs):
        if s not in allst:
            raise error.Abort(_("'%s' not recognized") % s)

    # creating a dirnode object for the root of the repo
    rootobj = dirnode('')
    pstatus = ('modified', 'added', 'deleted', 'clean', 'unknown',
               'ignored', 'removed')

    tersedict = {}
    for attrname in pstatus:
        statuschar = attrname[0:1]
        for f in getattr(statuslist, attrname):
            rootobj.addfile(f, statuschar)
        tersedict[statuschar] = []

    # we won't be tersing the root dir, so add files in it
    for st, fpath in rootobj.iterfilepaths():
        tersedict[st].append(fpath)

    # process each sub-directory and build tersedict
    for subdir in rootobj.subdirs.values():
        for st, f in subdir.tersewalk(terseargs):
            tersedict[st].append(f)

    tersedlist = []
    for st in allst:
        tersedict[st].sort()
        tersedlist.append(tersedict[st])

    return tersedlist

def _commentlines(raw):
    '''Surround lineswith a comment char and a new line'''
    lines = raw.splitlines()
    commentedlines = ['# %s' % line for line in lines]
    return '\n'.join(commentedlines) + '\n'

def _conflictsmsg(repo):
    mergestate = mergemod.mergestate.read(repo)
    if not mergestate.active():
        return

    m = scmutil.match(repo[None])
    unresolvedlist = [f for f in mergestate.unresolved() if m(f)]
    if unresolvedlist:
        mergeliststr = '\n'.join(
            ['    %s' % util.pathto(repo.root, encoding.getcwd(), path)
             for path in sorted(unresolvedlist)])
        msg = _('''Unresolved merge conflicts:

%s

To mark files as resolved:  hg resolve --mark FILE''') % mergeliststr
    else:
        msg = _('No unresolved merge conflicts.')

    return _commentlines(msg)

def _helpmessage(continuecmd, abortcmd):
    msg = _('To continue:    %s\n'
            'To abort:       %s') % (continuecmd, abortcmd)
    return _commentlines(msg)

def _rebasemsg():
    return _helpmessage('hg rebase --continue', 'hg rebase --abort')

def _histeditmsg():
    return _helpmessage('hg histedit --continue', 'hg histedit --abort')

def _unshelvemsg():
    return _helpmessage('hg unshelve --continue', 'hg unshelve --abort')

def _graftmsg():
    # tweakdefaults requires `update` to have a rev hence the `.`
    return _helpmessage('hg graft --continue', 'hg graft --abort')

def _mergemsg():
    # tweakdefaults requires `update` to have a rev hence the `.`
    return _helpmessage('hg commit', 'hg merge --abort')

def _bisectmsg():
    msg = _('To mark the changeset good:    hg bisect --good\n'
            'To mark the changeset bad:     hg bisect --bad\n'
            'To abort:                      hg bisect --reset\n')
    return _commentlines(msg)

def fileexistspredicate(filename):
    return lambda repo: repo.vfs.exists(filename)

def _mergepredicate(repo):
    return len(repo[None].parents()) > 1

STATES = (
    # (state, predicate to detect states, helpful message function)
    ('histedit', fileexistspredicate('histedit-state'), _histeditmsg),
    ('bisect', fileexistspredicate('bisect.state'), _bisectmsg),
    ('graft', fileexistspredicate('graftstate'), _graftmsg),
    ('unshelve', fileexistspredicate('shelvedstate'), _unshelvemsg),
    ('rebase', fileexistspredicate('rebasestate'), _rebasemsg),
    # The merge state is part of a list that will be iterated over.
    # They need to be last because some of the other unfinished states may also
    # be in a merge or update state (eg. rebase, histedit, graft, etc).
    # We want those to have priority.
    ('merge', _mergepredicate, _mergemsg),
)

def _getrepostate(repo):
    # experimental config: commands.status.skipstates
    skip = set(repo.ui.configlist('commands', 'status.skipstates'))
    for state, statedetectionpredicate, msgfn in STATES:
        if state in skip:
            continue
        if statedetectionpredicate(repo):
            return (state, statedetectionpredicate, msgfn)

def morestatus(repo, fm):
    statetuple = _getrepostate(repo)
    label = 'status.morestatus'
    if statetuple:
        state, statedetectionpredicate, helpfulmsg = statetuple
        statemsg = _('The repository is in an unfinished *%s* state.') % state
        fm.plain('%s\n' % _commentlines(statemsg), label=label)
        conmsg = _conflictsmsg(repo)
        if conmsg:
            fm.plain('%s\n' % conmsg, label=label)
        if helpfulmsg:
            helpmsg = helpfulmsg()
            fm.plain('%s\n' % helpmsg, label=label)

def findpossible(cmd, table, strict=False):
    """
    Return cmd -> (aliases, command table entry)
    for each matching command.
    Return debug commands (or their aliases) only if no normal command matches.
    """
    choice = {}
    debugchoice = {}

    if cmd in table:
        # short-circuit exact matches, "log" alias beats "log|history"
        keys = [cmd]
    else:
        keys = table.keys()

    allcmds = []
    for e in keys:
        aliases = parsealiases(e)
        allcmds.extend(aliases)
        found = None
        if cmd in aliases:
            found = cmd
        elif not strict:
            for a in aliases:
                if a.startswith(cmd):
                    found = a
                    break
        if found is not None:
            if aliases[0].startswith("debug") or found.startswith("debug"):
                debugchoice[found] = (aliases, table[e])
            else:
                choice[found] = (aliases, table[e])

    if not choice and debugchoice:
        choice = debugchoice

    return choice, allcmds

def findcmd(cmd, table, strict=True):
    """Return (aliases, command table entry) for command string."""
    choice, allcmds = findpossible(cmd, table, strict)

    if cmd in choice:
        return choice[cmd]

    if len(choice) > 1:
        clist = sorted(choice)
        raise error.AmbiguousCommand(cmd, clist)

    if choice:
        return list(choice.values())[0]

    raise error.UnknownCommand(cmd, allcmds)

def changebranch(ui, repo, revs, label):
    """ Change the branch name of given revs to label """

    with repo.wlock(), repo.lock(), repo.transaction('branches'):
        # abort in case of uncommitted merge or dirty wdir
        bailifchanged(repo)
        revs = scmutil.revrange(repo, revs)
        if not revs:
            raise error.Abort("empty revision set")
        roots = repo.revs('roots(%ld)', revs)
        if len(roots) > 1:
            raise error.Abort(_("cannot change branch of non-linear revisions"))
        rewriteutil.precheck(repo, revs, 'change branch of')

        root = repo[roots.first()]
        if not root.p1().branch() == label and label in repo.branchmap():
            raise error.Abort(_("a branch of the same name already exists"))

        if repo.revs('merge() and %ld', revs):
            raise error.Abort(_("cannot change branch of a merge commit"))
        if repo.revs('obsolete() and %ld', revs):
            raise error.Abort(_("cannot change branch of a obsolete changeset"))

        # make sure only topological heads
        if repo.revs('heads(%ld) - head()', revs):
            raise error.Abort(_("cannot change branch in middle of a stack"))

        replacements = {}
        # avoid import cycle mercurial.cmdutil -> mercurial.context ->
        # mercurial.subrepo -> mercurial.cmdutil
        from . import context
        for rev in revs:
            ctx = repo[rev]
            oldbranch = ctx.branch()
            # check if ctx has same branch
            if oldbranch == label:
                continue

            def filectxfn(repo, newctx, path):
                try:
                    return ctx[path]
                except error.ManifestLookupError:
                    return None

            ui.debug("changing branch of '%s' from '%s' to '%s'\n"
                     % (hex(ctx.node()), oldbranch, label))
            extra = ctx.extra()
            extra['branch_change'] = hex(ctx.node())
            # While changing branch of set of linear commits, make sure that
            # we base our commits on new parent rather than old parent which
            # was obsoleted while changing the branch
            p1 = ctx.p1().node()
            p2 = ctx.p2().node()
            if p1 in replacements:
                p1 = replacements[p1][0]
            if p2 in replacements:
                p2 = replacements[p2][0]

            mc = context.memctx(repo, (p1, p2),
                                ctx.description(),
                                ctx.files(),
                                filectxfn,
                                user=ctx.user(),
                                date=ctx.date(),
                                extra=extra,
                                branch=label)

            newnode = repo.commitctx(mc)
            replacements[ctx.node()] = (newnode,)
            ui.debug('new node id is %s\n' % hex(newnode))

        # create obsmarkers and move bookmarks
        scmutil.cleanupnodes(repo, replacements, 'branch-change', fixphase=True)

        # move the working copy too
        wctx = repo[None]
        # in-progress merge is a bit too complex for now.
        if len(wctx.parents()) == 1:
            newid = replacements.get(wctx.p1().node())
            if newid is not None:
                # avoid import cycle mercurial.cmdutil -> mercurial.hg ->
                # mercurial.cmdutil
                from . import hg
                hg.update(repo, newid[0], quietempty=True)

        ui.status(_("changed branch on %d changesets\n") % len(replacements))

def findrepo(p):
    while not os.path.isdir(os.path.join(p, ".hg")):
        oldp, p = p, os.path.dirname(p)
        if p == oldp:
            return None

    return p

def bailifchanged(repo, merge=True, hint=None):
    """ enforce the precondition that working directory must be clean.

    'merge' can be set to false if a pending uncommitted merge should be
    ignored (such as when 'update --check' runs).

    'hint' is the usual hint given to Abort exception.
    """

    if merge and repo.dirstate.p2() != nullid:
        raise error.Abort(_('outstanding uncommitted merge'), hint=hint)
    modified, added, removed, deleted = repo.status()[:4]
    if modified or added or removed or deleted:
        raise error.Abort(_('uncommitted changes'), hint=hint)
    ctx = repo[None]
    for s in sorted(ctx.substate):
        ctx.sub(s).bailifchanged(hint=hint)

def logmessage(ui, opts):
    """ get the log message according to -m and -l option """
    message = opts.get('message')
    logfile = opts.get('logfile')

    if message and logfile:
        raise error.Abort(_('options --message and --logfile are mutually '
                           'exclusive'))
    if not message and logfile:
        try:
            if isstdiofilename(logfile):
                message = ui.fin.read()
            else:
                message = '\n'.join(util.readfile(logfile).splitlines())
        except IOError as inst:
            raise error.Abort(_("can't read commit message '%s': %s") %
                             (logfile, encoding.strtolocal(inst.strerror)))
    return message

def mergeeditform(ctxorbool, baseformname):
    """return appropriate editform name (referencing a committemplate)

    'ctxorbool' is either a ctx to be committed, or a bool indicating whether
    merging is committed.

    This returns baseformname with '.merge' appended if it is a merge,
    otherwise '.normal' is appended.
    """
    if isinstance(ctxorbool, bool):
        if ctxorbool:
            return baseformname + ".merge"
    elif len(ctxorbool.parents()) > 1:
        return baseformname + ".merge"

    return baseformname + ".normal"

def getcommiteditor(edit=False, finishdesc=None, extramsg=None,
                    editform='', **opts):
    """get appropriate commit message editor according to '--edit' option

    'finishdesc' is a function to be called with edited commit message
    (= 'description' of the new changeset) just after editing, but
    before checking empty-ness. It should return actual text to be
    stored into history. This allows to change description before
    storing.

    'extramsg' is a extra message to be shown in the editor instead of
    'Leave message empty to abort commit' line. 'HG: ' prefix and EOL
    is automatically added.

    'editform' is a dot-separated list of names, to distinguish
    the purpose of commit text editing.

    'getcommiteditor' returns 'commitforceeditor' regardless of
    'edit', if one of 'finishdesc' or 'extramsg' is specified, because
    they are specific for usage in MQ.
    """
    if edit or finishdesc or extramsg:
        return lambda r, c, s: commitforceeditor(r, c, s,
                                                 finishdesc=finishdesc,
                                                 extramsg=extramsg,
                                                 editform=editform)
    elif editform:
        return lambda r, c, s: commiteditor(r, c, s, editform=editform)
    else:
        return commiteditor

def _escapecommandtemplate(tmpl):
    parts = []
    for typ, start, end in templater.scantemplate(tmpl, raw=True):
        if typ == b'string':
            parts.append(stringutil.escapestr(tmpl[start:end]))
        else:
            parts.append(tmpl[start:end])
    return b''.join(parts)

def rendercommandtemplate(ui, tmpl, props):
    r"""Expand a literal template 'tmpl' in a way suitable for command line

    '\' in outermost string is not taken as an escape character because it
    is a directory separator on Windows.

    >>> from . import ui as uimod
    >>> ui = uimod.ui()
    >>> rendercommandtemplate(ui, b'c:\\{path}', {b'path': b'foo'})
    'c:\\foo'
    >>> rendercommandtemplate(ui, b'{"c:\\{path}"}', {'path': b'foo'})
    'c:{path}'
    """
    if not tmpl:
        return tmpl
    t = formatter.maketemplater(ui, _escapecommandtemplate(tmpl))
    return t.renderdefault(props)

def rendertemplate(ctx, tmpl, props=None):
    """Expand a literal template 'tmpl' byte-string against one changeset

    Each props item must be a stringify-able value or a callable returning
    such value, i.e. no bare list nor dict should be passed.
    """
    repo = ctx.repo()
    tres = formatter.templateresources(repo.ui, repo)
    t = formatter.maketemplater(repo.ui, tmpl, defaults=templatekw.keywords,
                                resources=tres)
    mapping = {'ctx': ctx}
    if props:
        mapping.update(props)
    return t.renderdefault(mapping)

def _buildfntemplate(pat, total=None, seqno=None, revwidth=None, pathname=None):
    r"""Convert old-style filename format string to template string

    >>> _buildfntemplate(b'foo-%b-%n.patch', seqno=0)
    'foo-{reporoot|basename}-{seqno}.patch'
    >>> _buildfntemplate(b'%R{tags % "{tag}"}%H')
    '{rev}{tags % "{tag}"}{node}'

    '\' in outermost strings has to be escaped because it is a directory
    separator on Windows:

    >>> _buildfntemplate(b'c:\\tmp\\%R\\%n.patch', seqno=0)
    'c:\\\\tmp\\\\{rev}\\\\{seqno}.patch'
    >>> _buildfntemplate(b'\\\\foo\\bar.patch')
    '\\\\\\\\foo\\\\bar.patch'
    >>> _buildfntemplate(b'\\{tags % "{tag}"}')
    '\\\\{tags % "{tag}"}'

    but inner strings follow the template rules (i.e. '\' is taken as an
    escape character):

    >>> _buildfntemplate(br'{"c:\tmp"}', seqno=0)
    '{"c:\\tmp"}'
    """
    expander = {
        b'H': b'{node}',
        b'R': b'{rev}',
        b'h': b'{node|short}',
        b'm': br'{sub(r"[^\w]", "_", desc|firstline)}',
        b'r': b'{if(revwidth, pad(rev, revwidth, "0", left=True), rev)}',
        b'%': b'%',
        b'b': b'{reporoot|basename}',
    }
    if total is not None:
        expander[b'N'] = b'{total}'
    if seqno is not None:
        expander[b'n'] = b'{seqno}'
    if total is not None and seqno is not None:
        expander[b'n'] = b'{pad(seqno, total|stringify|count, "0", left=True)}'
    if pathname is not None:
        expander[b's'] = b'{pathname|basename}'
        expander[b'd'] = b'{if(pathname|dirname, pathname|dirname, ".")}'
        expander[b'p'] = b'{pathname}'

    newname = []
    for typ, start, end in templater.scantemplate(pat, raw=True):
        if typ != b'string':
            newname.append(pat[start:end])
            continue
        i = start
        while i < end:
            n = pat.find(b'%', i, end)
            if n < 0:
                newname.append(stringutil.escapestr(pat[i:end]))
                break
            newname.append(stringutil.escapestr(pat[i:n]))
            if n + 2 > end:
                raise error.Abort(_("incomplete format spec in output "
                                    "filename"))
            c = pat[n + 1:n + 2]
            i = n + 2
            try:
                newname.append(expander[c])
            except KeyError:
                raise error.Abort(_("invalid format spec '%%%s' in output "
                                    "filename") % c)
    return ''.join(newname)

def makefilename(ctx, pat, **props):
    if not pat:
        return pat
    tmpl = _buildfntemplate(pat, **props)
    # BUG: alias expansion shouldn't be made against template fragments
    # rewritten from %-format strings, but we have no easy way to partially
    # disable the expansion.
    return rendertemplate(ctx, tmpl, pycompat.byteskwargs(props))

def isstdiofilename(pat):
    """True if the given pat looks like a filename denoting stdin/stdout"""
    return not pat or pat == '-'

class _unclosablefile(object):
    def __init__(self, fp):
        self._fp = fp

    def close(self):
        pass

    def __iter__(self):
        return iter(self._fp)

    def __getattr__(self, attr):
        return getattr(self._fp, attr)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        pass

def makefileobj(ctx, pat, mode='wb', **props):
    writable = mode not in ('r', 'rb')

    if isstdiofilename(pat):
        repo = ctx.repo()
        if writable:
            fp = repo.ui.fout
        else:
            fp = repo.ui.fin
        return _unclosablefile(fp)
    fn = makefilename(ctx, pat, **props)
    return open(fn, mode)

def openstorage(repo, cmd, file_, opts, returnrevlog=False):
    """opens the changelog, manifest, a filelog or a given revlog"""
    cl = opts['changelog']
    mf = opts['manifest']
    dir = opts['dir']
    msg = None
    if cl and mf:
        msg = _('cannot specify --changelog and --manifest at the same time')
    elif cl and dir:
        msg = _('cannot specify --changelog and --dir at the same time')
    elif cl or mf or dir:
        if file_:
            msg = _('cannot specify filename with --changelog or --manifest')
        elif not repo:
            msg = _('cannot specify --changelog or --manifest or --dir '
                    'without a repository')
    if msg:
        raise error.Abort(msg)

    r = None
    if repo:
        if cl:
            r = repo.unfiltered().changelog
        elif dir:
            if 'treemanifest' not in repo.requirements:
                raise error.Abort(_("--dir can only be used on repos with "
                                   "treemanifest enabled"))
            if not dir.endswith('/'):
                dir = dir + '/'
            dirlog = repo.manifestlog.getstorage(dir)
            if len(dirlog):
                r = dirlog
        elif mf:
            r = repo.manifestlog.getstorage(b'')
        elif file_:
            filelog = repo.file(file_)
            if len(filelog):
                r = filelog

        # Not all storage may be revlogs. If requested, try to return an actual
        # revlog instance.
        if returnrevlog:
            if isinstance(r, revlog.revlog):
                pass
            elif util.safehasattr(r, '_revlog'):
                r = r._revlog
            elif r is not None:
                raise error.Abort(_('%r does not appear to be a revlog') % r)

    if not r:
        if not returnrevlog:
            raise error.Abort(_('cannot give path to non-revlog'))

        if not file_:
            raise error.CommandError(cmd, _('invalid arguments'))
        if not os.path.isfile(file_):
            raise error.Abort(_("revlog '%s' not found") % file_)
        r = revlog.revlog(vfsmod.vfs(encoding.getcwd(), audit=False),
                          file_[:-2] + ".i")
    return r

def openrevlog(repo, cmd, file_, opts):
    """Obtain a revlog backing storage of an item.

    This is similar to ``openstorage()`` except it always returns a revlog.

    In most cases, a caller cares about the main storage object - not the
    revlog backing it. Therefore, this function should only be used by code
    that needs to examine low-level revlog implementation details. e.g. debug
    commands.
    """
    return openstorage(repo, cmd, file_, opts, returnrevlog=True)

def copy(ui, repo, pats, opts, rename=False):
    # called with the repo lock held
    #
    # hgsep => pathname that uses "/" to separate directories
    # ossep => pathname that uses os.sep to separate directories
    cwd = repo.getcwd()
    targets = {}
    after = opts.get("after")
    dryrun = opts.get("dry_run")
    wctx = repo[None]

    def walkpat(pat):
        srcs = []
        if after:
            badstates = '?'
        else:
            badstates = '?r'
        m = scmutil.match(wctx, [pat], opts, globbed=True)
        for abs in wctx.walk(m):
            state = repo.dirstate[abs]
            rel = m.rel(abs)
            exact = m.exact(abs)
            if state in badstates:
                if exact and state == '?':
                    ui.warn(_('%s: not copying - file is not managed\n') % rel)
                if exact and state == 'r':
                    ui.warn(_('%s: not copying - file has been marked for'
                              ' remove\n') % rel)
                continue
            # abs: hgsep
            # rel: ossep
            srcs.append((abs, rel, exact))
        return srcs

    # abssrc: hgsep
    # relsrc: ossep
    # otarget: ossep
    def copyfile(abssrc, relsrc, otarget, exact):
        abstarget = pathutil.canonpath(repo.root, cwd, otarget)
        if '/' in abstarget:
            # We cannot normalize abstarget itself, this would prevent
            # case only renames, like a => A.
            abspath, absname = abstarget.rsplit('/', 1)
            abstarget = repo.dirstate.normalize(abspath) + '/' + absname
        reltarget = repo.pathto(abstarget, cwd)
        target = repo.wjoin(abstarget)
        src = repo.wjoin(abssrc)
        state = repo.dirstate[abstarget]

        scmutil.checkportable(ui, abstarget)

        # check for collisions
        prevsrc = targets.get(abstarget)
        if prevsrc is not None:
            ui.warn(_('%s: not overwriting - %s collides with %s\n') %
                    (reltarget, repo.pathto(abssrc, cwd),
                     repo.pathto(prevsrc, cwd)))
            return True # report a failure

        # check for overwrites
        exists = os.path.lexists(target)
        samefile = False
        if exists and abssrc != abstarget:
            if (repo.dirstate.normalize(abssrc) ==
                repo.dirstate.normalize(abstarget)):
                if not rename:
                    ui.warn(_("%s: can't copy - same file\n") % reltarget)
                    return True # report a failure
                exists = False
                samefile = True

        if not after and exists or after and state in 'mn':
            if not opts['force']:
                if state in 'mn':
                    msg = _('%s: not overwriting - file already committed\n')
                    if after:
                        flags = '--after --force'
                    else:
                        flags = '--force'
                    if rename:
                        hint = _("('hg rename %s' to replace the file by "
                                 'recording a rename)\n') % flags
                    else:
                        hint = _("('hg copy %s' to replace the file by "
                                 'recording a copy)\n') % flags
                else:
                    msg = _('%s: not overwriting - file exists\n')
                    if rename:
                        hint = _("('hg rename --after' to record the rename)\n")
                    else:
                        hint = _("('hg copy --after' to record the copy)\n")
                ui.warn(msg % reltarget)
                ui.warn(hint)
                return True # report a failure

        if after:
            if not exists:
                if rename:
                    ui.warn(_('%s: not recording move - %s does not exist\n') %
                            (relsrc, reltarget))
                else:
                    ui.warn(_('%s: not recording copy - %s does not exist\n') %
                            (relsrc, reltarget))
                return True # report a failure
        elif not dryrun:
            try:
                if exists:
                    os.unlink(target)
                targetdir = os.path.dirname(target) or '.'
                if not os.path.isdir(targetdir):
                    os.makedirs(targetdir)
                if samefile:
                    tmp = target + "~hgrename"
                    os.rename(src, tmp)
                    os.rename(tmp, target)
                else:
                    # Preserve stat info on renames, not on copies; this matches
                    # Linux CLI behavior.
                    util.copyfile(src, target, copystat=rename)
                srcexists = True
            except IOError as inst:
                if inst.errno == errno.ENOENT:
                    ui.warn(_('%s: deleted in working directory\n') % relsrc)
                    srcexists = False
                else:
                    ui.warn(_('%s: cannot copy - %s\n') %
                            (relsrc, encoding.strtolocal(inst.strerror)))
                    if rename:
                        hint = _("('hg rename --after' to record the rename)\n")
                    else:
                        hint = _("('hg copy --after' to record the copy)\n")
                    return True # report a failure

        if ui.verbose or not exact:
            if rename:
                ui.status(_('moving %s to %s\n') % (relsrc, reltarget))
            else:
                ui.status(_('copying %s to %s\n') % (relsrc, reltarget))

        targets[abstarget] = abssrc

        # fix up dirstate
        scmutil.dirstatecopy(ui, repo, wctx, abssrc, abstarget,
                             dryrun=dryrun, cwd=cwd)
        if rename and not dryrun:
            if not after and srcexists and not samefile:
                rmdir = repo.ui.configbool('experimental', 'removeemptydirs')
                repo.wvfs.unlinkpath(abssrc, rmdir=rmdir)
            wctx.forget([abssrc])

    # pat: ossep
    # dest ossep
    # srcs: list of (hgsep, hgsep, ossep, bool)
    # return: function that takes hgsep and returns ossep
    def targetpathfn(pat, dest, srcs):
        if os.path.isdir(pat):
            abspfx = pathutil.canonpath(repo.root, cwd, pat)
            abspfx = util.localpath(abspfx)
            if destdirexists:
                striplen = len(os.path.split(abspfx)[0])
            else:
                striplen = len(abspfx)
            if striplen:
                striplen += len(pycompat.ossep)
            res = lambda p: os.path.join(dest, util.localpath(p)[striplen:])
        elif destdirexists:
            res = lambda p: os.path.join(dest,
                                         os.path.basename(util.localpath(p)))
        else:
            res = lambda p: dest
        return res

    # pat: ossep
    # dest ossep
    # srcs: list of (hgsep, hgsep, ossep, bool)
    # return: function that takes hgsep and returns ossep
    def targetpathafterfn(pat, dest, srcs):
        if matchmod.patkind(pat):
            # a mercurial pattern
            res = lambda p: os.path.join(dest,
                                         os.path.basename(util.localpath(p)))
        else:
            abspfx = pathutil.canonpath(repo.root, cwd, pat)
            if len(abspfx) < len(srcs[0][0]):
                # A directory. Either the target path contains the last
                # component of the source path or it does not.
                def evalpath(striplen):
                    score = 0
                    for s in srcs:
                        t = os.path.join(dest, util.localpath(s[0])[striplen:])
                        if os.path.lexists(t):
                            score += 1
                    return score

                abspfx = util.localpath(abspfx)
                striplen = len(abspfx)
                if striplen:
                    striplen += len(pycompat.ossep)
                if os.path.isdir(os.path.join(dest, os.path.split(abspfx)[1])):
                    score = evalpath(striplen)
                    striplen1 = len(os.path.split(abspfx)[0])
                    if striplen1:
                        striplen1 += len(pycompat.ossep)
                    if evalpath(striplen1) > score:
                        striplen = striplen1
                res = lambda p: os.path.join(dest,
                                             util.localpath(p)[striplen:])
            else:
                # a file
                if destdirexists:
                    res = lambda p: os.path.join(dest,
                                        os.path.basename(util.localpath(p)))
                else:
                    res = lambda p: dest
        return res

    pats = scmutil.expandpats(pats)
    if not pats:
        raise error.Abort(_('no source or destination specified'))
    if len(pats) == 1:
        raise error.Abort(_('no destination specified'))
    dest = pats.pop()
    destdirexists = os.path.isdir(dest) and not os.path.islink(dest)
    if not destdirexists:
        if len(pats) > 1 or matchmod.patkind(pats[0]):
            raise error.Abort(_('with multiple sources, destination must be an '
                               'existing directory'))
        if util.endswithsep(dest):
            raise error.Abort(_('destination %s is not a directory') % dest)

    tfn = targetpathfn
    if after:
        tfn = targetpathafterfn
    copylist = []
    for pat in pats:
        srcs = walkpat(pat)
        if not srcs:
            continue
        copylist.append((tfn(pat, dest, srcs), srcs))
    if not copylist:
        raise error.Abort(_('no files to copy'))

    errors = 0
    for targetpath, srcs in copylist:
        for abssrc, relsrc, exact in srcs:
            if copyfile(abssrc, relsrc, targetpath(abssrc), exact):
                errors += 1

    return errors != 0

## facility to let extension process additional data into an import patch
# list of identifier to be executed in order
extrapreimport = []  # run before commit
extrapostimport = [] # run after commit
# mapping from identifier to actual import function
#
# 'preimport' are run before the commit is made and are provided the following
# arguments:
# - repo: the localrepository instance,
# - patchdata: data extracted from patch header (cf m.patch.patchheadermap),
# - extra: the future extra dictionary of the changeset, please mutate it,
# - opts: the import options.
# XXX ideally, we would just pass an ctx ready to be computed, that would allow
# mutation of in memory commit and more. Feel free to rework the code to get
# there.
extrapreimportmap = {}
# 'postimport' are run after the commit is made and are provided the following
# argument:
# - ctx: the changectx created by import.
extrapostimportmap = {}

def tryimportone(ui, repo, patchdata, parents, opts, msgs, updatefunc):
    """Utility function used by commands.import to import a single patch

    This function is explicitly defined here to help the evolve extension to
    wrap this part of the import logic.

    The API is currently a bit ugly because it a simple code translation from
    the import command. Feel free to make it better.

    :patchdata: a dictionary containing parsed patch data (such as from
                ``patch.extract()``)
    :parents: nodes that will be parent of the created commit
    :opts: the full dict of option passed to the import command
    :msgs: list to save commit message to.
           (used in case we need to save it when failing)
    :updatefunc: a function that update a repo to a given node
                 updatefunc(<repo>, <node>)
    """
    # avoid cycle context -> subrepo -> cmdutil
    from . import context

    tmpname = patchdata.get('filename')
    message = patchdata.get('message')
    user = opts.get('user') or patchdata.get('user')
    date = opts.get('date') or patchdata.get('date')
    branch = patchdata.get('branch')
    nodeid = patchdata.get('nodeid')
    p1 = patchdata.get('p1')
    p2 = patchdata.get('p2')

    nocommit = opts.get('no_commit')
    importbranch = opts.get('import_branch')
    update = not opts.get('bypass')
    strip = opts["strip"]
    prefix = opts["prefix"]
    sim = float(opts.get('similarity') or 0)

    if not tmpname:
        return None, None, False

    rejects = False

    cmdline_message = logmessage(ui, opts)
    if cmdline_message:
        # pickup the cmdline msg
        message = cmdline_message
    elif message:
        # pickup the patch msg
        message = message.strip()
    else:
        # launch the editor
        message = None
    ui.debug('message:\n%s\n' % (message or ''))

    if len(parents) == 1:
        parents.append(repo[nullid])
    if opts.get('exact'):
        if not nodeid or not p1:
            raise error.Abort(_('not a Mercurial patch'))
        p1 = repo[p1]
        p2 = repo[p2 or nullid]
    elif p2:
        try:
            p1 = repo[p1]
            p2 = repo[p2]
            # Without any options, consider p2 only if the
            # patch is being applied on top of the recorded
            # first parent.
            if p1 != parents[0]:
                p1 = parents[0]
                p2 = repo[nullid]
        except error.RepoError:
            p1, p2 = parents
        if p2.node() == nullid:
            ui.warn(_("warning: import the patch as a normal revision\n"
                      "(use --exact to import the patch as a merge)\n"))
    else:
        p1, p2 = parents

    n = None
    if update:
        if p1 != parents[0]:
            updatefunc(repo, p1.node())
        if p2 != parents[1]:
            repo.setparents(p1.node(), p2.node())

        if opts.get('exact') or importbranch:
            repo.dirstate.setbranch(branch or 'default')

        partial = opts.get('partial', False)
        files = set()
        try:
            patch.patch(ui, repo, tmpname, strip=strip, prefix=prefix,
                        files=files, eolmode=None, similarity=sim / 100.0)
        except error.PatchError as e:
            if not partial:
                raise error.Abort(pycompat.bytestr(e))
            if partial:
                rejects = True

        files = list(files)
        if nocommit:
            if message:
                msgs.append(message)
        else:
            if opts.get('exact') or p2:
                # If you got here, you either use --force and know what
                # you are doing or used --exact or a merge patch while
                # being updated to its first parent.
                m = None
            else:
                m = scmutil.matchfiles(repo, files or [])
            editform = mergeeditform(repo[None], 'import.normal')
            if opts.get('exact'):
                editor = None
            else:
                editor = getcommiteditor(editform=editform,
                                         **pycompat.strkwargs(opts))
            extra = {}
            for idfunc in extrapreimport:
                extrapreimportmap[idfunc](repo, patchdata, extra, opts)
            overrides = {}
            if partial:
                overrides[('ui', 'allowemptycommit')] = True
            with repo.ui.configoverride(overrides, 'import'):
                n = repo.commit(message, user,
                                date, match=m,
                                editor=editor, extra=extra)
                for idfunc in extrapostimport:
                    extrapostimportmap[idfunc](repo[n])
    else:
        if opts.get('exact') or importbranch:
            branch = branch or 'default'
        else:
            branch = p1.branch()
        store = patch.filestore()
        try:
            files = set()
            try:
                patch.patchrepo(ui, repo, p1, store, tmpname, strip, prefix,
                                files, eolmode=None)
            except error.PatchError as e:
                raise error.Abort(stringutil.forcebytestr(e))
            if opts.get('exact'):
                editor = None
            else:
                editor = getcommiteditor(editform='import.bypass')
            memctx = context.memctx(repo, (p1.node(), p2.node()),
                                    message,
                                    files=files,
                                    filectxfn=store,
                                    user=user,
                                    date=date,
                                    branch=branch,
                                    editor=editor)
            n = memctx.commit()
        finally:
            store.close()
    if opts.get('exact') and nocommit:
        # --exact with --no-commit is still useful in that it does merge
        # and branch bits
        ui.warn(_("warning: can't check exact import with --no-commit\n"))
    elif opts.get('exact') and (not n or hex(n) != nodeid):
        raise error.Abort(_('patch is damaged or loses information'))
    msg = _('applied to working directory')
    if n:
        # i18n: refers to a short changeset id
        msg = _('created %s') % short(n)
    return msg, n, rejects

# facility to let extensions include additional data in an exported patch
# list of identifiers to be executed in order
extraexport = []
# mapping from identifier to actual export function
# function as to return a string to be added to the header or None
# it is given two arguments (sequencenumber, changectx)
extraexportmap = {}

def _exportsingle(repo, ctx, fm, match, switch_parent, seqno, diffopts):
    node = scmutil.binnode(ctx)
    parents = [p.node() for p in ctx.parents() if p]
    branch = ctx.branch()
    if switch_parent:
        parents.reverse()

    if parents:
        prev = parents[0]
    else:
        prev = nullid

    fm.context(ctx=ctx)
    fm.plain('# HG changeset patch\n')
    fm.write('user', '# User %s\n', ctx.user())
    fm.plain('# Date %d %d\n' % ctx.date())
    fm.write('date', '#      %s\n', fm.formatdate(ctx.date()))
    fm.condwrite(branch and branch != 'default',
                 'branch', '# Branch %s\n', branch)
    fm.write('node', '# Node ID %s\n', hex(node))
    fm.plain('# Parent  %s\n' % hex(prev))
    if len(parents) > 1:
        fm.plain('# Parent  %s\n' % hex(parents[1]))
    fm.data(parents=fm.formatlist(pycompat.maplist(hex, parents), name='node'))

    # TODO: redesign extraexportmap function to support formatter
    for headerid in extraexport:
        header = extraexportmap[headerid](seqno, ctx)
        if header is not None:
            fm.plain('# %s\n' % header)

    fm.write('desc', '%s\n', ctx.description().rstrip())
    fm.plain('\n')

    if fm.isplain():
        chunkiter = patch.diffui(repo, prev, node, match, opts=diffopts)
        for chunk, label in chunkiter:
            fm.plain(chunk, label=label)
    else:
        chunkiter = patch.diff(repo, prev, node, match, opts=diffopts)
        # TODO: make it structured?
        fm.data(diff=b''.join(chunkiter))

def _exportfile(repo, revs, fm, dest, switch_parent, diffopts, match):
    """Export changesets to stdout or a single file"""
    for seqno, rev in enumerate(revs, 1):
        ctx = repo[rev]
        if not dest.startswith('<'):
            repo.ui.note("%s\n" % dest)
        fm.startitem()
        _exportsingle(repo, ctx, fm, match, switch_parent, seqno, diffopts)

def _exportfntemplate(repo, revs, basefm, fntemplate, switch_parent, diffopts,
                      match):
    """Export changesets to possibly multiple files"""
    total = len(revs)
    revwidth = max(len(str(rev)) for rev in revs)
    filemap = util.sortdict()  # filename: [(seqno, rev), ...]

    for seqno, rev in enumerate(revs, 1):
        ctx = repo[rev]
        dest = makefilename(ctx, fntemplate,
                            total=total, seqno=seqno, revwidth=revwidth)
        filemap.setdefault(dest, []).append((seqno, rev))

    for dest in filemap:
        with formatter.maybereopen(basefm, dest) as fm:
            repo.ui.note("%s\n" % dest)
            for seqno, rev in filemap[dest]:
                fm.startitem()
                ctx = repo[rev]
                _exportsingle(repo, ctx, fm, match, switch_parent, seqno,
                              diffopts)

def export(repo, revs, basefm, fntemplate='hg-%h.patch', switch_parent=False,
           opts=None, match=None):
    '''export changesets as hg patches

    Args:
      repo: The repository from which we're exporting revisions.
      revs: A list of revisions to export as revision numbers.
      basefm: A formatter to which patches should be written.
      fntemplate: An optional string to use for generating patch file names.
      switch_parent: If True, show diffs against second parent when not nullid.
                     Default is false, which always shows diff against p1.
      opts: diff options to use for generating the patch.
      match: If specified, only export changes to files matching this matcher.

    Returns:
      Nothing.

    Side Effect:
      "HG Changeset Patch" data is emitted to one of the following
      destinations:
        fntemplate specified: Each rev is written to a unique file named using
                            the given template.
        Otherwise: All revs will be written to basefm.
    '''
    scmutil.prefetchfiles(repo, revs, match)

    if not fntemplate:
        _exportfile(repo, revs, basefm, '<unnamed>', switch_parent, opts, match)
    else:
        _exportfntemplate(repo, revs, basefm, fntemplate, switch_parent, opts,
                          match)

def exportfile(repo, revs, fp, switch_parent=False, opts=None, match=None):
    """Export changesets to the given file stream"""
    scmutil.prefetchfiles(repo, revs, match)

    dest = getattr(fp, 'name', '<unnamed>')
    with formatter.formatter(repo.ui, fp, 'export', {}) as fm:
        _exportfile(repo, revs, fm, dest, switch_parent, opts, match)

def showmarker(fm, marker, index=None):
    """utility function to display obsolescence marker in a readable way

    To be used by debug function."""
    if index is not None:
        fm.write('index', '%i ', index)
    fm.write('prednode', '%s ', hex(marker.prednode()))
    succs = marker.succnodes()
    fm.condwrite(succs, 'succnodes', '%s ',
                 fm.formatlist(map(hex, succs), name='node'))
    fm.write('flag', '%X ', marker.flags())
    parents = marker.parentnodes()
    if parents is not None:
        fm.write('parentnodes', '{%s} ',
                 fm.formatlist(map(hex, parents), name='node', sep=', '))
    fm.write('date', '(%s) ', fm.formatdate(marker.date()))
    meta = marker.metadata().copy()
    meta.pop('date', None)
    smeta = pycompat.rapply(pycompat.maybebytestr, meta)
    fm.write('metadata', '{%s}', fm.formatdict(smeta, fmt='%r: %r', sep=', '))
    fm.plain('\n')

def finddate(ui, repo, date):
    """Find the tipmost changeset that matches the given date spec"""

    df = dateutil.matchdate(date)
    m = scmutil.matchall(repo)
    results = {}

    def prep(ctx, fns):
        d = ctx.date()
        if df(d[0]):
            results[ctx.rev()] = d

    for ctx in walkchangerevs(repo, m, {'rev': None}, prep):
        rev = ctx.rev()
        if rev in results:
            ui.status(_("found revision %s from %s\n") %
                      (rev, dateutil.datestr(results[rev])))
            return '%d' % rev

    raise error.Abort(_("revision matching date not found"))

def increasingwindows(windowsize=8, sizelimit=512):
    while True:
        yield windowsize
        if windowsize < sizelimit:
            windowsize *= 2

def _walkrevs(repo, opts):
    # Default --rev value depends on --follow but --follow behavior
    # depends on revisions resolved from --rev...
    follow = opts.get('follow') or opts.get('follow_first')
    if opts.get('rev'):
        revs = scmutil.revrange(repo, opts['rev'])
    elif follow and repo.dirstate.p1() == nullid:
        revs = smartset.baseset()
    elif follow:
        revs = repo.revs('reverse(:.)')
    else:
        revs = smartset.spanset(repo)
        revs.reverse()
    return revs

class FileWalkError(Exception):
    pass

def walkfilerevs(repo, match, follow, revs, fncache):
    '''Walks the file history for the matched files.

    Returns the changeset revs that are involved in the file history.

    Throws FileWalkError if the file history can't be walked using
    filelogs alone.
    '''
    wanted = set()
    copies = []
    minrev, maxrev = min(revs), max(revs)
    def filerevgen(filelog, last):
        """
        Only files, no patterns.  Check the history of each file.

        Examines filelog entries within minrev, maxrev linkrev range
        Returns an iterator yielding (linkrev, parentlinkrevs, copied)
        tuples in backwards order
        """
        cl_count = len(repo)
        revs = []
        for j in pycompat.xrange(0, last + 1):
            linkrev = filelog.linkrev(j)
            if linkrev < minrev:
                continue
            # only yield rev for which we have the changelog, it can
            # happen while doing "hg log" during a pull or commit
            if linkrev >= cl_count:
                break

            parentlinkrevs = []
            for p in filelog.parentrevs(j):
                if p != nullrev:
                    parentlinkrevs.append(filelog.linkrev(p))
            n = filelog.node(j)
            revs.append((linkrev, parentlinkrevs,
                         follow and filelog.renamed(n)))

        return reversed(revs)
    def iterfiles():
        pctx = repo['.']
        for filename in match.files():
            if follow:
                if filename not in pctx:
                    raise error.Abort(_('cannot follow file not in parent '
                                       'revision: "%s"') % filename)
                yield filename, pctx[filename].filenode()
            else:
                yield filename, None
        for filename_node in copies:
            yield filename_node

    for file_, node in iterfiles():
        filelog = repo.file(file_)
        if not len(filelog):
            if node is None:
                # A zero count may be a directory or deleted file, so
                # try to find matching entries on the slow path.
                if follow:
                    raise error.Abort(
                        _('cannot follow nonexistent file: "%s"') % file_)
                raise FileWalkError("Cannot walk via filelog")
            else:
                continue

        if node is None:
            last = len(filelog) - 1
        else:
            last = filelog.rev(node)

        # keep track of all ancestors of the file
        ancestors = {filelog.linkrev(last)}

        # iterate from latest to oldest revision
        for rev, flparentlinkrevs, copied in filerevgen(filelog, last):
            if not follow:
                if rev > maxrev:
                    continue
            else:
                # Note that last might not be the first interesting
                # rev to us:
                # if the file has been changed after maxrev, we'll
                # have linkrev(last) > maxrev, and we still need
                # to explore the file graph
                if rev not in ancestors:
                    continue
                # XXX insert 1327 fix here
                if flparentlinkrevs:
                    ancestors.update(flparentlinkrevs)

            fncache.setdefault(rev, []).append(file_)
            wanted.add(rev)
            if copied:
                copies.append(copied)

    return wanted

class _followfilter(object):
    def __init__(self, repo, onlyfirst=False):
        self.repo = repo
        self.startrev = nullrev
        self.roots = set()
        self.onlyfirst = onlyfirst

    def match(self, rev):
        def realparents(rev):
            if self.onlyfirst:
                return self.repo.changelog.parentrevs(rev)[0:1]
            else:
                return filter(lambda x: x != nullrev,
                              self.repo.changelog.parentrevs(rev))

        if self.startrev == nullrev:
            self.startrev = rev
            return True

        if rev > self.startrev:
            # forward: all descendants
            if not self.roots:
                self.roots.add(self.startrev)
            for parent in realparents(rev):
                if parent in self.roots:
                    self.roots.add(rev)
                    return True
        else:
            # backwards: all parents
            if not self.roots:
                self.roots.update(realparents(self.startrev))
            if rev in self.roots:
                self.roots.remove(rev)
                self.roots.update(realparents(rev))
                return True

        return False

def walkchangerevs(repo, match, opts, prepare):
    '''Iterate over files and the revs in which they changed.

    Callers most commonly need to iterate backwards over the history
    in which they are interested. Doing so has awful (quadratic-looking)
    performance, so we use iterators in a "windowed" way.

    We walk a window of revisions in the desired order.  Within the
    window, we first walk forwards to gather data, then in the desired
    order (usually backwards) to display it.

    This function returns an iterator yielding contexts. Before
    yielding each context, the iterator will first call the prepare
    function on each context in the window in forward order.'''

    allfiles = opts.get('all_files')
    follow = opts.get('follow') or opts.get('follow_first')
    revs = _walkrevs(repo, opts)
    if not revs:
        return []
    wanted = set()
    slowpath = match.anypats() or (not match.always() and opts.get('removed'))
    fncache = {}
    change = repo.__getitem__

    # First step is to fill wanted, the set of revisions that we want to yield.
    # When it does not induce extra cost, we also fill fncache for revisions in
    # wanted: a cache of filenames that were changed (ctx.files()) and that
    # match the file filtering conditions.

    if match.always() or allfiles:
        # No files, no patterns.  Display all revs.
        wanted = revs
    elif not slowpath:
        # We only have to read through the filelog to find wanted revisions

        try:
            wanted = walkfilerevs(repo, match, follow, revs, fncache)
        except FileWalkError:
            slowpath = True

            # We decided to fall back to the slowpath because at least one
            # of the paths was not a file. Check to see if at least one of them
            # existed in history, otherwise simply return
            for path in match.files():
                if path == '.' or path in repo.store:
                    break
            else:
                return []

    if slowpath:
        # We have to read the changelog to match filenames against
        # changed files

        if follow:
            raise error.Abort(_('can only follow copies/renames for explicit '
                               'filenames'))

        # The slow path checks files modified in every changeset.
        # This is really slow on large repos, so compute the set lazily.
        class lazywantedset(object):
            def __init__(self):
                self.set = set()
                self.revs = set(revs)

            # No need to worry about locality here because it will be accessed
            # in the same order as the increasing window below.
            def __contains__(self, value):
                if value in self.set:
                    return True
                elif not value in self.revs:
                    return False
                else:
                    self.revs.discard(value)
                    ctx = change(value)
                    matches = [f for f in ctx.files() if match(f)]
                    if matches:
                        fncache[value] = matches
                        self.set.add(value)
                        return True
                    return False

            def discard(self, value):
                self.revs.discard(value)
                self.set.discard(value)

        wanted = lazywantedset()

    # it might be worthwhile to do this in the iterator if the rev range
    # is descending and the prune args are all within that range
    for rev in opts.get('prune', ()):
        rev = repo[rev].rev()
        ff = _followfilter(repo)
        stop = min(revs[0], revs[-1])
        for x in pycompat.xrange(rev, stop - 1, -1):
            if ff.match(x):
                wanted = wanted - [x]

    # Now that wanted is correctly initialized, we can iterate over the
    # revision range, yielding only revisions in wanted.
    def iterate():
        if follow and match.always():
            ff = _followfilter(repo, onlyfirst=opts.get('follow_first'))
            def want(rev):
                return ff.match(rev) and rev in wanted
        else:
            def want(rev):
                return rev in wanted

        it = iter(revs)
        stopiteration = False
        for windowsize in increasingwindows():
            nrevs = []
            for i in pycompat.xrange(windowsize):
                rev = next(it, None)
                if rev is None:
                    stopiteration = True
                    break
                elif want(rev):
                    nrevs.append(rev)
            for rev in sorted(nrevs):
                fns = fncache.get(rev)
                ctx = change(rev)
                if not fns:
                    def fns_generator():
                        if allfiles:
                            fiter = iter(ctx)
                        else:
                            fiter = ctx.files()
                        for f in fiter:
                            if match(f):
                                yield f
                    fns = fns_generator()
                prepare(ctx, fns)
            for rev in nrevs:
                yield change(rev)

            if stopiteration:
                break

    return iterate()

def add(ui, repo, match, prefix, explicitonly, **opts):
    join = lambda f: os.path.join(prefix, f)
    bad = []

    badfn = lambda x, y: bad.append(x) or match.bad(x, y)
    names = []
    wctx = repo[None]
    cca = None
    abort, warn = scmutil.checkportabilityalert(ui)
    if abort or warn:
        cca = scmutil.casecollisionauditor(ui, abort, repo.dirstate)

    match = repo.narrowmatch(match, includeexact=True)
    badmatch = matchmod.badmatch(match, badfn)
    dirstate = repo.dirstate
    # We don't want to just call wctx.walk here, since it would return a lot of
    # clean files, which we aren't interested in and takes time.
    for f in sorted(dirstate.walk(badmatch, subrepos=sorted(wctx.substate),
                                  unknown=True, ignored=False, full=False)):
        exact = match.exact(f)
        if exact or not explicitonly and f not in wctx and repo.wvfs.lexists(f):
            if cca:
                cca(f)
            names.append(f)
            if ui.verbose or not exact:
                ui.status(_('adding %s\n') % match.rel(f),
                          label='ui.addremove.added')

    for subpath in sorted(wctx.substate):
        sub = wctx.sub(subpath)
        try:
            submatch = matchmod.subdirmatcher(subpath, match)
            if opts.get(r'subrepos'):
                bad.extend(sub.add(ui, submatch, prefix, False, **opts))
            else:
                bad.extend(sub.add(ui, submatch, prefix, True, **opts))
        except error.LookupError:
            ui.status(_("skipping missing subrepository: %s\n")
                           % join(subpath))

    if not opts.get(r'dry_run'):
        rejected = wctx.add(names, prefix)
        bad.extend(f for f in rejected if f in match.files())
    return bad

def addwebdirpath(repo, serverpath, webconf):
    webconf[serverpath] = repo.root
    repo.ui.debug('adding %s = %s\n' % (serverpath, repo.root))

    for r in repo.revs('filelog("path:.hgsub")'):
        ctx = repo[r]
        for subpath in ctx.substate:
            ctx.sub(subpath).addwebdirpath(serverpath, webconf)

def forget(ui, repo, match, prefix, explicitonly, dryrun, interactive):
    if dryrun and interactive:
        raise error.Abort(_("cannot specify both --dry-run and --interactive"))
    join = lambda f: os.path.join(prefix, f)
    bad = []
    badfn = lambda x, y: bad.append(x) or match.bad(x, y)
    wctx = repo[None]
    forgot = []

    s = repo.status(match=matchmod.badmatch(match, badfn), clean=True)
    forget = sorted(s.modified + s.added + s.deleted + s.clean)
    if explicitonly:
        forget = [f for f in forget if match.exact(f)]

    for subpath in sorted(wctx.substate):
        sub = wctx.sub(subpath)
        try:
            submatch = matchmod.subdirmatcher(subpath, match)
            subbad, subforgot = sub.forget(submatch, prefix, dryrun=dryrun,
                                           interactive=interactive)
            bad.extend([subpath + '/' + f for f in subbad])
            forgot.extend([subpath + '/' + f for f in subforgot])
        except error.LookupError:
            ui.status(_("skipping missing subrepository: %s\n")
                           % join(subpath))

    if not explicitonly:
        for f in match.files():
            if f not in repo.dirstate and not repo.wvfs.isdir(f):
                if f not in forgot:
                    if repo.wvfs.exists(f):
                        # Don't complain if the exact case match wasn't given.
                        # But don't do this until after checking 'forgot', so
                        # that subrepo files aren't normalized, and this op is
                        # purely from data cached by the status walk above.
                        if repo.dirstate.normalize(f) in repo.dirstate:
                            continue
                        ui.warn(_('not removing %s: '
                                  'file is already untracked\n')
                                % match.rel(f))
                    bad.append(f)

    if interactive:
        responses = _('[Ynsa?]'
                      '$$ &Yes, forget this file'
                      '$$ &No, skip this file'
                      '$$ &Skip remaining files'
                      '$$ Include &all remaining files'
                      '$$ &? (display help)')
        for filename in forget[:]:
            r = ui.promptchoice(_('forget %s %s') % (filename, responses))
            if r == 4: # ?
                while r == 4:
                    for c, t in ui.extractchoices(responses)[1]:
                        ui.write('%s - %s\n' % (c, encoding.lower(t)))
                    r = ui.promptchoice(_('forget %s %s') % (filename,
                                                                 responses))
            if r == 0: # yes
                continue
            elif r == 1: # no
                forget.remove(filename)
            elif r == 2: # Skip
                fnindex = forget.index(filename)
                del forget[fnindex:]
                break
            elif r == 3: # All
                break

    for f in forget:
        if ui.verbose or not match.exact(f) or interactive:
            ui.status(_('removing %s\n') % match.rel(f),
                      label='ui.addremove.removed')

    if not dryrun:
        rejected = wctx.forget(forget, prefix)
        bad.extend(f for f in rejected if f in match.files())
        forgot.extend(f for f in forget if f not in rejected)
    return bad, forgot

def files(ui, ctx, m, fm, fmt, subrepos):
    ret = 1

    needsfctx = ui.verbose or {'size', 'flags'} & fm.datahint()
    for f in ctx.matches(m):
        fm.startitem()
        fm.context(ctx=ctx)
        if needsfctx:
            fc = ctx[f]
            fm.write('size flags', '% 10d % 1s ', fc.size(), fc.flags())
        fm.data(path=f)
        fm.plain(fmt % m.rel(f))
        ret = 0

    for subpath in sorted(ctx.substate):
        submatch = matchmod.subdirmatcher(subpath, m)
        if (subrepos or m.exact(subpath) or any(submatch.files())):
            sub = ctx.sub(subpath)
            try:
                recurse = m.exact(subpath) or subrepos
                if sub.printfiles(ui, submatch, fm, fmt, recurse) == 0:
                    ret = 0
            except error.LookupError:
                ui.status(_("skipping missing subrepository: %s\n")
                               % m.abs(subpath))

    return ret

def remove(ui, repo, m, prefix, after, force, subrepos, dryrun, warnings=None):
    join = lambda f: os.path.join(prefix, f)
    ret = 0
    s = repo.status(match=m, clean=True)
    modified, added, deleted, clean = s[0], s[1], s[3], s[6]

    wctx = repo[None]

    if warnings is None:
        warnings = []
        warn = True
    else:
        warn = False

    subs = sorted(wctx.substate)
    progress = ui.makeprogress(_('searching'), total=len(subs),
                               unit=_('subrepos'))
    for subpath in subs:
        submatch = matchmod.subdirmatcher(subpath, m)
        if subrepos or m.exact(subpath) or any(submatch.files()):
            progress.increment()
            sub = wctx.sub(subpath)
            try:
                if sub.removefiles(submatch, prefix, after, force, subrepos,
                                   dryrun, warnings):
                    ret = 1
            except error.LookupError:
                warnings.append(_("skipping missing subrepository: %s\n")
                               % join(subpath))
    progress.complete()

    # warn about failure to delete explicit files/dirs
    deleteddirs = util.dirs(deleted)
    files = m.files()
    progress = ui.makeprogress(_('deleting'), total=len(files),
                               unit=_('files'))
    for f in files:
        def insubrepo():
            for subpath in wctx.substate:
                if f.startswith(subpath + '/'):
                    return True
            return False

        progress.increment()
        isdir = f in deleteddirs or wctx.hasdir(f)
        if (f in repo.dirstate or isdir or f == '.'
            or insubrepo() or f in subs):
            continue

        if repo.wvfs.exists(f):
            if repo.wvfs.isdir(f):
                warnings.append(_('not removing %s: no tracked files\n')
                        % m.rel(f))
            else:
                warnings.append(_('not removing %s: file is untracked\n')
                        % m.rel(f))
        # missing files will generate a warning elsewhere
        ret = 1
    progress.complete()

    if force:
        list = modified + deleted + clean + added
    elif after:
        list = deleted
        remaining = modified + added + clean
        progress = ui.makeprogress(_('skipping'), total=len(remaining),
                                   unit=_('files'))
        for f in remaining:
            progress.increment()
            if ui.verbose or (f in files):
                warnings.append(_('not removing %s: file still exists\n')
                                % m.rel(f))
            ret = 1
        progress.complete()
    else:
        list = deleted + clean
        progress = ui.makeprogress(_('skipping'),
                                   total=(len(modified) + len(added)),
                                   unit=_('files'))
        for f in modified:
            progress.increment()
            warnings.append(_('not removing %s: file is modified (use -f'
                      ' to force removal)\n') % m.rel(f))
            ret = 1
        for f in added:
            progress.increment()
            warnings.append(_("not removing %s: file has been marked for add"
                      " (use 'hg forget' to undo add)\n") % m.rel(f))
            ret = 1
        progress.complete()

    list = sorted(list)
    progress = ui.makeprogress(_('deleting'), total=len(list),
                               unit=_('files'))
    for f in list:
        if ui.verbose or not m.exact(f):
            progress.increment()
            ui.status(_('removing %s\n') % m.rel(f),
                      label='ui.addremove.removed')
    progress.complete()

    if not dryrun:
        with repo.wlock():
            if not after:
                for f in list:
                    if f in added:
                        continue # we never unlink added files on remove
                    rmdir = repo.ui.configbool('experimental',
                                               'removeemptydirs')
                    repo.wvfs.unlinkpath(f, ignoremissing=True, rmdir=rmdir)
            repo[None].forget(list)

    if warn:
        for warning in warnings:
            ui.warn(warning)

    return ret

def _updatecatformatter(fm, ctx, matcher, path, decode):
    """Hook for adding data to the formatter used by ``hg cat``.

    Extensions (e.g., lfs) can wrap this to inject keywords/data, but must call
    this method first."""
    data = ctx[path].data()
    if decode:
        data = ctx.repo().wwritedata(path, data)
    fm.startitem()
    fm.context(ctx=ctx)
    fm.write('data', '%s', data)
    fm.data(path=path)

def cat(ui, repo, ctx, matcher, basefm, fntemplate, prefix, **opts):
    err = 1
    opts = pycompat.byteskwargs(opts)

    def write(path):
        filename = None
        if fntemplate:
            filename = makefilename(ctx, fntemplate,
                                    pathname=os.path.join(prefix, path))
            # attempt to create the directory if it does not already exist
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError:
                pass
        with formatter.maybereopen(basefm, filename) as fm:
            _updatecatformatter(fm, ctx, matcher, path, opts.get('decode'))

    # Automation often uses hg cat on single files, so special case it
    # for performance to avoid the cost of parsing the manifest.
    if len(matcher.files()) == 1 and not matcher.anypats():
        file = matcher.files()[0]
        mfl = repo.manifestlog
        mfnode = ctx.manifestnode()
        try:
            if mfnode and mfl[mfnode].find(file)[0]:
                scmutil.prefetchfiles(repo, [ctx.rev()], matcher)
                write(file)
                return 0
        except KeyError:
            pass

    scmutil.prefetchfiles(repo, [ctx.rev()], matcher)

    for abs in ctx.walk(matcher):
        write(abs)
        err = 0

    for subpath in sorted(ctx.substate):
        sub = ctx.sub(subpath)
        try:
            submatch = matchmod.subdirmatcher(subpath, matcher)

            if not sub.cat(submatch, basefm, fntemplate,
                           os.path.join(prefix, sub._path),
                           **pycompat.strkwargs(opts)):
                err = 0
        except error.RepoLookupError:
            ui.status(_("skipping missing subrepository: %s\n")
                           % os.path.join(prefix, subpath))

    return err

def commit(ui, repo, commitfunc, pats, opts):
    '''commit the specified files or all outstanding changes'''
    date = opts.get('date')
    if date:
        opts['date'] = dateutil.parsedate(date)
    message = logmessage(ui, opts)
    matcher = scmutil.match(repo[None], pats, opts)

    dsguard = None
    # extract addremove carefully -- this function can be called from a command
    # that doesn't support addremove
    if opts.get('addremove'):
        dsguard = dirstateguard.dirstateguard(repo, 'commit')
    with dsguard or util.nullcontextmanager():
        if dsguard:
            if scmutil.addremove(repo, matcher, "", opts) != 0:
                raise error.Abort(
                    _("failed to mark all new/missing files as added/removed"))

        return commitfunc(ui, repo, message, matcher, opts)

def samefile(f, ctx1, ctx2):
    if f in ctx1.manifest():
        a = ctx1.filectx(f)
        if f in ctx2.manifest():
            b = ctx2.filectx(f)
            return (not a.cmp(b)
                    and a.flags() == b.flags())
        else:
            return False
    else:
        return f not in ctx2.manifest()

def amend(ui, repo, old, extra, pats, opts):
    # avoid cycle context -> subrepo -> cmdutil
    from . import context

    # amend will reuse the existing user if not specified, but the obsolete
    # marker creation requires that the current user's name is specified.
    if obsolete.isenabled(repo, obsolete.createmarkersopt):
        ui.username() # raise exception if username not set

    ui.note(_('amending changeset %s\n') % old)
    base = old.p1()

    with repo.wlock(), repo.lock(), repo.transaction('amend'):
        # Participating changesets:
        #
        # wctx     o - workingctx that contains changes from working copy
        #          |   to go into amending commit
        #          |
        # old      o - changeset to amend
        #          |
        # base     o - first parent of the changeset to amend
        wctx = repo[None]

        # Copy to avoid mutating input
        extra = extra.copy()
        # Update extra dict from amended commit (e.g. to preserve graft
        # source)
        extra.update(old.extra())

        # Also update it from the from the wctx
        extra.update(wctx.extra())

        user = opts.get('user') or old.user()
        date = opts.get('date') or old.date()

        # Parse the date to allow comparison between date and old.date()
        date = dateutil.parsedate(date)

        if len(old.parents()) > 1:
            # ctx.files() isn't reliable for merges, so fall back to the
            # slower repo.status() method
            files = set([fn for st in base.status(old)[:3]
                         for fn in st])
        else:
            files = set(old.files())

        # add/remove the files to the working copy if the "addremove" option
        # was specified.
        matcher = scmutil.match(wctx, pats, opts)
        if (opts.get('addremove')
            and scmutil.addremove(repo, matcher, "", opts)):
            raise error.Abort(
                _("failed to mark all new/missing files as added/removed"))

        # Check subrepos. This depends on in-place wctx._status update in
        # subrepo.precommit(). To minimize the risk of this hack, we do
        # nothing if .hgsub does not exist.
        if '.hgsub' in wctx or '.hgsub' in old:
            subs, commitsubs, newsubstate = subrepoutil.precommit(
                ui, wctx, wctx._status, matcher)
            # amend should abort if commitsubrepos is enabled
            assert not commitsubs
            if subs:
                subrepoutil.writestate(repo, newsubstate)

        ms = mergemod.mergestate.read(repo)
        mergeutil.checkunresolved(ms)

        filestoamend = set(f for f in wctx.files() if matcher(f))

        changes = (len(filestoamend) > 0)
        if changes:
            # Recompute copies (avoid recording a -> b -> a)
            copied = copies.pathcopies(base, wctx, matcher)
            if old.p2:
                copied.update(copies.pathcopies(old.p2(), wctx, matcher))

            # Prune files which were reverted by the updates: if old
            # introduced file X and the file was renamed in the working
            # copy, then those two files are the same and
            # we can discard X from our list of files. Likewise if X
            # was removed, it's no longer relevant. If X is missing (aka
            # deleted), old X must be preserved.
            files.update(filestoamend)
            files = [f for f in files if (not samefile(f, wctx, base)
                                          or f in wctx.deleted())]

            def filectxfn(repo, ctx_, path):
                try:
                    # If the file being considered is not amongst the files
                    # to be amended, we should return the file context from the
                    # old changeset. This avoids issues when only some files in
                    # the working copy are being amended but there are also
                    # changes to other files from the old changeset.
                    if path not in filestoamend:
                        return old.filectx(path)

                    # Return None for removed files.
                    if path in wctx.removed():
                        return None

                    fctx = wctx[path]
                    flags = fctx.flags()
                    mctx = context.memfilectx(repo, ctx_,
                                              fctx.path(), fctx.data(),
                                              islink='l' in flags,
                                              isexec='x' in flags,
                                              copied=copied.get(path))
                    return mctx
                except KeyError:
                    return None
        else:
            ui.note(_('copying changeset %s to %s\n') % (old, base))

            # Use version of files as in the old cset
            def filectxfn(repo, ctx_, path):
                try:
                    return old.filectx(path)
                except KeyError:
                    return None

        # See if we got a message from -m or -l, if not, open the editor with
        # the message of the changeset to amend.
        message = logmessage(ui, opts)

        editform = mergeeditform(old, 'commit.amend')
        editor = getcommiteditor(editform=editform,
                                 **pycompat.strkwargs(opts))

        if not message:
            editor = getcommiteditor(edit=True, editform=editform)
            message = old.description()

        pureextra = extra.copy()
        extra['amend_source'] = old.hex()

        new = context.memctx(repo,
                             parents=[base.node(), old.p2().node()],
                             text=message,
                             files=files,
                             filectxfn=filectxfn,
                             user=user,
                             date=date,
                             extra=extra,
                             editor=editor)

        newdesc = changelog.stripdesc(new.description())
        if ((not changes)
            and newdesc == old.description()
            and user == old.user()
            and date == old.date()
            and pureextra == old.extra()):
            # nothing changed. continuing here would create a new node
            # anyway because of the amend_source noise.
            #
            # This not what we expect from amend.
            return old.node()

        commitphase = None
        if opts.get('secret'):
            commitphase = phases.secret
        newid = repo.commitctx(new)

        # Reroute the working copy parent to the new changeset
        repo.setparents(newid, nullid)
        mapping = {old.node(): (newid,)}
        obsmetadata = None
        if opts.get('note'):
            obsmetadata = {'note': encoding.fromlocal(opts['note'])}
        backup = ui.configbool('ui', 'history-editing-backup')
        scmutil.cleanupnodes(repo, mapping, 'amend', metadata=obsmetadata,
                             fixphase=True, targetphase=commitphase,
                             backup=backup)

        # Fixing the dirstate because localrepo.commitctx does not update
        # it. This is rather convenient because we did not need to update
        # the dirstate for all the files in the new commit which commitctx
        # could have done if it updated the dirstate. Now, we can
        # selectively update the dirstate only for the amended files.
        dirstate = repo.dirstate

        # Update the state of the files which were added and
        # and modified in the amend to "normal" in the dirstate.
        normalfiles = set(wctx.modified() + wctx.added()) & filestoamend
        for f in normalfiles:
            dirstate.normal(f)

        # Update the state of files which were removed in the amend
        # to "removed" in the dirstate.
        removedfiles = set(wctx.removed()) & filestoamend
        for f in removedfiles:
            dirstate.drop(f)

    return newid

def commiteditor(repo, ctx, subs, editform=''):
    if ctx.description():
        return ctx.description()
    return commitforceeditor(repo, ctx, subs, editform=editform,
                             unchangedmessagedetection=True)

def commitforceeditor(repo, ctx, subs, finishdesc=None, extramsg=None,
                      editform='', unchangedmessagedetection=False):
    if not extramsg:
        extramsg = _("Leave message empty to abort commit.")

    forms = [e for e in editform.split('.') if e]
    forms.insert(0, 'changeset')
    templatetext = None
    while forms:
        ref = '.'.join(forms)
        if repo.ui.config('committemplate', ref):
            templatetext = committext = buildcommittemplate(
                repo, ctx, subs, extramsg, ref)
            break
        forms.pop()
    else:
        committext = buildcommittext(repo, ctx, subs, extramsg)

    # run editor in the repository root
    olddir = encoding.getcwd()
    os.chdir(repo.root)

    # make in-memory changes visible to external process
    tr = repo.currenttransaction()
    repo.dirstate.write(tr)
    pending = tr and tr.writepending() and repo.root

    editortext = repo.ui.edit(committext, ctx.user(), ctx.extra(),
                              editform=editform, pending=pending,
                              repopath=repo.path, action='commit')
    text = editortext

    # strip away anything below this special string (used for editors that want
    # to display the diff)
    stripbelow = re.search(_linebelow, text, flags=re.MULTILINE)
    if stripbelow:
        text = text[:stripbelow.start()]

    text = re.sub("(?m)^HG:.*(\n|$)", "", text)
    os.chdir(olddir)

    if finishdesc:
        text = finishdesc(text)
    if not text.strip():
        raise error.Abort(_("empty commit message"))
    if unchangedmessagedetection and editortext == templatetext:
        raise error.Abort(_("commit message unchanged"))

    return text

def buildcommittemplate(repo, ctx, subs, extramsg, ref):
    ui = repo.ui
    spec = formatter.templatespec(ref, None, None)
    t = logcmdutil.changesettemplater(ui, repo, spec)
    t.t.cache.update((k, templater.unquotestring(v))
                     for k, v in repo.ui.configitems('committemplate'))

    if not extramsg:
        extramsg = '' # ensure that extramsg is string

    ui.pushbuffer()
    t.show(ctx, extramsg=extramsg)
    return ui.popbuffer()

def hgprefix(msg):
    return "\n".join(["HG: %s" % a for a in msg.split("\n") if a])

def buildcommittext(repo, ctx, subs, extramsg):
    edittext = []
    modified, added, removed = ctx.modified(), ctx.added(), ctx.removed()
    if ctx.description():
        edittext.append(ctx.description())
    edittext.append("")
    edittext.append("") # Empty line between message and comments.
    edittext.append(hgprefix(_("Enter commit message."
                      "  Lines beginning with 'HG:' are removed.")))
    edittext.append(hgprefix(extramsg))
    edittext.append("HG: --")
    edittext.append(hgprefix(_("user: %s") % ctx.user()))
    if ctx.p2():
        edittext.append(hgprefix(_("branch merge")))
    if ctx.branch():
        edittext.append(hgprefix(_("branch '%s'") % ctx.branch()))
    if bookmarks.isactivewdirparent(repo):
        edittext.append(hgprefix(_("bookmark '%s'") % repo._activebookmark))
    edittext.extend([hgprefix(_("subrepo %s") % s) for s in subs])
    edittext.extend([hgprefix(_("added %s") % f) for f in added])
    edittext.extend([hgprefix(_("changed %s") % f) for f in modified])
    edittext.extend([hgprefix(_("removed %s") % f) for f in removed])
    if not added and not modified and not removed:
        edittext.append(hgprefix(_("no files changed")))
    edittext.append("")

    return "\n".join(edittext)

def commitstatus(repo, node, branch, bheads=None, opts=None):
    if opts is None:
        opts = {}
    ctx = repo[node]
    parents = ctx.parents()

    if (not opts.get('amend') and bheads and node not in bheads and not
        [x for x in parents if x.node() in bheads and x.branch() == branch]):
        repo.ui.status(_('created new head\n'))
        # The message is not printed for initial roots. For the other
        # changesets, it is printed in the following situations:
        #
        # Par column: for the 2 parents with ...
        #   N: null or no parent
        #   B: parent is on another named branch
        #   C: parent is a regular non head changeset
        #   H: parent was a branch head of the current branch
        # Msg column: whether we print "created new head" message
        # In the following, it is assumed that there already exists some
        # initial branch heads of the current branch, otherwise nothing is
        # printed anyway.
        #
        # Par Msg Comment
        # N N  y  additional topo root
        #
        # B N  y  additional branch root
        # C N  y  additional topo head
        # H N  n  usual case
        #
        # B B  y  weird additional branch root
        # C B  y  branch merge
        # H B  n  merge with named branch
        #
        # C C  y  additional head from merge
        # C H  n  merge with a head
        #
        # H H  n  head merge: head count decreases

    if not opts.get('close_branch'):
        for r in parents:
            if r.closesbranch() and r.branch() == branch:
                repo.ui.status(_('reopening closed branch head %d\n') % r.rev())

    if repo.ui.debugflag:
        repo.ui.write(_('committed changeset %d:%s\n') % (ctx.rev(), ctx.hex()))
    elif repo.ui.verbose:
        repo.ui.write(_('committed changeset %d:%s\n') % (ctx.rev(), ctx))

def postcommitstatus(repo, pats, opts):
    return repo.status(match=scmutil.match(repo[None], pats, opts))

def revert(ui, repo, ctx, parents, *pats, **opts):
    opts = pycompat.byteskwargs(opts)
    parent, p2 = parents
    node = ctx.node()

    mf = ctx.manifest()
    if node == p2:
        parent = p2

    # need all matching names in dirstate and manifest of target rev,
    # so have to walk both. do not print errors if files exist in one
    # but not other. in both cases, filesets should be evaluated against
    # workingctx to get consistent result (issue4497). this means 'set:**'
    # cannot be used to select missing files from target rev.

    # `names` is a mapping for all elements in working copy and target revision
    # The mapping is in the form:
    #   <abs path in repo> -> (<path from CWD>, <exactly specified by matcher?>)
    names = {}

    with repo.wlock():
        ## filling of the `names` mapping
        # walk dirstate to fill `names`

        interactive = opts.get('interactive', False)
        wctx = repo[None]
        m = scmutil.match(wctx, pats, opts)

        # we'll need this later
        targetsubs = sorted(s for s in wctx.substate if m(s))

        if not m.always():
            matcher = matchmod.badmatch(m, lambda x, y: False)
            for abs in wctx.walk(matcher):
                names[abs] = m.rel(abs), m.exact(abs)

            # walk target manifest to fill `names`

            def badfn(path, msg):
                if path in names:
                    return
                if path in ctx.substate:
                    return
                path_ = path + '/'
                for f in names:
                    if f.startswith(path_):
                        return
                ui.warn("%s: %s\n" % (m.rel(path), msg))

            for abs in ctx.walk(matchmod.badmatch(m, badfn)):
                if abs not in names:
                    names[abs] = m.rel(abs), m.exact(abs)

            # Find status of all file in `names`.
            m = scmutil.matchfiles(repo, names)

            changes = repo.status(node1=node, match=m,
                                  unknown=True, ignored=True, clean=True)
        else:
            changes = repo.status(node1=node, match=m)
            for kind in changes:
                for abs in kind:
                    names[abs] = m.rel(abs), m.exact(abs)

            m = scmutil.matchfiles(repo, names)

        modified = set(changes.modified)
        added    = set(changes.added)
        removed  = set(changes.removed)
        _deleted = set(changes.deleted)
        unknown  = set(changes.unknown)
        unknown.update(changes.ignored)
        clean    = set(changes.clean)
        modadded = set()

        # We need to account for the state of the file in the dirstate,
        # even when we revert against something else than parent. This will
        # slightly alter the behavior of revert (doing back up or not, delete
        # or just forget etc).
        if parent == node:
            dsmodified = modified
            dsadded = added
            dsremoved = removed
            # store all local modifications, useful later for rename detection
            localchanges = dsmodified | dsadded
            modified, added, removed = set(), set(), set()
        else:
            changes = repo.status(node1=parent, match=m)
            dsmodified = set(changes.modified)
            dsadded    = set(changes.added)
            dsremoved  = set(changes.removed)
            # store all local modifications, useful later for rename detection
            localchanges = dsmodified | dsadded

            # only take into account for removes between wc and target
            clean |= dsremoved - removed
            dsremoved &= removed
            # distinct between dirstate remove and other
            removed -= dsremoved

            modadded = added & dsmodified
            added -= modadded

            # tell newly modified apart.
            dsmodified &= modified
            dsmodified |= modified & dsadded # dirstate added may need backup
            modified -= dsmodified

            # We need to wait for some post-processing to update this set
            # before making the distinction. The dirstate will be used for
            # that purpose.
            dsadded = added

        # in case of merge, files that are actually added can be reported as
        # modified, we need to post process the result
        if p2 != nullid:
            mergeadd = set(dsmodified)
            for path in dsmodified:
                if path in mf:
                    mergeadd.remove(path)
            dsadded |= mergeadd
            dsmodified -= mergeadd

        # if f is a rename, update `names` to also revert the source
        cwd = repo.getcwd()
        for f in localchanges:
            src = repo.dirstate.copied(f)
            # XXX should we check for rename down to target node?
            if src and src not in names and repo.dirstate[src] == 'r':
                dsremoved.add(src)
                names[src] = (repo.pathto(src, cwd), True)

        # determine the exact nature of the deleted changesets
        deladded = set(_deleted)
        for path in _deleted:
            if path in mf:
                deladded.remove(path)
        deleted = _deleted - deladded

        # distinguish between file to forget and the other
        added = set()
        for abs in dsadded:
            if repo.dirstate[abs] != 'a':
                added.add(abs)
        dsadded -= added

        for abs in deladded:
            if repo.dirstate[abs] == 'a':
                dsadded.add(abs)
        deladded -= dsadded

        # For files marked as removed, we check if an unknown file is present at
        # the same path. If a such file exists it may need to be backed up.
        # Making the distinction at this stage helps have simpler backup
        # logic.
        removunk = set()
        for abs in removed:
            target = repo.wjoin(abs)
            if os.path.lexists(target):
                removunk.add(abs)
        removed -= removunk

        dsremovunk = set()
        for abs in dsremoved:
            target = repo.wjoin(abs)
            if os.path.lexists(target):
                dsremovunk.add(abs)
        dsremoved -= dsremovunk

        # action to be actually performed by revert
        # (<list of file>, message>) tuple
        actions = {'revert': ([], _('reverting %s\n')),
                   'add': ([], _('adding %s\n')),
                   'remove': ([], _('removing %s\n')),
                   'drop': ([], _('removing %s\n')),
                   'forget': ([], _('forgetting %s\n')),
                   'undelete': ([], _('undeleting %s\n')),
                   'noop': (None, _('no changes needed to %s\n')),
                   'unknown': (None, _('file not managed: %s\n')),
                  }

        # "constant" that convey the backup strategy.
        # All set to `discard` if `no-backup` is set do avoid checking
        # no_backup lower in the code.
        # These values are ordered for comparison purposes
        backupinteractive = 3 # do backup if interactively modified
        backup = 2  # unconditionally do backup
        check = 1   # check if the existing file differs from target
        discard = 0 # never do backup
        if opts.get('no_backup'):
            backupinteractive = backup = check = discard
        if interactive:
            dsmodifiedbackup = backupinteractive
        else:
            dsmodifiedbackup = backup
        tobackup = set()

        backupanddel = actions['remove']
        if not opts.get('no_backup'):
            backupanddel = actions['drop']

        disptable = (
            # dispatch table:
            #   file state
            #   action
            #   make backup

            ## Sets that results that will change file on disk
            # Modified compared to target, no local change
            (modified,      actions['revert'],   discard),
            # Modified compared to target, but local file is deleted
            (deleted,       actions['revert'],   discard),
            # Modified compared to target, local change
            (dsmodified,    actions['revert'],   dsmodifiedbackup),
            # Added since target
            (added,         actions['remove'],   discard),
            # Added in working directory
            (dsadded,       actions['forget'],   discard),
            # Added since target, have local modification
            (modadded,      backupanddel,        backup),
            # Added since target but file is missing in working directory
            (deladded,      actions['drop'],   discard),
            # Removed since  target, before working copy parent
            (removed,       actions['add'],      discard),
            # Same as `removed` but an unknown file exists at the same path
            (removunk,      actions['add'],      check),
            # Removed since targe, marked as such in working copy parent
            (dsremoved,     actions['undelete'], discard),
            # Same as `dsremoved` but an unknown file exists at the same path
            (dsremovunk,    actions['undelete'], check),
            ## the following sets does not result in any file changes
            # File with no modification
            (clean,         actions['noop'],     discard),
            # Existing file, not tracked anywhere
            (unknown,       actions['unknown'],  discard),
            )

        for abs, (rel, exact) in sorted(names.items()):
            # target file to be touch on disk (relative to cwd)
            target = repo.wjoin(abs)
            # search the entry in the dispatch table.
            # if the file is in any of these sets, it was touched in the working
            # directory parent and we are sure it needs to be reverted.
            for table, (xlist, msg), dobackup in disptable:
                if abs not in table:
                    continue
                if xlist is not None:
                    xlist.append(abs)
                    if dobackup:
                        # If in interactive mode, don't automatically create
                        # .orig files (issue4793)
                        if dobackup == backupinteractive:
                            tobackup.add(abs)
                        elif (backup <= dobackup or wctx[abs].cmp(ctx[abs])):
                            bakname = scmutil.origpath(ui, repo, rel)
                            ui.note(_('saving current version of %s as %s\n') %
                                    (rel, bakname))
                            if not opts.get('dry_run'):
                                if interactive:
                                    util.copyfile(target, bakname)
                                else:
                                    util.rename(target, bakname)
                    if opts.get('dry_run'):
                        if ui.verbose or not exact:
                            ui.status(msg % rel)
                elif exact:
                    ui.warn(msg % rel)
                break

        if not opts.get('dry_run'):
            needdata = ('revert', 'add', 'undelete')
            oplist = [actions[name][0] for name in needdata]
            prefetch = scmutil.prefetchfiles
            matchfiles = scmutil.matchfiles
            prefetch(repo, [ctx.rev()],
                     matchfiles(repo,
                                [f for sublist in oplist for f in sublist]))
            _performrevert(repo, parents, ctx, names, actions, interactive,
                           tobackup)

        if targetsubs:
            # Revert the subrepos on the revert list
            for sub in targetsubs:
                try:
                    wctx.sub(sub).revert(ctx.substate[sub], *pats,
                                         **pycompat.strkwargs(opts))
                except KeyError:
                    raise error.Abort("subrepository '%s' does not exist in %s!"
                                      % (sub, short(ctx.node())))

def _performrevert(repo, parents, ctx, names, actions, interactive=False,
                   tobackup=None):
    """function that actually perform all the actions computed for revert

    This is an independent function to let extension to plug in and react to
    the imminent revert.

    Make sure you have the working directory locked when calling this function.
    """
    parent, p2 = parents
    node = ctx.node()
    excluded_files = []

    def checkout(f):
        fc = ctx[f]
        repo.wwrite(f, fc.data(), fc.flags())

    def doremove(f):
        try:
            rmdir = repo.ui.configbool('experimental', 'removeemptydirs')
            repo.wvfs.unlinkpath(f, rmdir=rmdir)
        except OSError:
            pass
        repo.dirstate.remove(f)

    def prntstatusmsg(action, f):
        rel, exact = names[f]
        if repo.ui.verbose or not exact:
            repo.ui.status(actions[action][1] % rel)

    audit_path = pathutil.pathauditor(repo.root, cached=True)
    for f in actions['forget'][0]:
        if interactive:
            choice = repo.ui.promptchoice(
                _("forget added file %s (Yn)?$$ &Yes $$ &No") % f)
            if choice == 0:
                prntstatusmsg('forget', f)
                repo.dirstate.drop(f)
            else:
                excluded_files.append(f)
        else:
            prntstatusmsg('forget', f)
            repo.dirstate.drop(f)
    for f in actions['remove'][0]:
        audit_path(f)
        if interactive:
            choice = repo.ui.promptchoice(
                _("remove added file %s (Yn)?$$ &Yes $$ &No") % f)
            if choice == 0:
                prntstatusmsg('remove', f)
                doremove(f)
            else:
                excluded_files.append(f)
        else:
            prntstatusmsg('remove', f)
            doremove(f)
    for f in actions['drop'][0]:
        audit_path(f)
        prntstatusmsg('drop', f)
        repo.dirstate.remove(f)

    normal = None
    if node == parent:
        # We're reverting to our parent. If possible, we'd like status
        # to report the file as clean. We have to use normallookup for
        # merges to avoid losing information about merged/dirty files.
        if p2 != nullid:
            normal = repo.dirstate.normallookup
        else:
            normal = repo.dirstate.normal

    newlyaddedandmodifiedfiles = set()
    if interactive:
        # Prompt the user for changes to revert
        torevert = [f for f in actions['revert'][0] if f not in excluded_files]
        m = scmutil.matchfiles(repo, torevert)
        diffopts = patch.difffeatureopts(repo.ui, whitespace=True)
        diffopts.nodates = True
        diffopts.git = True
        operation = 'discard'
        reversehunks = True
        if node != parent:
            operation = 'apply'
            reversehunks = False
        if reversehunks:
            diff = patch.diff(repo, ctx.node(), None, m, opts=diffopts)
        else:
            diff = patch.diff(repo, None, ctx.node(), m, opts=diffopts)
        originalchunks = patch.parsepatch(diff)

        try:

            chunks, opts = recordfilter(repo.ui, originalchunks,
                                        operation=operation)
            if reversehunks:
                chunks = patch.reversehunks(chunks)

        except error.PatchError as err:
            raise error.Abort(_('error parsing patch: %s') % err)

        newlyaddedandmodifiedfiles = newandmodified(chunks, originalchunks)
        if tobackup is None:
            tobackup = set()
        # Apply changes
        fp = stringio()
        # chunks are serialized per file, but files aren't sorted
        for f in sorted(set(c.header.filename() for c in chunks if ishunk(c))):
            prntstatusmsg('revert', f)
        for c in chunks:
            if ishunk(c):
                abs = c.header.filename()
                # Create a backup file only if this hunk should be backed up
                if c.header.filename() in tobackup:
                    target = repo.wjoin(abs)
                    bakname = scmutil.origpath(repo.ui, repo, m.rel(abs))
                    util.copyfile(target, bakname)
                    tobackup.remove(abs)
            c.write(fp)
        dopatch = fp.tell()
        fp.seek(0)
        if dopatch:
            try:
                patch.internalpatch(repo.ui, repo, fp, 1, eolmode=None)
            except error.PatchError as err:
                raise error.Abort(pycompat.bytestr(err))
        del fp
    else:
        for f in actions['revert'][0]:
            prntstatusmsg('revert', f)
            checkout(f)
            if normal:
                normal(f)

    for f in actions['add'][0]:
        # Don't checkout modified files, they are already created by the diff
        if f not in newlyaddedandmodifiedfiles:
            prntstatusmsg('add', f)
            checkout(f)
            repo.dirstate.add(f)

    normal = repo.dirstate.normallookup
    if node == parent and p2 == nullid:
        normal = repo.dirstate.normal
    for f in actions['undelete'][0]:
        prntstatusmsg('undelete', f)
        checkout(f)
        normal(f)

    copied = copies.pathcopies(repo[parent], ctx)

    for f in actions['add'][0] + actions['undelete'][0] + actions['revert'][0]:
        if f in copied:
            repo.dirstate.copy(copied[f], f)

# a list of (ui, repo, otherpeer, opts, missing) functions called by
# commands.outgoing.  "missing" is "missing" of the result of
# "findcommonoutgoing()"
outgoinghooks = util.hooks()

# a list of (ui, repo) functions called by commands.summary
summaryhooks = util.hooks()

# a list of (ui, repo, opts, changes) functions called by commands.summary.
#
# functions should return tuple of booleans below, if 'changes' is None:
#  (whether-incomings-are-needed, whether-outgoings-are-needed)
#
# otherwise, 'changes' is a tuple of tuples below:
#  - (sourceurl, sourcebranch, sourcepeer, incoming)
#  - (desturl,   destbranch,   destpeer,   outgoing)
summaryremotehooks = util.hooks()

# A list of state files kept by multistep operations like graft.
# Since graft cannot be aborted, it is considered 'clearable' by update.
# note: bisect is intentionally excluded
# (state file, clearable, allowcommit, error, hint)
unfinishedstates = [
    ('graftstate', True, False, _('graft in progress'),
     _("use 'hg graft --continue' or 'hg graft --stop' to stop")),
    ('updatestate', True, False, _('last update was interrupted'),
     _("use 'hg update' to get a consistent checkout"))
    ]

def checkunfinished(repo, commit=False):
    '''Look for an unfinished multistep operation, like graft, and abort
    if found. It's probably good to check this right before
    bailifchanged().
    '''
    # Check for non-clearable states first, so things like rebase will take
    # precedence over update.
    for f, clearable, allowcommit, msg, hint in unfinishedstates:
        if clearable or (commit and allowcommit):
            continue
        if repo.vfs.exists(f):
            raise error.Abort(msg, hint=hint)

    for f, clearable, allowcommit, msg, hint in unfinishedstates:
        if not clearable or (commit and allowcommit):
            continue
        if repo.vfs.exists(f):
            raise error.Abort(msg, hint=hint)

def clearunfinished(repo):
    '''Check for unfinished operations (as above), and clear the ones
    that are clearable.
    '''
    for f, clearable, allowcommit, msg, hint in unfinishedstates:
        if not clearable and repo.vfs.exists(f):
            raise error.Abort(msg, hint=hint)
    for f, clearable, allowcommit, msg, hint in unfinishedstates:
        if clearable and repo.vfs.exists(f):
            util.unlink(repo.vfs.join(f))

afterresolvedstates = [
    ('graftstate',
     _('hg graft --continue')),
    ]

def howtocontinue(repo):
    '''Check for an unfinished operation and return the command to finish
    it.

    afterresolvedstates tuples define a .hg/{file} and the corresponding
    command needed to finish it.

    Returns a (msg, warning) tuple. 'msg' is a string and 'warning' is
    a boolean.
    '''
    contmsg = _("continue: %s")
    for f, msg in afterresolvedstates:
        if repo.vfs.exists(f):
            return contmsg % msg, True
    if repo[None].dirty(missing=True, merge=False, branch=False):
        return contmsg % _("hg commit"), False
    return None, None

def checkafterresolved(repo):
    '''Inform the user about the next action after completing hg resolve

    If there's a matching afterresolvedstates, howtocontinue will yield
    repo.ui.warn as the reporter.

    Otherwise, it will yield repo.ui.note.
    '''
    msg, warning = howtocontinue(repo)
    if msg is not None:
        if warning:
            repo.ui.warn("%s\n" % msg)
        else:
            repo.ui.note("%s\n" % msg)

def wrongtooltocontinue(repo, task):
    '''Raise an abort suggesting how to properly continue if there is an
    active task.

    Uses howtocontinue() to find the active task.

    If there's no task (repo.ui.note for 'hg commit'), it does not offer
    a hint.
    '''
    after = howtocontinue(repo)
    hint = None
    if after[1]:
        hint = after[0]
    raise error.Abort(_('no %s in progress') % task, hint=hint)
