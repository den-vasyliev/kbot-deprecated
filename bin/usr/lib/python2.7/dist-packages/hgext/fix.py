# fix - rewrite file content in changesets and working copy
#
# Copyright 2018 Google LLC.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.
"""rewrite file content in changesets or working copy (EXPERIMENTAL)

Provides a command that runs configured tools on the contents of modified files,
writing back any fixes to the working copy or replacing changesets.

Here is an example configuration that causes :hg:`fix` to apply automatic
formatting fixes to modified lines in C++ code::

  [fix]
  clang-format:command=clang-format --assume-filename={rootpath}
  clang-format:linerange=--lines={first}:{last}
  clang-format:fileset=set:**.cpp or **.hpp

The :command suboption forms the first part of the shell command that will be
used to fix a file. The content of the file is passed on standard input, and the
fixed file content is expected on standard output. If there is any output on
standard error, the file will not be affected. Some values may be substituted
into the command::

  {rootpath}  The path of the file being fixed, relative to the repo root
  {basename}  The name of the file being fixed, without the directory path

If the :linerange suboption is set, the tool will only be run if there are
changed lines in a file. The value of this suboption is appended to the shell
command once for every range of changed lines in the file. Some values may be
substituted into the command::

  {first}   The 1-based line number of the first line in the modified range
  {last}    The 1-based line number of the last line in the modified range

The :fileset suboption determines which files will be passed through each
configured tool. See :hg:`help fileset` for possible values. If there are file
arguments to :hg:`fix`, the intersection of these filesets is used.

There is also a configurable limit for the maximum size of file that will be
processed by :hg:`fix`::

  [fix]
  maxfilesize=2MB

"""

from __future__ import absolute_import

import collections
import itertools
import os
import re
import subprocess

from mercurial.i18n import _
from mercurial.node import nullrev
from mercurial.node import wdirrev

from mercurial.utils import (
    procutil,
)

from mercurial import (
    cmdutil,
    context,
    copies,
    error,
    mdiff,
    merge,
    obsolete,
    pycompat,
    registrar,
    scmutil,
    util,
    worker,
)

# Note for extension authors: ONLY specify testedwith = 'ships-with-hg-core' for
# extensions which SHIP WITH MERCURIAL. Non-mainline extensions should
# be specifying the version(s) of Mercurial they are tested with, or
# leave the attribute unspecified.
testedwith = 'ships-with-hg-core'

cmdtable = {}
command = registrar.command(cmdtable)

configtable = {}
configitem = registrar.configitem(configtable)

# Register the suboptions allowed for each configured fixer.
FIXER_ATTRS = ('command', 'linerange', 'fileset')

for key in FIXER_ATTRS:
    configitem('fix', '.*(:%s)?' % key, default=None, generic=True)

# A good default size allows most source code files to be fixed, but avoids
# letting fixer tools choke on huge inputs, which could be surprising to the
# user.
configitem('fix', 'maxfilesize', default='2MB')

allopt = ('', 'all', False, _('fix all non-public non-obsolete revisions'))
baseopt = ('', 'base', [], _('revisions to diff against (overrides automatic '
                             'selection, and applies to every revision being '
                             'fixed)'), _('REV'))
revopt = ('r', 'rev', [], _('revisions to fix'), _('REV'))
wdiropt = ('w', 'working-dir', False, _('fix the working directory'))
wholeopt = ('', 'whole', False, _('always fix every line of a file'))
usage = _('[OPTION]... [FILE]...')

@command('fix', [allopt, baseopt, revopt, wdiropt, wholeopt], usage,
        helpcategory=command.CATEGORY_FILE_CONTENTS)
def fix(ui, repo, *pats, **opts):
    """rewrite file content in changesets or working directory

    Runs any configured tools to fix the content of files. Only affects files
    with changes, unless file arguments are provided. Only affects changed lines
    of files, unless the --whole flag is used. Some tools may always affect the
    whole file regardless of --whole.

    If revisions are specified with --rev, those revisions will be checked, and
    they may be replaced with new revisions that have fixed file content.  It is
    desirable to specify all descendants of each specified revision, so that the
    fixes propagate to the descendants. If all descendants are fixed at the same
    time, no merging, rebasing, or evolution will be required.

    If --working-dir is used, files with uncommitted changes in the working copy
    will be fixed. If the checked-out revision is also fixed, the working
    directory will update to the replacement revision.

    When determining what lines of each file to fix at each revision, the whole
    set of revisions being fixed is considered, so that fixes to earlier
    revisions are not forgotten in later ones. The --base flag can be used to
    override this default behavior, though it is not usually desirable to do so.
    """
    opts = pycompat.byteskwargs(opts)
    if opts['all']:
        if opts['rev']:
            raise error.Abort(_('cannot specify both "--rev" and "--all"'))
        opts['rev'] = ['not public() and not obsolete()']
        opts['working_dir'] = True
    with repo.wlock(), repo.lock(), repo.transaction('fix'):
        revstofix = getrevstofix(ui, repo, opts)
        basectxs = getbasectxs(repo, opts, revstofix)
        workqueue, numitems = getworkqueue(ui, repo, pats, opts, revstofix,
                                           basectxs)
        fixers = getfixers(ui)

        # There are no data dependencies between the workers fixing each file
        # revision, so we can use all available parallelism.
        def getfixes(items):
            for rev, path in items:
                ctx = repo[rev]
                olddata = ctx[path].data()
                newdata = fixfile(ui, opts, fixers, ctx, path, basectxs[rev])
                # Don't waste memory/time passing unchanged content back, but
                # produce one result per item either way.
                yield (rev, path, newdata if newdata != olddata else None)
        results = worker.worker(ui, 1.0, getfixes, tuple(), workqueue,
                                threadsafe=False)

        # We have to hold on to the data for each successor revision in memory
        # until all its parents are committed. We ensure this by committing and
        # freeing memory for the revisions in some topological order. This
        # leaves a little bit of memory efficiency on the table, but also makes
        # the tests deterministic. It might also be considered a feature since
        # it makes the results more easily reproducible.
        filedata = collections.defaultdict(dict)
        replacements = {}
        wdirwritten = False
        commitorder = sorted(revstofix, reverse=True)
        with ui.makeprogress(topic=_('fixing'), unit=_('files'),
                             total=sum(numitems.values())) as progress:
            for rev, path, newdata in results:
                progress.increment(item=path)
                if newdata is not None:
                    filedata[rev][path] = newdata
                numitems[rev] -= 1
                # Apply the fixes for this and any other revisions that are
                # ready and sitting at the front of the queue. Using a loop here
                # prevents the queue from being blocked by the first revision to
                # be ready out of order.
                while commitorder and not numitems[commitorder[-1]]:
                    rev = commitorder.pop()
                    ctx = repo[rev]
                    if rev == wdirrev:
                        writeworkingdir(repo, ctx, filedata[rev], replacements)
                        wdirwritten = bool(filedata[rev])
                    else:
                        replacerev(ui, repo, ctx, filedata[rev], replacements)
                    del filedata[rev]

        cleanup(repo, replacements, wdirwritten)

def cleanup(repo, replacements, wdirwritten):
    """Calls scmutil.cleanupnodes() with the given replacements.

    "replacements" is a dict from nodeid to nodeid, with one key and one value
    for every revision that was affected by fixing. This is slightly different
    from cleanupnodes().

    "wdirwritten" is a bool which tells whether the working copy was affected by
    fixing, since it has no entry in "replacements".

    Useful as a hook point for extending "hg fix" with output summarizing the
    effects of the command, though we choose not to output anything here.
    """
    replacements = {prec: [succ] for prec, succ in replacements.iteritems()}
    scmutil.cleanupnodes(repo, replacements, 'fix', fixphase=True)

def getworkqueue(ui, repo, pats, opts, revstofix, basectxs):
    """"Constructs the list of files to be fixed at specific revisions

    It is up to the caller how to consume the work items, and the only
    dependence between them is that replacement revisions must be committed in
    topological order. Each work item represents a file in the working copy or
    in some revision that should be fixed and written back to the working copy
    or into a replacement revision.

    Work items for the same revision are grouped together, so that a worker
    pool starting with the first N items in parallel is likely to finish the
    first revision's work before other revisions. This can allow us to write
    the result to disk and reduce memory footprint. At time of writing, the
    partition strategy in worker.py seems favorable to this. We also sort the
    items by ascending revision number to match the order in which we commit
    the fixes later.
    """
    workqueue = []
    numitems = collections.defaultdict(int)
    maxfilesize = ui.configbytes('fix', 'maxfilesize')
    for rev in sorted(revstofix):
        fixctx = repo[rev]
        match = scmutil.match(fixctx, pats, opts)
        for path in pathstofix(ui, repo, pats, opts, match, basectxs[rev],
                               fixctx):
            if path not in fixctx:
                continue
            fctx = fixctx[path]
            if fctx.islink():
                continue
            if fctx.size() > maxfilesize:
                ui.warn(_('ignoring file larger than %s: %s\n') %
                        (util.bytecount(maxfilesize), path))
                continue
            workqueue.append((rev, path))
            numitems[rev] += 1
    return workqueue, numitems

def getrevstofix(ui, repo, opts):
    """Returns the set of revision numbers that should be fixed"""
    revs = set(scmutil.revrange(repo, opts['rev']))
    for rev in revs:
        checkfixablectx(ui, repo, repo[rev])
    if revs:
        cmdutil.checkunfinished(repo)
        checknodescendants(repo, revs)
    if opts.get('working_dir'):
        revs.add(wdirrev)
        if list(merge.mergestate.read(repo).unresolved()):
            raise error.Abort('unresolved conflicts', hint="use 'hg resolve'")
    if not revs:
        raise error.Abort(
            'no changesets specified', hint='use --rev or --working-dir')
    return revs

def checknodescendants(repo, revs):
    if (not obsolete.isenabled(repo, obsolete.allowunstableopt) and
        repo.revs('(%ld::) - (%ld)', revs, revs)):
        raise error.Abort(_('can only fix a changeset together '
                            'with all its descendants'))

def checkfixablectx(ui, repo, ctx):
    """Aborts if the revision shouldn't be replaced with a fixed one."""
    if not ctx.mutable():
        raise error.Abort('can\'t fix immutable changeset %s' %
                          (scmutil.formatchangeid(ctx),))
    if ctx.obsolete():
        # It would be better to actually check if the revision has a successor.
        allowdivergence = ui.configbool('experimental',
                                        'evolution.allowdivergence')
        if not allowdivergence:
            raise error.Abort('fixing obsolete revision could cause divergence')

def pathstofix(ui, repo, pats, opts, match, basectxs, fixctx):
    """Returns the set of files that should be fixed in a context

    The result depends on the base contexts; we include any file that has
    changed relative to any of the base contexts. Base contexts should be
    ancestors of the context being fixed.
    """
    files = set()
    for basectx in basectxs:
        stat = basectx.status(fixctx, match=match, listclean=bool(pats),
                              listunknown=bool(pats))
        files.update(
            set(itertools.chain(stat.added, stat.modified, stat.clean,
                                stat.unknown)))
    return files

def lineranges(opts, path, basectxs, fixctx, content2):
    """Returns the set of line ranges that should be fixed in a file

    Of the form [(10, 20), (30, 40)].

    This depends on the given base contexts; we must consider lines that have
    changed versus any of the base contexts, and whether the file has been
    renamed versus any of them.

    Another way to understand this is that we exclude line ranges that are
    common to the file in all base contexts.
    """
    if opts.get('whole'):
        # Return a range containing all lines. Rely on the diff implementation's
        # idea of how many lines are in the file, instead of reimplementing it.
        return difflineranges('', content2)

    rangeslist = []
    for basectx in basectxs:
        basepath = copies.pathcopies(basectx, fixctx).get(path, path)
        if basepath in basectx:
            content1 = basectx[basepath].data()
        else:
            content1 = ''
        rangeslist.extend(difflineranges(content1, content2))
    return unionranges(rangeslist)

def unionranges(rangeslist):
    """Return the union of some closed intervals

    >>> unionranges([])
    []
    >>> unionranges([(1, 100)])
    [(1, 100)]
    >>> unionranges([(1, 100), (1, 100)])
    [(1, 100)]
    >>> unionranges([(1, 100), (2, 100)])
    [(1, 100)]
    >>> unionranges([(1, 99), (1, 100)])
    [(1, 100)]
    >>> unionranges([(1, 100), (40, 60)])
    [(1, 100)]
    >>> unionranges([(1, 49), (50, 100)])
    [(1, 100)]
    >>> unionranges([(1, 48), (50, 100)])
    [(1, 48), (50, 100)]
    >>> unionranges([(1, 2), (3, 4), (5, 6)])
    [(1, 6)]
    """
    rangeslist = sorted(set(rangeslist))
    unioned = []
    if rangeslist:
        unioned, rangeslist = [rangeslist[0]], rangeslist[1:]
    for a, b in rangeslist:
        c, d = unioned[-1]
        if a > d + 1:
            unioned.append((a, b))
        else:
            unioned[-1] = (c, max(b, d))
    return unioned

def difflineranges(content1, content2):
    """Return list of line number ranges in content2 that differ from content1.

    Line numbers are 1-based. The numbers are the first and last line contained
    in the range. Single-line ranges have the same line number for the first and
    last line. Excludes any empty ranges that result from lines that are only
    present in content1. Relies on mdiff's idea of where the line endings are in
    the string.

    >>> from mercurial import pycompat
    >>> lines = lambda s: b'\\n'.join([c for c in pycompat.iterbytestr(s)])
    >>> difflineranges2 = lambda a, b: difflineranges(lines(a), lines(b))
    >>> difflineranges2(b'', b'')
    []
    >>> difflineranges2(b'a', b'')
    []
    >>> difflineranges2(b'', b'A')
    [(1, 1)]
    >>> difflineranges2(b'a', b'a')
    []
    >>> difflineranges2(b'a', b'A')
    [(1, 1)]
    >>> difflineranges2(b'ab', b'')
    []
    >>> difflineranges2(b'', b'AB')
    [(1, 2)]
    >>> difflineranges2(b'abc', b'ac')
    []
    >>> difflineranges2(b'ab', b'aCb')
    [(2, 2)]
    >>> difflineranges2(b'abc', b'aBc')
    [(2, 2)]
    >>> difflineranges2(b'ab', b'AB')
    [(1, 2)]
    >>> difflineranges2(b'abcde', b'aBcDe')
    [(2, 2), (4, 4)]
    >>> difflineranges2(b'abcde', b'aBCDe')
    [(2, 4)]
    """
    ranges = []
    for lines, kind in mdiff.allblocks(content1, content2):
        firstline, lastline = lines[2:4]
        if kind == '!' and firstline != lastline:
            ranges.append((firstline + 1, lastline))
    return ranges

def getbasectxs(repo, opts, revstofix):
    """Returns a map of the base contexts for each revision

    The base contexts determine which lines are considered modified when we
    attempt to fix just the modified lines in a file. It also determines which
    files we attempt to fix, so it is important to compute this even when
    --whole is used.
    """
    # The --base flag overrides the usual logic, and we give every revision
    # exactly the set of baserevs that the user specified.
    if opts.get('base'):
        baserevs = set(scmutil.revrange(repo, opts.get('base')))
        if not baserevs:
            baserevs = {nullrev}
        basectxs = {repo[rev] for rev in baserevs}
        return {rev: basectxs for rev in revstofix}

    # Proceed in topological order so that we can easily determine each
    # revision's baserevs by looking at its parents and their baserevs.
    basectxs = collections.defaultdict(set)
    for rev in sorted(revstofix):
        ctx = repo[rev]
        for pctx in ctx.parents():
            if pctx.rev() in basectxs:
                basectxs[rev].update(basectxs[pctx.rev()])
            else:
                basectxs[rev].add(pctx)
    return basectxs

def fixfile(ui, opts, fixers, fixctx, path, basectxs):
    """Run any configured fixers that should affect the file in this context

    Returns the file content that results from applying the fixers in some order
    starting with the file's content in the fixctx. Fixers that support line
    ranges will affect lines that have changed relative to any of the basectxs
    (i.e. they will only avoid lines that are common to all basectxs).

    A fixer tool's stdout will become the file's new content if and only if it
    exits with code zero.
    """
    newdata = fixctx[path].data()
    for fixername, fixer in fixers.iteritems():
        if fixer.affects(opts, fixctx, path):
            rangesfn = lambda: lineranges(opts, path, basectxs, fixctx, newdata)
            command = fixer.command(ui, path, rangesfn)
            if command is None:
                continue
            ui.debug('subprocess: %s\n' % (command,))
            proc = subprocess.Popen(
                procutil.tonativestr(command),
                shell=True,
                cwd=procutil.tonativestr(b'/'),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            newerdata, stderr = proc.communicate(newdata)
            if stderr:
                showstderr(ui, fixctx.rev(), fixername, stderr)
            if proc.returncode == 0:
                newdata = newerdata
            elif not stderr:
                showstderr(ui, fixctx.rev(), fixername,
                           _('exited with status %d\n') % (proc.returncode,))
    return newdata

def showstderr(ui, rev, fixername, stderr):
    """Writes the lines of the stderr string as warnings on the ui

    Uses the revision number and fixername to give more context to each line of
    the error message. Doesn't include file names, since those take up a lot of
    space and would tend to be included in the error message if they were
    relevant.
    """
    for line in re.split('[\r\n]+', stderr):
        if line:
            ui.warn(('['))
            if rev is None:
                ui.warn(_('wdir'), label='evolve.rev')
            else:
                ui.warn((str(rev)), label='evolve.rev')
            ui.warn(('] %s: %s\n') % (fixername, line))

def writeworkingdir(repo, ctx, filedata, replacements):
    """Write new content to the working copy and check out the new p1 if any

    We check out a new revision if and only if we fixed something in both the
    working directory and its parent revision. This avoids the need for a full
    update/merge, and means that the working directory simply isn't affected
    unless the --working-dir flag is given.

    Directly updates the dirstate for the affected files.
    """
    for path, data in filedata.iteritems():
        fctx = ctx[path]
        fctx.write(data, fctx.flags())
        if repo.dirstate[path] == 'n':
            repo.dirstate.normallookup(path)

    oldparentnodes = repo.dirstate.parents()
    newparentnodes = [replacements.get(n, n) for n in oldparentnodes]
    if newparentnodes != oldparentnodes:
        repo.setparents(*newparentnodes)

def replacerev(ui, repo, ctx, filedata, replacements):
    """Commit a new revision like the given one, but with file content changes

    "ctx" is the original revision to be replaced by a modified one.

    "filedata" is a dict that maps paths to their new file content. All other
    paths will be recreated from the original revision without changes.
    "filedata" may contain paths that didn't exist in the original revision;
    they will be added.

    "replacements" is a dict that maps a single node to a single node, and it is
    updated to indicate the original revision is replaced by the newly created
    one. No entry is added if the replacement's node already exists.

    The new revision has the same parents as the old one, unless those parents
    have already been replaced, in which case those replacements are the parents
    of this new revision. Thus, if revisions are replaced in topological order,
    there is no need to rebase them into the original topology later.
    """

    p1rev, p2rev = repo.changelog.parentrevs(ctx.rev())
    p1ctx, p2ctx = repo[p1rev], repo[p2rev]
    newp1node = replacements.get(p1ctx.node(), p1ctx.node())
    newp2node = replacements.get(p2ctx.node(), p2ctx.node())

    def filectxfn(repo, memctx, path):
        if path not in ctx:
            return None
        fctx = ctx[path]
        copied = fctx.renamed()
        if copied:
            copied = copied[0]
        return context.memfilectx(
            repo,
            memctx,
            path=fctx.path(),
            data=filedata.get(path, fctx.data()),
            islink=fctx.islink(),
            isexec=fctx.isexec(),
            copied=copied)

    memctx = context.memctx(
        repo,
        parents=(newp1node, newp2node),
        text=ctx.description(),
        files=set(ctx.files()) | set(filedata.keys()),
        filectxfn=filectxfn,
        user=ctx.user(),
        date=ctx.date(),
        extra=ctx.extra(),
        branch=ctx.branch(),
        editor=None)
    sucnode = memctx.commit()
    prenode = ctx.node()
    if prenode == sucnode:
        ui.debug('node %s already existed\n' % (ctx.hex()))
    else:
        replacements[ctx.node()] = sucnode

def getfixers(ui):
    """Returns a map of configured fixer tools indexed by their names

    Each value is a Fixer object with methods that implement the behavior of the
    fixer's config suboptions. Does not validate the config values.
    """
    result = {}
    for name in fixernames(ui):
        result[name] = Fixer()
        attrs = ui.configsuboptions('fix', name)[1]
        for key in FIXER_ATTRS:
            setattr(result[name], pycompat.sysstr('_' + key),
                    attrs.get(key, ''))
    return result

def fixernames(ui):
    """Returns the names of [fix] config options that have suboptions"""
    names = set()
    for k, v in ui.configitems('fix'):
        if ':' in k:
            names.add(k.split(':', 1)[0])
    return names

class Fixer(object):
    """Wraps the raw config values for a fixer with methods"""

    def affects(self, opts, fixctx, path):
        """Should this fixer run on the file at the given path and context?"""
        return scmutil.match(fixctx, [self._fileset], opts)(path)

    def command(self, ui, path, rangesfn):
        """A shell command to use to invoke this fixer on the given file/lines

        May return None if there is no appropriate command to run for the given
        parameters.
        """
        expand = cmdutil.rendercommandtemplate
        parts = [expand(ui, self._command,
                        {'rootpath': path, 'basename': os.path.basename(path)})]
        if self._linerange:
            ranges = rangesfn()
            if not ranges:
                # No line ranges to fix, so don't run the fixer.
                return None
            for first, last in ranges:
                parts.append(expand(ui, self._linerange,
                                    {'first': first, 'last': last}))
        return ' '.join(parts)
