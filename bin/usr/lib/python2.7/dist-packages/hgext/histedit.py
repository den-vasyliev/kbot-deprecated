# histedit.py - interactive history editing for mercurial
#
# Copyright 2009 Augie Fackler <raf@durin42.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.
"""interactive history editing

With this extension installed, Mercurial gains one new command: histedit. Usage
is as follows, assuming the following history::

 @  3[tip]   7c2fd3b9020c   2009-04-27 18:04 -0500   durin42
 |    Add delta
 |
 o  2   030b686bedc4   2009-04-27 18:04 -0500   durin42
 |    Add gamma
 |
 o  1   c561b4e977df   2009-04-27 18:04 -0500   durin42
 |    Add beta
 |
 o  0   d8d2fcd0e319   2009-04-27 18:04 -0500   durin42
      Add alpha

If you were to run ``hg histedit c561b4e977df``, you would see the following
file open in your editor::

 pick c561b4e977df Add beta
 pick 030b686bedc4 Add gamma
 pick 7c2fd3b9020c Add delta

 # Edit history between c561b4e977df and 7c2fd3b9020c
 #
 # Commits are listed from least to most recent
 #
 # Commands:
 #  p, pick = use commit
 #  e, edit = use commit, but stop for amending
 #  f, fold = use commit, but combine it with the one above
 #  r, roll = like fold, but discard this commit's description and date
 #  d, drop = remove commit from history
 #  m, mess = edit commit message without changing commit content
 #  b, base = checkout changeset and apply further changesets from there
 #

In this file, lines beginning with ``#`` are ignored. You must specify a rule
for each revision in your history. For example, if you had meant to add gamma
before beta, and then wanted to add delta in the same revision as beta, you
would reorganize the file to look like this::

 pick 030b686bedc4 Add gamma
 pick c561b4e977df Add beta
 fold 7c2fd3b9020c Add delta

 # Edit history between c561b4e977df and 7c2fd3b9020c
 #
 # Commits are listed from least to most recent
 #
 # Commands:
 #  p, pick = use commit
 #  e, edit = use commit, but stop for amending
 #  f, fold = use commit, but combine it with the one above
 #  r, roll = like fold, but discard this commit's description and date
 #  d, drop = remove commit from history
 #  m, mess = edit commit message without changing commit content
 #  b, base = checkout changeset and apply further changesets from there
 #

At which point you close the editor and ``histedit`` starts working. When you
specify a ``fold`` operation, ``histedit`` will open an editor when it folds
those revisions together, offering you a chance to clean up the commit message::

 Add beta
 ***
 Add delta

Edit the commit message to your liking, then close the editor. The date used
for the commit will be the later of the two commits' dates. For this example,
let's assume that the commit message was changed to ``Add beta and delta.``
After histedit has run and had a chance to remove any old or temporary
revisions it needed, the history looks like this::

 @  2[tip]   989b4d060121   2009-04-27 18:04 -0500   durin42
 |    Add beta and delta.
 |
 o  1   081603921c3f   2009-04-27 18:04 -0500   durin42
 |    Add gamma
 |
 o  0   d8d2fcd0e319   2009-04-27 18:04 -0500   durin42
      Add alpha

Note that ``histedit`` does *not* remove any revisions (even its own temporary
ones) until after it has completed all the editing operations, so it will
probably perform several strip operations when it's done. For the above example,
it had to run strip twice. Strip can be slow depending on a variety of factors,
so you might need to be a little patient. You can choose to keep the original
revisions by passing the ``--keep`` flag.

The ``edit`` operation will drop you back to a command prompt,
allowing you to edit files freely, or even use ``hg record`` to commit
some changes as a separate commit. When you're done, any remaining
uncommitted changes will be committed as well. When done, run ``hg
histedit --continue`` to finish this step. If there are uncommitted
changes, you'll be prompted for a new commit message, but the default
commit message will be the original message for the ``edit`` ed
revision, and the date of the original commit will be preserved.

The ``message`` operation will give you a chance to revise a commit
message without changing the contents. It's a shortcut for doing
``edit`` immediately followed by `hg histedit --continue``.

If ``histedit`` encounters a conflict when moving a revision (while
handling ``pick`` or ``fold``), it'll stop in a similar manner to
``edit`` with the difference that it won't prompt you for a commit
message when done. If you decide at this point that you don't like how
much work it will be to rearrange history, or that you made a mistake,
you can use ``hg histedit --abort`` to abandon the new changes you
have made and return to the state before you attempted to edit your
history.

If we clone the histedit-ed example repository above and add four more
changes, such that we have the following history::

   @  6[tip]   038383181893   2009-04-27 18:04 -0500   stefan
   |    Add theta
   |
   o  5   140988835471   2009-04-27 18:04 -0500   stefan
   |    Add eta
   |
   o  4   122930637314   2009-04-27 18:04 -0500   stefan
   |    Add zeta
   |
   o  3   836302820282   2009-04-27 18:04 -0500   stefan
   |    Add epsilon
   |
   o  2   989b4d060121   2009-04-27 18:04 -0500   durin42
   |    Add beta and delta.
   |
   o  1   081603921c3f   2009-04-27 18:04 -0500   durin42
   |    Add gamma
   |
   o  0   d8d2fcd0e319   2009-04-27 18:04 -0500   durin42
        Add alpha

If you run ``hg histedit --outgoing`` on the clone then it is the same
as running ``hg histedit 836302820282``. If you need plan to push to a
repository that Mercurial does not detect to be related to the source
repo, you can add a ``--force`` option.

Config
------

Histedit rule lines are truncated to 80 characters by default. You
can customize this behavior by setting a different length in your
configuration file::

  [histedit]
  linelen = 120      # truncate rule lines at 120 characters

``hg histedit`` attempts to automatically choose an appropriate base
revision to use. To change which base revision is used, define a
revset in your configuration file::

  [histedit]
  defaultrev = only(.) & draft()

By default each edited revision needs to be present in histedit commands.
To remove revision you need to use ``drop`` operation. You can configure
the drop to be implicit for missing commits by adding::

  [histedit]
  dropmissing = True

By default, histedit will close the transaction after each action. For
performance purposes, you can configure histedit to use a single transaction
across the entire histedit. WARNING: This setting introduces a significant risk
of losing the work you've done in a histedit if the histedit aborts
unexpectedly::

  [histedit]
  singletransaction = True

"""

from __future__ import absolute_import

import os

from mercurial.i18n import _
from mercurial import (
    bundle2,
    cmdutil,
    context,
    copies,
    destutil,
    discovery,
    error,
    exchange,
    extensions,
    hg,
    lock,
    merge as mergemod,
    mergeutil,
    node,
    obsolete,
    pycompat,
    registrar,
    repair,
    scmutil,
    state as statemod,
    util,
)
from mercurial.utils import (
    stringutil,
)

pickle = util.pickle
release = lock.release
cmdtable = {}
command = registrar.command(cmdtable)

configtable = {}
configitem = registrar.configitem(configtable)
configitem('experimental', 'histedit.autoverb',
    default=False,
)
configitem('histedit', 'defaultrev',
    default=None,
)
configitem('histedit', 'dropmissing',
    default=False,
)
configitem('histedit', 'linelen',
    default=80,
)
configitem('histedit', 'singletransaction',
    default=False,
)

# Note for extension authors: ONLY specify testedwith = 'ships-with-hg-core' for
# extensions which SHIP WITH MERCURIAL. Non-mainline extensions should
# be specifying the version(s) of Mercurial they are tested with, or
# leave the attribute unspecified.
testedwith = 'ships-with-hg-core'

actiontable = {}
primaryactions = set()
secondaryactions = set()
tertiaryactions = set()
internalactions = set()

def geteditcomment(ui, first, last):
    """ construct the editor comment
    The comment includes::
     - an intro
     - sorted primary commands
     - sorted short commands
     - sorted long commands
     - additional hints

    Commands are only included once.
    """
    intro = _("""Edit history between %s and %s

Commits are listed from least to most recent

You can reorder changesets by reordering the lines

Commands:
""")
    actions = []
    def addverb(v):
        a = actiontable[v]
        lines = a.message.split("\n")
        if len(a.verbs):
            v = ', '.join(sorted(a.verbs, key=lambda v: len(v)))
        actions.append(" %s = %s" % (v, lines[0]))
        actions.extend(['  %s' for l in lines[1:]])

    for v in (
         sorted(primaryactions) +
         sorted(secondaryactions) +
         sorted(tertiaryactions)
        ):
        addverb(v)
    actions.append('')

    hints = []
    if ui.configbool('histedit', 'dropmissing'):
        hints.append("Deleting a changeset from the list "
                     "will DISCARD it from the edited history!")

    lines = (intro % (first, last)).split('\n') + actions + hints

    return ''.join(['# %s\n' % l if l else '#\n' for l in lines])

class histeditstate(object):
    def __init__(self, repo, parentctxnode=None, actions=None, keep=None,
            topmost=None, replacements=None, lock=None, wlock=None):
        self.repo = repo
        self.actions = actions
        self.keep = keep
        self.topmost = topmost
        self.parentctxnode = parentctxnode
        self.lock = lock
        self.wlock = wlock
        self.backupfile = None
        self.stateobj = statemod.cmdstate(repo, 'histedit-state')
        if replacements is None:
            self.replacements = []
        else:
            self.replacements = replacements

    def read(self):
        """Load histedit state from disk and set fields appropriately."""
        if not self.stateobj.exists():
            cmdutil.wrongtooltocontinue(self.repo, _('histedit'))

        data = self._read()

        self.parentctxnode = data['parentctxnode']
        actions = parserules(data['rules'], self)
        self.actions = actions
        self.keep = data['keep']
        self.topmost = data['topmost']
        self.replacements = data['replacements']
        self.backupfile = data['backupfile']

    def _read(self):
        fp = self.repo.vfs.read('histedit-state')
        if fp.startswith('v1\n'):
            data = self._load()
            parentctxnode, rules, keep, topmost, replacements, backupfile = data
        else:
            data = pickle.loads(fp)
            parentctxnode, rules, keep, topmost, replacements = data
            backupfile = None
        rules = "\n".join(["%s %s" % (verb, rest) for [verb, rest] in rules])

        return {'parentctxnode': parentctxnode, "rules": rules, "keep": keep,
                "topmost": topmost, "replacements": replacements,
                "backupfile": backupfile}

    def write(self, tr=None):
        if tr:
            tr.addfilegenerator('histedit-state', ('histedit-state',),
                                self._write, location='plain')
        else:
            with self.repo.vfs("histedit-state", "w") as f:
                self._write(f)

    def _write(self, fp):
        fp.write('v1\n')
        fp.write('%s\n' % node.hex(self.parentctxnode))
        fp.write('%s\n' % node.hex(self.topmost))
        fp.write('%s\n' % ('True' if self.keep else 'False'))
        fp.write('%d\n' % len(self.actions))
        for action in self.actions:
            fp.write('%s\n' % action.tostate())
        fp.write('%d\n' % len(self.replacements))
        for replacement in self.replacements:
            fp.write('%s%s\n' % (node.hex(replacement[0]), ''.join(node.hex(r)
                for r in replacement[1])))
        backupfile = self.backupfile
        if not backupfile:
            backupfile = ''
        fp.write('%s\n' % backupfile)

    def _load(self):
        fp = self.repo.vfs('histedit-state', 'r')
        lines = [l[:-1] for l in fp.readlines()]

        index = 0
        lines[index] # version number
        index += 1

        parentctxnode = node.bin(lines[index])
        index += 1

        topmost = node.bin(lines[index])
        index += 1

        keep = lines[index] == 'True'
        index += 1

        # Rules
        rules = []
        rulelen = int(lines[index])
        index += 1
        for i in pycompat.xrange(rulelen):
            ruleaction = lines[index]
            index += 1
            rule = lines[index]
            index += 1
            rules.append((ruleaction, rule))

        # Replacements
        replacements = []
        replacementlen = int(lines[index])
        index += 1
        for i in pycompat.xrange(replacementlen):
            replacement = lines[index]
            original = node.bin(replacement[:40])
            succ = [node.bin(replacement[i:i + 40]) for i in
                    range(40, len(replacement), 40)]
            replacements.append((original, succ))
            index += 1

        backupfile = lines[index]
        index += 1

        fp.close()

        return parentctxnode, rules, keep, topmost, replacements, backupfile

    def clear(self):
        if self.inprogress():
            self.repo.vfs.unlink('histedit-state')

    def inprogress(self):
        return self.repo.vfs.exists('histedit-state')


class histeditaction(object):
    def __init__(self, state, node):
        self.state = state
        self.repo = state.repo
        self.node = node

    @classmethod
    def fromrule(cls, state, rule):
        """Parses the given rule, returning an instance of the histeditaction.
        """
        ruleid = rule.strip().split(' ', 1)[0]
        # ruleid can be anything from rev numbers, hashes, "bookmarks" etc
        # Check for validation of rule ids and get the rulehash
        try:
            rev = node.bin(ruleid)
        except TypeError:
            try:
                _ctx = scmutil.revsingle(state.repo, ruleid)
                rulehash = _ctx.hex()
                rev = node.bin(rulehash)
            except error.RepoLookupError:
                raise error.ParseError(_("invalid changeset %s") % ruleid)
        return cls(state, rev)

    def verify(self, prev, expected, seen):
        """ Verifies semantic correctness of the rule"""
        repo = self.repo
        ha = node.hex(self.node)
        self.node = scmutil.resolvehexnodeidprefix(repo, ha)
        if self.node is None:
            raise error.ParseError(_('unknown changeset %s listed') % ha[:12])
        self._verifynodeconstraints(prev, expected, seen)

    def _verifynodeconstraints(self, prev, expected, seen):
        # by default command need a node in the edited list
        if self.node not in expected:
            raise error.ParseError(_('%s "%s" changeset was not a candidate')
                                   % (self.verb, node.short(self.node)),
                                   hint=_('only use listed changesets'))
        # and only one command per node
        if self.node in seen:
            raise error.ParseError(_('duplicated command for changeset %s') %
                                   node.short(self.node))

    def torule(self):
        """build a histedit rule line for an action

        by default lines are in the form:
        <hash> <rev> <summary>
        """
        ctx = self.repo[self.node]
        summary = _getsummary(ctx)
        line = '%s %s %d %s' % (self.verb, ctx, ctx.rev(), summary)
        # trim to 75 columns by default so it's not stupidly wide in my editor
        # (the 5 more are left for verb)
        maxlen = self.repo.ui.configint('histedit', 'linelen')
        maxlen = max(maxlen, 22) # avoid truncating hash
        return stringutil.ellipsis(line, maxlen)

    def tostate(self):
        """Print an action in format used by histedit state files
           (the first line is a verb, the remainder is the second)
        """
        return "%s\n%s" % (self.verb, node.hex(self.node))

    def run(self):
        """Runs the action. The default behavior is simply apply the action's
        rulectx onto the current parentctx."""
        self.applychange()
        self.continuedirty()
        return self.continueclean()

    def applychange(self):
        """Applies the changes from this action's rulectx onto the current
        parentctx, but does not commit them."""
        repo = self.repo
        rulectx = repo[self.node]
        repo.ui.pushbuffer(error=True, labeled=True)
        hg.update(repo, self.state.parentctxnode, quietempty=True)
        stats = applychanges(repo.ui, repo, rulectx, {})
        repo.dirstate.setbranch(rulectx.branch())
        if stats.unresolvedcount:
            buf = repo.ui.popbuffer()
            repo.ui.write(buf)
            raise error.InterventionRequired(
                _('Fix up the change (%s %s)') %
                (self.verb, node.short(self.node)),
                hint=_('hg histedit --continue to resume'))
        else:
            repo.ui.popbuffer()

    def continuedirty(self):
        """Continues the action when changes have been applied to the working
        copy. The default behavior is to commit the dirty changes."""
        repo = self.repo
        rulectx = repo[self.node]

        editor = self.commiteditor()
        commit = commitfuncfor(repo, rulectx)

        commit(text=rulectx.description(), user=rulectx.user(),
               date=rulectx.date(), extra=rulectx.extra(), editor=editor)

    def commiteditor(self):
        """The editor to be used to edit the commit message."""
        return False

    def continueclean(self):
        """Continues the action when the working copy is clean. The default
        behavior is to accept the current commit as the new version of the
        rulectx."""
        ctx = self.repo['.']
        if ctx.node() == self.state.parentctxnode:
            self.repo.ui.warn(_('%s: skipping changeset (no changes)\n') %
                              node.short(self.node))
            return ctx, [(self.node, tuple())]
        if ctx.node() == self.node:
            # Nothing changed
            return ctx, []
        return ctx, [(self.node, (ctx.node(),))]

def commitfuncfor(repo, src):
    """Build a commit function for the replacement of <src>

    This function ensure we apply the same treatment to all changesets.

    - Add a 'histedit_source' entry in extra.

    Note that fold has its own separated logic because its handling is a bit
    different and not easily factored out of the fold method.
    """
    phasemin = src.phase()
    def commitfunc(**kwargs):
        overrides = {('phases', 'new-commit'): phasemin}
        with repo.ui.configoverride(overrides, 'histedit'):
            extra = kwargs.get(r'extra', {}).copy()
            extra['histedit_source'] = src.hex()
            kwargs[r'extra'] = extra
            return repo.commit(**kwargs)
    return commitfunc

def applychanges(ui, repo, ctx, opts):
    """Merge changeset from ctx (only) in the current working directory"""
    wcpar = repo.dirstate.parents()[0]
    if ctx.p1().node() == wcpar:
        # edits are "in place" we do not need to make any merge,
        # just applies changes on parent for editing
        cmdutil.revert(ui, repo, ctx, (wcpar, node.nullid), all=True)
        stats = mergemod.updateresult(0, 0, 0, 0)
    else:
        try:
            # ui.forcemerge is an internal variable, do not document
            repo.ui.setconfig('ui', 'forcemerge', opts.get('tool', ''),
                              'histedit')
            stats = mergemod.graft(repo, ctx, ctx.p1(), ['local', 'histedit'])
        finally:
            repo.ui.setconfig('ui', 'forcemerge', '', 'histedit')
    return stats

def collapse(repo, firstctx, lastctx, commitopts, skipprompt=False):
    """collapse the set of revisions from first to last as new one.

    Expected commit options are:
        - message
        - date
        - username
    Commit message is edited in all cases.

    This function works in memory."""
    ctxs = list(repo.set('%d::%d', firstctx.rev(), lastctx.rev()))
    if not ctxs:
        return None
    for c in ctxs:
        if not c.mutable():
            raise error.ParseError(
                _("cannot fold into public change %s") % node.short(c.node()))
    base = firstctx.parents()[0]

    # commit a new version of the old changeset, including the update
    # collect all files which might be affected
    files = set()
    for ctx in ctxs:
        files.update(ctx.files())

    # Recompute copies (avoid recording a -> b -> a)
    copied = copies.pathcopies(base, lastctx)

    # prune files which were reverted by the updates
    files = [f for f in files if not cmdutil.samefile(f, lastctx, base)]
    # commit version of these files as defined by head
    headmf = lastctx.manifest()
    def filectxfn(repo, ctx, path):
        if path in headmf:
            fctx = lastctx[path]
            flags = fctx.flags()
            mctx = context.memfilectx(repo, ctx,
                                      fctx.path(), fctx.data(),
                                      islink='l' in flags,
                                      isexec='x' in flags,
                                      copied=copied.get(path))
            return mctx
        return None

    if commitopts.get('message'):
        message = commitopts['message']
    else:
        message = firstctx.description()
    user = commitopts.get('user')
    date = commitopts.get('date')
    extra = commitopts.get('extra')

    parents = (firstctx.p1().node(), firstctx.p2().node())
    editor = None
    if not skipprompt:
        editor = cmdutil.getcommiteditor(edit=True, editform='histedit.fold')
    new = context.memctx(repo,
                         parents=parents,
                         text=message,
                         files=files,
                         filectxfn=filectxfn,
                         user=user,
                         date=date,
                         extra=extra,
                         editor=editor)
    return repo.commitctx(new)

def _isdirtywc(repo):
    return repo[None].dirty(missing=True)

def abortdirty():
    raise error.Abort(_('working copy has pending changes'),
        hint=_('amend, commit, or revert them and run histedit '
            '--continue, or abort with histedit --abort'))

def action(verbs, message, priority=False, internal=False):
    def wrap(cls):
        assert not priority or not internal
        verb = verbs[0]
        if priority:
            primaryactions.add(verb)
        elif internal:
            internalactions.add(verb)
        elif len(verbs) > 1:
            secondaryactions.add(verb)
        else:
            tertiaryactions.add(verb)

        cls.verb = verb
        cls.verbs = verbs
        cls.message = message
        for verb in verbs:
            actiontable[verb] = cls
        return cls
    return wrap

@action(['pick', 'p'],
        _('use commit'),
        priority=True)
class pick(histeditaction):
    def run(self):
        rulectx = self.repo[self.node]
        if rulectx.parents()[0].node() == self.state.parentctxnode:
            self.repo.ui.debug('node %s unchanged\n' % node.short(self.node))
            return rulectx, []

        return super(pick, self).run()

@action(['edit', 'e'],
        _('use commit, but stop for amending'),
        priority=True)
class edit(histeditaction):
    def run(self):
        repo = self.repo
        rulectx = repo[self.node]
        hg.update(repo, self.state.parentctxnode, quietempty=True)
        applychanges(repo.ui, repo, rulectx, {})
        raise error.InterventionRequired(
            _('Editing (%s), you may commit or record as needed now.')
            % node.short(self.node),
            hint=_('hg histedit --continue to resume'))

    def commiteditor(self):
        return cmdutil.getcommiteditor(edit=True, editform='histedit.edit')

@action(['fold', 'f'],
        _('use commit, but combine it with the one above'))
class fold(histeditaction):
    def verify(self, prev, expected, seen):
        """ Verifies semantic correctness of the fold rule"""
        super(fold, self).verify(prev, expected, seen)
        repo = self.repo
        if not prev:
            c = repo[self.node].parents()[0]
        elif not prev.verb in ('pick', 'base'):
            return
        else:
            c = repo[prev.node]
        if not c.mutable():
            raise error.ParseError(
                _("cannot fold into public change %s") % node.short(c.node()))


    def continuedirty(self):
        repo = self.repo
        rulectx = repo[self.node]

        commit = commitfuncfor(repo, rulectx)
        commit(text='fold-temp-revision %s' % node.short(self.node),
               user=rulectx.user(), date=rulectx.date(),
               extra=rulectx.extra())

    def continueclean(self):
        repo = self.repo
        ctx = repo['.']
        rulectx = repo[self.node]
        parentctxnode = self.state.parentctxnode
        if ctx.node() == parentctxnode:
            repo.ui.warn(_('%s: empty changeset\n') %
                              node.short(self.node))
            return ctx, [(self.node, (parentctxnode,))]

        parentctx = repo[parentctxnode]
        newcommits = set(c.node() for c in repo.set('(%d::. - %d)',
                                                    parentctx.rev(),
                                                    parentctx.rev()))
        if not newcommits:
            repo.ui.warn(_('%s: cannot fold - working copy is not a '
                           'descendant of previous commit %s\n') %
                           (node.short(self.node), node.short(parentctxnode)))
            return ctx, [(self.node, (ctx.node(),))]

        middlecommits = newcommits.copy()
        middlecommits.discard(ctx.node())

        return self.finishfold(repo.ui, repo, parentctx, rulectx, ctx.node(),
                               middlecommits)

    def skipprompt(self):
        """Returns true if the rule should skip the message editor.

        For example, 'fold' wants to show an editor, but 'rollup'
        doesn't want to.
        """
        return False

    def mergedescs(self):
        """Returns true if the rule should merge messages of multiple changes.

        This exists mainly so that 'rollup' rules can be a subclass of
        'fold'.
        """
        return True

    def firstdate(self):
        """Returns true if the rule should preserve the date of the first
        change.

        This exists mainly so that 'rollup' rules can be a subclass of
        'fold'.
        """
        return False

    def finishfold(self, ui, repo, ctx, oldctx, newnode, internalchanges):
        parent = ctx.parents()[0].node()
        hg.updaterepo(repo, parent, overwrite=False)
        ### prepare new commit data
        commitopts = {}
        commitopts['user'] = ctx.user()
        # commit message
        if not self.mergedescs():
            newmessage = ctx.description()
        else:
            newmessage = '\n***\n'.join(
                [ctx.description()] +
                [repo[r].description() for r in internalchanges] +
                [oldctx.description()]) + '\n'
        commitopts['message'] = newmessage
        # date
        if self.firstdate():
            commitopts['date'] = ctx.date()
        else:
            commitopts['date'] = max(ctx.date(), oldctx.date())
        extra = ctx.extra().copy()
        # histedit_source
        # note: ctx is likely a temporary commit but that the best we can do
        #       here. This is sufficient to solve issue3681 anyway.
        extra['histedit_source'] = '%s,%s' % (ctx.hex(), oldctx.hex())
        commitopts['extra'] = extra
        phasemin = max(ctx.phase(), oldctx.phase())
        overrides = {('phases', 'new-commit'): phasemin}
        with repo.ui.configoverride(overrides, 'histedit'):
            n = collapse(repo, ctx, repo[newnode], commitopts,
                         skipprompt=self.skipprompt())
        if n is None:
            return ctx, []
        hg.updaterepo(repo, n, overwrite=False)
        replacements = [(oldctx.node(), (newnode,)),
                        (ctx.node(), (n,)),
                        (newnode, (n,)),
                       ]
        for ich in internalchanges:
            replacements.append((ich, (n,)))
        return repo[n], replacements

@action(['base', 'b'],
        _('checkout changeset and apply further changesets from there'))
class base(histeditaction):

    def run(self):
        if self.repo['.'].node() != self.node:
            mergemod.update(self.repo, self.node, branchmerge=False, force=True)
        return self.continueclean()

    def continuedirty(self):
        abortdirty()

    def continueclean(self):
        basectx = self.repo['.']
        return basectx, []

    def _verifynodeconstraints(self, prev, expected, seen):
        # base can only be use with a node not in the edited set
        if self.node in expected:
            msg = _('%s "%s" changeset was an edited list candidate')
            raise error.ParseError(
                msg % (self.verb, node.short(self.node)),
                hint=_('base must only use unlisted changesets'))

@action(['_multifold'],
        _(
    """fold subclass used for when multiple folds happen in a row

    We only want to fire the editor for the folded message once when
    (say) four changes are folded down into a single change. This is
    similar to rollup, but we should preserve both messages so that
    when the last fold operation runs we can show the user all the
    commit messages in their editor.
    """),
        internal=True)
class _multifold(fold):
    def skipprompt(self):
        return True

@action(["roll", "r"],
        _("like fold, but discard this commit's description and date"))
class rollup(fold):
    def mergedescs(self):
        return False

    def skipprompt(self):
        return True

    def firstdate(self):
        return True

@action(["drop", "d"],
        _('remove commit from history'))
class drop(histeditaction):
    def run(self):
        parentctx = self.repo[self.state.parentctxnode]
        return parentctx, [(self.node, tuple())]

@action(["mess", "m"],
        _('edit commit message without changing commit content'),
        priority=True)
class message(histeditaction):
    def commiteditor(self):
        return cmdutil.getcommiteditor(edit=True, editform='histedit.mess')

def findoutgoing(ui, repo, remote=None, force=False, opts=None):
    """utility function to find the first outgoing changeset

    Used by initialization code"""
    if opts is None:
        opts = {}
    dest = ui.expandpath(remote or 'default-push', remote or 'default')
    dest, branches = hg.parseurl(dest, None)[:2]
    ui.status(_('comparing with %s\n') % util.hidepassword(dest))

    revs, checkout = hg.addbranchrevs(repo, repo, branches, None)
    other = hg.peer(repo, opts, dest)

    if revs:
        revs = [repo.lookup(rev) for rev in revs]

    outgoing = discovery.findcommonoutgoing(repo, other, revs, force=force)
    if not outgoing.missing:
        raise error.Abort(_('no outgoing ancestors'))
    roots = list(repo.revs("roots(%ln)", outgoing.missing))
    if len(roots) > 1:
        msg = _('there are ambiguous outgoing revisions')
        hint = _("see 'hg help histedit' for more detail")
        raise error.Abort(msg, hint=hint)
    return repo[roots[0]].node()

@command('histedit',
    [('', 'commands', '',
      _('read history edits from the specified file'), _('FILE')),
     ('c', 'continue', False, _('continue an edit already in progress')),
     ('', 'edit-plan', False, _('edit remaining actions list')),
     ('k', 'keep', False,
      _("don't strip old nodes after edit is complete")),
     ('', 'abort', False, _('abort an edit in progress')),
     ('o', 'outgoing', False, _('changesets not found in destination')),
     ('f', 'force', False,
      _('force outgoing even for unrelated repositories')),
     ('r', 'rev', [], _('first revision to be edited'), _('REV'))] +
    cmdutil.formatteropts,
     _("[OPTIONS] ([ANCESTOR] | --outgoing [URL])"),
    helpcategory=command.CATEGORY_CHANGE_MANAGEMENT)
def histedit(ui, repo, *freeargs, **opts):
    """interactively edit changeset history

    This command lets you edit a linear series of changesets (up to
    and including the working directory, which should be clean).
    You can:

    - `pick` to [re]order a changeset

    - `drop` to omit changeset

    - `mess` to reword the changeset commit message

    - `fold` to combine it with the preceding changeset (using the later date)

    - `roll` like fold, but discarding this commit's description and date

    - `edit` to edit this changeset (preserving date)

    - `base` to checkout changeset and apply further changesets from there

    There are a number of ways to select the root changeset:

    - Specify ANCESTOR directly

    - Use --outgoing -- it will be the first linear changeset not
      included in destination. (See :hg:`help config.paths.default-push`)

    - Otherwise, the value from the "histedit.defaultrev" config option
      is used as a revset to select the base revision when ANCESTOR is not
      specified. The first revision returned by the revset is used. By
      default, this selects the editable history that is unique to the
      ancestry of the working directory.

    .. container:: verbose

       If you use --outgoing, this command will abort if there are ambiguous
       outgoing revisions. For example, if there are multiple branches
       containing outgoing revisions.

       Use "min(outgoing() and ::.)" or similar revset specification
       instead of --outgoing to specify edit target revision exactly in
       such ambiguous situation. See :hg:`help revsets` for detail about
       selecting revisions.

    .. container:: verbose

       Examples:

         - A number of changes have been made.
           Revision 3 is no longer needed.

           Start history editing from revision 3::

             hg histedit -r 3

           An editor opens, containing the list of revisions,
           with specific actions specified::

             pick 5339bf82f0ca 3 Zworgle the foobar
             pick 8ef592ce7cc4 4 Bedazzle the zerlog
             pick 0a9639fcda9d 5 Morgify the cromulancy

           Additional information about the possible actions
           to take appears below the list of revisions.

           To remove revision 3 from the history,
           its action (at the beginning of the relevant line)
           is changed to 'drop'::

             drop 5339bf82f0ca 3 Zworgle the foobar
             pick 8ef592ce7cc4 4 Bedazzle the zerlog
             pick 0a9639fcda9d 5 Morgify the cromulancy

         - A number of changes have been made.
           Revision 2 and 4 need to be swapped.

           Start history editing from revision 2::

             hg histedit -r 2

           An editor opens, containing the list of revisions,
           with specific actions specified::

             pick 252a1af424ad 2 Blorb a morgwazzle
             pick 5339bf82f0ca 3 Zworgle the foobar
             pick 8ef592ce7cc4 4 Bedazzle the zerlog

           To swap revision 2 and 4, its lines are swapped
           in the editor::

             pick 8ef592ce7cc4 4 Bedazzle the zerlog
             pick 5339bf82f0ca 3 Zworgle the foobar
             pick 252a1af424ad 2 Blorb a morgwazzle

    Returns 0 on success, 1 if user intervention is required (not only
    for intentional "edit" command, but also for resolving unexpected
    conflicts).
    """
    state = histeditstate(repo)
    try:
        state.wlock = repo.wlock()
        state.lock = repo.lock()
        _histedit(ui, repo, state, *freeargs, **opts)
    finally:
        release(state.lock, state.wlock)

goalcontinue = 'continue'
goalabort = 'abort'
goaleditplan = 'edit-plan'
goalnew = 'new'

def _getgoal(opts):
    if opts.get('continue'):
        return goalcontinue
    if opts.get('abort'):
        return goalabort
    if opts.get('edit_plan'):
        return goaleditplan
    return goalnew

def _readfile(ui, path):
    if path == '-':
        with ui.timeblockedsection('histedit'):
            return ui.fin.read()
    else:
        with open(path, 'rb') as f:
            return f.read()

def _validateargs(ui, repo, state, freeargs, opts, goal, rules, revs):
    # TODO only abort if we try to histedit mq patches, not just
    # blanket if mq patches are applied somewhere
    mq = getattr(repo, 'mq', None)
    if mq and mq.applied:
        raise error.Abort(_('source has mq patches applied'))

    # basic argument incompatibility processing
    outg = opts.get('outgoing')
    editplan = opts.get('edit_plan')
    abort = opts.get('abort')
    force = opts.get('force')
    if force and not outg:
        raise error.Abort(_('--force only allowed with --outgoing'))
    if goal == 'continue':
        if any((outg, abort, revs, freeargs, rules, editplan)):
            raise error.Abort(_('no arguments allowed with --continue'))
    elif goal == 'abort':
        if any((outg, revs, freeargs, rules, editplan)):
            raise error.Abort(_('no arguments allowed with --abort'))
    elif goal == 'edit-plan':
        if any((outg, revs, freeargs)):
            raise error.Abort(_('only --commands argument allowed with '
                               '--edit-plan'))
    else:
        if state.inprogress():
            raise error.Abort(_('history edit already in progress, try '
                               '--continue or --abort'))
        if outg:
            if revs:
                raise error.Abort(_('no revisions allowed with --outgoing'))
            if len(freeargs) > 1:
                raise error.Abort(
                    _('only one repo argument allowed with --outgoing'))
        else:
            revs.extend(freeargs)
            if len(revs) == 0:
                defaultrev = destutil.desthistedit(ui, repo)
                if defaultrev is not None:
                    revs.append(defaultrev)

            if len(revs) != 1:
                raise error.Abort(
                    _('histedit requires exactly one ancestor revision'))

def _histedit(ui, repo, state, *freeargs, **opts):
    opts = pycompat.byteskwargs(opts)
    fm = ui.formatter('histedit', opts)
    fm.startitem()
    goal = _getgoal(opts)
    revs = opts.get('rev', [])
    # experimental config: ui.history-editing-backup
    nobackup = not ui.configbool('ui', 'history-editing-backup')
    rules = opts.get('commands', '')
    state.keep = opts.get('keep', False)

    _validateargs(ui, repo, state, freeargs, opts, goal, rules, revs)

    # rebuild state
    if goal == goalcontinue:
        state.read()
        state = bootstrapcontinue(ui, state, opts)
    elif goal == goaleditplan:
        _edithisteditplan(ui, repo, state, rules)
        return
    elif goal == goalabort:
        _aborthistedit(ui, repo, state, nobackup=nobackup)
        return
    else:
        # goal == goalnew
        _newhistedit(ui, repo, state, revs, freeargs, opts)

    _continuehistedit(ui, repo, state)
    _finishhistedit(ui, repo, state, fm)
    fm.end()

def _continuehistedit(ui, repo, state):
    """This function runs after either:
    - bootstrapcontinue (if the goal is 'continue')
    - _newhistedit (if the goal is 'new')
    """
    # preprocess rules so that we can hide inner folds from the user
    # and only show one editor
    actions = state.actions[:]
    for idx, (action, nextact) in enumerate(
            zip(actions, actions[1:] + [None])):
        if action.verb == 'fold' and nextact and nextact.verb == 'fold':
            state.actions[idx].__class__ = _multifold

    # Force an initial state file write, so the user can run --abort/continue
    # even if there's an exception before the first transaction serialize.
    state.write()

    tr = None
    # Don't use singletransaction by default since it rolls the entire
    # transaction back if an unexpected exception happens (like a
    # pretxncommit hook throws, or the user aborts the commit msg editor).
    if ui.configbool("histedit", "singletransaction"):
        # Don't use a 'with' for the transaction, since actions may close
        # and reopen a transaction. For example, if the action executes an
        # external process it may choose to commit the transaction first.
        tr = repo.transaction('histedit')
    progress = ui.makeprogress(_("editing"), unit=_('changes'),
                               total=len(state.actions))
    with progress, util.acceptintervention(tr):
        while state.actions:
            state.write(tr=tr)
            actobj = state.actions[0]
            progress.increment(item=actobj.torule())
            ui.debug('histedit: processing %s %s\n' % (actobj.verb,\
                                                       actobj.torule()))
            parentctx, replacement_ = actobj.run()
            state.parentctxnode = parentctx.node()
            state.replacements.extend(replacement_)
            state.actions.pop(0)

    state.write()

def _finishhistedit(ui, repo, state, fm):
    """This action runs when histedit is finishing its session"""
    hg.updaterepo(repo, state.parentctxnode, overwrite=False)

    mapping, tmpnodes, created, ntm = processreplacement(state)
    if mapping:
        for prec, succs in mapping.iteritems():
            if not succs:
                ui.debug('histedit: %s is dropped\n' % node.short(prec))
            else:
                ui.debug('histedit: %s is replaced by %s\n' % (
                    node.short(prec), node.short(succs[0])))
                if len(succs) > 1:
                    m = 'histedit:                            %s'
                    for n in succs[1:]:
                        ui.debug(m % node.short(n))

    if not state.keep:
        if mapping:
            movetopmostbookmarks(repo, state.topmost, ntm)
            # TODO update mq state
    else:
        mapping = {}

    for n in tmpnodes:
        if n in repo:
            mapping[n] = ()

    # remove entries about unknown nodes
    nodemap = repo.unfiltered().changelog.nodemap
    mapping = {k: v for k, v in mapping.items()
               if k in nodemap and all(n in nodemap for n in v)}
    scmutil.cleanupnodes(repo, mapping, 'histedit')
    hf = fm.hexfunc
    fl = fm.formatlist
    fd = fm.formatdict
    nodechanges = fd({hf(oldn): fl([hf(n) for n in newn], name='node')
                      for oldn, newn in mapping.iteritems()},
                     key="oldnode", value="newnodes")
    fm.data(nodechanges=nodechanges)

    state.clear()
    if os.path.exists(repo.sjoin('undo')):
        os.unlink(repo.sjoin('undo'))
    if repo.vfs.exists('histedit-last-edit.txt'):
        repo.vfs.unlink('histedit-last-edit.txt')

def _aborthistedit(ui, repo, state, nobackup=False):
    try:
        state.read()
        __, leafs, tmpnodes, __ = processreplacement(state)
        ui.debug('restore wc to old parent %s\n'
                % node.short(state.topmost))

        # Recover our old commits if necessary
        if not state.topmost in repo and state.backupfile:
            backupfile = repo.vfs.join(state.backupfile)
            f = hg.openpath(ui, backupfile)
            gen = exchange.readbundle(ui, f, backupfile)
            with repo.transaction('histedit.abort') as tr:
                bundle2.applybundle(repo, gen, tr, source='histedit',
                                    url='bundle:' + backupfile)

            os.remove(backupfile)

        # check whether we should update away
        if repo.unfiltered().revs('parents() and (%n  or %ln::)',
                                state.parentctxnode, leafs | tmpnodes):
            hg.clean(repo, state.topmost, show_stats=True, quietempty=True)
        cleanupnode(ui, repo, tmpnodes, nobackup=nobackup)
        cleanupnode(ui, repo, leafs, nobackup=nobackup)
    except Exception:
        if state.inprogress():
            ui.warn(_('warning: encountered an exception during histedit '
                '--abort; the repository may not have been completely '
                'cleaned up\n'))
        raise
    finally:
            state.clear()

def _edithisteditplan(ui, repo, state, rules):
    state.read()
    if not rules:
        comment = geteditcomment(ui,
                                 node.short(state.parentctxnode),
                                 node.short(state.topmost))
        rules = ruleeditor(repo, ui, state.actions, comment)
    else:
        rules = _readfile(ui, rules)
    actions = parserules(rules, state)
    ctxs = [repo[act.node] \
            for act in state.actions if act.node]
    warnverifyactions(ui, repo, actions, state, ctxs)
    state.actions = actions
    state.write()

def _newhistedit(ui, repo, state, revs, freeargs, opts):
    outg = opts.get('outgoing')
    rules = opts.get('commands', '')
    force = opts.get('force')

    cmdutil.checkunfinished(repo)
    cmdutil.bailifchanged(repo)

    topmost, empty = repo.dirstate.parents()
    if outg:
        if freeargs:
            remote = freeargs[0]
        else:
            remote = None
        root = findoutgoing(ui, repo, remote, force, opts)
    else:
        rr = list(repo.set('roots(%ld)', scmutil.revrange(repo, revs)))
        if len(rr) != 1:
            raise error.Abort(_('The specified revisions must have '
                'exactly one common root'))
        root = rr[0].node()

    revs = between(repo, root, topmost, state.keep)
    if not revs:
        raise error.Abort(_('%s is not an ancestor of working directory') %
                         node.short(root))

    ctxs = [repo[r] for r in revs]
    if not rules:
        comment = geteditcomment(ui, node.short(root), node.short(topmost))
        actions = [pick(state, r) for r in revs]
        rules = ruleeditor(repo, ui, actions, comment)
    else:
        rules = _readfile(ui, rules)
    actions = parserules(rules, state)
    warnverifyactions(ui, repo, actions, state, ctxs)

    parentctxnode = repo[root].parents()[0].node()

    state.parentctxnode = parentctxnode
    state.actions = actions
    state.topmost = topmost
    state.replacements = []

    ui.log("histedit", "%d actions to histedit", len(actions),
           histedit_num_actions=len(actions))

    # Create a backup so we can always abort completely.
    backupfile = None
    if not obsolete.isenabled(repo, obsolete.createmarkersopt):
        backupfile = repair.backupbundle(repo, [parentctxnode],
                                         [topmost], root, 'histedit')
    state.backupfile = backupfile

def _getsummary(ctx):
    # a common pattern is to extract the summary but default to the empty
    # string
    summary = ctx.description() or ''
    if summary:
        summary = summary.splitlines()[0]
    return summary

def bootstrapcontinue(ui, state, opts):
    repo = state.repo

    ms = mergemod.mergestate.read(repo)
    mergeutil.checkunresolved(ms)

    if state.actions:
        actobj = state.actions.pop(0)

        if _isdirtywc(repo):
            actobj.continuedirty()
            if _isdirtywc(repo):
                abortdirty()

        parentctx, replacements = actobj.continueclean()

        state.parentctxnode = parentctx.node()
        state.replacements.extend(replacements)

    return state

def between(repo, old, new, keep):
    """select and validate the set of revision to edit

    When keep is false, the specified set can't have children."""
    revs = repo.revs('%n::%n', old, new)
    if revs and not keep:
        if (not obsolete.isenabled(repo, obsolete.allowunstableopt) and
            repo.revs('(%ld::) - (%ld)', revs, revs)):
            raise error.Abort(_('can only histedit a changeset together '
                                'with all its descendants'))
        if repo.revs('(%ld) and merge()', revs):
            raise error.Abort(_('cannot edit history that contains merges'))
        root = repo[revs.first()]  # list is already sorted by repo.revs()
        if not root.mutable():
            raise error.Abort(_('cannot edit public changeset: %s') % root,
                             hint=_("see 'hg help phases' for details"))
    return pycompat.maplist(repo.changelog.node, revs)

def ruleeditor(repo, ui, actions, editcomment=""):
    """open an editor to edit rules

    rules are in the format [ [act, ctx], ...] like in state.rules
    """
    if repo.ui.configbool("experimental", "histedit.autoverb"):
        newact = util.sortdict()
        for act in actions:
            ctx = repo[act.node]
            summary = _getsummary(ctx)
            fword = summary.split(' ', 1)[0].lower()
            added = False

            # if it doesn't end with the special character '!' just skip this
            if fword.endswith('!'):
                fword = fword[:-1]
                if fword in primaryactions | secondaryactions | tertiaryactions:
                    act.verb = fword
                    # get the target summary
                    tsum = summary[len(fword) + 1:].lstrip()
                    # safe but slow: reverse iterate over the actions so we
                    # don't clash on two commits having the same summary
                    for na, l in reversed(list(newact.iteritems())):
                        actx = repo[na.node]
                        asum = _getsummary(actx)
                        if asum == tsum:
                            added = True
                            l.append(act)
                            break

            if not added:
                newact[act] = []

        # copy over and flatten the new list
        actions = []
        for na, l in newact.iteritems():
            actions.append(na)
            actions += l

    rules = '\n'.join([act.torule() for act in actions])
    rules += '\n\n'
    rules += editcomment
    rules = ui.edit(rules, ui.username(), {'prefix': 'histedit'},
                    repopath=repo.path, action='histedit')

    # Save edit rules in .hg/histedit-last-edit.txt in case
    # the user needs to ask for help after something
    # surprising happens.
    with repo.vfs('histedit-last-edit.txt', 'wb') as f:
        f.write(rules)

    return rules

def parserules(rules, state):
    """Read the histedit rules string and return list of action objects """
    rules = [l for l in (r.strip() for r in rules.splitlines())
                if l and not l.startswith('#')]
    actions = []
    for r in rules:
        if ' ' not in r:
            raise error.ParseError(_('malformed line "%s"') % r)
        verb, rest = r.split(' ', 1)

        if verb not in actiontable:
            raise error.ParseError(_('unknown action "%s"') % verb)

        action = actiontable[verb].fromrule(state, rest)
        actions.append(action)
    return actions

def warnverifyactions(ui, repo, actions, state, ctxs):
    try:
        verifyactions(actions, state, ctxs)
    except error.ParseError:
        if repo.vfs.exists('histedit-last-edit.txt'):
            ui.warn(_('warning: histedit rules saved '
                      'to: .hg/histedit-last-edit.txt\n'))
        raise

def verifyactions(actions, state, ctxs):
    """Verify that there exists exactly one action per given changeset and
    other constraints.

    Will abort if there are to many or too few rules, a malformed rule,
    or a rule on a changeset outside of the user-given range.
    """
    expected = set(c.node() for c in ctxs)
    seen = set()
    prev = None

    if actions and actions[0].verb in ['roll', 'fold']:
        raise error.ParseError(_('first changeset cannot use verb "%s"') %
                               actions[0].verb)

    for action in actions:
        action.verify(prev, expected, seen)
        prev = action
        if action.node is not None:
            seen.add(action.node)
    missing = sorted(expected - seen)  # sort to stabilize output

    if state.repo.ui.configbool('histedit', 'dropmissing'):
        if len(actions) == 0:
            raise error.ParseError(_('no rules provided'),
                    hint=_('use strip extension to remove commits'))

        drops = [drop(state, n) for n in missing]
        # put the in the beginning so they execute immediately and
        # don't show in the edit-plan in the future
        actions[:0] = drops
    elif missing:
        raise error.ParseError(_('missing rules for changeset %s') %
                node.short(missing[0]),
                hint=_('use "drop %s" to discard, see also: '
                       "'hg help -e histedit.config'")
                       % node.short(missing[0]))

def adjustreplacementsfrommarkers(repo, oldreplacements):
    """Adjust replacements from obsolescence markers

    Replacements structure is originally generated based on
    histedit's state and does not account for changes that are
    not recorded there. This function fixes that by adding
    data read from obsolescence markers"""
    if not obsolete.isenabled(repo, obsolete.createmarkersopt):
        return oldreplacements

    unfi = repo.unfiltered()
    nm = unfi.changelog.nodemap
    obsstore = repo.obsstore
    newreplacements = list(oldreplacements)
    oldsuccs = [r[1] for r in oldreplacements]
    # successors that have already been added to succstocheck once
    seensuccs = set().union(*oldsuccs) # create a set from an iterable of tuples
    succstocheck = list(seensuccs)
    while succstocheck:
        n = succstocheck.pop()
        missing = nm.get(n) is None
        markers = obsstore.successors.get(n, ())
        if missing and not markers:
            # dead end, mark it as such
            newreplacements.append((n, ()))
        for marker in markers:
            nsuccs = marker[1]
            newreplacements.append((n, nsuccs))
            for nsucc in nsuccs:
                if nsucc not in seensuccs:
                    seensuccs.add(nsucc)
                    succstocheck.append(nsucc)

    return newreplacements

def processreplacement(state):
    """process the list of replacements to return

    1) the final mapping between original and created nodes
    2) the list of temporary node created by histedit
    3) the list of new commit created by histedit"""
    replacements = adjustreplacementsfrommarkers(state.repo, state.replacements)
    allsuccs = set()
    replaced = set()
    fullmapping = {}
    # initialize basic set
    # fullmapping records all operations recorded in replacement
    for rep in replacements:
        allsuccs.update(rep[1])
        replaced.add(rep[0])
        fullmapping.setdefault(rep[0], set()).update(rep[1])
    new = allsuccs - replaced
    tmpnodes = allsuccs & replaced
    # Reduce content fullmapping into direct relation between original nodes
    # and final node created during history edition
    # Dropped changeset are replaced by an empty list
    toproceed = set(fullmapping)
    final = {}
    while toproceed:
        for x in list(toproceed):
            succs = fullmapping[x]
            for s in list(succs):
                if s in toproceed:
                    # non final node with unknown closure
                    # We can't process this now
                    break
                elif s in final:
                    # non final node, replace with closure
                    succs.remove(s)
                    succs.update(final[s])
            else:
                final[x] = succs
                toproceed.remove(x)
    # remove tmpnodes from final mapping
    for n in tmpnodes:
        del final[n]
    # we expect all changes involved in final to exist in the repo
    # turn `final` into list (topologically sorted)
    nm = state.repo.changelog.nodemap
    for prec, succs in final.items():
        final[prec] = sorted(succs, key=nm.get)

    # computed topmost element (necessary for bookmark)
    if new:
        newtopmost = sorted(new, key=state.repo.changelog.rev)[-1]
    elif not final:
        # Nothing rewritten at all. we won't need `newtopmost`
        # It is the same as `oldtopmost` and `processreplacement` know it
        newtopmost = None
    else:
        # every body died. The newtopmost is the parent of the root.
        r = state.repo.changelog.rev
        newtopmost = state.repo[sorted(final, key=r)[0]].p1().node()

    return final, tmpnodes, new, newtopmost

def movetopmostbookmarks(repo, oldtopmost, newtopmost):
    """Move bookmark from oldtopmost to newly created topmost

    This is arguably a feature and we may only want that for the active
    bookmark. But the behavior is kept compatible with the old version for now.
    """
    if not oldtopmost or not newtopmost:
        return
    oldbmarks = repo.nodebookmarks(oldtopmost)
    if oldbmarks:
        with repo.lock(), repo.transaction('histedit') as tr:
            marks = repo._bookmarks
            changes = []
            for name in oldbmarks:
                changes.append((name, newtopmost))
            marks.applychanges(repo, tr, changes)

def cleanupnode(ui, repo, nodes, nobackup=False):
    """strip a group of nodes from the repository

    The set of node to strip may contains unknown nodes."""
    with repo.lock():
        # do not let filtering get in the way of the cleanse
        # we should probably get rid of obsolescence marker created during the
        # histedit, but we currently do not have such information.
        repo = repo.unfiltered()
        # Find all nodes that need to be stripped
        # (we use %lr instead of %ln to silently ignore unknown items)
        nm = repo.changelog.nodemap
        nodes = sorted(n for n in nodes if n in nm)
        roots = [c.node() for c in repo.set("roots(%ln)", nodes)]
        if roots:
            backup = not nobackup
            repair.strip(ui, repo, roots, backup=backup)

def stripwrapper(orig, ui, repo, nodelist, *args, **kwargs):
    if isinstance(nodelist, str):
        nodelist = [nodelist]
    state = histeditstate(repo)
    if state.inprogress():
        state.read()
        histedit_nodes = {action.node for action
                          in state.actions if action.node}
        common_nodes = histedit_nodes & set(nodelist)
        if common_nodes:
            raise error.Abort(_("histedit in progress, can't strip %s")
                             % ', '.join(node.short(x) for x in common_nodes))
    return orig(ui, repo, nodelist, *args, **kwargs)

extensions.wrapfunction(repair, 'strip', stripwrapper)

def summaryhook(ui, repo):
    state = histeditstate(repo)
    if not state.inprogress():
        return
    state.read()
    if state.actions:
        # i18n: column positioning for "hg summary"
        ui.write(_('hist:   %s (histedit --continue)\n') %
                 (ui.label(_('%d remaining'), 'histedit.remaining') %
                  len(state.actions)))

def extsetup(ui):
    cmdutil.summaryhooks.add('histedit', summaryhook)
    cmdutil.unfinishedstates.append(
        ['histedit-state', False, True, _('histedit in progress'),
         _("use 'hg histedit --continue' or 'hg histedit --abort'")])
    cmdutil.afterresolvedstates.append(
        ['histedit-state', _('hg histedit --continue')])
