# churn.py - create a graph of revisions count grouped by template
#
# Copyright 2006 Josef "Jeff" Sipek <jeffpc@josefsipek.net>
# Copyright 2008 Alexander Solovyov <piranha@piranha.org.ua>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

'''command to display statistics about repository history'''

from __future__ import absolute_import, division

import datetime
import os
import time

from mercurial.i18n import _
from mercurial import (
    cmdutil,
    encoding,
    logcmdutil,
    patch,
    pycompat,
    registrar,
    scmutil,
)
from mercurial.utils import dateutil

cmdtable = {}
command = registrar.command(cmdtable)
# Note for extension authors: ONLY specify testedwith = 'ships-with-hg-core' for
# extensions which SHIP WITH MERCURIAL. Non-mainline extensions should
# be specifying the version(s) of Mercurial they are tested with, or
# leave the attribute unspecified.
testedwith = 'ships-with-hg-core'

def changedlines(ui, repo, ctx1, ctx2, fns):
    added, removed = 0, 0
    fmatch = scmutil.matchfiles(repo, fns)
    diff = ''.join(patch.diff(repo, ctx1.node(), ctx2.node(), fmatch))
    for l in diff.split('\n'):
        if l.startswith("+") and not l.startswith("+++ "):
            added += 1
        elif l.startswith("-") and not l.startswith("--- "):
            removed += 1
    return (added, removed)

def countrate(ui, repo, amap, *pats, **opts):
    """Calculate stats"""
    opts = pycompat.byteskwargs(opts)
    if opts.get('dateformat'):
        def getkey(ctx):
            t, tz = ctx.date()
            date = datetime.datetime(*time.gmtime(float(t) - tz)[:6])
            return encoding.strtolocal(
                date.strftime(encoding.strfromlocal(opts['dateformat'])))
    else:
        tmpl = opts.get('oldtemplate') or opts.get('template')
        tmpl = logcmdutil.maketemplater(ui, repo, tmpl)
        def getkey(ctx):
            ui.pushbuffer()
            tmpl.show(ctx)
            return ui.popbuffer()

    progress = ui.makeprogress(_('analyzing'), unit=_('revisions'),
                               total=len(repo))
    rate = {}
    df = False
    if opts.get('date'):
        df = dateutil.matchdate(opts['date'])

    m = scmutil.match(repo[None], pats, opts)
    def prep(ctx, fns):
        rev = ctx.rev()
        if df and not df(ctx.date()[0]): # doesn't match date format
            return

        key = getkey(ctx).strip()
        key = amap.get(key, key) # alias remap
        if opts.get('changesets'):
            rate[key] = (rate.get(key, (0,))[0] + 1, 0)
        else:
            parents = ctx.parents()
            if len(parents) > 1:
                ui.note(_('revision %d is a merge, ignoring...\n') % (rev,))
                return

            ctx1 = parents[0]
            lines = changedlines(ui, repo, ctx1, ctx, fns)
            rate[key] = [r + l for r, l in zip(rate.get(key, (0, 0)), lines)]

        progress.increment()

    for ctx in cmdutil.walkchangerevs(repo, m, opts, prep):
        continue

    progress.complete()

    return rate


@command('churn',
    [('r', 'rev', [],
     _('count rate for the specified revision or revset'), _('REV')),
    ('d', 'date', '',
     _('count rate for revisions matching date spec'), _('DATE')),
    ('t', 'oldtemplate', '',
     _('template to group changesets (DEPRECATED)'), _('TEMPLATE')),
    ('T', 'template', '{author|email}',
     _('template to group changesets'), _('TEMPLATE')),
    ('f', 'dateformat', '',
     _('strftime-compatible format for grouping by date'), _('FORMAT')),
    ('c', 'changesets', False, _('count rate by number of changesets')),
    ('s', 'sort', False, _('sort by key (default: sort by count)')),
    ('', 'diffstat', False, _('display added/removed lines separately')),
    ('', 'aliases', '', _('file with email aliases'), _('FILE')),
    ] + cmdutil.walkopts,
    _("hg churn [-d DATE] [-r REV] [--aliases FILE] [FILE]"),
    helpcategory=command.CATEGORY_MAINTENANCE,
    inferrepo=True)
def churn(ui, repo, *pats, **opts):
    '''histogram of changes to the repository

    This command will display a histogram representing the number
    of changed lines or revisions, grouped according to the given
    template. The default template will group changes by author.
    The --dateformat option may be used to group the results by
    date instead.

    Statistics are based on the number of changed lines, or
    alternatively the number of matching revisions if the
    --changesets option is specified.

    Examples::

      # display count of changed lines for every committer
      hg churn -T "{author|email}"

      # display daily activity graph
      hg churn -f "%H" -s -c

      # display activity of developers by month
      hg churn -f "%Y-%m" -s -c

      # display count of lines changed in every year
      hg churn -f "%Y" -s

    It is possible to map alternate email addresses to a main address
    by providing a file using the following format::

      <alias email> = <actual email>

    Such a file may be specified with the --aliases option, otherwise
    a .hgchurn file will be looked for in the working directory root.
    Aliases will be split from the rightmost "=".
    '''
    def pad(s, l):
        return s + " " * (l - encoding.colwidth(s))

    amap = {}
    aliases = opts.get(r'aliases')
    if not aliases and os.path.exists(repo.wjoin('.hgchurn')):
        aliases = repo.wjoin('.hgchurn')
    if aliases:
        for l in open(aliases, "rb"):
            try:
                alias, actual = l.rsplit('=' in l and '=' or None, 1)
                amap[alias.strip()] = actual.strip()
            except ValueError:
                l = l.strip()
                if l:
                    ui.warn(_("skipping malformed alias: %s\n") % l)
                continue

    rate = list(countrate(ui, repo, amap, *pats, **opts).items())
    if not rate:
        return

    if opts.get(r'sort'):
        rate.sort()
    else:
        rate.sort(key=lambda x: (-sum(x[1]), x))

    # Be careful not to have a zero maxcount (issue833)
    maxcount = float(max(sum(v) for k, v in rate)) or 1.0
    maxname = max(len(k) for k, v in rate)

    ttywidth = ui.termwidth()
    ui.debug("assuming %i character terminal\n" % ttywidth)
    width = ttywidth - maxname - 2 - 2 - 2

    if opts.get(r'diffstat'):
        width -= 15
        def format(name, diffstat):
            added, removed = diffstat
            return "%s %15s %s%s\n" % (pad(name, maxname),
                                       '+%d/-%d' % (added, removed),
                                       ui.label('+' * charnum(added),
                                                'diffstat.inserted'),
                                       ui.label('-' * charnum(removed),
                                                'diffstat.deleted'))
    else:
        width -= 6
        def format(name, count):
            return "%s %6d %s\n" % (pad(name, maxname), sum(count),
                                    '*' * charnum(sum(count)))

    def charnum(count):
        return int(count * width // maxcount)

    for name, count in rate:
        ui.write(format(name, count))
