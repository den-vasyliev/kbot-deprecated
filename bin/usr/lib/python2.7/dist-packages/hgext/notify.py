# notify.py - email notifications for mercurial
#
# Copyright 2006 Vadim Gelfer <vadim.gelfer@gmail.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

'''hooks for sending email push notifications

This extension implements hooks to send email notifications when
changesets are sent from or received by the local repository.

First, enable the extension as explained in :hg:`help extensions`, and
register the hook you want to run. ``incoming`` and ``changegroup`` hooks
are run when changesets are received, while ``outgoing`` hooks are for
changesets sent to another repository::

  [hooks]
  # one email for each incoming changeset
  incoming.notify = python:hgext.notify.hook
  # one email for all incoming changesets
  changegroup.notify = python:hgext.notify.hook

  # one email for all outgoing changesets
  outgoing.notify = python:hgext.notify.hook

This registers the hooks. To enable notification, subscribers must
be assigned to repositories. The ``[usersubs]`` section maps multiple
repositories to a given recipient. The ``[reposubs]`` section maps
multiple recipients to a single repository::

  [usersubs]
  # key is subscriber email, value is a comma-separated list of repo patterns
  user@host = pattern

  [reposubs]
  # key is repo pattern, value is a comma-separated list of subscriber emails
  pattern = user@host

A ``pattern`` is a ``glob`` matching the absolute path to a repository,
optionally combined with a revset expression. A revset expression, if
present, is separated from the glob by a hash. Example::

  [reposubs]
  */widgets#branch(release) = qa-team@example.com

This sends to ``qa-team@example.com`` whenever a changeset on the ``release``
branch triggers a notification in any repository ending in ``widgets``.

In order to place them under direct user management, ``[usersubs]`` and
``[reposubs]`` sections may be placed in a separate ``hgrc`` file and
incorporated by reference::

  [notify]
  config = /path/to/subscriptionsfile

Notifications will not be sent until the ``notify.test`` value is set
to ``False``; see below.

Notifications content can be tweaked with the following configuration entries:

notify.test
  If ``True``, print messages to stdout instead of sending them. Default: True.

notify.sources
  Space-separated list of change sources. Notifications are activated only
  when a changeset's source is in this list. Sources may be:

  :``serve``: changesets received via http or ssh
  :``pull``: changesets received via ``hg pull``
  :``unbundle``: changesets received via ``hg unbundle``
  :``push``: changesets sent or received via ``hg push``
  :``bundle``: changesets sent via ``hg unbundle``

  Default: serve.

notify.strip
  Number of leading slashes to strip from url paths. By default, notifications
  reference repositories with their absolute path. ``notify.strip`` lets you
  turn them into relative paths. For example, ``notify.strip=3`` will change
  ``/long/path/repository`` into ``repository``. Default: 0.

notify.domain
  Default email domain for sender or recipients with no explicit domain.

notify.style
  Style file to use when formatting emails.

notify.template
  Template to use when formatting emails.

notify.incoming
  Template to use when run as an incoming hook, overriding ``notify.template``.

notify.outgoing
  Template to use when run as an outgoing hook, overriding ``notify.template``.

notify.changegroup
  Template to use when running as a changegroup hook, overriding
  ``notify.template``.

notify.maxdiff
  Maximum number of diff lines to include in notification email. Set to 0
  to disable the diff, or -1 to include all of it. Default: 300.

notify.maxdiffstat
  Maximum number of diffstat lines to include in notification email. Set to -1
  to include all of it. Default: -1.

notify.maxsubject
  Maximum number of characters in email's subject line. Default: 67.

notify.diffstat
  Set to True to include a diffstat before diff content. Default: True.

notify.showfunc
  If set, override ``diff.showfunc`` for the diff content. Default: None.

notify.merge
  If True, send notifications for merge changesets. Default: True.

notify.mbox
  If set, append mails to this mbox file instead of sending. Default: None.

notify.fromauthor
  If set, use the committer of the first changeset in a changegroup for
  the "From" field of the notification mail. If not set, take the user
  from the pushing repo.  Default: False.

If set, the following entries will also be used to customize the
notifications:

email.from
  Email ``From`` address to use if none can be found in the generated
  email content.

web.baseurl
  Root repository URL to combine with repository paths when making
  references. See also ``notify.strip``.

'''
from __future__ import absolute_import

import email.errors as emailerrors
import email.parser as emailparser
import fnmatch
import socket
import time

from mercurial.i18n import _
from mercurial import (
    encoding,
    error,
    logcmdutil,
    mail,
    patch,
    registrar,
    util,
)
from mercurial.utils import (
    dateutil,
    stringutil,
)

# Note for extension authors: ONLY specify testedwith = 'ships-with-hg-core' for
# extensions which SHIP WITH MERCURIAL. Non-mainline extensions should
# be specifying the version(s) of Mercurial they are tested with, or
# leave the attribute unspecified.
testedwith = 'ships-with-hg-core'

configtable = {}
configitem = registrar.configitem(configtable)

configitem('notify', 'changegroup',
    default=None,
)
configitem('notify', 'config',
    default=None,
)
configitem('notify', 'diffstat',
    default=True,
)
configitem('notify', 'domain',
    default=None,
)
configitem('notify', 'fromauthor',
    default=None,
)
configitem('notify', 'incoming',
    default=None,
)
configitem('notify', 'maxdiff',
    default=300,
)
configitem('notify', 'maxdiffstat',
    default=-1,
)
configitem('notify', 'maxsubject',
    default=67,
)
configitem('notify', 'mbox',
    default=None,
)
configitem('notify', 'merge',
    default=True,
)
configitem('notify', 'outgoing',
    default=None,
)
configitem('notify', 'sources',
    default='serve',
)
configitem('notify', 'showfunc',
    default=None,
)
configitem('notify', 'strip',
    default=0,
)
configitem('notify', 'style',
    default=None,
)
configitem('notify', 'template',
    default=None,
)
configitem('notify', 'test',
    default=True,
)

# template for single changeset can include email headers.
single_template = b'''
Subject: changeset in {webroot}: {desc|firstline|strip}
From: {author}

changeset {node|short} in {root}
details: {baseurl}{webroot}?cmd=changeset;node={node|short}
description:
\t{desc|tabindent|strip}
'''.lstrip()

# template for multiple changesets should not contain email headers,
# because only first set of headers will be used and result will look
# strange.
multiple_template = b'''
changeset {node|short} in {root}
details: {baseurl}{webroot}?cmd=changeset;node={node|short}
summary: {desc|firstline}
'''

deftemplates = {
    'changegroup': multiple_template,
}

class notifier(object):
    '''email notification class.'''

    def __init__(self, ui, repo, hooktype):
        self.ui = ui
        cfg = self.ui.config('notify', 'config')
        if cfg:
            self.ui.readconfig(cfg, sections=['usersubs', 'reposubs'])
        self.repo = repo
        self.stripcount = int(self.ui.config('notify', 'strip'))
        self.root = self.strip(self.repo.root)
        self.domain = self.ui.config('notify', 'domain')
        self.mbox = self.ui.config('notify', 'mbox')
        self.test = self.ui.configbool('notify', 'test')
        self.charsets = mail._charsets(self.ui)
        self.subs = self.subscribers()
        self.merge = self.ui.configbool('notify', 'merge')
        self.showfunc = self.ui.configbool('notify', 'showfunc')
        if self.showfunc is None:
            self.showfunc = self.ui.configbool('diff', 'showfunc')

        mapfile = None
        template = (self.ui.config('notify', hooktype) or
                    self.ui.config('notify', 'template'))
        if not template:
            mapfile = self.ui.config('notify', 'style')
        if not mapfile and not template:
            template = deftemplates.get(hooktype) or single_template
        spec = logcmdutil.templatespec(template, mapfile)
        self.t = logcmdutil.changesettemplater(self.ui, self.repo, spec)

    def strip(self, path):
        '''strip leading slashes from local path, turn into web-safe path.'''

        path = util.pconvert(path)
        count = self.stripcount
        while count > 0:
            c = path.find('/')
            if c == -1:
                break
            path = path[c + 1:]
            count -= 1
        return path

    def fixmail(self, addr):
        '''try to clean up email addresses.'''

        addr = stringutil.email(addr.strip())
        if self.domain:
            a = addr.find('@localhost')
            if a != -1:
                addr = addr[:a]
            if '@' not in addr:
                return addr + '@' + self.domain
        return addr

    def subscribers(self):
        '''return list of email addresses of subscribers to this repo.'''
        subs = set()
        for user, pats in self.ui.configitems('usersubs'):
            for pat in pats.split(','):
                if '#' in pat:
                    pat, revs = pat.split('#', 1)
                else:
                    revs = None
                if fnmatch.fnmatch(self.repo.root, pat.strip()):
                    subs.add((self.fixmail(user), revs))
        for pat, users in self.ui.configitems('reposubs'):
            if '#' in pat:
                pat, revs = pat.split('#', 1)
            else:
                revs = None
            if fnmatch.fnmatch(self.repo.root, pat):
                for user in users.split(','):
                    subs.add((self.fixmail(user), revs))
        return [(mail.addressencode(self.ui, s, self.charsets, self.test), r)
                for s, r in sorted(subs)]

    def node(self, ctx, **props):
        '''format one changeset, unless it is a suppressed merge.'''
        if not self.merge and len(ctx.parents()) > 1:
            return False
        self.t.show(ctx, changes=ctx.changeset(),
                    baseurl=self.ui.config('web', 'baseurl'),
                    root=self.repo.root, webroot=self.root, **props)
        return True

    def skipsource(self, source):
        '''true if incoming changes from this source should be skipped.'''
        ok_sources = self.ui.config('notify', 'sources').split()
        return source not in ok_sources

    def send(self, ctx, count, data):
        '''send message.'''

        # Select subscribers by revset
        subs = set()
        for sub, spec in self.subs:
            if spec is None:
                subs.add(sub)
                continue
            revs = self.repo.revs('%r and %d:', spec, ctx.rev())
            if len(revs):
                subs.add(sub)
                continue
        if len(subs) == 0:
            self.ui.debug('notify: no subscribers to selected repo '
                          'and revset\n')
            return

        p = emailparser.Parser()
        try:
            msg = p.parsestr(encoding.strfromlocal(data))
        except emailerrors.MessageParseError as inst:
            raise error.Abort(inst)

        # store sender and subject
        sender = encoding.strtolocal(msg[r'From'])
        subject = encoding.strtolocal(msg[r'Subject'])
        del msg[r'From'], msg[r'Subject']

        if not msg.is_multipart():
            # create fresh mime message from scratch
            # (multipart templates must take care of this themselves)
            headers = msg.items()
            payload = msg.get_payload()
            # for notification prefer readability over data precision
            msg = mail.mimeencode(self.ui, payload, self.charsets, self.test)
            # reinstate custom headers
            for k, v in headers:
                msg[k] = v

        msg[r'Date'] = encoding.strfromlocal(
            dateutil.datestr(format="%a, %d %b %Y %H:%M:%S %1%2"))

        # try to make subject line exist and be useful
        if not subject:
            if count > 1:
                subject = _('%s: %d new changesets') % (self.root, count)
            else:
                s = ctx.description().lstrip().split('\n', 1)[0].rstrip()
                subject = '%s: %s' % (self.root, s)
        maxsubject = int(self.ui.config('notify', 'maxsubject'))
        if maxsubject:
            subject = stringutil.ellipsis(subject, maxsubject)
        msg[r'Subject'] = encoding.strfromlocal(
            mail.headencode(self.ui, subject, self.charsets, self.test))

        # try to make message have proper sender
        if not sender:
            sender = self.ui.config('email', 'from') or self.ui.username()
        if '@' not in sender or '@localhost' in sender:
            sender = self.fixmail(sender)
        msg[r'From'] = encoding.strfromlocal(
            mail.addressencode(self.ui, sender, self.charsets, self.test))

        msg[r'X-Hg-Notification'] = r'changeset %s' % ctx
        if not msg[r'Message-Id']:
            msg[r'Message-Id'] = encoding.strfromlocal(
                '<hg.%s.%d.%d@%s>' % (ctx, int(time.time()),
                                      hash(self.repo.root),
                                      encoding.strtolocal(socket.getfqdn())))
        msg[r'To'] = encoding.strfromlocal(', '.join(sorted(subs)))

        msgtext = encoding.strtolocal(msg.as_string())
        if self.test:
            self.ui.write(msgtext)
            if not msgtext.endswith('\n'):
                self.ui.write('\n')
        else:
            self.ui.status(_('notify: sending %d subscribers %d changes\n') %
                           (len(subs), count))
            mail.sendmail(self.ui, stringutil.email(msg[r'From']),
                          subs, msgtext, mbox=self.mbox)

    def diff(self, ctx, ref=None):

        maxdiff = int(self.ui.config('notify', 'maxdiff'))
        prev = ctx.p1().node()
        if ref:
            ref = ref.node()
        else:
            ref = ctx.node()
        diffopts = patch.diffallopts(self.ui)
        diffopts.showfunc = self.showfunc
        chunks = patch.diff(self.repo, prev, ref, opts=diffopts)
        difflines = ''.join(chunks).splitlines()

        if self.ui.configbool('notify', 'diffstat'):
            maxdiffstat = int(self.ui.config('notify', 'maxdiffstat'))
            s = patch.diffstat(difflines)
            # s may be nil, don't include the header if it is
            if s:
                if maxdiffstat >= 0 and s.count("\n") > maxdiffstat + 1:
                    s = s.split("\n")
                    msg = _('\ndiffstat (truncated from %d to %d lines):\n\n')
                    self.ui.write(msg % (len(s) - 2, maxdiffstat))
                    self.ui.write("\n".join(s[:maxdiffstat] + s[-2:]))
                else:
                    self.ui.write(_('\ndiffstat:\n\n%s') % s)

        if maxdiff == 0:
            return
        elif maxdiff > 0 and len(difflines) > maxdiff:
            msg = _('\ndiffs (truncated from %d to %d lines):\n\n')
            self.ui.write(msg % (len(difflines), maxdiff))
            difflines = difflines[:maxdiff]
        elif difflines:
            self.ui.write(_('\ndiffs (%d lines):\n\n') % len(difflines))

        self.ui.write("\n".join(difflines))

def hook(ui, repo, hooktype, node=None, source=None, **kwargs):
    '''send email notifications to interested subscribers.

    if used as changegroup hook, send one email for all changesets in
    changegroup. else send one email per changeset.'''

    n = notifier(ui, repo, hooktype)
    ctx = repo.unfiltered()[node]

    if not n.subs:
        ui.debug('notify: no subscribers to repository %s\n' % n.root)
        return
    if n.skipsource(source):
        ui.debug('notify: changes have source "%s" - skipping\n' % source)
        return

    ui.pushbuffer()
    data = ''
    count = 0
    author = ''
    if hooktype == 'changegroup' or hooktype == 'outgoing':
        for rev in repo.changelog.revs(start=ctx.rev()):
            if n.node(repo[rev]):
                count += 1
                if not author:
                    author = repo[rev].user()
            else:
                data += ui.popbuffer()
                ui.note(_('notify: suppressing notification for merge %d:%s\n')
                        % (rev, repo[rev].hex()[:12]))
                ui.pushbuffer()
        if count:
            n.diff(ctx, repo['tip'])
    elif ctx.rev() in repo:
        if not n.node(ctx):
            ui.popbuffer()
            ui.note(_('notify: suppressing notification for merge %d:%s\n') %
                    (ctx.rev(), ctx.hex()[:12]))
            return
        count += 1
        n.diff(ctx)
        if not author:
            author = ctx.user()

    data += ui.popbuffer()
    fromauthor = ui.config('notify', 'fromauthor')
    if author and fromauthor:
        data = '\n'.join(['From: %s' % author, data])

    if count:
        n.send(ctx, count, data)
