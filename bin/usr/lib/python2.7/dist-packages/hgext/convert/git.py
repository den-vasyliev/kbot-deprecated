# git.py - git support for the convert extension
#
#  Copyright 2005-2009 Matt Mackall <mpm@selenic.com> and others
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.
from __future__ import absolute_import

import os

from mercurial.i18n import _
from mercurial import (
    config,
    error,
    node as nodemod,
)

from . import (
    common,
)

class submodule(object):
    def __init__(self, path, node, url):
        self.path = path
        self.node = node
        self.url = url

    def hgsub(self):
        return "%s = [git]%s" % (self.path, self.url)

    def hgsubstate(self):
        return "%s %s" % (self.node, self.path)

# Keys in extra fields that should not be copied if the user requests.
bannedextrakeys = {
    # Git commit object built-ins.
    'tree',
    'parent',
    'author',
    'committer',
    # Mercurial built-ins.
    'branch',
    'close',
}

class convert_git(common.converter_source, common.commandline):
    # Windows does not support GIT_DIR= construct while other systems
    # cannot remove environment variable. Just assume none have
    # both issues.

    def _gitcmd(self, cmd, *args, **kwargs):
        return cmd('--git-dir=%s' % self.path, *args, **kwargs)

    def gitrun0(self, *args, **kwargs):
        return self._gitcmd(self.run0, *args, **kwargs)

    def gitrun(self, *args, **kwargs):
        return self._gitcmd(self.run, *args, **kwargs)

    def gitrunlines0(self, *args, **kwargs):
        return self._gitcmd(self.runlines0, *args, **kwargs)

    def gitrunlines(self, *args, **kwargs):
        return self._gitcmd(self.runlines, *args, **kwargs)

    def gitpipe(self, *args, **kwargs):
        return self._gitcmd(self._run3, *args, **kwargs)

    def __init__(self, ui, repotype, path, revs=None):
        super(convert_git, self).__init__(ui, repotype, path, revs=revs)
        common.commandline.__init__(self, ui, 'git')

        # Pass an absolute path to git to prevent from ever being interpreted
        # as a URL
        path = os.path.abspath(path)

        if os.path.isdir(path + "/.git"):
            path += "/.git"
        if not os.path.exists(path + "/objects"):
            raise common.NoRepo(_("%s does not look like a Git repository") %
                                path)

        # The default value (50) is based on the default for 'git diff'.
        similarity = ui.configint('convert', 'git.similarity')
        if similarity < 0 or similarity > 100:
            raise error.Abort(_('similarity must be between 0 and 100'))
        if similarity > 0:
            self.simopt = ['-C%d%%' % similarity]
            findcopiesharder = ui.configbool('convert', 'git.findcopiesharder')
            if findcopiesharder:
                self.simopt.append('--find-copies-harder')

            renamelimit = ui.configint('convert', 'git.renamelimit')
            self.simopt.append('-l%d' % renamelimit)
        else:
            self.simopt = []

        common.checktool('git', 'git', debname="git-core")

        self.path = path
        self.submodules = []

        self.catfilepipe = self.gitpipe('cat-file', '--batch')

        self.copyextrakeys = self.ui.configlist('convert', 'git.extrakeys')
        banned = set(self.copyextrakeys) & bannedextrakeys
        if banned:
            raise error.Abort(_('copying of extra key is forbidden: %s') %
                              _(', ').join(sorted(banned)))

        committeractions = self.ui.configlist('convert', 'git.committeractions')

        messagedifferent = None
        messagealways = None
        for a in committeractions:
            if a.startswith(('messagedifferent', 'messagealways')):
                k = a
                v = None
                if '=' in a:
                    k, v = a.split('=', 1)

                if k == 'messagedifferent':
                    messagedifferent = v or 'committer:'
                elif k == 'messagealways':
                    messagealways = v or 'committer:'

        if messagedifferent and messagealways:
            raise error.Abort(_('committeractions cannot define both '
                                'messagedifferent and messagealways'))

        dropcommitter = 'dropcommitter' in committeractions
        replaceauthor = 'replaceauthor' in committeractions

        if dropcommitter and replaceauthor:
            raise error.Abort(_('committeractions cannot define both '
                                'dropcommitter and replaceauthor'))

        if dropcommitter and messagealways:
            raise error.Abort(_('committeractions cannot define both '
                                'dropcommitter and messagealways'))

        if not messagedifferent and not messagealways:
            messagedifferent = 'committer:'

        self.committeractions = {
            'dropcommitter': dropcommitter,
            'replaceauthor': replaceauthor,
            'messagedifferent': messagedifferent,
            'messagealways': messagealways,
        }

    def after(self):
        for f in self.catfilepipe:
            f.close()

    def getheads(self):
        if not self.revs:
            output, status = self.gitrun('rev-parse', '--branches', '--remotes')
            heads = output.splitlines()
            if status:
                raise error.Abort(_('cannot retrieve git heads'))
        else:
            heads = []
            for rev in self.revs:
                rawhead, ret = self.gitrun('rev-parse', '--verify', rev)
                heads.append(rawhead[:-1])
                if ret:
                    raise error.Abort(_('cannot retrieve git head "%s"') % rev)
        return heads

    def catfile(self, rev, ftype):
        if rev == nodemod.nullhex:
            raise IOError
        self.catfilepipe[0].write(rev+'\n')
        self.catfilepipe[0].flush()
        info = self.catfilepipe[1].readline().split()
        if info[1] != ftype:
            raise error.Abort(_('cannot read %r object at %s') % (ftype, rev))
        size = int(info[2])
        data = self.catfilepipe[1].read(size)
        if len(data) < size:
            raise error.Abort(_('cannot read %r object at %s: unexpected size')
                              % (ftype, rev))
        # read the trailing newline
        self.catfilepipe[1].read(1)
        return data

    def getfile(self, name, rev):
        if rev == nodemod.nullhex:
            return None, None
        if name == '.hgsub':
            data = '\n'.join([m.hgsub() for m in self.submoditer()])
            mode = ''
        elif name == '.hgsubstate':
            data = '\n'.join([m.hgsubstate() for m in self.submoditer()])
            mode = ''
        else:
            data = self.catfile(rev, "blob")
            mode = self.modecache[(name, rev)]
        return data, mode

    def submoditer(self):
        null = nodemod.nullhex
        for m in sorted(self.submodules, key=lambda p: p.path):
            if m.node != null:
                yield m

    def parsegitmodules(self, content):
        """Parse the formatted .gitmodules file, example file format:
        [submodule "sub"]\n
        \tpath = sub\n
        \turl = git://giturl\n
        """
        self.submodules = []
        c = config.config()
        # Each item in .gitmodules starts with whitespace that cant be parsed
        c.parse('.gitmodules', '\n'.join(line.strip() for line in
                               content.split('\n')))
        for sec in c.sections():
            s = c[sec]
            if 'url' in s and 'path' in s:
                self.submodules.append(submodule(s['path'], '', s['url']))

    def retrievegitmodules(self, version):
        modules, ret = self.gitrun('show', '%s:%s' % (version, '.gitmodules'))
        if ret:
            # This can happen if a file is in the repo that has permissions
            # 160000, but there is no .gitmodules file.
            self.ui.warn(_("warning: cannot read submodules config file in "
                           "%s\n") % version)
            return

        try:
            self.parsegitmodules(modules)
        except error.ParseError:
            self.ui.warn(_("warning: unable to parse .gitmodules in %s\n")
                         % version)
            return

        for m in self.submodules:
            node, ret = self.gitrun('rev-parse', '%s:%s' % (version, m.path))
            if ret:
                continue
            m.node = node.strip()

    def getchanges(self, version, full):
        if full:
            raise error.Abort(_("convert from git does not support --full"))
        self.modecache = {}
        cmd = ['diff-tree','-z', '--root', '-m', '-r'] + self.simopt + [version]
        output, status = self.gitrun(*cmd)
        if status:
            raise error.Abort(_('cannot read changes in %s') % version)
        changes = []
        copies = {}
        seen = set()
        entry = None
        subexists = [False]
        subdeleted = [False]
        difftree = output.split('\x00')
        lcount = len(difftree)
        i = 0

        skipsubmodules = self.ui.configbool('convert', 'git.skipsubmodules')
        def add(entry, f, isdest):
            seen.add(f)
            h = entry[3]
            p = (entry[1] == "100755")
            s = (entry[1] == "120000")
            renamesource = (not isdest and entry[4][0] == 'R')

            if f == '.gitmodules':
                if skipsubmodules:
                    return

                subexists[0] = True
                if entry[4] == 'D' or renamesource:
                    subdeleted[0] = True
                    changes.append(('.hgsub', nodemod.nullhex))
                else:
                    changes.append(('.hgsub', ''))
            elif entry[1] == '160000' or entry[0] == ':160000':
                if not skipsubmodules:
                    subexists[0] = True
            else:
                if renamesource:
                    h = nodemod.nullhex
                self.modecache[(f, h)] = (p and "x") or (s and "l") or ""
                changes.append((f, h))

        while i < lcount:
            l = difftree[i]
            i += 1
            if not entry:
                if not l.startswith(':'):
                    continue
                entry = l.split()
                continue
            f = l
            if entry[4][0] == 'C':
                copysrc = f
                copydest = difftree[i]
                i += 1
                f = copydest
                copies[copydest] = copysrc
            if f not in seen:
                add(entry, f, False)
            # A file can be copied multiple times, or modified and copied
            # simultaneously. So f can be repeated even if fdest isn't.
            if entry[4][0] == 'R':
                # rename: next line is the destination
                fdest = difftree[i]
                i += 1
                if fdest not in seen:
                    add(entry, fdest, True)
                    # .gitmodules isn't imported at all, so it being copied to
                    # and fro doesn't really make sense
                    if f != '.gitmodules' and fdest != '.gitmodules':
                        copies[fdest] = f
            entry = None

        if subexists[0]:
            if subdeleted[0]:
                changes.append(('.hgsubstate', nodemod.nullhex))
            else:
                self.retrievegitmodules(version)
                changes.append(('.hgsubstate', ''))
        return (changes, copies, set())

    def getcommit(self, version):
        c = self.catfile(version, "commit") # read the commit hash
        end = c.find("\n\n")
        message = c[end + 2:]
        message = self.recode(message)
        l = c[:end].splitlines()
        parents = []
        author = committer = None
        extra = {}
        for e in l[1:]:
            n, v = e.split(" ", 1)
            if n == "author":
                p = v.split()
                tm, tz = p[-2:]
                author = " ".join(p[:-2])
                if author[0] == "<":
                    author = author[1:-1]
                author = self.recode(author)
            if n == "committer":
                p = v.split()
                tm, tz = p[-2:]
                committer = " ".join(p[:-2])
                if committer[0] == "<":
                    committer = committer[1:-1]
                committer = self.recode(committer)
            if n == "parent":
                parents.append(v)
            if n in self.copyextrakeys:
                extra[n] = v

        if self.committeractions['dropcommitter']:
            committer = None
        elif self.committeractions['replaceauthor']:
            author = committer

        if committer:
            messagealways = self.committeractions['messagealways']
            messagedifferent = self.committeractions['messagedifferent']
            if messagealways:
                message += '\n%s %s\n' % (messagealways, committer)
            elif messagedifferent and author != committer:
                message += '\n%s %s\n' % (messagedifferent, committer)

        tzs, tzh, tzm = tz[-5:-4] + "1", tz[-4:-2], tz[-2:]
        tz = -int(tzs) * (int(tzh) * 3600 + int(tzm))
        date = tm + " " + (b"%d" % tz)
        saverev = self.ui.configbool('convert', 'git.saverev')

        c = common.commit(parents=parents, date=date, author=author,
                          desc=message,
                          rev=version,
                          extra=extra,
                          saverev=saverev)
        return c

    def numcommits(self):
        output, ret = self.gitrunlines('rev-list', '--all')
        if ret:
            raise error.Abort(_('cannot retrieve number of commits in %s') \
                              % self.path)
        return len(output)

    def gettags(self):
        tags = {}
        alltags = {}
        output, status = self.gitrunlines('ls-remote', '--tags', self.path)

        if status:
            raise error.Abort(_('cannot read tags from %s') % self.path)
        prefix = 'refs/tags/'

        # Build complete list of tags, both annotated and bare ones
        for line in output:
            line = line.strip()
            if line.startswith("error:") or line.startswith("fatal:"):
                raise error.Abort(_('cannot read tags from %s') % self.path)
            node, tag = line.split(None, 1)
            if not tag.startswith(prefix):
                continue
            alltags[tag[len(prefix):]] = node

        # Filter out tag objects for annotated tag refs
        for tag in alltags:
            if tag.endswith('^{}'):
                tags[tag[:-3]] = alltags[tag]
            else:
                if tag + '^{}' in alltags:
                    continue
                else:
                    tags[tag] = alltags[tag]

        return tags

    def getchangedfiles(self, version, i):
        changes = []
        if i is None:
            output, status = self.gitrunlines('diff-tree', '--root', '-m',
                                              '-r', version)
            if status:
                raise error.Abort(_('cannot read changes in %s') % version)
            for l in output:
                if "\t" not in l:
                    continue
                m, f = l[:-1].split("\t")
                changes.append(f)
        else:
            output, status = self.gitrunlines('diff-tree', '--name-only',
                                              '--root', '-r', version,
                                              '%s^%d' % (version, i + 1), '--')
            if status:
                raise error.Abort(_('cannot read changes in %s') % version)
            changes = [f.rstrip('\n') for f in output]

        return changes

    def getbookmarks(self):
        bookmarks = {}

        # Handle local and remote branches
        remoteprefix = self.ui.config('convert', 'git.remoteprefix')
        reftypes = [
            # (git prefix, hg prefix)
            ('refs/remotes/origin/', remoteprefix + '/'),
            ('refs/heads/', '')
        ]

        exclude = {
            'refs/remotes/origin/HEAD',
        }

        try:
            output, status = self.gitrunlines('show-ref')
            for line in output:
                line = line.strip()
                rev, name = line.split(None, 1)
                # Process each type of branch
                for gitprefix, hgprefix in reftypes:
                    if not name.startswith(gitprefix) or name in exclude:
                        continue
                    name = '%s%s' % (hgprefix, name[len(gitprefix):])
                    bookmarks[name] = rev
        except Exception:
            pass

        return bookmarks

    def checkrevformat(self, revstr, mapname='splicemap'):
        """ git revision string is a 40 byte hex """
        self.checkhexformat(revstr, mapname)
