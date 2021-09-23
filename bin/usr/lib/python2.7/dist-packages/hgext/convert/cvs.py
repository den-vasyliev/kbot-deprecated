# cvs.py: CVS conversion code inspired by hg-cvs-import and git-cvsimport
#
#  Copyright 2005-2009 Matt Mackall <mpm@selenic.com> and others
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.
from __future__ import absolute_import

import errno
import os
import re
import socket

from mercurial.i18n import _
from mercurial import (
    encoding,
    error,
    util,
)
from mercurial.utils import (
    dateutil,
    procutil,
)

from . import (
    common,
    cvsps,
)

stringio = util.stringio
checktool = common.checktool
commit = common.commit
converter_source = common.converter_source
makedatetimestamp = common.makedatetimestamp
NoRepo = common.NoRepo

class convert_cvs(converter_source):
    def __init__(self, ui, repotype, path, revs=None):
        super(convert_cvs, self).__init__(ui, repotype, path, revs=revs)

        cvs = os.path.join(path, "CVS")
        if not os.path.exists(cvs):
            raise NoRepo(_("%s does not look like a CVS checkout") % path)

        checktool('cvs', debname='cvs')

        self.changeset = None
        self.files = {}
        self.tags = {}
        self.lastbranch = {}
        self.socket = None
        self.cvsroot = open(os.path.join(cvs, "Root"), 'rb').read()[:-1]
        self.cvsrepo = open(os.path.join(cvs, "Repository"), 'rb').read()[:-1]
        self.encoding = encoding.encoding

        self._connect()

    def _parse(self):
        if self.changeset is not None:
            return
        self.changeset = {}

        maxrev = 0
        if self.revs:
            if len(self.revs) > 1:
                raise error.Abort(_('cvs source does not support specifying '
                                   'multiple revs'))
            # TODO: handle tags
            try:
                # patchset number?
                maxrev = int(self.revs[0])
            except ValueError:
                raise error.Abort(_('revision %s is not a patchset number')
                                 % self.revs[0])

        d = encoding.getcwd()
        try:
            os.chdir(self.path)
            id = None

            cache = 'update'
            if not self.ui.configbool('convert', 'cvsps.cache'):
                cache = None
            db = cvsps.createlog(self.ui, cache=cache)
            db = cvsps.createchangeset(self.ui, db,
                fuzz=int(self.ui.config('convert', 'cvsps.fuzz')),
                mergeto=self.ui.config('convert', 'cvsps.mergeto'),
                mergefrom=self.ui.config('convert', 'cvsps.mergefrom'))

            for cs in db:
                if maxrev and cs.id > maxrev:
                    break
                id = (b"%d" % cs.id)
                cs.author = self.recode(cs.author)
                self.lastbranch[cs.branch] = id
                cs.comment = self.recode(cs.comment)
                if self.ui.configbool('convert', 'localtimezone'):
                    cs.date = makedatetimestamp(cs.date[0])
                date = dateutil.datestr(cs.date, '%Y-%m-%d %H:%M:%S %1%2')
                self.tags.update(dict.fromkeys(cs.tags, id))

                files = {}
                for f in cs.entries:
                    files[f.file] = "%s%s" % ('.'.join([(b"%d" % x)
                                                        for x in f.revision]),
                                              ['', '(DEAD)'][f.dead])

                # add current commit to set
                c = commit(author=cs.author, date=date,
                           parents=[(b"%d" % p.id) for p in cs.parents],
                           desc=cs.comment, branch=cs.branch or '')
                self.changeset[id] = c
                self.files[id] = files

            self.heads = self.lastbranch.values()
        finally:
            os.chdir(d)

    def _connect(self):
        root = self.cvsroot
        conntype = None
        user, host = None, None
        cmd = ['cvs', 'server']

        self.ui.status(_("connecting to %s\n") % root)

        if root.startswith(":pserver:"):
            root = root[9:]
            m = re.match(r'(?:(.*?)(?::(.*?))?@)?([^:\/]*)(?::(\d*))?(.*)',
                         root)
            if m:
                conntype = "pserver"
                user, passw, serv, port, root = m.groups()
                if not user:
                    user = "anonymous"
                if not port:
                    port = 2401
                else:
                    port = int(port)
                format0 = ":pserver:%s@%s:%s" % (user, serv, root)
                format1 = ":pserver:%s@%s:%d%s" % (user, serv, port, root)

                if not passw:
                    passw = "A"
                    cvspass = os.path.expanduser("~/.cvspass")
                    try:
                        pf = open(cvspass, 'rb')
                        for line in pf.read().splitlines():
                            part1, part2 = line.split(' ', 1)
                            # /1 :pserver:user@example.com:2401/cvsroot/foo
                            # Ah<Z
                            if part1 == '/1':
                                part1, part2 = part2.split(' ', 1)
                                format = format1
                            # :pserver:user@example.com:/cvsroot/foo Ah<Z
                            else:
                                format = format0
                            if part1 == format:
                                passw = part2
                                break
                        pf.close()
                    except IOError as inst:
                        if inst.errno != errno.ENOENT:
                            if not getattr(inst, 'filename', None):
                                inst.filename = cvspass
                            raise

                sck = socket.socket()
                sck.connect((serv, port))
                sck.send("\n".join(["BEGIN AUTH REQUEST", root, user, passw,
                                    "END AUTH REQUEST", ""]))
                if sck.recv(128) != "I LOVE YOU\n":
                    raise error.Abort(_("CVS pserver authentication failed"))

                self.writep = self.readp = sck.makefile('r+')

        if not conntype and root.startswith(":local:"):
            conntype = "local"
            root = root[7:]

        if not conntype:
            # :ext:user@host/home/user/path/to/cvsroot
            if root.startswith(":ext:"):
                root = root[5:]
            m = re.match(br'(?:([^@:/]+)@)?([^:/]+):?(.*)', root)
            # Do not take Windows path "c:\foo\bar" for a connection strings
            if os.path.isdir(root) or not m:
                conntype = "local"
            else:
                conntype = "rsh"
                user, host, root = m.group(1), m.group(2), m.group(3)

        if conntype != "pserver":
            if conntype == "rsh":
                rsh = encoding.environ.get("CVS_RSH") or "ssh"
                if user:
                    cmd = [rsh, '-l', user, host] + cmd
                else:
                    cmd = [rsh, host] + cmd

            # popen2 does not support argument lists under Windows
            cmd = [procutil.shellquote(arg) for arg in cmd]
            cmd = procutil.quotecommand(' '.join(cmd))
            self.writep, self.readp = procutil.popen2(cmd)

        self.realroot = root

        self.writep.write("Root %s\n" % root)
        self.writep.write("Valid-responses ok error Valid-requests Mode"
                          " M Mbinary E Checked-in Created Updated"
                          " Merged Removed\n")
        self.writep.write("valid-requests\n")
        self.writep.flush()
        r = self.readp.readline()
        if not r.startswith("Valid-requests"):
            raise error.Abort(_('unexpected response from CVS server '
                               '(expected "Valid-requests", but got %r)')
                             % r)
        if "UseUnchanged" in r:
            self.writep.write("UseUnchanged\n")
            self.writep.flush()
            r = self.readp.readline()

    def getheads(self):
        self._parse()
        return self.heads

    def getfile(self, name, rev):

        def chunkedread(fp, count):
            # file-objects returned by socket.makefile() do not handle
            # large read() requests very well.
            chunksize = 65536
            output = stringio()
            while count > 0:
                data = fp.read(min(count, chunksize))
                if not data:
                    raise error.Abort(_("%d bytes missing from remote file")
                                     % count)
                count -= len(data)
                output.write(data)
            return output.getvalue()

        self._parse()
        if rev.endswith("(DEAD)"):
            return None, None

        args = ("-N -P -kk -r %s --" % rev).split()
        args.append(self.cvsrepo + '/' + name)
        for x in args:
            self.writep.write("Argument %s\n" % x)
        self.writep.write("Directory .\n%s\nco\n" % self.realroot)
        self.writep.flush()

        data = ""
        mode = None
        while True:
            line = self.readp.readline()
            if line.startswith("Created ") or line.startswith("Updated "):
                self.readp.readline() # path
                self.readp.readline() # entries
                mode = self.readp.readline()[:-1]
                count = int(self.readp.readline()[:-1])
                data = chunkedread(self.readp, count)
            elif line.startswith(" "):
                data += line[1:]
            elif line.startswith("M "):
                pass
            elif line.startswith("Mbinary "):
                count = int(self.readp.readline()[:-1])
                data = chunkedread(self.readp, count)
            else:
                if line == "ok\n":
                    if mode is None:
                        raise error.Abort(_('malformed response from CVS'))
                    return (data, "x" in mode and "x" or "")
                elif line.startswith("E "):
                    self.ui.warn(_("cvs server: %s\n") % line[2:])
                elif line.startswith("Remove"):
                    self.readp.readline()
                else:
                    raise error.Abort(_("unknown CVS response: %s") % line)

    def getchanges(self, rev, full):
        if full:
            raise error.Abort(_("convert from cvs does not support --full"))
        self._parse()
        return sorted(self.files[rev].iteritems()), {}, set()

    def getcommit(self, rev):
        self._parse()
        return self.changeset[rev]

    def gettags(self):
        self._parse()
        return self.tags

    def getchangedfiles(self, rev, i):
        self._parse()
        return sorted(self.files[rev])
