# changelog.py - changelog class for mercurial
#
# Copyright 2005-2007 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

from .i18n import _
from .node import (
    bin,
    hex,
    nullid,
)
from .thirdparty import (
    attr,
)

from . import (
    encoding,
    error,
    pycompat,
    revlog,
)
from .utils import (
    dateutil,
    stringutil,
)

_defaultextra = {'branch': 'default'}

def _string_escape(text):
    """
    >>> from .pycompat import bytechr as chr
    >>> d = {b'nl': chr(10), b'bs': chr(92), b'cr': chr(13), b'nul': chr(0)}
    >>> s = b"ab%(nl)scd%(bs)s%(bs)sn%(nul)sab%(cr)scd%(bs)s%(nl)s" % d
    >>> s
    'ab\\ncd\\\\\\\\n\\x00ab\\rcd\\\\\\n'
    >>> res = _string_escape(s)
    >>> s == stringutil.unescapestr(res)
    True
    """
    # subset of the string_escape codec
    text = text.replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r')
    return text.replace('\0', '\\0')

def decodeextra(text):
    """
    >>> from .pycompat import bytechr as chr
    >>> sorted(decodeextra(encodeextra({b'foo': b'bar', b'baz': chr(0) + b'2'})
    ...                    ).items())
    [('baz', '\\x002'), ('branch', 'default'), ('foo', 'bar')]
    >>> sorted(decodeextra(encodeextra({b'foo': b'bar',
    ...                                 b'baz': chr(92) + chr(0) + b'2'})
    ...                    ).items())
    [('baz', '\\\\\\x002'), ('branch', 'default'), ('foo', 'bar')]
    """
    extra = _defaultextra.copy()
    for l in text.split('\0'):
        if l:
            if '\\0' in l:
                # fix up \0 without getting into trouble with \\0
                l = l.replace('\\\\', '\\\\\n')
                l = l.replace('\\0', '\0')
                l = l.replace('\n', '')
            k, v = stringutil.unescapestr(l).split(':', 1)
            extra[k] = v
    return extra

def encodeextra(d):
    # keys must be sorted to produce a deterministic changelog entry
    items = [_string_escape('%s:%s' % (k, d[k])) for k in sorted(d)]
    return "\0".join(items)

def stripdesc(desc):
    """strip trailing whitespace and leading and trailing empty lines"""
    return '\n'.join([l.rstrip() for l in desc.splitlines()]).strip('\n')

class appender(object):
    '''the changelog index must be updated last on disk, so we use this class
    to delay writes to it'''
    def __init__(self, vfs, name, mode, buf):
        self.data = buf
        fp = vfs(name, mode)
        self.fp = fp
        self.offset = fp.tell()
        self.size = vfs.fstat(fp).st_size
        self._end = self.size

    def end(self):
        return self._end
    def tell(self):
        return self.offset
    def flush(self):
        pass

    @property
    def closed(self):
        return self.fp.closed

    def close(self):
        self.fp.close()

    def seek(self, offset, whence=0):
        '''virtual file offset spans real file and data'''
        if whence == 0:
            self.offset = offset
        elif whence == 1:
            self.offset += offset
        elif whence == 2:
            self.offset = self.end() + offset
        if self.offset < self.size:
            self.fp.seek(self.offset)

    def read(self, count=-1):
        '''only trick here is reads that span real file and data'''
        ret = ""
        if self.offset < self.size:
            s = self.fp.read(count)
            ret = s
            self.offset += len(s)
            if count > 0:
                count -= len(s)
        if count != 0:
            doff = self.offset - self.size
            self.data.insert(0, "".join(self.data))
            del self.data[1:]
            s = self.data[0][doff:doff + count]
            self.offset += len(s)
            ret += s
        return ret

    def write(self, s):
        self.data.append(bytes(s))
        self.offset += len(s)
        self._end += len(s)

    def __enter__(self):
        self.fp.__enter__()
        return self

    def __exit__(self, *args):
        return self.fp.__exit__(*args)

def _divertopener(opener, target):
    """build an opener that writes in 'target.a' instead of 'target'"""
    def _divert(name, mode='r', checkambig=False):
        if name != target:
            return opener(name, mode)
        return opener(name + ".a", mode)
    return _divert

def _delayopener(opener, target, buf):
    """build an opener that stores chunks in 'buf' instead of 'target'"""
    def _delay(name, mode='r', checkambig=False):
        if name != target:
            return opener(name, mode)
        return appender(opener, name, mode, buf)
    return _delay

@attr.s
class _changelogrevision(object):
    # Extensions might modify _defaultextra, so let the constructor below pass
    # it in
    extra = attr.ib()
    manifest = attr.ib(default=nullid)
    user = attr.ib(default='')
    date = attr.ib(default=(0, 0))
    files = attr.ib(default=attr.Factory(list))
    description = attr.ib(default='')

class changelogrevision(object):
    """Holds results of a parsed changelog revision.

    Changelog revisions consist of multiple pieces of data, including
    the manifest node, user, and date. This object exposes a view into
    the parsed object.
    """

    __slots__ = (
        u'_offsets',
        u'_text',
    )

    def __new__(cls, text):
        if not text:
            return _changelogrevision(extra=_defaultextra)

        self = super(changelogrevision, cls).__new__(cls)
        # We could return here and implement the following as an __init__.
        # But doing it here is equivalent and saves an extra function call.

        # format used:
        # nodeid\n        : manifest node in ascii
        # user\n          : user, no \n or \r allowed
        # time tz extra\n : date (time is int or float, timezone is int)
        #                 : extra is metadata, encoded and separated by '\0'
        #                 : older versions ignore it
        # files\n\n       : files modified by the cset, no \n or \r allowed
        # (.*)            : comment (free text, ideally utf-8)
        #
        # changelog v0 doesn't use extra

        nl1 = text.index('\n')
        nl2 = text.index('\n', nl1 + 1)
        nl3 = text.index('\n', nl2 + 1)

        # The list of files may be empty. Which means nl3 is the first of the
        # double newline that precedes the description.
        if text[nl3 + 1:nl3 + 2] == '\n':
            doublenl = nl3
        else:
            doublenl = text.index('\n\n', nl3 + 1)

        self._offsets = (nl1, nl2, nl3, doublenl)
        self._text = text

        return self

    @property
    def manifest(self):
        return bin(self._text[0:self._offsets[0]])

    @property
    def user(self):
        off = self._offsets
        return encoding.tolocal(self._text[off[0] + 1:off[1]])

    @property
    def _rawdate(self):
        off = self._offsets
        dateextra = self._text[off[1] + 1:off[2]]
        return dateextra.split(' ', 2)[0:2]

    @property
    def _rawextra(self):
        off = self._offsets
        dateextra = self._text[off[1] + 1:off[2]]
        fields = dateextra.split(' ', 2)
        if len(fields) != 3:
            return None

        return fields[2]

    @property
    def date(self):
        raw = self._rawdate
        time = float(raw[0])
        # Various tools did silly things with the timezone.
        try:
            timezone = int(raw[1])
        except ValueError:
            timezone = 0

        return time, timezone

    @property
    def extra(self):
        raw = self._rawextra
        if raw is None:
            return _defaultextra

        return decodeextra(raw)

    @property
    def files(self):
        off = self._offsets
        if off[2] == off[3]:
            return []

        return self._text[off[2] + 1:off[3]].split('\n')

    @property
    def description(self):
        return encoding.tolocal(self._text[self._offsets[3] + 2:])

class changelog(revlog.revlog):
    def __init__(self, opener, trypending=False):
        """Load a changelog revlog using an opener.

        If ``trypending`` is true, we attempt to load the index from a
        ``00changelog.i.a`` file instead of the default ``00changelog.i``.
        The ``00changelog.i.a`` file contains index (and possibly inline
        revision) data for a transaction that hasn't been finalized yet.
        It exists in a separate file to facilitate readers (such as
        hooks processes) accessing data before a transaction is finalized.
        """
        if trypending and opener.exists('00changelog.i.a'):
            indexfile = '00changelog.i.a'
        else:
            indexfile = '00changelog.i'

        datafile = '00changelog.d'
        revlog.revlog.__init__(self, opener, indexfile, datafile=datafile,
                               checkambig=True, mmaplargeindex=True)

        if self._initempty:
            # changelogs don't benefit from generaldelta
            self.version &= ~revlog.FLAG_GENERALDELTA
            self._generaldelta = False

        # Delta chains for changelogs tend to be very small because entries
        # tend to be small and don't delta well with each. So disable delta
        # chains.
        self._storedeltachains = False

        self._realopener = opener
        self._delayed = False
        self._delaybuf = None
        self._divert = False
        self.filteredrevs = frozenset()

    def tiprev(self):
        for i in pycompat.xrange(len(self) -1, -2, -1):
            if i not in self.filteredrevs:
                return i

    def tip(self):
        """filtered version of revlog.tip"""
        return self.node(self.tiprev())

    def __contains__(self, rev):
        """filtered version of revlog.__contains__"""
        return (0 <= rev < len(self)
                and rev not in self.filteredrevs)

    def __iter__(self):
        """filtered version of revlog.__iter__"""
        if len(self.filteredrevs) == 0:
            return revlog.revlog.__iter__(self)

        def filterediter():
            for i in pycompat.xrange(len(self)):
                if i not in self.filteredrevs:
                    yield i

        return filterediter()

    def revs(self, start=0, stop=None):
        """filtered version of revlog.revs"""
        for i in super(changelog, self).revs(start, stop):
            if i not in self.filteredrevs:
                yield i

    def reachableroots(self, minroot, heads, roots, includepath=False):
        return self.index.reachableroots2(minroot, heads, roots, includepath)

    def headrevs(self):
        if self.filteredrevs:
            try:
                return self.index.headrevsfiltered(self.filteredrevs)
            # AttributeError covers non-c-extension environments and
            # old c extensions without filter handling.
            except AttributeError:
                return self._headrevs()

        return super(changelog, self).headrevs()

    def strip(self, *args, **kwargs):
        # XXX make something better than assert
        # We can't expect proper strip behavior if we are filtered.
        assert not self.filteredrevs
        super(changelog, self).strip(*args, **kwargs)

    def rev(self, node):
        """filtered version of revlog.rev"""
        r = super(changelog, self).rev(node)
        if r in self.filteredrevs:
            raise error.FilteredLookupError(hex(node), self.indexfile,
                                            _('filtered node'))
        return r

    def node(self, rev):
        """filtered version of revlog.node"""
        if rev in self.filteredrevs:
            raise error.FilteredIndexError(rev)
        return super(changelog, self).node(rev)

    def linkrev(self, rev):
        """filtered version of revlog.linkrev"""
        if rev in self.filteredrevs:
            raise error.FilteredIndexError(rev)
        return super(changelog, self).linkrev(rev)

    def parentrevs(self, rev):
        """filtered version of revlog.parentrevs"""
        if rev in self.filteredrevs:
            raise error.FilteredIndexError(rev)
        return super(changelog, self).parentrevs(rev)

    def flags(self, rev):
        """filtered version of revlog.flags"""
        if rev in self.filteredrevs:
            raise error.FilteredIndexError(rev)
        return super(changelog, self).flags(rev)

    def delayupdate(self, tr):
        "delay visibility of index updates to other readers"

        if not self._delayed:
            if len(self) == 0:
                self._divert = True
                if self._realopener.exists(self.indexfile + '.a'):
                    self._realopener.unlink(self.indexfile + '.a')
                self.opener = _divertopener(self._realopener, self.indexfile)
            else:
                self._delaybuf = []
                self.opener = _delayopener(self._realopener, self.indexfile,
                                           self._delaybuf)
        self._delayed = True
        tr.addpending('cl-%i' % id(self), self._writepending)
        tr.addfinalize('cl-%i' % id(self), self._finalize)

    def _finalize(self, tr):
        "finalize index updates"
        self._delayed = False
        self.opener = self._realopener
        # move redirected index data back into place
        if self._divert:
            assert not self._delaybuf
            tmpname = self.indexfile + ".a"
            nfile = self.opener.open(tmpname)
            nfile.close()
            self.opener.rename(tmpname, self.indexfile, checkambig=True)
        elif self._delaybuf:
            fp = self.opener(self.indexfile, 'a', checkambig=True)
            fp.write("".join(self._delaybuf))
            fp.close()
            self._delaybuf = None
        self._divert = False
        # split when we're done
        self._enforceinlinesize(tr)

    def _writepending(self, tr):
        "create a file containing the unfinalized state for pretxnchangegroup"
        if self._delaybuf:
            # make a temporary copy of the index
            fp1 = self._realopener(self.indexfile)
            pendingfilename = self.indexfile + ".a"
            # register as a temp file to ensure cleanup on failure
            tr.registertmp(pendingfilename)
            # write existing data
            fp2 = self._realopener(pendingfilename, "w")
            fp2.write(fp1.read())
            # add pending data
            fp2.write("".join(self._delaybuf))
            fp2.close()
            # switch modes so finalize can simply rename
            self._delaybuf = None
            self._divert = True
            self.opener = _divertopener(self._realopener, self.indexfile)

        if self._divert:
            return True

        return False

    def _enforceinlinesize(self, tr, fp=None):
        if not self._delayed:
            revlog.revlog._enforceinlinesize(self, tr, fp)

    def read(self, node):
        """Obtain data from a parsed changelog revision.

        Returns a 6-tuple of:

           - manifest node in binary
           - author/user as a localstr
           - date as a 2-tuple of (time, timezone)
           - list of files
           - commit message as a localstr
           - dict of extra metadata

        Unless you need to access all fields, consider calling
        ``changelogrevision`` instead, as it is faster for partial object
        access.
        """
        c = changelogrevision(self.revision(node))
        return (
            c.manifest,
            c.user,
            c.date,
            c.files,
            c.description,
            c.extra
        )

    def changelogrevision(self, nodeorrev):
        """Obtain a ``changelogrevision`` for a node or revision."""
        return changelogrevision(self.revision(nodeorrev))

    def readfiles(self, node):
        """
        short version of read that only returns the files modified by the cset
        """
        text = self.revision(node)
        if not text:
            return []
        last = text.index("\n\n")
        l = text[:last].split('\n')
        return l[3:]

    def add(self, manifest, files, desc, transaction, p1, p2,
                  user, date=None, extra=None):
        # Convert to UTF-8 encoded bytestrings as the very first
        # thing: calling any method on a localstr object will turn it
        # into a str object and the cached UTF-8 string is thus lost.
        user, desc = encoding.fromlocal(user), encoding.fromlocal(desc)

        user = user.strip()
        # An empty username or a username with a "\n" will make the
        # revision text contain two "\n\n" sequences -> corrupt
        # repository since read cannot unpack the revision.
        if not user:
            raise error.StorageError(_("empty username"))
        if "\n" in user:
            raise error.StorageError(_("username %r contains a newline")
                                     % pycompat.bytestr(user))

        desc = stripdesc(desc)

        if date:
            parseddate = "%d %d" % dateutil.parsedate(date)
        else:
            parseddate = "%d %d" % dateutil.makedate()
        if extra:
            branch = extra.get("branch")
            if branch in ("default", ""):
                del extra["branch"]
            elif branch in (".", "null", "tip"):
                raise error.StorageError(_('the name \'%s\' is reserved')
                                         % branch)
        if extra:
            extra = encodeextra(extra)
            parseddate = "%s %s" % (parseddate, extra)
        l = [hex(manifest), user, parseddate] + sorted(files) + ["", desc]
        text = "\n".join(l)
        return self.addrevision(text, transaction, len(self), p1, p2)

    def branchinfo(self, rev):
        """return the branch name and open/close state of a revision

        This function exists because creating a changectx object
        just to access this is costly."""
        extra = self.read(rev)[5]
        return encoding.tolocal(extra.get("branch")), 'close' in extra

    def _nodeduplicatecallback(self, transaction, node):
        # keep track of revisions that got "re-added", eg: unbunde of know rev.
        #
        # We track them in a list to preserve their order from the source bundle
        duplicates = transaction.changes.setdefault('revduplicates', [])
        duplicates.append(self.rev(node))
