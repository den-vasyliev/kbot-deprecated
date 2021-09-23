# templatefilters.py - common template expansion filters
#
# Copyright 2005-2008 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import os
import re
import time

from .i18n import _
from . import (
    encoding,
    error,
    node,
    pycompat,
    registrar,
    templateutil,
    url,
    util,
)
from .utils import (
    dateutil,
    stringutil,
)

urlerr = util.urlerr
urlreq = util.urlreq

if pycompat.ispy3:
    long = int

# filters are callables like:
#   fn(obj)
# with:
#   obj - object to be filtered (text, date, list and so on)
filters = {}

templatefilter = registrar.templatefilter(filters)

@templatefilter('addbreaks', intype=bytes)
def addbreaks(text):
    """Any text. Add an XHTML "<br />" tag before the end of
    every line except the last.
    """
    return text.replace('\n', '<br/>\n')

agescales = [("year", 3600 * 24 * 365, 'Y'),
             ("month", 3600 * 24 * 30, 'M'),
             ("week", 3600 * 24 * 7, 'W'),
             ("day", 3600 * 24, 'd'),
             ("hour", 3600, 'h'),
             ("minute", 60, 'm'),
             ("second", 1, 's')]

@templatefilter('age', intype=templateutil.date)
def age(date, abbrev=False):
    """Date. Returns a human-readable date/time difference between the
    given date/time and the current date/time.
    """

    def plural(t, c):
        if c == 1:
            return t
        return t + "s"
    def fmt(t, c, a):
        if abbrev:
            return "%d%s" % (c, a)
        return "%d %s" % (c, plural(t, c))

    now = time.time()
    then = date[0]
    future = False
    if then > now:
        future = True
        delta = max(1, int(then - now))
        if delta > agescales[0][1] * 30:
            return 'in the distant future'
    else:
        delta = max(1, int(now - then))
        if delta > agescales[0][1] * 2:
            return dateutil.shortdate(date)

    for t, s, a in agescales:
        n = delta // s
        if n >= 2 or s == 1:
            if future:
                return '%s from now' % fmt(t, n, a)
            return '%s ago' % fmt(t, n, a)

@templatefilter('basename', intype=bytes)
def basename(path):
    """Any text. Treats the text as a path, and returns the last
    component of the path after splitting by the path separator.
    For example, "foo/bar/baz" becomes "baz" and "foo/bar//" becomes "".
    """
    return os.path.basename(path)

@templatefilter('commondir')
def commondir(filelist):
    """List of text. Treats each list item as file name with /
    as path separator and returns the longest common directory
    prefix shared by all list items.
    Returns the empty string if no common prefix exists.

    The list items are not normalized, i.e. "foo/../bar" is handled as
    file "bar" in the directory "foo/..". Leading slashes are ignored.

    For example, ["foo/bar/baz", "foo/baz/bar"] becomes "foo" and
    ["foo/bar", "baz"] becomes "".
    """
    def common(a, b):
        if len(a) > len(b):
            a = b[:len(a)]
        elif len(b) > len(a):
            b = b[:len(a)]
        if a == b:
            return a
        for i in pycompat.xrange(len(a)):
            if a[i] != b[i]:
                return a[:i]
        return a
    try:
        if not filelist:
            return ""
        dirlist = [f.lstrip('/').split('/')[:-1] for f in filelist]
        if len(dirlist) == 1:
            return '/'.join(dirlist[0])
        a = min(dirlist)
        b = max(dirlist)
        # The common prefix of a and b is shared with all
        # elements of the list since Python sorts lexicographical
        # and [1, x] after [1].
        return '/'.join(common(a, b))
    except TypeError:
        raise error.ParseError(_('argument is not a list of text'))

@templatefilter('count')
def count(i):
    """List or text. Returns the length as an integer."""
    try:
        return len(i)
    except TypeError:
        raise error.ParseError(_('not countable'))

@templatefilter('dirname', intype=bytes)
def dirname(path):
    """Any text. Treats the text as a path, and strips the last
    component of the path after splitting by the path separator.
    """
    return os.path.dirname(path)

@templatefilter('domain', intype=bytes)
def domain(author):
    """Any text. Finds the first string that looks like an email
    address, and extracts just the domain component. Example: ``User
    <user@example.com>`` becomes ``example.com``.
    """
    f = author.find('@')
    if f == -1:
        return ''
    author = author[f + 1:]
    f = author.find('>')
    if f >= 0:
        author = author[:f]
    return author

@templatefilter('email', intype=bytes)
def email(text):
    """Any text. Extracts the first string that looks like an email
    address. Example: ``User <user@example.com>`` becomes
    ``user@example.com``.
    """
    return stringutil.email(text)

@templatefilter('escape', intype=bytes)
def escape(text):
    """Any text. Replaces the special XML/XHTML characters "&", "<"
    and ">" with XML entities, and filters out NUL characters.
    """
    return url.escape(text.replace('\0', ''), True)

para_re = None
space_re = None

def fill(text, width, initindent='', hangindent=''):
    '''fill many paragraphs with optional indentation.'''
    global para_re, space_re
    if para_re is None:
        para_re = re.compile('(\n\n|\n\\s*[-*]\\s*)', re.M)
        space_re = re.compile(br'  +')

    def findparas():
        start = 0
        while True:
            m = para_re.search(text, start)
            if not m:
                uctext = encoding.unifromlocal(text[start:])
                w = len(uctext)
                while w > 0 and uctext[w - 1].isspace():
                    w -= 1
                yield (encoding.unitolocal(uctext[:w]),
                       encoding.unitolocal(uctext[w:]))
                break
            yield text[start:m.start(0)], m.group(1)
            start = m.end(1)

    return "".join([stringutil.wrap(space_re.sub(' ',
                                                 stringutil.wrap(para, width)),
                                    width, initindent, hangindent) + rest
                    for para, rest in findparas()])

@templatefilter('fill68', intype=bytes)
def fill68(text):
    """Any text. Wraps the text to fit in 68 columns."""
    return fill(text, 68)

@templatefilter('fill76', intype=bytes)
def fill76(text):
    """Any text. Wraps the text to fit in 76 columns."""
    return fill(text, 76)

@templatefilter('firstline', intype=bytes)
def firstline(text):
    """Any text. Returns the first line of text."""
    try:
        return text.splitlines(True)[0].rstrip('\r\n')
    except IndexError:
        return ''

@templatefilter('hex', intype=bytes)
def hexfilter(text):
    """Any text. Convert a binary Mercurial node identifier into
    its long hexadecimal representation.
    """
    return node.hex(text)

@templatefilter('hgdate', intype=templateutil.date)
def hgdate(text):
    """Date. Returns the date as a pair of numbers: "1157407993
    25200" (Unix timestamp, timezone offset).
    """
    return "%d %d" % text

@templatefilter('isodate', intype=templateutil.date)
def isodate(text):
    """Date. Returns the date in ISO 8601 format: "2009-08-18 13:00
    +0200".
    """
    return dateutil.datestr(text, '%Y-%m-%d %H:%M %1%2')

@templatefilter('isodatesec', intype=templateutil.date)
def isodatesec(text):
    """Date. Returns the date in ISO 8601 format, including
    seconds: "2009-08-18 13:00:13 +0200". See also the rfc3339date
    filter.
    """
    return dateutil.datestr(text, '%Y-%m-%d %H:%M:%S %1%2')

def indent(text, prefix):
    '''indent each non-empty line of text after first with prefix.'''
    lines = text.splitlines()
    num_lines = len(lines)
    endswithnewline = text[-1:] == '\n'
    def indenter():
        for i in pycompat.xrange(num_lines):
            l = lines[i]
            if i and l.strip():
                yield prefix
            yield l
            if i < num_lines - 1 or endswithnewline:
                yield '\n'
    return "".join(indenter())

@templatefilter('json')
def json(obj, paranoid=True):
    """Any object. Serializes the object to a JSON formatted text."""
    if obj is None:
        return 'null'
    elif obj is False:
        return 'false'
    elif obj is True:
        return 'true'
    elif isinstance(obj, (int, long, float)):
        return pycompat.bytestr(obj)
    elif isinstance(obj, bytes):
        return '"%s"' % encoding.jsonescape(obj, paranoid=paranoid)
    elif isinstance(obj, type(u'')):
        raise error.ProgrammingError(
            'Mercurial only does output with bytes: %r' % obj)
    elif util.safehasattr(obj, 'keys'):
        out = ['"%s": %s' % (encoding.jsonescape(k, paranoid=paranoid),
                             json(v, paranoid))
               for k, v in sorted(obj.iteritems())]
        return '{' + ', '.join(out) + '}'
    elif util.safehasattr(obj, '__iter__'):
        out = [json(i, paranoid) for i in obj]
        return '[' + ', '.join(out) + ']'
    raise error.ProgrammingError('cannot encode %r' % obj)

@templatefilter('lower', intype=bytes)
def lower(text):
    """Any text. Converts the text to lowercase."""
    return encoding.lower(text)

@templatefilter('nonempty', intype=bytes)
def nonempty(text):
    """Any text. Returns '(none)' if the string is empty."""
    return text or "(none)"

@templatefilter('obfuscate', intype=bytes)
def obfuscate(text):
    """Any text. Returns the input text rendered as a sequence of
    XML entities.
    """
    text = unicode(text, pycompat.sysstr(encoding.encoding), r'replace')
    return ''.join(['&#%d;' % ord(c) for c in text])

@templatefilter('permissions', intype=bytes)
def permissions(flags):
    if "l" in flags:
        return "lrwxrwxrwx"
    if "x" in flags:
        return "-rwxr-xr-x"
    return "-rw-r--r--"

@templatefilter('person', intype=bytes)
def person(author):
    """Any text. Returns the name before an email address,
    interpreting it as per RFC 5322.
    """
    return stringutil.person(author)

@templatefilter('revescape', intype=bytes)
def revescape(text):
    """Any text. Escapes all "special" characters, except @.
    Forward slashes are escaped twice to prevent web servers from prematurely
    unescaping them. For example, "@foo bar/baz" becomes "@foo%20bar%252Fbaz".
    """
    return urlreq.quote(text, safe='/@').replace('/', '%252F')

@templatefilter('rfc3339date', intype=templateutil.date)
def rfc3339date(text):
    """Date. Returns a date using the Internet date format
    specified in RFC 3339: "2009-08-18T13:00:13+02:00".
    """
    return dateutil.datestr(text, "%Y-%m-%dT%H:%M:%S%1:%2")

@templatefilter('rfc822date', intype=templateutil.date)
def rfc822date(text):
    """Date. Returns a date using the same format used in email
    headers: "Tue, 18 Aug 2009 13:00:13 +0200".
    """
    return dateutil.datestr(text, "%a, %d %b %Y %H:%M:%S %1%2")

@templatefilter('short', intype=bytes)
def short(text):
    """Changeset hash. Returns the short form of a changeset hash,
    i.e. a 12 hexadecimal digit string.
    """
    return text[:12]

@templatefilter('shortbisect', intype=bytes)
def shortbisect(label):
    """Any text. Treats `label` as a bisection status, and
    returns a single-character representing the status (G: good, B: bad,
    S: skipped, U: untested, I: ignored). Returns single space if `text`
    is not a valid bisection status.
    """
    if label:
        return label[0:1].upper()
    return ' '

@templatefilter('shortdate', intype=templateutil.date)
def shortdate(text):
    """Date. Returns a date like "2006-09-18"."""
    return dateutil.shortdate(text)

@templatefilter('slashpath', intype=bytes)
def slashpath(path):
    """Any text. Replaces the native path separator with slash."""
    return util.pconvert(path)

@templatefilter('splitlines', intype=bytes)
def splitlines(text):
    """Any text. Split text into a list of lines."""
    return templateutil.hybridlist(text.splitlines(), name='line')

@templatefilter('stringescape', intype=bytes)
def stringescape(text):
    return stringutil.escapestr(text)

@templatefilter('stringify', intype=bytes)
def stringify(thing):
    """Any type. Turns the value into text by converting values into
    text and concatenating them.
    """
    return thing  # coerced by the intype

@templatefilter('stripdir', intype=bytes)
def stripdir(text):
    """Treat the text as path and strip a directory level, if
    possible. For example, "foo" and "foo/bar" becomes "foo".
    """
    dir = os.path.dirname(text)
    if dir == "":
        return os.path.basename(text)
    else:
        return dir

@templatefilter('tabindent', intype=bytes)
def tabindent(text):
    """Any text. Returns the text, with every non-empty line
    except the first starting with a tab character.
    """
    return indent(text, '\t')

@templatefilter('upper', intype=bytes)
def upper(text):
    """Any text. Converts the text to uppercase."""
    return encoding.upper(text)

@templatefilter('urlescape', intype=bytes)
def urlescape(text):
    """Any text. Escapes all "special" characters. For example,
    "foo bar" becomes "foo%20bar".
    """
    return urlreq.quote(text)

@templatefilter('user', intype=bytes)
def userfilter(text):
    """Any text. Returns a short representation of a user name or email
    address."""
    return stringutil.shortuser(text)

@templatefilter('emailuser', intype=bytes)
def emailuser(text):
    """Any text. Returns the user portion of an email address."""
    return stringutil.emailuser(text)

@templatefilter('utf8', intype=bytes)
def utf8(text):
    """Any text. Converts from the local character encoding to UTF-8."""
    return encoding.fromlocal(text)

@templatefilter('xmlescape', intype=bytes)
def xmlescape(text):
    text = (text
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;')
            .replace('"', '&quot;')
            .replace("'", '&#39;')) # &apos; invalid in HTML
    return re.sub('[\x00-\x08\x0B\x0C\x0E-\x1F]', ' ', text)

def websub(text, websubtable):
    """:websub: Any text. Only applies to hgweb. Applies the regular
    expression replacements defined in the websub section.
    """
    if websubtable:
        for regexp, format in websubtable:
            text = regexp.sub(format, text)
    return text

def loadfilter(ui, extname, registrarobj):
    """Load template filter from specified registrarobj
    """
    for name, func in registrarobj._table.iteritems():
        filters[name] = func

# tell hggettext to extract docstrings from these functions:
i18nfunctions = filters.values()
