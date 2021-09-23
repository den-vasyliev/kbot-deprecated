# templatefuncs.py - common template functions
#
# Copyright 2005, 2006 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import re

from .i18n import _
from .node import (
    bin,
    wdirid,
)
from . import (
    color,
    encoding,
    error,
    minirst,
    obsutil,
    registrar,
    revset as revsetmod,
    revsetlang,
    scmutil,
    templatefilters,
    templatekw,
    templateutil,
    util,
)
from .utils import (
    dateutil,
    stringutil,
)

evalrawexp = templateutil.evalrawexp
evalwrapped = templateutil.evalwrapped
evalfuncarg = templateutil.evalfuncarg
evalboolean = templateutil.evalboolean
evaldate = templateutil.evaldate
evalinteger = templateutil.evalinteger
evalstring = templateutil.evalstring
evalstringliteral = templateutil.evalstringliteral

# dict of template built-in functions
funcs = {}
templatefunc = registrar.templatefunc(funcs)

@templatefunc('date(date[, fmt])')
def date(context, mapping, args):
    """Format a date. See :hg:`help dates` for formatting
    strings. The default is a Unix date format, including the timezone:
    "Mon Sep 04 15:13:13 2006 0700"."""
    if not (1 <= len(args) <= 2):
        # i18n: "date" is a keyword
        raise error.ParseError(_("date expects one or two arguments"))

    date = evaldate(context, mapping, args[0],
                    # i18n: "date" is a keyword
                    _("date expects a date information"))
    fmt = None
    if len(args) == 2:
        fmt = evalstring(context, mapping, args[1])
    if fmt is None:
        return dateutil.datestr(date)
    else:
        return dateutil.datestr(date, fmt)

@templatefunc('dict([[key=]value...])', argspec='*args **kwargs')
def dict_(context, mapping, args):
    """Construct a dict from key-value pairs. A key may be omitted if
    a value expression can provide an unambiguous name."""
    data = util.sortdict()

    for v in args['args']:
        k = templateutil.findsymbolicname(v)
        if not k:
            raise error.ParseError(_('dict key cannot be inferred'))
        if k in data or k in args['kwargs']:
            raise error.ParseError(_("duplicated dict key '%s' inferred") % k)
        data[k] = evalfuncarg(context, mapping, v)

    data.update((k, evalfuncarg(context, mapping, v))
                for k, v in args['kwargs'].iteritems())
    return templateutil.hybriddict(data)

@templatefunc('diff([includepattern [, excludepattern]])', requires={'ctx'})
def diff(context, mapping, args):
    """Show a diff, optionally
    specifying files to include or exclude."""
    if len(args) > 2:
        # i18n: "diff" is a keyword
        raise error.ParseError(_("diff expects zero, one, or two arguments"))

    def getpatterns(i):
        if i < len(args):
            s = evalstring(context, mapping, args[i]).strip()
            if s:
                return [s]
        return []

    ctx = context.resource(mapping, 'ctx')
    chunks = ctx.diff(match=ctx.match([], getpatterns(0), getpatterns(1)))

    return ''.join(chunks)

@templatefunc('extdata(source)', argspec='source', requires={'ctx', 'cache'})
def extdata(context, mapping, args):
    """Show a text read from the specified extdata source. (EXPERIMENTAL)"""
    if 'source' not in args:
        # i18n: "extdata" is a keyword
        raise error.ParseError(_('extdata expects one argument'))

    source = evalstring(context, mapping, args['source'])
    if not source:
        sym = templateutil.findsymbolicname(args['source'])
        if sym:
            raise error.ParseError(_('empty data source specified'),
                                   hint=_("did you mean extdata('%s')?") % sym)
        else:
            raise error.ParseError(_('empty data source specified'))
    cache = context.resource(mapping, 'cache').setdefault('extdata', {})
    ctx = context.resource(mapping, 'ctx')
    if source in cache:
        data = cache[source]
    else:
        data = cache[source] = scmutil.extdatasource(ctx.repo(), source)
    return data.get(ctx.rev(), '')

@templatefunc('files(pattern)', requires={'ctx'})
def files(context, mapping, args):
    """All files of the current changeset matching the pattern. See
    :hg:`help patterns`."""
    if not len(args) == 1:
        # i18n: "files" is a keyword
        raise error.ParseError(_("files expects one argument"))

    raw = evalstring(context, mapping, args[0])
    ctx = context.resource(mapping, 'ctx')
    m = ctx.match([raw])
    files = list(ctx.matches(m))
    return templateutil.compatfileslist(context, mapping, "file", files)

@templatefunc('fill(text[, width[, initialident[, hangindent]]])')
def fill(context, mapping, args):
    """Fill many
    paragraphs with optional indentation. See the "fill" filter."""
    if not (1 <= len(args) <= 4):
        # i18n: "fill" is a keyword
        raise error.ParseError(_("fill expects one to four arguments"))

    text = evalstring(context, mapping, args[0])
    width = 76
    initindent = ''
    hangindent = ''
    if 2 <= len(args) <= 4:
        width = evalinteger(context, mapping, args[1],
                            # i18n: "fill" is a keyword
                            _("fill expects an integer width"))
        try:
            initindent = evalstring(context, mapping, args[2])
            hangindent = evalstring(context, mapping, args[3])
        except IndexError:
            pass

    return templatefilters.fill(text, width, initindent, hangindent)

@templatefunc('filter(iterable[, expr])')
def filter_(context, mapping, args):
    """Remove empty elements from a list or a dict. If expr specified, it's
    applied to each element to test emptiness."""
    if not (1 <= len(args) <= 2):
        # i18n: "filter" is a keyword
        raise error.ParseError(_("filter expects one or two arguments"))
    iterable = evalwrapped(context, mapping, args[0])
    if len(args) == 1:
        def select(w):
            return w.tobool(context, mapping)
    else:
        def select(w):
            if not isinstance(w, templateutil.mappable):
                raise error.ParseError(_("not filterable by expression"))
            lm = context.overlaymap(mapping, w.tomap(context))
            return evalboolean(context, lm, args[1])
    return iterable.filter(context, mapping, select)

@templatefunc('formatnode(node)', requires={'ui'})
def formatnode(context, mapping, args):
    """Obtain the preferred form of a changeset hash. (DEPRECATED)"""
    if len(args) != 1:
        # i18n: "formatnode" is a keyword
        raise error.ParseError(_("formatnode expects one argument"))

    ui = context.resource(mapping, 'ui')
    node = evalstring(context, mapping, args[0])
    if ui.debugflag:
        return node
    return templatefilters.short(node)

@templatefunc('mailmap(author)', requires={'repo', 'cache'})
def mailmap(context, mapping, args):
    """Return the author, updated according to the value
    set in the .mailmap file"""
    if len(args) != 1:
        raise error.ParseError(_("mailmap expects one argument"))

    author = evalstring(context, mapping, args[0])

    cache = context.resource(mapping, 'cache')
    repo = context.resource(mapping, 'repo')

    if 'mailmap' not in cache:
        data = repo.wvfs.tryread('.mailmap')
        cache['mailmap'] = stringutil.parsemailmap(data)

    return stringutil.mapname(cache['mailmap'], author)

@templatefunc(
    'pad(text, width[, fillchar=\' \'[, left=False[, truncate=False]]])',
    argspec='text width fillchar left truncate')
def pad(context, mapping, args):
    """Pad text with a
    fill character."""
    if 'text' not in args or 'width' not in args:
        # i18n: "pad" is a keyword
        raise error.ParseError(_("pad() expects two to four arguments"))

    width = evalinteger(context, mapping, args['width'],
                        # i18n: "pad" is a keyword
                        _("pad() expects an integer width"))

    text = evalstring(context, mapping, args['text'])

    truncate = False
    left = False
    fillchar = ' '
    if 'fillchar' in args:
        fillchar = evalstring(context, mapping, args['fillchar'])
        if len(color.stripeffects(fillchar)) != 1:
            # i18n: "pad" is a keyword
            raise error.ParseError(_("pad() expects a single fill character"))
    if 'left' in args:
        left = evalboolean(context, mapping, args['left'])
    if 'truncate' in args:
        truncate = evalboolean(context, mapping, args['truncate'])

    fillwidth = width - encoding.colwidth(color.stripeffects(text))
    if fillwidth < 0 and truncate:
        return encoding.trim(color.stripeffects(text), width, leftside=left)
    if fillwidth <= 0:
        return text
    if left:
        return fillchar * fillwidth + text
    else:
        return text + fillchar * fillwidth

@templatefunc('indent(text, indentchars[, firstline])')
def indent(context, mapping, args):
    """Indents all non-empty lines
    with the characters given in the indentchars string. An optional
    third parameter will override the indent for the first line only
    if present."""
    if not (2 <= len(args) <= 3):
        # i18n: "indent" is a keyword
        raise error.ParseError(_("indent() expects two or three arguments"))

    text = evalstring(context, mapping, args[0])
    indent = evalstring(context, mapping, args[1])

    if len(args) == 3:
        firstline = evalstring(context, mapping, args[2])
    else:
        firstline = indent

    # the indent function doesn't indent the first line, so we do it here
    return templatefilters.indent(firstline + text, indent)

@templatefunc('get(dict, key)')
def get(context, mapping, args):
    """Get an attribute/key from an object. Some keywords
    are complex types. This function allows you to obtain the value of an
    attribute on these types."""
    if len(args) != 2:
        # i18n: "get" is a keyword
        raise error.ParseError(_("get() expects two arguments"))

    dictarg = evalwrapped(context, mapping, args[0])
    key = evalrawexp(context, mapping, args[1])
    try:
        return dictarg.getmember(context, mapping, key)
    except error.ParseError as err:
        # i18n: "get" is a keyword
        hint = _("get() expects a dict as first argument")
        raise error.ParseError(bytes(err), hint=hint)

@templatefunc('if(expr, then[, else])')
def if_(context, mapping, args):
    """Conditionally execute based on the result of
    an expression."""
    if not (2 <= len(args) <= 3):
        # i18n: "if" is a keyword
        raise error.ParseError(_("if expects two or three arguments"))

    test = evalboolean(context, mapping, args[0])
    if test:
        return evalrawexp(context, mapping, args[1])
    elif len(args) == 3:
        return evalrawexp(context, mapping, args[2])

@templatefunc('ifcontains(needle, haystack, then[, else])')
def ifcontains(context, mapping, args):
    """Conditionally execute based
    on whether the item "needle" is in "haystack"."""
    if not (3 <= len(args) <= 4):
        # i18n: "ifcontains" is a keyword
        raise error.ParseError(_("ifcontains expects three or four arguments"))

    haystack = evalwrapped(context, mapping, args[1])
    try:
        needle = evalrawexp(context, mapping, args[0])
        found = haystack.contains(context, mapping, needle)
    except error.ParseError:
        found = False

    if found:
        return evalrawexp(context, mapping, args[2])
    elif len(args) == 4:
        return evalrawexp(context, mapping, args[3])

@templatefunc('ifeq(expr1, expr2, then[, else])')
def ifeq(context, mapping, args):
    """Conditionally execute based on
    whether 2 items are equivalent."""
    if not (3 <= len(args) <= 4):
        # i18n: "ifeq" is a keyword
        raise error.ParseError(_("ifeq expects three or four arguments"))

    test = evalstring(context, mapping, args[0])
    match = evalstring(context, mapping, args[1])
    if test == match:
        return evalrawexp(context, mapping, args[2])
    elif len(args) == 4:
        return evalrawexp(context, mapping, args[3])

@templatefunc('join(list, sep)')
def join(context, mapping, args):
    """Join items in a list with a delimiter."""
    if not (1 <= len(args) <= 2):
        # i18n: "join" is a keyword
        raise error.ParseError(_("join expects one or two arguments"))

    joinset = evalwrapped(context, mapping, args[0])
    joiner = " "
    if len(args) > 1:
        joiner = evalstring(context, mapping, args[1])
    return joinset.join(context, mapping, joiner)

@templatefunc('label(label, expr)', requires={'ui'})
def label(context, mapping, args):
    """Apply a label to generated content. Content with
    a label applied can result in additional post-processing, such as
    automatic colorization."""
    if len(args) != 2:
        # i18n: "label" is a keyword
        raise error.ParseError(_("label expects two arguments"))

    ui = context.resource(mapping, 'ui')
    thing = evalstring(context, mapping, args[1])
    # preserve unknown symbol as literal so effects like 'red', 'bold',
    # etc. don't need to be quoted
    label = evalstringliteral(context, mapping, args[0])

    return ui.label(thing, label)

@templatefunc('latesttag([pattern])')
def latesttag(context, mapping, args):
    """The global tags matching the given pattern on the
    most recent globally tagged ancestor of this changeset.
    If no such tags exist, the "{tag}" template resolves to
    the string "null". See :hg:`help revisions.patterns` for the pattern
    syntax.
    """
    if len(args) > 1:
        # i18n: "latesttag" is a keyword
        raise error.ParseError(_("latesttag expects at most one argument"))

    pattern = None
    if len(args) == 1:
        pattern = evalstring(context, mapping, args[0])
    return templatekw.showlatesttags(context, mapping, pattern)

@templatefunc('localdate(date[, tz])')
def localdate(context, mapping, args):
    """Converts a date to the specified timezone.
    The default is local date."""
    if not (1 <= len(args) <= 2):
        # i18n: "localdate" is a keyword
        raise error.ParseError(_("localdate expects one or two arguments"))

    date = evaldate(context, mapping, args[0],
                    # i18n: "localdate" is a keyword
                    _("localdate expects a date information"))
    if len(args) >= 2:
        tzoffset = None
        tz = evalfuncarg(context, mapping, args[1])
        if isinstance(tz, bytes):
            tzoffset, remainder = dateutil.parsetimezone(tz)
            if remainder:
                tzoffset = None
        if tzoffset is None:
            try:
                tzoffset = int(tz)
            except (TypeError, ValueError):
                # i18n: "localdate" is a keyword
                raise error.ParseError(_("localdate expects a timezone"))
    else:
        tzoffset = dateutil.makedate()[1]
    return templateutil.date((date[0], tzoffset))

@templatefunc('max(iterable)')
def max_(context, mapping, args, **kwargs):
    """Return the max of an iterable"""
    if len(args) != 1:
        # i18n: "max" is a keyword
        raise error.ParseError(_("max expects one argument"))

    iterable = evalwrapped(context, mapping, args[0])
    try:
        return iterable.getmax(context, mapping)
    except error.ParseError as err:
        # i18n: "max" is a keyword
        hint = _("max first argument should be an iterable")
        raise error.ParseError(bytes(err), hint=hint)

@templatefunc('min(iterable)')
def min_(context, mapping, args, **kwargs):
    """Return the min of an iterable"""
    if len(args) != 1:
        # i18n: "min" is a keyword
        raise error.ParseError(_("min expects one argument"))

    iterable = evalwrapped(context, mapping, args[0])
    try:
        return iterable.getmin(context, mapping)
    except error.ParseError as err:
        # i18n: "min" is a keyword
        hint = _("min first argument should be an iterable")
        raise error.ParseError(bytes(err), hint=hint)

@templatefunc('mod(a, b)')
def mod(context, mapping, args):
    """Calculate a mod b such that a / b + a mod b == a"""
    if not len(args) == 2:
        # i18n: "mod" is a keyword
        raise error.ParseError(_("mod expects two arguments"))

    func = lambda a, b: a % b
    return templateutil.runarithmetic(context, mapping,
                                      (func, args[0], args[1]))

@templatefunc('obsfateoperations(markers)')
def obsfateoperations(context, mapping, args):
    """Compute obsfate related information based on markers (EXPERIMENTAL)"""
    if len(args) != 1:
        # i18n: "obsfateoperations" is a keyword
        raise error.ParseError(_("obsfateoperations expects one argument"))

    markers = evalfuncarg(context, mapping, args[0])

    try:
        data = obsutil.markersoperations(markers)
        return templateutil.hybridlist(data, name='operation')
    except (TypeError, KeyError):
        # i18n: "obsfateoperations" is a keyword
        errmsg = _("obsfateoperations first argument should be an iterable")
        raise error.ParseError(errmsg)

@templatefunc('obsfatedate(markers)')
def obsfatedate(context, mapping, args):
    """Compute obsfate related information based on markers (EXPERIMENTAL)"""
    if len(args) != 1:
        # i18n: "obsfatedate" is a keyword
        raise error.ParseError(_("obsfatedate expects one argument"))

    markers = evalfuncarg(context, mapping, args[0])

    try:
        # TODO: maybe this has to be a wrapped list of date wrappers?
        data = obsutil.markersdates(markers)
        return templateutil.hybridlist(data, name='date', fmt='%d %d')
    except (TypeError, KeyError):
        # i18n: "obsfatedate" is a keyword
        errmsg = _("obsfatedate first argument should be an iterable")
        raise error.ParseError(errmsg)

@templatefunc('obsfateusers(markers)')
def obsfateusers(context, mapping, args):
    """Compute obsfate related information based on markers (EXPERIMENTAL)"""
    if len(args) != 1:
        # i18n: "obsfateusers" is a keyword
        raise error.ParseError(_("obsfateusers expects one argument"))

    markers = evalfuncarg(context, mapping, args[0])

    try:
        data = obsutil.markersusers(markers)
        return templateutil.hybridlist(data, name='user')
    except (TypeError, KeyError, ValueError):
        # i18n: "obsfateusers" is a keyword
        msg = _("obsfateusers first argument should be an iterable of "
                "obsmakers")
        raise error.ParseError(msg)

@templatefunc('obsfateverb(successors, markers)')
def obsfateverb(context, mapping, args):
    """Compute obsfate related information based on successors (EXPERIMENTAL)"""
    if len(args) != 2:
        # i18n: "obsfateverb" is a keyword
        raise error.ParseError(_("obsfateverb expects two arguments"))

    successors = evalfuncarg(context, mapping, args[0])
    markers = evalfuncarg(context, mapping, args[1])

    try:
        return obsutil.obsfateverb(successors, markers)
    except TypeError:
        # i18n: "obsfateverb" is a keyword
        errmsg = _("obsfateverb first argument should be countable")
        raise error.ParseError(errmsg)

@templatefunc('relpath(path)', requires={'repo'})
def relpath(context, mapping, args):
    """Convert a repository-absolute path into a filesystem path relative to
    the current working directory."""
    if len(args) != 1:
        # i18n: "relpath" is a keyword
        raise error.ParseError(_("relpath expects one argument"))

    repo = context.resource(mapping, 'repo')
    path = evalstring(context, mapping, args[0])
    return repo.pathto(path)

@templatefunc('revset(query[, formatargs...])', requires={'repo', 'cache'})
def revset(context, mapping, args):
    """Execute a revision set query. See
    :hg:`help revset`."""
    if not len(args) > 0:
        # i18n: "revset" is a keyword
        raise error.ParseError(_("revset expects one or more arguments"))

    raw = evalstring(context, mapping, args[0])
    repo = context.resource(mapping, 'repo')

    def query(expr):
        m = revsetmod.match(repo.ui, expr, lookup=revsetmod.lookupfn(repo))
        return m(repo)

    if len(args) > 1:
        formatargs = [evalfuncarg(context, mapping, a) for a in args[1:]]
        revs = query(revsetlang.formatspec(raw, *formatargs))
        revs = list(revs)
    else:
        cache = context.resource(mapping, 'cache')
        revsetcache = cache.setdefault("revsetcache", {})
        if raw in revsetcache:
            revs = revsetcache[raw]
        else:
            revs = query(raw)
            revs = list(revs)
            revsetcache[raw] = revs
    return templatekw.showrevslist(context, mapping, "revision", revs)

@templatefunc('rstdoc(text, style)')
def rstdoc(context, mapping, args):
    """Format reStructuredText."""
    if len(args) != 2:
        # i18n: "rstdoc" is a keyword
        raise error.ParseError(_("rstdoc expects two arguments"))

    text = evalstring(context, mapping, args[0])
    style = evalstring(context, mapping, args[1])

    return minirst.format(text, style=style, keep=['verbose'])

@templatefunc('separate(sep, args...)', argspec='sep *args')
def separate(context, mapping, args):
    """Add a separator between non-empty arguments."""
    if 'sep' not in args:
        # i18n: "separate" is a keyword
        raise error.ParseError(_("separate expects at least one argument"))

    sep = evalstring(context, mapping, args['sep'])
    first = True
    for arg in args['args']:
        argstr = evalstring(context, mapping, arg)
        if not argstr:
            continue
        if first:
            first = False
        else:
            yield sep
        yield argstr

@templatefunc('shortest(node, minlength=4)', requires={'repo', 'cache'})
def shortest(context, mapping, args):
    """Obtain the shortest representation of
    a node."""
    if not (1 <= len(args) <= 2):
        # i18n: "shortest" is a keyword
        raise error.ParseError(_("shortest() expects one or two arguments"))

    hexnode = evalstring(context, mapping, args[0])

    minlength = 4
    if len(args) > 1:
        minlength = evalinteger(context, mapping, args[1],
                                # i18n: "shortest" is a keyword
                                _("shortest() expects an integer minlength"))

    repo = context.resource(mapping, 'repo')
    if len(hexnode) > 40:
        return hexnode
    elif len(hexnode) == 40:
        try:
            node = bin(hexnode)
        except TypeError:
            return hexnode
    else:
        try:
            node = scmutil.resolvehexnodeidprefix(repo, hexnode)
        except error.WdirUnsupported:
            node = wdirid
        except error.LookupError:
            return hexnode
        if not node:
            return hexnode
    cache = context.resource(mapping, 'cache')
    try:
        return scmutil.shortesthexnodeidprefix(repo, node, minlength, cache)
    except error.RepoLookupError:
        return hexnode

@templatefunc('strip(text[, chars])')
def strip(context, mapping, args):
    """Strip characters from a string. By default,
    strips all leading and trailing whitespace."""
    if not (1 <= len(args) <= 2):
        # i18n: "strip" is a keyword
        raise error.ParseError(_("strip expects one or two arguments"))

    text = evalstring(context, mapping, args[0])
    if len(args) == 2:
        chars = evalstring(context, mapping, args[1])
        return text.strip(chars)
    return text.strip()

@templatefunc('sub(pattern, replacement, expression)')
def sub(context, mapping, args):
    """Perform text substitution
    using regular expressions."""
    if len(args) != 3:
        # i18n: "sub" is a keyword
        raise error.ParseError(_("sub expects three arguments"))

    pat = evalstring(context, mapping, args[0])
    rpl = evalstring(context, mapping, args[1])
    src = evalstring(context, mapping, args[2])
    try:
        patre = re.compile(pat)
    except re.error:
        # i18n: "sub" is a keyword
        raise error.ParseError(_("sub got an invalid pattern: %s") % pat)
    try:
        yield patre.sub(rpl, src)
    except re.error:
        # i18n: "sub" is a keyword
        raise error.ParseError(_("sub got an invalid replacement: %s") % rpl)

@templatefunc('startswith(pattern, text)')
def startswith(context, mapping, args):
    """Returns the value from the "text" argument
    if it begins with the content from the "pattern" argument."""
    if len(args) != 2:
        # i18n: "startswith" is a keyword
        raise error.ParseError(_("startswith expects two arguments"))

    patn = evalstring(context, mapping, args[0])
    text = evalstring(context, mapping, args[1])
    if text.startswith(patn):
        return text
    return ''

@templatefunc('word(number, text[, separator])')
def word(context, mapping, args):
    """Return the nth word from a string."""
    if not (2 <= len(args) <= 3):
        # i18n: "word" is a keyword
        raise error.ParseError(_("word expects two or three arguments, got %d")
                               % len(args))

    num = evalinteger(context, mapping, args[0],
                      # i18n: "word" is a keyword
                      _("word expects an integer index"))
    text = evalstring(context, mapping, args[1])
    if len(args) == 3:
        splitter = evalstring(context, mapping, args[2])
    else:
        splitter = None

    tokens = text.split(splitter)
    if num >= len(tokens) or num < -len(tokens):
        return ''
    else:
        return tokens[num]

def loadfunction(ui, extname, registrarobj):
    """Load template function from specified registrarobj
    """
    for name, func in registrarobj._table.iteritems():
        funcs[name] = func

# tell hggettext to extract docstrings from these functions:
i18nfunctions = funcs.values()
