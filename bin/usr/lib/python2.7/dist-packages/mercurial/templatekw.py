# templatekw.py - common changeset template keywords
#
# Copyright 2005-2009 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

from .i18n import _
from .node import (
    hex,
    nullid,
    wdirid,
    wdirrev,
)

from . import (
    diffutil,
    encoding,
    error,
    hbisect,
    i18n,
    obsutil,
    patch,
    pycompat,
    registrar,
    scmutil,
    templateutil,
    util,
)
from .utils import (
    stringutil,
)

_hybrid = templateutil.hybrid
hybriddict = templateutil.hybriddict
hybridlist = templateutil.hybridlist
compatdict = templateutil.compatdict
compatlist = templateutil.compatlist
_showcompatlist = templateutil._showcompatlist

def getlatesttags(context, mapping, pattern=None):
    '''return date, distance and name for the latest tag of rev'''
    repo = context.resource(mapping, 'repo')
    ctx = context.resource(mapping, 'ctx')
    cache = context.resource(mapping, 'cache')

    cachename = 'latesttags'
    if pattern is not None:
        cachename += '-' + pattern
        match = stringutil.stringmatcher(pattern)[2]
    else:
        match = util.always

    if cachename not in cache:
        # Cache mapping from rev to a tuple with tag date, tag
        # distance and tag name
        cache[cachename] = {-1: (0, 0, ['null'])}
    latesttags = cache[cachename]

    rev = ctx.rev()
    todo = [rev]
    while todo:
        rev = todo.pop()
        if rev in latesttags:
            continue
        ctx = repo[rev]
        tags = [t for t in ctx.tags()
                if (repo.tagtype(t) and repo.tagtype(t) != 'local'
                    and match(t))]
        if tags:
            latesttags[rev] = ctx.date()[0], 0, [t for t in sorted(tags)]
            continue
        try:
            ptags = [latesttags[p.rev()] for p in ctx.parents()]
            if len(ptags) > 1:
                if ptags[0][2] == ptags[1][2]:
                    # The tuples are laid out so the right one can be found by
                    # comparison in this case.
                    pdate, pdist, ptag = max(ptags)
                else:
                    def key(x):
                        changessincetag = len(repo.revs('only(%d, %s)',
                                                        ctx.rev(), x[2][0]))
                        # Smallest number of changes since tag wins. Date is
                        # used as tiebreaker.
                        return [-changessincetag, x[0]]
                    pdate, pdist, ptag = max(ptags, key=key)
            else:
                pdate, pdist, ptag = ptags[0]
        except KeyError:
            # Cache miss - recurse
            todo.append(rev)
            todo.extend(p.rev() for p in ctx.parents())
            continue
        latesttags[rev] = pdate, pdist + 1, ptag
    return latesttags[rev]

def getrenamedfn(repo, endrev=None):
    rcache = {}
    if endrev is None:
        endrev = len(repo)

    def getrenamed(fn, rev):
        '''looks up all renames for a file (up to endrev) the first
        time the file is given. It indexes on the changerev and only
        parses the manifest if linkrev != changerev.
        Returns rename info for fn at changerev rev.'''
        if fn not in rcache:
            rcache[fn] = {}
            fl = repo.file(fn)
            for i in fl:
                lr = fl.linkrev(i)
                renamed = fl.renamed(fl.node(i))
                rcache[fn][lr] = renamed and renamed[0]
                if lr >= endrev:
                    break
        if rev in rcache[fn]:
            return rcache[fn][rev]

        # If linkrev != rev (i.e. rev not found in rcache) fallback to
        # filectx logic.
        try:
            renamed = repo[rev][fn].renamed()
            return renamed and renamed[0]
        except error.LookupError:
            return None

    return getrenamed

def getlogcolumns():
    """Return a dict of log column labels"""
    _ = pycompat.identity  # temporarily disable gettext
    # i18n: column positioning for "hg log"
    columns = _('bookmark:    %s\n'
                'branch:      %s\n'
                'changeset:   %s\n'
                'copies:      %s\n'
                'date:        %s\n'
                'extra:       %s=%s\n'
                'files+:      %s\n'
                'files-:      %s\n'
                'files:       %s\n'
                'instability: %s\n'
                'manifest:    %s\n'
                'obsolete:    %s\n'
                'parent:      %s\n'
                'phase:       %s\n'
                'summary:     %s\n'
                'tag:         %s\n'
                'user:        %s\n')
    return dict(zip([s.split(':', 1)[0] for s in columns.splitlines()],
                    i18n._(columns).splitlines(True)))

# default templates internally used for rendering of lists
defaulttempl = {
    'parent': '{rev}:{node|formatnode} ',
    'manifest': '{rev}:{node|formatnode}',
    'file_copy': '{name} ({source})',
    'envvar': '{key}={value}',
    'extra': '{key}={value|stringescape}'
}
# filecopy is preserved for compatibility reasons
defaulttempl['filecopy'] = defaulttempl['file_copy']

# keywords are callables (see registrar.templatekeyword for details)
keywords = {}
templatekeyword = registrar.templatekeyword(keywords)

@templatekeyword('author', requires={'ctx'})
def showauthor(context, mapping):
    """Alias for ``{user}``"""
    return showuser(context, mapping)

@templatekeyword('bisect', requires={'repo', 'ctx'})
def showbisect(context, mapping):
    """String. The changeset bisection status."""
    repo = context.resource(mapping, 'repo')
    ctx = context.resource(mapping, 'ctx')
    return hbisect.label(repo, ctx.node())

@templatekeyword('branch', requires={'ctx'})
def showbranch(context, mapping):
    """String. The name of the branch on which the changeset was
    committed.
    """
    ctx = context.resource(mapping, 'ctx')
    return ctx.branch()

@templatekeyword('branches', requires={'ctx'})
def showbranches(context, mapping):
    """List of strings. The name of the branch on which the
    changeset was committed. Will be empty if the branch name was
    default. (DEPRECATED)
    """
    ctx = context.resource(mapping, 'ctx')
    branch = ctx.branch()
    if branch != 'default':
        return compatlist(context, mapping, 'branch', [branch],
                          plural='branches')
    return compatlist(context, mapping, 'branch', [], plural='branches')

@templatekeyword('bookmarks', requires={'repo', 'ctx'})
def showbookmarks(context, mapping):
    """List of strings. Any bookmarks associated with the
    changeset. Also sets 'active', the name of the active bookmark.
    """
    repo = context.resource(mapping, 'repo')
    ctx = context.resource(mapping, 'ctx')
    bookmarks = ctx.bookmarks()
    active = repo._activebookmark
    makemap = lambda v: {'bookmark': v, 'active': active, 'current': active}
    f = _showcompatlist(context, mapping, 'bookmark', bookmarks)
    return _hybrid(f, bookmarks, makemap, pycompat.identity)

@templatekeyword('children', requires={'ctx'})
def showchildren(context, mapping):
    """List of strings. The children of the changeset."""
    ctx = context.resource(mapping, 'ctx')
    childrevs = ['%d:%s' % (cctx.rev(), cctx) for cctx in ctx.children()]
    return compatlist(context, mapping, 'children', childrevs, element='child')

# Deprecated, but kept alive for help generation a purpose.
@templatekeyword('currentbookmark', requires={'repo', 'ctx'})
def showcurrentbookmark(context, mapping):
    """String. The active bookmark, if it is associated with the changeset.
    (DEPRECATED)"""
    return showactivebookmark(context, mapping)

@templatekeyword('activebookmark', requires={'repo', 'ctx'})
def showactivebookmark(context, mapping):
    """String. The active bookmark, if it is associated with the changeset."""
    repo = context.resource(mapping, 'repo')
    ctx = context.resource(mapping, 'ctx')
    active = repo._activebookmark
    if active and active in ctx.bookmarks():
        return active
    return ''

@templatekeyword('date', requires={'ctx'})
def showdate(context, mapping):
    """Date information. The date when the changeset was committed."""
    ctx = context.resource(mapping, 'ctx')
    # the default string format is '<float(unixtime)><tzoffset>' because
    # python-hglib splits date at decimal separator.
    return templateutil.date(ctx.date(), showfmt='%d.0%d')

@templatekeyword('desc', requires={'ctx'})
def showdescription(context, mapping):
    """String. The text of the changeset description."""
    ctx = context.resource(mapping, 'ctx')
    s = ctx.description()
    if isinstance(s, encoding.localstr):
        # try hard to preserve utf-8 bytes
        return encoding.tolocal(encoding.fromlocal(s).strip())
    elif isinstance(s, encoding.safelocalstr):
        return encoding.safelocalstr(s.strip())
    else:
        return s.strip()

@templatekeyword('diffstat', requires={'ui', 'ctx'})
def showdiffstat(context, mapping):
    """String. Statistics of changes with the following format:
    "modified files: +added/-removed lines"
    """
    ui = context.resource(mapping, 'ui')
    ctx = context.resource(mapping, 'ctx')
    diffopts = diffutil.diffallopts(ui, {'noprefix': False})
    diff = ctx.diff(opts=diffopts)
    stats = patch.diffstatdata(util.iterlines(diff))
    maxname, maxtotal, adds, removes, binary = patch.diffstatsum(stats)
    return '%d: +%d/-%d' % (len(stats), adds, removes)

@templatekeyword('envvars', requires={'ui'})
def showenvvars(context, mapping):
    """A dictionary of environment variables. (EXPERIMENTAL)"""
    ui = context.resource(mapping, 'ui')
    env = ui.exportableenviron()
    env = util.sortdict((k, env[k]) for k in sorted(env))
    return compatdict(context, mapping, 'envvar', env, plural='envvars')

@templatekeyword('extras', requires={'ctx'})
def showextras(context, mapping):
    """List of dicts with key, value entries of the 'extras'
    field of this changeset."""
    ctx = context.resource(mapping, 'ctx')
    extras = ctx.extra()
    extras = util.sortdict((k, extras[k]) for k in sorted(extras))
    makemap = lambda k: {'key': k, 'value': extras[k]}
    c = [makemap(k) for k in extras]
    f = _showcompatlist(context, mapping, 'extra', c, plural='extras')
    return _hybrid(f, extras, makemap,
                   lambda k: '%s=%s' % (k, stringutil.escapestr(extras[k])))

def _getfilestatus(context, mapping, listall=False):
    ctx = context.resource(mapping, 'ctx')
    revcache = context.resource(mapping, 'revcache')
    if 'filestatus' not in revcache or revcache['filestatusall'] < listall:
        stat = ctx.p1().status(ctx, listignored=listall, listclean=listall,
                               listunknown=listall)
        revcache['filestatus'] = stat
        revcache['filestatusall'] = listall
    return revcache['filestatus']

def _getfilestatusmap(context, mapping, listall=False):
    revcache = context.resource(mapping, 'revcache')
    if 'filestatusmap' not in revcache or revcache['filestatusall'] < listall:
        stat = _getfilestatus(context, mapping, listall=listall)
        revcache['filestatusmap'] = statmap = {}
        for char, files in zip(pycompat.iterbytestr('MAR!?IC'), stat):
            statmap.update((f, char) for f in files)
    return revcache['filestatusmap']  # {path: statchar}

def _showfilesbystat(context, mapping, name, index):
    stat = _getfilestatus(context, mapping)
    files = stat[index]
    return templateutil.compatfileslist(context, mapping, name, files)

@templatekeyword('file_adds', requires={'ctx', 'revcache'})
def showfileadds(context, mapping):
    """List of strings. Files added by this changeset."""
    return _showfilesbystat(context, mapping, 'file_add', 1)

@templatekeyword('file_copies',
                 requires={'repo', 'ctx', 'cache', 'revcache'})
def showfilecopies(context, mapping):
    """List of strings. Files copied in this changeset with
    their sources.
    """
    repo = context.resource(mapping, 'repo')
    ctx = context.resource(mapping, 'ctx')
    cache = context.resource(mapping, 'cache')
    copies = context.resource(mapping, 'revcache').get('copies')
    if copies is None:
        if 'getrenamed' not in cache:
            cache['getrenamed'] = getrenamedfn(repo)
        copies = []
        getrenamed = cache['getrenamed']
        for fn in ctx.files():
            rename = getrenamed(fn, ctx.rev())
            if rename:
                copies.append((fn, rename))
    return templateutil.compatfilecopiesdict(context, mapping, 'file_copy',
                                             copies)

# showfilecopiesswitch() displays file copies only if copy records are
# provided before calling the templater, usually with a --copies
# command line switch.
@templatekeyword('file_copies_switch', requires={'revcache'})
def showfilecopiesswitch(context, mapping):
    """List of strings. Like "file_copies" but displayed
    only if the --copied switch is set.
    """
    copies = context.resource(mapping, 'revcache').get('copies') or []
    return templateutil.compatfilecopiesdict(context, mapping, 'file_copy',
                                             copies)

@templatekeyword('file_dels', requires={'ctx', 'revcache'})
def showfiledels(context, mapping):
    """List of strings. Files removed by this changeset."""
    return _showfilesbystat(context, mapping, 'file_del', 2)

@templatekeyword('file_mods', requires={'ctx', 'revcache'})
def showfilemods(context, mapping):
    """List of strings. Files modified by this changeset."""
    return _showfilesbystat(context, mapping, 'file_mod', 0)

@templatekeyword('files', requires={'ctx'})
def showfiles(context, mapping):
    """List of strings. All files modified, added, or removed by this
    changeset.
    """
    ctx = context.resource(mapping, 'ctx')
    return templateutil.compatfileslist(context, mapping, 'file', ctx.files())

@templatekeyword('graphnode', requires={'repo', 'ctx'})
def showgraphnode(context, mapping):
    """String. The character representing the changeset node in an ASCII
    revision graph."""
    repo = context.resource(mapping, 'repo')
    ctx = context.resource(mapping, 'ctx')
    return getgraphnode(repo, ctx)

def getgraphnode(repo, ctx):
    return getgraphnodecurrent(repo, ctx) or getgraphnodesymbol(ctx)

def getgraphnodecurrent(repo, ctx):
    wpnodes = repo.dirstate.parents()
    if wpnodes[1] == nullid:
        wpnodes = wpnodes[:1]
    if ctx.node() in wpnodes:
        return '@'
    else:
        return ''

def getgraphnodesymbol(ctx):
    if ctx.obsolete():
        return 'x'
    elif ctx.isunstable():
        return '*'
    elif ctx.closesbranch():
        return '_'
    else:
        return 'o'

@templatekeyword('graphwidth', requires=())
def showgraphwidth(context, mapping):
    """Integer. The width of the graph drawn by 'log --graph' or zero."""
    # just hosts documentation; should be overridden by template mapping
    return 0

@templatekeyword('index', requires=())
def showindex(context, mapping):
    """Integer. The current iteration of the loop. (0 indexed)"""
    # just hosts documentation; should be overridden by template mapping
    raise error.Abort(_("can't use index in this context"))

@templatekeyword('latesttag', requires={'repo', 'ctx', 'cache'})
def showlatesttag(context, mapping):
    """List of strings. The global tags on the most recent globally
    tagged ancestor of this changeset.  If no such tags exist, the list
    consists of the single string "null".
    """
    return showlatesttags(context, mapping, None)

def showlatesttags(context, mapping, pattern):
    """helper method for the latesttag keyword and function"""
    latesttags = getlatesttags(context, mapping, pattern)

    # latesttag[0] is an implementation detail for sorting csets on different
    # branches in a stable manner- it is the date the tagged cset was created,
    # not the date the tag was created.  Therefore it isn't made visible here.
    makemap = lambda v: {
        'changes': _showchangessincetag,
        'distance': latesttags[1],
        'latesttag': v,   # BC with {latesttag % '{latesttag}'}
        'tag': v
    }

    tags = latesttags[2]
    f = _showcompatlist(context, mapping, 'latesttag', tags, separator=':')
    return _hybrid(f, tags, makemap, pycompat.identity)

@templatekeyword('latesttagdistance', requires={'repo', 'ctx', 'cache'})
def showlatesttagdistance(context, mapping):
    """Integer. Longest path to the latest tag."""
    return getlatesttags(context, mapping)[1]

@templatekeyword('changessincelatesttag', requires={'repo', 'ctx', 'cache'})
def showchangessincelatesttag(context, mapping):
    """Integer. All ancestors not in the latest tag."""
    tag = getlatesttags(context, mapping)[2][0]
    mapping = context.overlaymap(mapping, {'tag': tag})
    return _showchangessincetag(context, mapping)

def _showchangessincetag(context, mapping):
    repo = context.resource(mapping, 'repo')
    ctx = context.resource(mapping, 'ctx')
    offset = 0
    revs = [ctx.rev()]
    tag = context.symbol(mapping, 'tag')

    # The only() revset doesn't currently support wdir()
    if ctx.rev() is None:
        offset = 1
        revs = [p.rev() for p in ctx.parents()]

    return len(repo.revs('only(%ld, %s)', revs, tag)) + offset

# teach templater latesttags.changes is switched to (context, mapping) API
_showchangessincetag._requires = {'repo', 'ctx'}

@templatekeyword('manifest', requires={'repo', 'ctx'})
def showmanifest(context, mapping):
    repo = context.resource(mapping, 'repo')
    ctx = context.resource(mapping, 'ctx')
    mnode = ctx.manifestnode()
    if mnode is None:
        mnode = wdirid
        mrev = wdirrev
    else:
        mrev = repo.manifestlog.rev(mnode)
    mhex = hex(mnode)
    mapping = context.overlaymap(mapping, {'rev': mrev, 'node': mhex})
    f = context.process('manifest', mapping)
    return templateutil.hybriditem(f, None, f,
                                   lambda x: {'rev': mrev, 'node': mhex})

@templatekeyword('obsfate', requires={'ui', 'repo', 'ctx'})
def showobsfate(context, mapping):
    # this function returns a list containing pre-formatted obsfate strings.
    #
    # This function will be replaced by templates fragments when we will have
    # the verbosity templatekw available.
    succsandmarkers = showsuccsandmarkers(context, mapping)

    ui = context.resource(mapping, 'ui')
    repo = context.resource(mapping, 'repo')
    values = []

    for x in succsandmarkers.tovalue(context, mapping):
        v = obsutil.obsfateprinter(ui, repo, x['successors'], x['markers'],
                                   scmutil.formatchangeid)
        values.append(v)

    return compatlist(context, mapping, "fate", values)

def shownames(context, mapping, namespace):
    """helper method to generate a template keyword for a namespace"""
    repo = context.resource(mapping, 'repo')
    ctx = context.resource(mapping, 'ctx')
    ns = repo.names[namespace]
    names = ns.names(repo, ctx.node())
    return compatlist(context, mapping, ns.templatename, names,
                      plural=namespace)

@templatekeyword('namespaces', requires={'repo', 'ctx'})
def shownamespaces(context, mapping):
    """Dict of lists. Names attached to this changeset per
    namespace."""
    repo = context.resource(mapping, 'repo')
    ctx = context.resource(mapping, 'ctx')

    namespaces = util.sortdict()
    def makensmapfn(ns):
        # 'name' for iterating over namespaces, templatename for local reference
        return lambda v: {'name': v, ns.templatename: v}

    for k, ns in repo.names.iteritems():
        names = ns.names(repo, ctx.node())
        f = _showcompatlist(context, mapping, 'name', names)
        namespaces[k] = _hybrid(f, names, makensmapfn(ns), pycompat.identity)

    f = _showcompatlist(context, mapping, 'namespace', list(namespaces))

    def makemap(ns):
        return {
            'namespace': ns,
            'names': namespaces[ns],
            'builtin': repo.names[ns].builtin,
            'colorname': repo.names[ns].colorname,
        }

    return _hybrid(f, namespaces, makemap, pycompat.identity)

@templatekeyword('node', requires={'ctx'})
def shownode(context, mapping):
    """String. The changeset identification hash, as a 40 hexadecimal
    digit string.
    """
    ctx = context.resource(mapping, 'ctx')
    return ctx.hex()

@templatekeyword('obsolete', requires={'ctx'})
def showobsolete(context, mapping):
    """String. Whether the changeset is obsolete. (EXPERIMENTAL)"""
    ctx = context.resource(mapping, 'ctx')
    if ctx.obsolete():
        return 'obsolete'
    return ''

@templatekeyword('path', requires={'fctx'})
def showpath(context, mapping):
    """String. Repository-absolute path of the current file. (EXPERIMENTAL)"""
    fctx = context.resource(mapping, 'fctx')
    return fctx.path()

@templatekeyword('peerurls', requires={'repo'})
def showpeerurls(context, mapping):
    """A dictionary of repository locations defined in the [paths] section
    of your configuration file."""
    repo = context.resource(mapping, 'repo')
    # see commands.paths() for naming of dictionary keys
    paths = repo.ui.paths
    urls = util.sortdict((k, p.rawloc) for k, p in sorted(paths.iteritems()))
    def makemap(k):
        p = paths[k]
        d = {'name': k, 'url': p.rawloc}
        d.update((o, v) for o, v in sorted(p.suboptions.iteritems()))
        return d
    return _hybrid(None, urls, makemap, lambda k: '%s=%s' % (k, urls[k]))

@templatekeyword("predecessors", requires={'repo', 'ctx'})
def showpredecessors(context, mapping):
    """Returns the list of the closest visible successors. (EXPERIMENTAL)"""
    repo = context.resource(mapping, 'repo')
    ctx = context.resource(mapping, 'ctx')
    predecessors = sorted(obsutil.closestpredecessors(repo, ctx.node()))
    predecessors = pycompat.maplist(hex, predecessors)

    return _hybrid(None, predecessors,
                   lambda x: {'ctx': repo[x]},
                   lambda x: scmutil.formatchangeid(repo[x]))

@templatekeyword('reporoot', requires={'repo'})
def showreporoot(context, mapping):
    """String. The root directory of the current repository."""
    repo = context.resource(mapping, 'repo')
    return repo.root

@templatekeyword('size', requires={'fctx'})
def showsize(context, mapping):
    """Integer. Size of the current file in bytes. (EXPERIMENTAL)"""
    fctx = context.resource(mapping, 'fctx')
    return fctx.size()

# requires 'fctx' to denote {status} depends on (ctx, path) pair
@templatekeyword('status', requires={'ctx', 'fctx', 'revcache'})
def showstatus(context, mapping):
    """String. Status code of the current file. (EXPERIMENTAL)"""
    path = templateutil.runsymbol(context, mapping, 'path')
    path = templateutil.stringify(context, mapping, path)
    if not path:
        return
    statmap = _getfilestatusmap(context, mapping)
    if path not in statmap:
        statmap = _getfilestatusmap(context, mapping, listall=True)
    return statmap.get(path)

@templatekeyword("successorssets", requires={'repo', 'ctx'})
def showsuccessorssets(context, mapping):
    """Returns a string of sets of successors for a changectx. Format used
    is: [ctx1, ctx2], [ctx3] if ctx has been split into ctx1 and ctx2
    while also diverged into ctx3. (EXPERIMENTAL)"""
    repo = context.resource(mapping, 'repo')
    ctx = context.resource(mapping, 'ctx')
    if not ctx.obsolete():
        return ''

    ssets = obsutil.successorssets(repo, ctx.node(), closest=True)
    ssets = [[hex(n) for n in ss] for ss in ssets]

    data = []
    for ss in ssets:
        h = _hybrid(None, ss, lambda x: {'ctx': repo[x]},
                    lambda x: scmutil.formatchangeid(repo[x]))
        data.append(h)

    # Format the successorssets
    def render(d):
        return templateutil.stringify(context, mapping, d)

    def gen(data):
        yield "; ".join(render(d) for d in data)

    return _hybrid(gen(data), data, lambda x: {'successorset': x},
                   pycompat.identity)

@templatekeyword("succsandmarkers", requires={'repo', 'ctx'})
def showsuccsandmarkers(context, mapping):
    """Returns a list of dict for each final successor of ctx. The dict
    contains successors node id in "successors" keys and the list of
    obs-markers from ctx to the set of successors in "markers".
    (EXPERIMENTAL)
    """
    repo = context.resource(mapping, 'repo')
    ctx = context.resource(mapping, 'ctx')

    values = obsutil.successorsandmarkers(repo, ctx)

    if values is None:
        values = []

    # Format successors and markers to avoid exposing binary to templates
    data = []
    for i in values:
        # Format successors
        successors = i['successors']

        successors = [hex(n) for n in successors]
        successors = _hybrid(None, successors,
                             lambda x: {'ctx': repo[x]},
                             lambda x: scmutil.formatchangeid(repo[x]))

        # Format markers
        finalmarkers = []
        for m in i['markers']:
            hexprec = hex(m[0])
            hexsucs = tuple(hex(n) for n in m[1])
            hexparents = None
            if m[5] is not None:
                hexparents = tuple(hex(n) for n in m[5])
            newmarker = (hexprec, hexsucs) + m[2:5] + (hexparents,) + m[6:]
            finalmarkers.append(newmarker)

        data.append({'successors': successors, 'markers': finalmarkers})

    return templateutil.mappinglist(data)

@templatekeyword('p1rev', requires={'ctx'})
def showp1rev(context, mapping):
    """Integer. The repository-local revision number of the changeset's
    first parent, or -1 if the changeset has no parents."""
    ctx = context.resource(mapping, 'ctx')
    return ctx.p1().rev()

@templatekeyword('p2rev', requires={'ctx'})
def showp2rev(context, mapping):
    """Integer. The repository-local revision number of the changeset's
    second parent, or -1 if the changeset has no second parent."""
    ctx = context.resource(mapping, 'ctx')
    return ctx.p2().rev()

@templatekeyword('p1node', requires={'ctx'})
def showp1node(context, mapping):
    """String. The identification hash of the changeset's first parent,
    as a 40 digit hexadecimal string. If the changeset has no parents, all
    digits are 0."""
    ctx = context.resource(mapping, 'ctx')
    return ctx.p1().hex()

@templatekeyword('p2node', requires={'ctx'})
def showp2node(context, mapping):
    """String. The identification hash of the changeset's second
    parent, as a 40 digit hexadecimal string. If the changeset has no second
    parent, all digits are 0."""
    ctx = context.resource(mapping, 'ctx')
    return ctx.p2().hex()

@templatekeyword('parents', requires={'repo', 'ctx'})
def showparents(context, mapping):
    """List of strings. The parents of the changeset in "rev:node"
    format. If the changeset has only one "natural" parent (the predecessor
    revision) nothing is shown."""
    repo = context.resource(mapping, 'repo')
    ctx = context.resource(mapping, 'ctx')
    pctxs = scmutil.meaningfulparents(repo, ctx)
    prevs = [p.rev() for p in pctxs]
    parents = [[('rev', p.rev()),
                ('node', p.hex()),
                ('phase', p.phasestr())]
               for p in pctxs]
    f = _showcompatlist(context, mapping, 'parent', parents)
    return _hybrid(f, prevs, lambda x: {'ctx': repo[x]},
                   lambda x: scmutil.formatchangeid(repo[x]), keytype=int)

@templatekeyword('phase', requires={'ctx'})
def showphase(context, mapping):
    """String. The changeset phase name."""
    ctx = context.resource(mapping, 'ctx')
    return ctx.phasestr()

@templatekeyword('phaseidx', requires={'ctx'})
def showphaseidx(context, mapping):
    """Integer. The changeset phase index. (ADVANCED)"""
    ctx = context.resource(mapping, 'ctx')
    return ctx.phase()

@templatekeyword('rev', requires={'ctx'})
def showrev(context, mapping):
    """Integer. The repository-local changeset revision number."""
    ctx = context.resource(mapping, 'ctx')
    return scmutil.intrev(ctx)

def showrevslist(context, mapping, name, revs):
    """helper to generate a list of revisions in which a mapped template will
    be evaluated"""
    repo = context.resource(mapping, 'repo')
    f = _showcompatlist(context, mapping, name, ['%d' % r for r in revs])
    return _hybrid(f, revs,
                   lambda x: {name: x, 'ctx': repo[x]},
                   pycompat.identity, keytype=int)

@templatekeyword('subrepos', requires={'ctx'})
def showsubrepos(context, mapping):
    """List of strings. Updated subrepositories in the changeset."""
    ctx = context.resource(mapping, 'ctx')
    substate = ctx.substate
    if not substate:
        return compatlist(context, mapping, 'subrepo', [])
    psubstate = ctx.parents()[0].substate or {}
    subrepos = []
    for sub in substate:
        if sub not in psubstate or substate[sub] != psubstate[sub]:
            subrepos.append(sub) # modified or newly added in ctx
    for sub in psubstate:
        if sub not in substate:
            subrepos.append(sub) # removed in ctx
    return compatlist(context, mapping, 'subrepo', sorted(subrepos))

# don't remove "showtags" definition, even though namespaces will put
# a helper function for "tags" keyword into "keywords" map automatically,
# because online help text is built without namespaces initialization
@templatekeyword('tags', requires={'repo', 'ctx'})
def showtags(context, mapping):
    """List of strings. Any tags associated with the changeset."""
    return shownames(context, mapping, 'tags')

@templatekeyword('termwidth', requires={'ui'})
def showtermwidth(context, mapping):
    """Integer. The width of the current terminal."""
    ui = context.resource(mapping, 'ui')
    return ui.termwidth()

@templatekeyword('user', requires={'ctx'})
def showuser(context, mapping):
    """String. The unmodified author of the changeset."""
    ctx = context.resource(mapping, 'ctx')
    return ctx.user()

@templatekeyword('instabilities', requires={'ctx'})
def showinstabilities(context, mapping):
    """List of strings. Evolution instabilities affecting the changeset.
    (EXPERIMENTAL)
    """
    ctx = context.resource(mapping, 'ctx')
    return compatlist(context, mapping, 'instability', ctx.instabilities(),
                      plural='instabilities')

@templatekeyword('verbosity', requires={'ui'})
def showverbosity(context, mapping):
    """String. The current output verbosity in 'debug', 'quiet', 'verbose',
    or ''."""
    ui = context.resource(mapping, 'ui')
    # see logcmdutil.changesettemplater for priority of these flags
    if ui.debugflag:
        return 'debug'
    elif ui.quiet:
        return 'quiet'
    elif ui.verbose:
        return 'verbose'
    return ''

@templatekeyword('whyunstable', requires={'repo', 'ctx'})
def showwhyunstable(context, mapping):
    """List of dicts explaining all instabilities of a changeset.
    (EXPERIMENTAL)
    """
    repo = context.resource(mapping, 'repo')
    ctx = context.resource(mapping, 'ctx')

    def formatnode(ctx):
        return '%s (%s)' % (scmutil.formatchangeid(ctx), ctx.phasestr())

    entries = obsutil.whyunstable(repo, ctx)

    for entry in entries:
        if entry.get('divergentnodes'):
            dnodes = entry['divergentnodes']
            dnhybrid = _hybrid(None, [dnode.hex() for dnode in dnodes],
                               lambda x: {'ctx': repo[x]},
                               lambda x: formatnode(repo[x]))
            entry['divergentnodes'] = dnhybrid

    tmpl = ('{instability}:{if(divergentnodes, " ")}{divergentnodes} '
            '{reason} {node|short}')
    return templateutil.mappinglist(entries, tmpl=tmpl, sep='\n')

def loadkeyword(ui, extname, registrarobj):
    """Load template keyword from specified registrarobj
    """
    for name, func in registrarobj._table.iteritems():
        keywords[name] = func

# tell hggettext to extract docstrings from these functions:
i18nfunctions = keywords.values()
