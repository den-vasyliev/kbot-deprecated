# debugcommands.py - command processing for debug* commands
#
# Copyright 2005-2016 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import codecs
import collections
import difflib
import errno
import operator
import os
import random
import re
import socket
import ssl
import stat
import string
import subprocess
import sys
import time

from .i18n import _
from .node import (
    bin,
    hex,
    nullhex,
    nullid,
    nullrev,
    short,
)
from . import (
    bundle2,
    changegroup,
    cmdutil,
    color,
    context,
    dagparser,
    encoding,
    error,
    exchange,
    extensions,
    filemerge,
    filesetlang,
    formatter,
    hg,
    httppeer,
    localrepo,
    lock as lockmod,
    logcmdutil,
    merge as mergemod,
    obsolete,
    obsutil,
    phases,
    policy,
    pvec,
    pycompat,
    registrar,
    repair,
    revlog,
    revset,
    revsetlang,
    scmutil,
    setdiscovery,
    simplemerge,
    sshpeer,
    sslutil,
    streamclone,
    templater,
    treediscovery,
    upgrade,
    url as urlmod,
    util,
    vfs as vfsmod,
    wireprotoframing,
    wireprotoserver,
    wireprotov2peer,
)
from .utils import (
    cborutil,
    dateutil,
    procutil,
    stringutil,
)

from .revlogutils import (
    deltas as deltautil
)

release = lockmod.release

command = registrar.command()

@command('debugancestor', [], _('[INDEX] REV1 REV2'), optionalrepo=True)
def debugancestor(ui, repo, *args):
    """find the ancestor revision of two revisions in a given index"""
    if len(args) == 3:
        index, rev1, rev2 = args
        r = revlog.revlog(vfsmod.vfs(encoding.getcwd(), audit=False), index)
        lookup = r.lookup
    elif len(args) == 2:
        if not repo:
            raise error.Abort(_('there is no Mercurial repository here '
                                '(.hg not found)'))
        rev1, rev2 = args
        r = repo.changelog
        lookup = repo.lookup
    else:
        raise error.Abort(_('either two or three arguments required'))
    a = r.ancestor(lookup(rev1), lookup(rev2))
    ui.write('%d:%s\n' % (r.rev(a), hex(a)))

@command('debugapplystreamclonebundle', [], 'FILE')
def debugapplystreamclonebundle(ui, repo, fname):
    """apply a stream clone bundle file"""
    f = hg.openpath(ui, fname)
    gen = exchange.readbundle(ui, f, fname)
    gen.apply(repo)

@command('debugbuilddag',
    [('m', 'mergeable-file', None, _('add single file mergeable changes')),
    ('o', 'overwritten-file', None, _('add single file all revs overwrite')),
    ('n', 'new-file', None, _('add new file at each rev'))],
    _('[OPTION]... [TEXT]'))
def debugbuilddag(ui, repo, text=None,
                  mergeable_file=False,
                  overwritten_file=False,
                  new_file=False):
    """builds a repo with a given DAG from scratch in the current empty repo

    The description of the DAG is read from stdin if not given on the
    command line.

    Elements:

     - "+n" is a linear run of n nodes based on the current default parent
     - "." is a single node based on the current default parent
     - "$" resets the default parent to null (implied at the start);
           otherwise the default parent is always the last node created
     - "<p" sets the default parent to the backref p
     - "*p" is a fork at parent p, which is a backref
     - "*p1/p2" is a merge of parents p1 and p2, which are backrefs
     - "/p2" is a merge of the preceding node and p2
     - ":tag" defines a local tag for the preceding node
     - "@branch" sets the named branch for subsequent nodes
     - "#...\\n" is a comment up to the end of the line

    Whitespace between the above elements is ignored.

    A backref is either

     - a number n, which references the node curr-n, where curr is the current
       node, or
     - the name of a local tag you placed earlier using ":tag", or
     - empty to denote the default parent.

    All string valued-elements are either strictly alphanumeric, or must
    be enclosed in double quotes ("..."), with "\\" as escape character.
    """

    if text is None:
        ui.status(_("reading DAG from stdin\n"))
        text = ui.fin.read()

    cl = repo.changelog
    if len(cl) > 0:
        raise error.Abort(_('repository is not empty'))

    # determine number of revs in DAG
    total = 0
    for type, data in dagparser.parsedag(text):
        if type == 'n':
            total += 1

    if mergeable_file:
        linesperrev = 2
        # make a file with k lines per rev
        initialmergedlines = ['%d' % i
                              for i in pycompat.xrange(0, total * linesperrev)]
        initialmergedlines.append("")

    tags = []
    progress = ui.makeprogress(_('building'), unit=_('revisions'),
                               total=total)
    with progress, repo.wlock(), repo.lock(), repo.transaction("builddag"):
        at = -1
        atbranch = 'default'
        nodeids = []
        id = 0
        progress.update(id)
        for type, data in dagparser.parsedag(text):
            if type == 'n':
                ui.note(('node %s\n' % pycompat.bytestr(data)))
                id, ps = data

                files = []
                filecontent = {}

                p2 = None
                if mergeable_file:
                    fn = "mf"
                    p1 = repo[ps[0]]
                    if len(ps) > 1:
                        p2 = repo[ps[1]]
                        pa = p1.ancestor(p2)
                        base, local, other = [x[fn].data() for x in (pa, p1,
                                                                     p2)]
                        m3 = simplemerge.Merge3Text(base, local, other)
                        ml = [l.strip() for l in m3.merge_lines()]
                        ml.append("")
                    elif at > 0:
                        ml = p1[fn].data().split("\n")
                    else:
                        ml = initialmergedlines
                    ml[id * linesperrev] += " r%i" % id
                    mergedtext = "\n".join(ml)
                    files.append(fn)
                    filecontent[fn] = mergedtext

                if overwritten_file:
                    fn = "of"
                    files.append(fn)
                    filecontent[fn] = "r%i\n" % id

                if new_file:
                    fn = "nf%i" % id
                    files.append(fn)
                    filecontent[fn] = "r%i\n" % id
                    if len(ps) > 1:
                        if not p2:
                            p2 = repo[ps[1]]
                        for fn in p2:
                            if fn.startswith("nf"):
                                files.append(fn)
                                filecontent[fn] = p2[fn].data()

                def fctxfn(repo, cx, path):
                    if path in filecontent:
                        return context.memfilectx(repo, cx, path,
                                                  filecontent[path])
                    return None

                if len(ps) == 0 or ps[0] < 0:
                    pars = [None, None]
                elif len(ps) == 1:
                    pars = [nodeids[ps[0]], None]
                else:
                    pars = [nodeids[p] for p in ps]
                cx = context.memctx(repo, pars, "r%i" % id, files, fctxfn,
                                    date=(id, 0),
                                    user="debugbuilddag",
                                    extra={'branch': atbranch})
                nodeid = repo.commitctx(cx)
                nodeids.append(nodeid)
                at = id
            elif type == 'l':
                id, name = data
                ui.note(('tag %s\n' % name))
                tags.append("%s %s\n" % (hex(repo.changelog.node(id)), name))
            elif type == 'a':
                ui.note(('branch %s\n' % data))
                atbranch = data
            progress.update(id)

        if tags:
            repo.vfs.write("localtags", "".join(tags))

def _debugchangegroup(ui, gen, all=None, indent=0, **opts):
    indent_string = ' ' * indent
    if all:
        ui.write(("%sformat: id, p1, p2, cset, delta base, len(delta)\n")
                 % indent_string)

        def showchunks(named):
            ui.write("\n%s%s\n" % (indent_string, named))
            for deltadata in gen.deltaiter():
                node, p1, p2, cs, deltabase, delta, flags = deltadata
                ui.write("%s%s %s %s %s %s %d\n" %
                         (indent_string, hex(node), hex(p1), hex(p2),
                          hex(cs), hex(deltabase), len(delta)))

        chunkdata = gen.changelogheader()
        showchunks("changelog")
        chunkdata = gen.manifestheader()
        showchunks("manifest")
        for chunkdata in iter(gen.filelogheader, {}):
            fname = chunkdata['filename']
            showchunks(fname)
    else:
        if isinstance(gen, bundle2.unbundle20):
            raise error.Abort(_('use debugbundle2 for this file'))
        chunkdata = gen.changelogheader()
        for deltadata in gen.deltaiter():
            node, p1, p2, cs, deltabase, delta, flags = deltadata
            ui.write("%s%s\n" % (indent_string, hex(node)))

def _debugobsmarkers(ui, part, indent=0, **opts):
    """display version and markers contained in 'data'"""
    opts = pycompat.byteskwargs(opts)
    data = part.read()
    indent_string = ' ' * indent
    try:
        version, markers = obsolete._readmarkers(data)
    except error.UnknownVersion as exc:
        msg = "%sunsupported version: %s (%d bytes)\n"
        msg %= indent_string, exc.version, len(data)
        ui.write(msg)
    else:
        msg = "%sversion: %d (%d bytes)\n"
        msg %= indent_string, version, len(data)
        ui.write(msg)
        fm = ui.formatter('debugobsolete', opts)
        for rawmarker in sorted(markers):
            m = obsutil.marker(None, rawmarker)
            fm.startitem()
            fm.plain(indent_string)
            cmdutil.showmarker(fm, m)
        fm.end()

def _debugphaseheads(ui, data, indent=0):
    """display version and markers contained in 'data'"""
    indent_string = ' ' * indent
    headsbyphase = phases.binarydecode(data)
    for phase in phases.allphases:
        for head in headsbyphase[phase]:
            ui.write(indent_string)
            ui.write('%s %s\n' % (hex(head), phases.phasenames[phase]))

def _quasirepr(thing):
    if isinstance(thing, (dict, util.sortdict, collections.OrderedDict)):
        return '{%s}' % (
            b', '.join(b'%s: %s' % (k, thing[k]) for k in sorted(thing)))
    return pycompat.bytestr(repr(thing))

def _debugbundle2(ui, gen, all=None, **opts):
    """lists the contents of a bundle2"""
    if not isinstance(gen, bundle2.unbundle20):
        raise error.Abort(_('not a bundle2 file'))
    ui.write(('Stream params: %s\n' % _quasirepr(gen.params)))
    parttypes = opts.get(r'part_type', [])
    for part in gen.iterparts():
        if parttypes and part.type not in parttypes:
            continue
        msg = '%s -- %s (mandatory: %r)\n'
        ui.write((msg % (part.type, _quasirepr(part.params), part.mandatory)))
        if part.type == 'changegroup':
            version = part.params.get('version', '01')
            cg = changegroup.getunbundler(version, part, 'UN')
            if not ui.quiet:
                _debugchangegroup(ui, cg, all=all, indent=4, **opts)
        if part.type == 'obsmarkers':
            if not ui.quiet:
                _debugobsmarkers(ui, part, indent=4, **opts)
        if part.type == 'phase-heads':
            if not ui.quiet:
                _debugphaseheads(ui, part, indent=4)

@command('debugbundle',
        [('a', 'all', None, _('show all details')),
         ('', 'part-type', [], _('show only the named part type')),
         ('', 'spec', None, _('print the bundlespec of the bundle'))],
        _('FILE'),
        norepo=True)
def debugbundle(ui, bundlepath, all=None, spec=None, **opts):
    """lists the contents of a bundle"""
    with hg.openpath(ui, bundlepath) as f:
        if spec:
            spec = exchange.getbundlespec(ui, f)
            ui.write('%s\n' % spec)
            return

        gen = exchange.readbundle(ui, f, bundlepath)
        if isinstance(gen, bundle2.unbundle20):
            return _debugbundle2(ui, gen, all=all, **opts)
        _debugchangegroup(ui, gen, all=all, **opts)

@command('debugcapabilities',
        [], _('PATH'),
        norepo=True)
def debugcapabilities(ui, path, **opts):
    """lists the capabilities of a remote peer"""
    opts = pycompat.byteskwargs(opts)
    peer = hg.peer(ui, opts, path)
    caps = peer.capabilities()
    ui.write(('Main capabilities:\n'))
    for c in sorted(caps):
        ui.write(('  %s\n') % c)
    b2caps = bundle2.bundle2caps(peer)
    if b2caps:
        ui.write(('Bundle2 capabilities:\n'))
        for key, values in sorted(b2caps.iteritems()):
            ui.write(('  %s\n') % key)
            for v in values:
                ui.write(('    %s\n') % v)

@command('debugcheckstate', [], '')
def debugcheckstate(ui, repo):
    """validate the correctness of the current dirstate"""
    parent1, parent2 = repo.dirstate.parents()
    m1 = repo[parent1].manifest()
    m2 = repo[parent2].manifest()
    errors = 0
    for f in repo.dirstate:
        state = repo.dirstate[f]
        if state in "nr" and f not in m1:
            ui.warn(_("%s in state %s, but not in manifest1\n") % (f, state))
            errors += 1
        if state in "a" and f in m1:
            ui.warn(_("%s in state %s, but also in manifest1\n") % (f, state))
            errors += 1
        if state in "m" and f not in m1 and f not in m2:
            ui.warn(_("%s in state %s, but not in either manifest\n") %
                    (f, state))
            errors += 1
    for f in m1:
        state = repo.dirstate[f]
        if state not in "nrm":
            ui.warn(_("%s in manifest1, but listed as state %s") % (f, state))
            errors += 1
    if errors:
        error = _(".hg/dirstate inconsistent with current parent's manifest")
        raise error.Abort(error)

@command('debugcolor',
        [('', 'style', None, _('show all configured styles'))],
        'hg debugcolor')
def debugcolor(ui, repo, **opts):
    """show available color, effects or style"""
    ui.write(('color mode: %s\n') % stringutil.pprint(ui._colormode))
    if opts.get(r'style'):
        return _debugdisplaystyle(ui)
    else:
        return _debugdisplaycolor(ui)

def _debugdisplaycolor(ui):
    ui = ui.copy()
    ui._styles.clear()
    for effect in color._activeeffects(ui).keys():
        ui._styles[effect] = effect
    if ui._terminfoparams:
        for k, v in ui.configitems('color'):
            if k.startswith('color.'):
                ui._styles[k] = k[6:]
            elif k.startswith('terminfo.'):
                ui._styles[k] = k[9:]
    ui.write(_('available colors:\n'))
    # sort label with a '_' after the other to group '_background' entry.
    items = sorted(ui._styles.items(),
                   key=lambda i: ('_' in i[0], i[0], i[1]))
    for colorname, label in items:
        ui.write(('%s\n') % colorname, label=label)

def _debugdisplaystyle(ui):
    ui.write(_('available style:\n'))
    if not ui._styles:
        return
    width = max(len(s) for s in ui._styles)
    for label, effects in sorted(ui._styles.items()):
        ui.write('%s' % label, label=label)
        if effects:
            # 50
            ui.write(': ')
            ui.write(' ' * (max(0, width - len(label))))
            ui.write(', '.join(ui.label(e, e) for e in effects.split()))
        ui.write('\n')

@command('debugcreatestreamclonebundle', [], 'FILE')
def debugcreatestreamclonebundle(ui, repo, fname):
    """create a stream clone bundle file

    Stream bundles are special bundles that are essentially archives of
    revlog files. They are commonly used for cloning very quickly.
    """
    # TODO we may want to turn this into an abort when this functionality
    # is moved into `hg bundle`.
    if phases.hassecret(repo):
        ui.warn(_('(warning: stream clone bundle will contain secret '
                  'revisions)\n'))

    requirements, gen = streamclone.generatebundlev1(repo)
    changegroup.writechunks(ui, gen, fname)

    ui.write(_('bundle requirements: %s\n') % ', '.join(sorted(requirements)))

@command('debugdag',
    [('t', 'tags', None, _('use tags as labels')),
    ('b', 'branches', None, _('annotate with branch names')),
    ('', 'dots', None, _('use dots for runs')),
    ('s', 'spaces', None, _('separate elements by spaces'))],
    _('[OPTION]... [FILE [REV]...]'),
    optionalrepo=True)
def debugdag(ui, repo, file_=None, *revs, **opts):
    """format the changelog or an index DAG as a concise textual description

    If you pass a revlog index, the revlog's DAG is emitted. If you list
    revision numbers, they get labeled in the output as rN.

    Otherwise, the changelog DAG of the current repo is emitted.
    """
    spaces = opts.get(r'spaces')
    dots = opts.get(r'dots')
    if file_:
        rlog = revlog.revlog(vfsmod.vfs(encoding.getcwd(), audit=False),
                             file_)
        revs = set((int(r) for r in revs))
        def events():
            for r in rlog:
                yield 'n', (r, list(p for p in rlog.parentrevs(r)
                                        if p != -1))
                if r in revs:
                    yield 'l', (r, "r%i" % r)
    elif repo:
        cl = repo.changelog
        tags = opts.get(r'tags')
        branches = opts.get(r'branches')
        if tags:
            labels = {}
            for l, n in repo.tags().items():
                labels.setdefault(cl.rev(n), []).append(l)
        def events():
            b = "default"
            for r in cl:
                if branches:
                    newb = cl.read(cl.node(r))[5]['branch']
                    if newb != b:
                        yield 'a', newb
                        b = newb
                yield 'n', (r, list(p for p in cl.parentrevs(r)
                                        if p != -1))
                if tags:
                    ls = labels.get(r)
                    if ls:
                        for l in ls:
                            yield 'l', (r, l)
    else:
        raise error.Abort(_('need repo for changelog dag'))

    for line in dagparser.dagtextlines(events(),
                                       addspaces=spaces,
                                       wraplabels=True,
                                       wrapannotations=True,
                                       wrapnonlinear=dots,
                                       usedots=dots,
                                       maxlinewidth=70):
        ui.write(line)
        ui.write("\n")

@command('debugdata', cmdutil.debugrevlogopts, _('-c|-m|FILE REV'))
def debugdata(ui, repo, file_, rev=None, **opts):
    """dump the contents of a data file revision"""
    opts = pycompat.byteskwargs(opts)
    if opts.get('changelog') or opts.get('manifest') or opts.get('dir'):
        if rev is not None:
            raise error.CommandError('debugdata', _('invalid arguments'))
        file_, rev = None, file_
    elif rev is None:
        raise error.CommandError('debugdata', _('invalid arguments'))
    r = cmdutil.openstorage(repo, 'debugdata', file_, opts)
    try:
        ui.write(r.revision(r.lookup(rev), raw=True))
    except KeyError:
        raise error.Abort(_('invalid revision identifier %s') % rev)

@command('debugdate',
    [('e', 'extended', None, _('try extended date formats'))],
    _('[-e] DATE [RANGE]'),
    norepo=True, optionalrepo=True)
def debugdate(ui, date, range=None, **opts):
    """parse and display a date"""
    if opts[r"extended"]:
        d = dateutil.parsedate(date, util.extendeddateformats)
    else:
        d = dateutil.parsedate(date)
    ui.write(("internal: %d %d\n") % d)
    ui.write(("standard: %s\n") % dateutil.datestr(d))
    if range:
        m = dateutil.matchdate(range)
        ui.write(("match: %s\n") % m(d[0]))

@command('debugdeltachain',
    cmdutil.debugrevlogopts + cmdutil.formatteropts,
    _('-c|-m|FILE'),
    optionalrepo=True)
def debugdeltachain(ui, repo, file_=None, **opts):
    """dump information about delta chains in a revlog

    Output can be templatized. Available template keywords are:

    :``rev``:       revision number
    :``chainid``:   delta chain identifier (numbered by unique base)
    :``chainlen``:  delta chain length to this revision
    :``prevrev``:   previous revision in delta chain
    :``deltatype``: role of delta / how it was computed
    :``compsize``:  compressed size of revision
    :``uncompsize``: uncompressed size of revision
    :``chainsize``: total size of compressed revisions in chain
    :``chainratio``: total chain size divided by uncompressed revision size
                    (new delta chains typically start at ratio 2.00)
    :``lindist``:   linear distance from base revision in delta chain to end
                    of this revision
    :``extradist``: total size of revisions not part of this delta chain from
                    base of delta chain to end of this revision; a measurement
                    of how much extra data we need to read/seek across to read
                    the delta chain for this revision
    :``extraratio``: extradist divided by chainsize; another representation of
                    how much unrelated data is needed to load this delta chain

    If the repository is configured to use the sparse read, additional keywords
    are available:

    :``readsize``:     total size of data read from the disk for a revision
                       (sum of the sizes of all the blocks)
    :``largestblock``: size of the largest block of data read from the disk
    :``readdensity``:  density of useful bytes in the data read from the disk
    :``srchunks``:  in how many data hunks the whole revision would be read

    The sparse read can be enabled with experimental.sparse-read = True
    """
    opts = pycompat.byteskwargs(opts)
    r = cmdutil.openrevlog(repo, 'debugdeltachain', file_, opts)
    index = r.index
    start = r.start
    length = r.length
    generaldelta = r.version & revlog.FLAG_GENERALDELTA
    withsparseread = getattr(r, '_withsparseread', False)

    def revinfo(rev):
        e = index[rev]
        compsize = e[1]
        uncompsize = e[2]
        chainsize = 0

        if generaldelta:
            if e[3] == e[5]:
                deltatype = 'p1'
            elif e[3] == e[6]:
                deltatype = 'p2'
            elif e[3] == rev - 1:
                deltatype = 'prev'
            elif e[3] == rev:
                deltatype = 'base'
            else:
                deltatype = 'other'
        else:
            if e[3] == rev:
                deltatype = 'base'
            else:
                deltatype = 'prev'

        chain = r._deltachain(rev)[0]
        for iterrev in chain:
            e = index[iterrev]
            chainsize += e[1]

        return compsize, uncompsize, deltatype, chain, chainsize

    fm = ui.formatter('debugdeltachain', opts)

    fm.plain('    rev  chain# chainlen     prev   delta       '
             'size    rawsize  chainsize     ratio   lindist extradist '
             'extraratio')
    if withsparseread:
        fm.plain('   readsize largestblk rddensity srchunks')
    fm.plain('\n')

    chainbases = {}
    for rev in r:
        comp, uncomp, deltatype, chain, chainsize = revinfo(rev)
        chainbase = chain[0]
        chainid = chainbases.setdefault(chainbase, len(chainbases) + 1)
        basestart = start(chainbase)
        revstart = start(rev)
        lineardist = revstart + comp - basestart
        extradist = lineardist - chainsize
        try:
            prevrev = chain[-2]
        except IndexError:
            prevrev = -1

        if uncomp != 0:
            chainratio = float(chainsize) / float(uncomp)
        else:
            chainratio = chainsize

        if chainsize != 0:
            extraratio = float(extradist) / float(chainsize)
        else:
            extraratio = extradist

        fm.startitem()
        fm.write('rev chainid chainlen prevrev deltatype compsize '
                 'uncompsize chainsize chainratio lindist extradist '
                 'extraratio',
                 '%7d %7d %8d %8d %7s %10d %10d %10d %9.5f %9d %9d %10.5f',
                 rev, chainid, len(chain), prevrev, deltatype, comp,
                 uncomp, chainsize, chainratio, lineardist, extradist,
                 extraratio,
                 rev=rev, chainid=chainid, chainlen=len(chain),
                 prevrev=prevrev, deltatype=deltatype, compsize=comp,
                 uncompsize=uncomp, chainsize=chainsize,
                 chainratio=chainratio, lindist=lineardist,
                 extradist=extradist, extraratio=extraratio)
        if withsparseread:
            readsize = 0
            largestblock = 0
            srchunks = 0

            for revschunk in deltautil.slicechunk(r, chain):
                srchunks += 1
                blkend = start(revschunk[-1]) + length(revschunk[-1])
                blksize = blkend - start(revschunk[0])

                readsize += blksize
                if largestblock < blksize:
                    largestblock = blksize

            if readsize:
                readdensity = float(chainsize) / float(readsize)
            else:
                readdensity = 1

            fm.write('readsize largestblock readdensity srchunks',
                     ' %10d %10d %9.5f %8d',
                     readsize, largestblock, readdensity, srchunks,
                     readsize=readsize, largestblock=largestblock,
                     readdensity=readdensity, srchunks=srchunks)

        fm.plain('\n')

    fm.end()

@command('debugdirstate|debugstate',
    [('', 'nodates', None, _('do not display the saved mtime (DEPRECATED)')),
     ('', 'dates', True, _('display the saved mtime')),
     ('', 'datesort', None, _('sort by saved mtime'))],
    _('[OPTION]...'))
def debugstate(ui, repo, **opts):
    """show the contents of the current dirstate"""

    nodates = not opts[r'dates']
    if opts.get(r'nodates') is not None:
        nodates = True
    datesort = opts.get(r'datesort')

    timestr = ""
    if datesort:
        keyfunc = lambda x: (x[1][3], x[0]) # sort by mtime, then by filename
    else:
        keyfunc = None # sort by filename
    for file_, ent in sorted(repo.dirstate._map.iteritems(), key=keyfunc):
        if ent[3] == -1:
            timestr = 'unset               '
        elif nodates:
            timestr = 'set                 '
        else:
            timestr = time.strftime(r"%Y-%m-%d %H:%M:%S ",
                                    time.localtime(ent[3]))
            timestr = encoding.strtolocal(timestr)
        if ent[1] & 0o20000:
            mode = 'lnk'
        else:
            mode = '%3o' % (ent[1] & 0o777 & ~util.umask)
        ui.write("%c %s %10d %s%s\n" % (ent[0], mode, ent[2], timestr, file_))
    for f in repo.dirstate.copies():
        ui.write(_("copy: %s -> %s\n") % (repo.dirstate.copied(f), f))

@command('debugdiscovery',
    [('', 'old', None, _('use old-style discovery')),
    ('', 'nonheads', None,
     _('use old-style discovery with non-heads included')),
    ('', 'rev', [], 'restrict discovery to this set of revs'),
    ] + cmdutil.remoteopts,
    _('[--rev REV] [OTHER]'))
def debugdiscovery(ui, repo, remoteurl="default", **opts):
    """runs the changeset discovery protocol in isolation"""
    opts = pycompat.byteskwargs(opts)
    remoteurl, branches = hg.parseurl(ui.expandpath(remoteurl))
    remote = hg.peer(repo, opts, remoteurl)
    ui.status(_('comparing with %s\n') % util.hidepassword(remoteurl))

    # make sure tests are repeatable
    random.seed(12323)

    def doit(pushedrevs, remoteheads, remote=remote):
        if opts.get('old'):
            if not util.safehasattr(remote, 'branches'):
                # enable in-client legacy support
                remote = localrepo.locallegacypeer(remote.local())
            common, _in, hds = treediscovery.findcommonincoming(repo, remote,
                                                                force=True)
            common = set(common)
            if not opts.get('nonheads'):
                ui.write(("unpruned common: %s\n") %
                         " ".join(sorted(short(n) for n in common)))

                clnode = repo.changelog.node
                common = repo.revs('heads(::%ln)', common)
                common = {clnode(r) for r in common}
        else:
            nodes = None
            if pushedrevs:
                revs = scmutil.revrange(repo, pushedrevs)
                nodes = [repo[r].node() for r in revs]
            common, any, hds = setdiscovery.findcommonheads(ui, repo, remote,
                                                            ancestorsof=nodes)
        common = set(common)
        rheads = set(hds)
        lheads = set(repo.heads())
        ui.write(("common heads: %s\n") %
                 " ".join(sorted(short(n) for n in common)))
        if lheads <= common:
            ui.write(("local is subset\n"))
        elif rheads <= common:
            ui.write(("remote is subset\n"))

    remoterevs, _checkout = hg.addbranchrevs(repo, remote, branches, revs=None)
    localrevs = opts['rev']
    doit(localrevs, remoterevs)

_chunksize = 4 << 10

@command('debugdownload',
    [
        ('o', 'output', '', _('path')),
    ],
    optionalrepo=True)
def debugdownload(ui, repo, url, output=None, **opts):
    """download a resource using Mercurial logic and config
    """
    fh = urlmod.open(ui, url, output)

    dest = ui
    if output:
        dest = open(output, "wb", _chunksize)
    try:
        data = fh.read(_chunksize)
        while data:
            dest.write(data)
            data = fh.read(_chunksize)
    finally:
        if output:
            dest.close()

@command('debugextensions', cmdutil.formatteropts, [], optionalrepo=True)
def debugextensions(ui, repo, **opts):
    '''show information about active extensions'''
    opts = pycompat.byteskwargs(opts)
    exts = extensions.extensions(ui)
    hgver = util.version()
    fm = ui.formatter('debugextensions', opts)
    for extname, extmod in sorted(exts, key=operator.itemgetter(0)):
        isinternal = extensions.ismoduleinternal(extmod)
        extsource = pycompat.fsencode(extmod.__file__)
        if isinternal:
            exttestedwith = []  # never expose magic string to users
        else:
            exttestedwith = getattr(extmod, 'testedwith', '').split()
        extbuglink = getattr(extmod, 'buglink', None)

        fm.startitem()

        if ui.quiet or ui.verbose:
            fm.write('name', '%s\n', extname)
        else:
            fm.write('name', '%s', extname)
            if isinternal or hgver in exttestedwith:
                fm.plain('\n')
            elif not exttestedwith:
                fm.plain(_(' (untested!)\n'))
            else:
                lasttestedversion = exttestedwith[-1]
                fm.plain(' (%s!)\n' % lasttestedversion)

        fm.condwrite(ui.verbose and extsource, 'source',
                 _('  location: %s\n'), extsource or "")

        if ui.verbose:
            fm.plain(_('  bundled: %s\n') % ['no', 'yes'][isinternal])
        fm.data(bundled=isinternal)

        fm.condwrite(ui.verbose and exttestedwith, 'testedwith',
                     _('  tested with: %s\n'),
                     fm.formatlist(exttestedwith, name='ver'))

        fm.condwrite(ui.verbose and extbuglink, 'buglink',
                 _('  bug reporting: %s\n'), extbuglink or "")

    fm.end()

@command('debugfileset',
    [('r', 'rev', '', _('apply the filespec on this revision'), _('REV')),
     ('', 'all-files', False,
      _('test files from all revisions and working directory')),
     ('s', 'show-matcher', None,
      _('print internal representation of matcher')),
     ('p', 'show-stage', [],
      _('print parsed tree at the given stage'), _('NAME'))],
    _('[-r REV] [--all-files] [OPTION]... FILESPEC'))
def debugfileset(ui, repo, expr, **opts):
    '''parse and apply a fileset specification'''
    from . import fileset
    fileset.symbols # force import of fileset so we have predicates to optimize
    opts = pycompat.byteskwargs(opts)
    ctx = scmutil.revsingle(repo, opts.get('rev'), None)

    stages = [
        ('parsed', pycompat.identity),
        ('analyzed', filesetlang.analyze),
        ('optimized', filesetlang.optimize),
    ]
    stagenames = set(n for n, f in stages)

    showalways = set()
    if ui.verbose and not opts['show_stage']:
        # show parsed tree by --verbose (deprecated)
        showalways.add('parsed')
    if opts['show_stage'] == ['all']:
        showalways.update(stagenames)
    else:
        for n in opts['show_stage']:
            if n not in stagenames:
                raise error.Abort(_('invalid stage name: %s') % n)
        showalways.update(opts['show_stage'])

    tree = filesetlang.parse(expr)
    for n, f in stages:
        tree = f(tree)
        if n in showalways:
            if opts['show_stage'] or n != 'parsed':
                ui.write(("* %s:\n") % n)
            ui.write(filesetlang.prettyformat(tree), "\n")

    files = set()
    if opts['all_files']:
        for r in repo:
            c = repo[r]
            files.update(c.files())
            files.update(c.substate)
    if opts['all_files'] or ctx.rev() is None:
        wctx = repo[None]
        files.update(repo.dirstate.walk(scmutil.matchall(repo),
                                        subrepos=list(wctx.substate),
                                        unknown=True, ignored=True))
        files.update(wctx.substate)
    else:
        files.update(ctx.files())
        files.update(ctx.substate)

    m = ctx.matchfileset(expr)
    if opts['show_matcher'] or (opts['show_matcher'] is None and ui.verbose):
        ui.write(('* matcher:\n'), stringutil.prettyrepr(m), '\n')
    for f in sorted(files):
        if not m(f):
            continue
        ui.write("%s\n" % f)

@command('debugformat',
         [] + cmdutil.formatteropts)
def debugformat(ui, repo, **opts):
    """display format information about the current repository

    Use --verbose to get extra information about current config value and
    Mercurial default."""
    opts = pycompat.byteskwargs(opts)
    maxvariantlength = max(len(fv.name) for fv in upgrade.allformatvariant)
    maxvariantlength = max(len('format-variant'), maxvariantlength)

    def makeformatname(name):
        return '%s:' + (' ' * (maxvariantlength - len(name)))

    fm = ui.formatter('debugformat', opts)
    if fm.isplain():
        def formatvalue(value):
            if util.safehasattr(value, 'startswith'):
                return value
            if value:
                return 'yes'
            else:
                return 'no'
    else:
        formatvalue = pycompat.identity

    fm.plain('format-variant')
    fm.plain(' ' * (maxvariantlength - len('format-variant')))
    fm.plain(' repo')
    if ui.verbose:
        fm.plain(' config default')
    fm.plain('\n')
    for fv in upgrade.allformatvariant:
        fm.startitem()
        repovalue = fv.fromrepo(repo)
        configvalue = fv.fromconfig(repo)

        if repovalue != configvalue:
            namelabel = 'formatvariant.name.mismatchconfig'
            repolabel = 'formatvariant.repo.mismatchconfig'
        elif repovalue != fv.default:
            namelabel = 'formatvariant.name.mismatchdefault'
            repolabel = 'formatvariant.repo.mismatchdefault'
        else:
            namelabel = 'formatvariant.name.uptodate'
            repolabel = 'formatvariant.repo.uptodate'

        fm.write('name', makeformatname(fv.name), fv.name,
                 label=namelabel)
        fm.write('repo', ' %3s', formatvalue(repovalue),
                 label=repolabel)
        if fv.default != configvalue:
            configlabel = 'formatvariant.config.special'
        else:
            configlabel = 'formatvariant.config.default'
        fm.condwrite(ui.verbose, 'config', ' %6s', formatvalue(configvalue),
                     label=configlabel)
        fm.condwrite(ui.verbose, 'default', ' %7s', formatvalue(fv.default),
                     label='formatvariant.default')
        fm.plain('\n')
    fm.end()

@command('debugfsinfo', [], _('[PATH]'), norepo=True)
def debugfsinfo(ui, path="."):
    """show information detected about current filesystem"""
    ui.write(('path: %s\n') % path)
    ui.write(('mounted on: %s\n') % (util.getfsmountpoint(path) or '(unknown)'))
    ui.write(('exec: %s\n') % (util.checkexec(path) and 'yes' or 'no'))
    ui.write(('fstype: %s\n') % (util.getfstype(path) or '(unknown)'))
    ui.write(('symlink: %s\n') % (util.checklink(path) and 'yes' or 'no'))
    ui.write(('hardlink: %s\n') % (util.checknlink(path) and 'yes' or 'no'))
    casesensitive = '(unknown)'
    try:
        with pycompat.namedtempfile(prefix='.debugfsinfo', dir=path) as f:
            casesensitive = util.fscasesensitive(f.name) and 'yes' or 'no'
    except OSError:
        pass
    ui.write(('case-sensitive: %s\n') % casesensitive)

@command('debuggetbundle',
    [('H', 'head', [], _('id of head node'), _('ID')),
    ('C', 'common', [], _('id of common node'), _('ID')),
    ('t', 'type', 'bzip2', _('bundle compression type to use'), _('TYPE'))],
    _('REPO FILE [-H|-C ID]...'),
    norepo=True)
def debuggetbundle(ui, repopath, bundlepath, head=None, common=None, **opts):
    """retrieves a bundle from a repo

    Every ID must be a full-length hex node id string. Saves the bundle to the
    given file.
    """
    opts = pycompat.byteskwargs(opts)
    repo = hg.peer(ui, opts, repopath)
    if not repo.capable('getbundle'):
        raise error.Abort("getbundle() not supported by target repository")
    args = {}
    if common:
        args[r'common'] = [bin(s) for s in common]
    if head:
        args[r'heads'] = [bin(s) for s in head]
    # TODO: get desired bundlecaps from command line.
    args[r'bundlecaps'] = None
    bundle = repo.getbundle('debug', **args)

    bundletype = opts.get('type', 'bzip2').lower()
    btypes = {'none': 'HG10UN',
              'bzip2': 'HG10BZ',
              'gzip': 'HG10GZ',
              'bundle2': 'HG20'}
    bundletype = btypes.get(bundletype)
    if bundletype not in bundle2.bundletypes:
        raise error.Abort(_('unknown bundle type specified with --type'))
    bundle2.writebundle(ui, bundle, bundlepath, bundletype)

@command('debugignore', [], '[FILE]')
def debugignore(ui, repo, *files, **opts):
    """display the combined ignore pattern and information about ignored files

    With no argument display the combined ignore pattern.

    Given space separated file names, shows if the given file is ignored and
    if so, show the ignore rule (file and line number) that matched it.
    """
    ignore = repo.dirstate._ignore
    if not files:
        # Show all the patterns
        ui.write("%s\n" % pycompat.byterepr(ignore))
    else:
        m = scmutil.match(repo[None], pats=files)
        for f in m.files():
            nf = util.normpath(f)
            ignored = None
            ignoredata = None
            if nf != '.':
                if ignore(nf):
                    ignored = nf
                    ignoredata = repo.dirstate._ignorefileandline(nf)
                else:
                    for p in util.finddirs(nf):
                        if ignore(p):
                            ignored = p
                            ignoredata = repo.dirstate._ignorefileandline(p)
                            break
            if ignored:
                if ignored == nf:
                    ui.write(_("%s is ignored\n") % m.uipath(f))
                else:
                    ui.write(_("%s is ignored because of "
                               "containing folder %s\n")
                             % (m.uipath(f), ignored))
                ignorefile, lineno, line = ignoredata
                ui.write(_("(ignore rule in %s, line %d: '%s')\n")
                         % (ignorefile, lineno, line))
            else:
                ui.write(_("%s is not ignored\n") % m.uipath(f))

@command('debugindex', cmdutil.debugrevlogopts + cmdutil.formatteropts,
         _('-c|-m|FILE'))
def debugindex(ui, repo, file_=None, **opts):
    """dump index data for a storage primitive"""
    opts = pycompat.byteskwargs(opts)
    store = cmdutil.openstorage(repo, 'debugindex', file_, opts)

    if ui.debugflag:
        shortfn = hex
    else:
        shortfn = short

    idlen = 12
    for i in store:
        idlen = len(shortfn(store.node(i)))
        break

    fm = ui.formatter('debugindex', opts)
    fm.plain(b'   rev linkrev %s %s p2\n' % (
        b'nodeid'.ljust(idlen),
        b'p1'.ljust(idlen)))

    for rev in store:
        node = store.node(rev)
        parents = store.parents(node)

        fm.startitem()
        fm.write(b'rev', b'%6d ', rev)
        fm.write(b'linkrev', '%7d ', store.linkrev(rev))
        fm.write(b'node', '%s ', shortfn(node))
        fm.write(b'p1', '%s ', shortfn(parents[0]))
        fm.write(b'p2', '%s', shortfn(parents[1]))
        fm.plain(b'\n')

    fm.end()

@command('debugindexdot', cmdutil.debugrevlogopts,
    _('-c|-m|FILE'), optionalrepo=True)
def debugindexdot(ui, repo, file_=None, **opts):
    """dump an index DAG as a graphviz dot file"""
    opts = pycompat.byteskwargs(opts)
    r = cmdutil.openstorage(repo, 'debugindexdot', file_, opts)
    ui.write(("digraph G {\n"))
    for i in r:
        node = r.node(i)
        pp = r.parents(node)
        ui.write("\t%d -> %d\n" % (r.rev(pp[0]), i))
        if pp[1] != nullid:
            ui.write("\t%d -> %d\n" % (r.rev(pp[1]), i))
    ui.write("}\n")

@command('debugindexstats', [])
def debugindexstats(ui, repo):
    """show stats related to the changelog index"""
    repo.changelog.shortest(nullid, 1)
    index = repo.changelog.index
    if not util.safehasattr(index, 'stats'):
        raise error.Abort(_('debugindexstats only works with native code'))
    for k, v in sorted(index.stats().items()):
        ui.write('%s: %s\n' % (k, v))

@command('debuginstall', [] + cmdutil.formatteropts, '', norepo=True)
def debuginstall(ui, **opts):
    '''test Mercurial installation

    Returns 0 on success.
    '''
    opts = pycompat.byteskwargs(opts)

    def writetemp(contents):
        (fd, name) = pycompat.mkstemp(prefix="hg-debuginstall-")
        f = os.fdopen(fd, r"wb")
        f.write(contents)
        f.close()
        return name

    problems = 0

    fm = ui.formatter('debuginstall', opts)
    fm.startitem()

    # encoding
    fm.write('encoding', _("checking encoding (%s)...\n"), encoding.encoding)
    err = None
    try:
        codecs.lookup(pycompat.sysstr(encoding.encoding))
    except LookupError as inst:
        err = stringutil.forcebytestr(inst)
        problems += 1
    fm.condwrite(err, 'encodingerror', _(" %s\n"
                 " (check that your locale is properly set)\n"), err)

    # Python
    fm.write('pythonexe', _("checking Python executable (%s)\n"),
             pycompat.sysexecutable)
    fm.write('pythonver', _("checking Python version (%s)\n"),
             ("%d.%d.%d" % sys.version_info[:3]))
    fm.write('pythonlib', _("checking Python lib (%s)...\n"),
             os.path.dirname(pycompat.fsencode(os.__file__)))

    security = set(sslutil.supportedprotocols)
    if sslutil.hassni:
        security.add('sni')

    fm.write('pythonsecurity', _("checking Python security support (%s)\n"),
             fm.formatlist(sorted(security), name='protocol',
                           fmt='%s', sep=','))

    # These are warnings, not errors. So don't increment problem count. This
    # may change in the future.
    if 'tls1.2' not in security:
        fm.plain(_('  TLS 1.2 not supported by Python install; '
                   'network connections lack modern security\n'))
    if 'sni' not in security:
        fm.plain(_('  SNI not supported by Python install; may have '
                   'connectivity issues with some servers\n'))

    # TODO print CA cert info

    # hg version
    hgver = util.version()
    fm.write('hgver', _("checking Mercurial version (%s)\n"),
             hgver.split('+')[0])
    fm.write('hgverextra', _("checking Mercurial custom build (%s)\n"),
             '+'.join(hgver.split('+')[1:]))

    # compiled modules
    fm.write('hgmodulepolicy', _("checking module policy (%s)\n"),
             policy.policy)
    fm.write('hgmodules', _("checking installed modules (%s)...\n"),
             os.path.dirname(pycompat.fsencode(__file__)))

    if policy.policy in ('c', 'allow'):
        err = None
        try:
            from .cext import (
                base85,
                bdiff,
                mpatch,
                osutil,
            )
            dir(bdiff), dir(mpatch), dir(base85), dir(osutil) # quiet pyflakes
        except Exception as inst:
            err = stringutil.forcebytestr(inst)
            problems += 1
        fm.condwrite(err, 'extensionserror', " %s\n", err)

    compengines = util.compengines._engines.values()
    fm.write('compengines', _('checking registered compression engines (%s)\n'),
             fm.formatlist(sorted(e.name() for e in compengines),
                           name='compengine', fmt='%s', sep=', '))
    fm.write('compenginesavail', _('checking available compression engines '
                                   '(%s)\n'),
             fm.formatlist(sorted(e.name() for e in compengines
                                  if e.available()),
                           name='compengine', fmt='%s', sep=', '))
    wirecompengines = util.compengines.supportedwireengines(util.SERVERROLE)
    fm.write('compenginesserver', _('checking available compression engines '
                                    'for wire protocol (%s)\n'),
             fm.formatlist([e.name() for e in wirecompengines
                            if e.wireprotosupport()],
                           name='compengine', fmt='%s', sep=', '))
    re2 = 'missing'
    if util._re2:
        re2 = 'available'
    fm.plain(_('checking "re2" regexp engine (%s)\n') % re2)
    fm.data(re2=bool(util._re2))

    # templates
    p = templater.templatepaths()
    fm.write('templatedirs', 'checking templates (%s)...\n', ' '.join(p))
    fm.condwrite(not p, '', _(" no template directories found\n"))
    if p:
        m = templater.templatepath("map-cmdline.default")
        if m:
            # template found, check if it is working
            err = None
            try:
                templater.templater.frommapfile(m)
            except Exception as inst:
                err = stringutil.forcebytestr(inst)
                p = None
            fm.condwrite(err, 'defaulttemplateerror', " %s\n", err)
        else:
            p = None
        fm.condwrite(p, 'defaulttemplate',
                     _("checking default template (%s)\n"), m)
        fm.condwrite(not m, 'defaulttemplatenotfound',
                     _(" template '%s' not found\n"), "default")
    if not p:
        problems += 1
    fm.condwrite(not p, '',
                 _(" (templates seem to have been installed incorrectly)\n"))

    # editor
    editor = ui.geteditor()
    editor = util.expandpath(editor)
    editorbin = procutil.shellsplit(editor)[0]
    fm.write('editor', _("checking commit editor... (%s)\n"), editorbin)
    cmdpath = procutil.findexe(editorbin)
    fm.condwrite(not cmdpath and editor == 'sensible-editor', 'vinotfound',
                 _(" No commit editor set and can't find %s in PATH\n"
                   " (specify a commit editor in your configuration"
                   " file)\n"), not cmdpath and editor == 'sensible-editor' and editorbin)
    fm.condwrite(not cmdpath and editor != 'sensible-editor', 'editornotfound',
                 _(" Can't find editor '%s' in PATH\n"
                   " (specify a commit editor in your configuration"
                   " file)\n"), not cmdpath and editorbin)
    if not cmdpath and editor != 'sensible-editor':
        problems += 1

    # check username
    username = None
    err = None
    try:
        username = ui.username()
    except error.Abort as e:
        err = stringutil.forcebytestr(e)
        problems += 1

    fm.condwrite(username, 'username',  _("checking username (%s)\n"), username)
    fm.condwrite(err, 'usernameerror', _("checking username...\n %s\n"
        " (specify a username in your configuration file)\n"), err)

    fm.condwrite(not problems, '',
                 _("no problems detected\n"))
    if not problems:
        fm.data(problems=problems)
    fm.condwrite(problems, 'problems',
                 _("%d problems detected,"
                   " please check your install!\n"), problems)
    fm.end()

    return problems

@command('debugknown', [], _('REPO ID...'), norepo=True)
def debugknown(ui, repopath, *ids, **opts):
    """test whether node ids are known to a repo

    Every ID must be a full-length hex node id string. Returns a list of 0s
    and 1s indicating unknown/known.
    """
    opts = pycompat.byteskwargs(opts)
    repo = hg.peer(ui, opts, repopath)
    if not repo.capable('known'):
        raise error.Abort("known() not supported by target repository")
    flags = repo.known([bin(s) for s in ids])
    ui.write("%s\n" % ("".join([f and "1" or "0" for f in flags])))

@command('debuglabelcomplete', [], _('LABEL...'))
def debuglabelcomplete(ui, repo, *args):
    '''backwards compatibility with old bash completion scripts (DEPRECATED)'''
    debugnamecomplete(ui, repo, *args)

@command('debuglocks',
         [('L', 'force-lock', None, _('free the store lock (DANGEROUS)')),
          ('W', 'force-wlock', None,
           _('free the working state lock (DANGEROUS)')),
          ('s', 'set-lock', None, _('set the store lock until stopped')),
          ('S', 'set-wlock', None,
           _('set the working state lock until stopped'))],
         _('[OPTION]...'))
def debuglocks(ui, repo, **opts):
    """show or modify state of locks

    By default, this command will show which locks are held. This
    includes the user and process holding the lock, the amount of time
    the lock has been held, and the machine name where the process is
    running if it's not local.

    Locks protect the integrity of Mercurial's data, so should be
    treated with care. System crashes or other interruptions may cause
    locks to not be properly released, though Mercurial will usually
    detect and remove such stale locks automatically.

    However, detecting stale locks may not always be possible (for
    instance, on a shared filesystem). Removing locks may also be
    blocked by filesystem permissions.

    Setting a lock will prevent other commands from changing the data.
    The command will wait until an interruption (SIGINT, SIGTERM, ...) occurs.
    The set locks are removed when the command exits.

    Returns 0 if no locks are held.

    """

    if opts.get(r'force_lock'):
        repo.svfs.unlink('lock')
    if opts.get(r'force_wlock'):
        repo.vfs.unlink('wlock')
    if opts.get(r'force_lock') or opts.get(r'force_wlock'):
        return 0

    locks = []
    try:
        if opts.get(r'set_wlock'):
            try:
                locks.append(repo.wlock(False))
            except error.LockHeld:
                raise error.Abort(_('wlock is already held'))
        if opts.get(r'set_lock'):
            try:
                locks.append(repo.lock(False))
            except error.LockHeld:
                raise error.Abort(_('lock is already held'))
        if len(locks):
            ui.promptchoice(_("ready to release the lock (y)? $$ &Yes"))
            return 0
    finally:
        release(*locks)

    now = time.time()
    held = 0

    def report(vfs, name, method):
        # this causes stale locks to get reaped for more accurate reporting
        try:
            l = method(False)
        except error.LockHeld:
            l = None

        if l:
            l.release()
        else:
            try:
                st = vfs.lstat(name)
                age = now - st[stat.ST_MTIME]
                user = util.username(st.st_uid)
                locker = vfs.readlock(name)
                if ":" in locker:
                    host, pid = locker.split(':')
                    if host == socket.gethostname():
                        locker = 'user %s, process %s' % (user or b'None', pid)
                    else:
                        locker = 'user %s, process %s, host %s' \
                                 % (user or b'None', pid, host)
                ui.write(("%-6s %s (%ds)\n") % (name + ":", locker, age))
                return 1
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise

        ui.write(("%-6s free\n") % (name + ":"))
        return 0

    held += report(repo.svfs, "lock", repo.lock)
    held += report(repo.vfs, "wlock", repo.wlock)

    return held

@command('debugmanifestfulltextcache', [
        ('', 'clear', False, _('clear the cache')),
        ('a', 'add', '', _('add the given manifest node to the cache'),
         _('NODE'))
    ], '')
def debugmanifestfulltextcache(ui, repo, add=None, **opts):
    """show, clear or amend the contents of the manifest fulltext cache"""
    with repo.lock():
        r = repo.manifestlog.getstorage(b'')
        try:
            cache = r._fulltextcache
        except AttributeError:
            ui.warn(_(
                "Current revlog implementation doesn't appear to have a "
                'manifest fulltext cache\n'))
            return

        if opts.get(r'clear'):
            cache.clear()

        if add:
            try:
                manifest = repo.manifestlog[r.lookup(add)]
            except error.LookupError as e:
                raise error.Abort(e, hint="Check your manifest node id")
            manifest.read()  # stores revisision in cache too

        if not len(cache):
            ui.write(_('Cache empty'))
        else:
            ui.write(
                _('Cache contains %d manifest entries, in order of most to '
                  'least recent:\n') % (len(cache),))
            totalsize = 0
            for nodeid in cache:
                # Use cache.get to not update the LRU order
                data = cache.get(nodeid)
                size = len(data)
                totalsize += size + 24   # 20 bytes nodeid, 4 bytes size
                ui.write(_('id: %s, size %s\n') % (
                    hex(nodeid), util.bytecount(size)))
            ondisk = cache._opener.stat('manifestfulltextcache').st_size
            ui.write(
                _('Total cache data size %s, on-disk %s\n') % (
                    util.bytecount(totalsize), util.bytecount(ondisk))
            )

@command('debugmergestate', [], '')
def debugmergestate(ui, repo, *args):
    """print merge state

    Use --verbose to print out information about whether v1 or v2 merge state
    was chosen."""
    def _hashornull(h):
        if h == nullhex:
            return 'null'
        else:
            return h

    def printrecords(version):
        ui.write(('* version %d records\n') % version)
        if version == 1:
            records = v1records
        else:
            records = v2records

        for rtype, record in records:
            # pretty print some record types
            if rtype == 'L':
                ui.write(('local: %s\n') % record)
            elif rtype == 'O':
                ui.write(('other: %s\n') % record)
            elif rtype == 'm':
                driver, mdstate = record.split('\0', 1)
                ui.write(('merge driver: %s (state "%s")\n')
                         % (driver, mdstate))
            elif rtype in 'FDC':
                r = record.split('\0')
                f, state, hash, lfile, afile, anode, ofile = r[0:7]
                if version == 1:
                    onode = 'not stored in v1 format'
                    flags = r[7]
                else:
                    onode, flags = r[7:9]
                ui.write(('file: %s (record type "%s", state "%s", hash %s)\n')
                         % (f, rtype, state, _hashornull(hash)))
                ui.write(('  local path: %s (flags "%s")\n') % (lfile, flags))
                ui.write(('  ancestor path: %s (node %s)\n')
                         % (afile, _hashornull(anode)))
                ui.write(('  other path: %s (node %s)\n')
                         % (ofile, _hashornull(onode)))
            elif rtype == 'f':
                filename, rawextras = record.split('\0', 1)
                extras = rawextras.split('\0')
                i = 0
                extrastrings = []
                while i < len(extras):
                    extrastrings.append('%s = %s' % (extras[i], extras[i + 1]))
                    i += 2

                ui.write(('file extras: %s (%s)\n')
                         % (filename, ', '.join(extrastrings)))
            elif rtype == 'l':
                labels = record.split('\0', 2)
                labels = [l for l in labels if len(l) > 0]
                ui.write(('labels:\n'))
                ui.write(('  local: %s\n' % labels[0]))
                ui.write(('  other: %s\n' % labels[1]))
                if len(labels) > 2:
                    ui.write(('  base:  %s\n' % labels[2]))
            else:
                ui.write(('unrecognized entry: %s\t%s\n')
                         % (rtype, record.replace('\0', '\t')))

    # Avoid mergestate.read() since it may raise an exception for unsupported
    # merge state records. We shouldn't be doing this, but this is OK since this
    # command is pretty low-level.
    ms = mergemod.mergestate(repo)

    # sort so that reasonable information is on top
    v1records = ms._readrecordsv1()
    v2records = ms._readrecordsv2()
    order = 'LOml'
    def key(r):
        idx = order.find(r[0])
        if idx == -1:
            return (1, r[1])
        else:
            return (0, idx)
    v1records.sort(key=key)
    v2records.sort(key=key)

    if not v1records and not v2records:
        ui.write(('no merge state found\n'))
    elif not v2records:
        ui.note(('no version 2 merge state\n'))
        printrecords(1)
    elif ms._v1v2match(v1records, v2records):
        ui.note(('v1 and v2 states match: using v2\n'))
        printrecords(2)
    else:
        ui.note(('v1 and v2 states mismatch: using v1\n'))
        printrecords(1)
        if ui.verbose:
            printrecords(2)

@command('debugnamecomplete', [], _('NAME...'))
def debugnamecomplete(ui, repo, *args):
    '''complete "names" - tags, open branch names, bookmark names'''

    names = set()
    # since we previously only listed open branches, we will handle that
    # specially (after this for loop)
    for name, ns in repo.names.iteritems():
        if name != 'branches':
            names.update(ns.listnames(repo))
    names.update(tag for (tag, heads, tip, closed)
                 in repo.branchmap().iterbranches() if not closed)
    completions = set()
    if not args:
        args = ['']
    for a in args:
        completions.update(n for n in names if n.startswith(a))
    ui.write('\n'.join(sorted(completions)))
    ui.write('\n')

@command('debugobsolete',
        [('', 'flags', 0, _('markers flag')),
         ('', 'record-parents', False,
          _('record parent information for the precursor')),
         ('r', 'rev', [], _('display markers relevant to REV')),
         ('', 'exclusive', False, _('restrict display to markers only '
                                    'relevant to REV')),
         ('', 'index', False, _('display index of the marker')),
         ('', 'delete', [], _('delete markers specified by indices')),
        ] + cmdutil.commitopts2 + cmdutil.formatteropts,
         _('[OBSOLETED [REPLACEMENT ...]]'))
def debugobsolete(ui, repo, precursor=None, *successors, **opts):
    """create arbitrary obsolete marker

    With no arguments, displays the list of obsolescence markers."""

    opts = pycompat.byteskwargs(opts)

    def parsenodeid(s):
        try:
            # We do not use revsingle/revrange functions here to accept
            # arbitrary node identifiers, possibly not present in the
            # local repository.
            n = bin(s)
            if len(n) != len(nullid):
                raise TypeError()
            return n
        except TypeError:
            raise error.Abort('changeset references must be full hexadecimal '
                             'node identifiers')

    if opts.get('delete'):
        indices = []
        for v in opts.get('delete'):
            try:
                indices.append(int(v))
            except ValueError:
                raise error.Abort(_('invalid index value: %r') % v,
                                  hint=_('use integers for indices'))

        if repo.currenttransaction():
            raise error.Abort(_('cannot delete obsmarkers in the middle '
                                'of transaction.'))

        with repo.lock():
            n = repair.deleteobsmarkers(repo.obsstore, indices)
            ui.write(_('deleted %i obsolescence markers\n') % n)

        return

    if precursor is not None:
        if opts['rev']:
            raise error.Abort('cannot select revision when creating marker')
        metadata = {}
        metadata['user'] = encoding.fromlocal(opts['user'] or ui.username())
        succs = tuple(parsenodeid(succ) for succ in successors)
        l = repo.lock()
        try:
            tr = repo.transaction('debugobsolete')
            try:
                date = opts.get('date')
                if date:
                    date = dateutil.parsedate(date)
                else:
                    date = None
                prec = parsenodeid(precursor)
                parents = None
                if opts['record_parents']:
                    if prec not in repo.unfiltered():
                        raise error.Abort('cannot used --record-parents on '
                                         'unknown changesets')
                    parents = repo.unfiltered()[prec].parents()
                    parents = tuple(p.node() for p in parents)
                repo.obsstore.create(tr, prec, succs, opts['flags'],
                                     parents=parents, date=date,
                                     metadata=metadata, ui=ui)
                tr.close()
            except ValueError as exc:
                raise error.Abort(_('bad obsmarker input: %s') %
                                  pycompat.bytestr(exc))
            finally:
                tr.release()
        finally:
            l.release()
    else:
        if opts['rev']:
            revs = scmutil.revrange(repo, opts['rev'])
            nodes = [repo[r].node() for r in revs]
            markers = list(obsutil.getmarkers(repo, nodes=nodes,
                                               exclusive=opts['exclusive']))
            markers.sort(key=lambda x: x._data)
        else:
            markers = obsutil.getmarkers(repo)

        markerstoiter = markers
        isrelevant = lambda m: True
        if opts.get('rev') and opts.get('index'):
            markerstoiter = obsutil.getmarkers(repo)
            markerset = set(markers)
            isrelevant = lambda m: m in markerset

        fm = ui.formatter('debugobsolete', opts)
        for i, m in enumerate(markerstoiter):
            if not isrelevant(m):
                # marker can be irrelevant when we're iterating over a set
                # of markers (markerstoiter) which is bigger than the set
                # of markers we want to display (markers)
                # this can happen if both --index and --rev options are
                # provided and thus we need to iterate over all of the markers
                # to get the correct indices, but only display the ones that
                # are relevant to --rev value
                continue
            fm.startitem()
            ind = i if opts.get('index') else None
            cmdutil.showmarker(fm, m, index=ind)
        fm.end()

@command('debugpathcomplete',
         [('f', 'full', None, _('complete an entire path')),
          ('n', 'normal', None, _('show only normal files')),
          ('a', 'added', None, _('show only added files')),
          ('r', 'removed', None, _('show only removed files'))],
         _('FILESPEC...'))
def debugpathcomplete(ui, repo, *specs, **opts):
    '''complete part or all of a tracked path

    This command supports shells that offer path name completion. It
    currently completes only files already known to the dirstate.

    Completion extends only to the next path segment unless
    --full is specified, in which case entire paths are used.'''

    def complete(path, acceptable):
        dirstate = repo.dirstate
        spec = os.path.normpath(os.path.join(encoding.getcwd(), path))
        rootdir = repo.root + pycompat.ossep
        if spec != repo.root and not spec.startswith(rootdir):
            return [], []
        if os.path.isdir(spec):
            spec += '/'
        spec = spec[len(rootdir):]
        fixpaths = pycompat.ossep != '/'
        if fixpaths:
            spec = spec.replace(pycompat.ossep, '/')
        speclen = len(spec)
        fullpaths = opts[r'full']
        files, dirs = set(), set()
        adddir, addfile = dirs.add, files.add
        for f, st in dirstate.iteritems():
            if f.startswith(spec) and st[0] in acceptable:
                if fixpaths:
                    f = f.replace('/', pycompat.ossep)
                if fullpaths:
                    addfile(f)
                    continue
                s = f.find(pycompat.ossep, speclen)
                if s >= 0:
                    adddir(f[:s])
                else:
                    addfile(f)
        return files, dirs

    acceptable = ''
    if opts[r'normal']:
        acceptable += 'nm'
    if opts[r'added']:
        acceptable += 'a'
    if opts[r'removed']:
        acceptable += 'r'
    cwd = repo.getcwd()
    if not specs:
        specs = ['.']

    files, dirs = set(), set()
    for spec in specs:
        f, d = complete(spec, acceptable or 'nmar')
        files.update(f)
        dirs.update(d)
    files.update(dirs)
    ui.write('\n'.join(repo.pathto(p, cwd) for p in sorted(files)))
    ui.write('\n')

@command('debugpeer', [], _('PATH'), norepo=True)
def debugpeer(ui, path):
    """establish a connection to a peer repository"""
    # Always enable peer request logging. Requires --debug to display
    # though.
    overrides = {
        ('devel', 'debug.peer-request'): True,
    }

    with ui.configoverride(overrides):
        peer = hg.peer(ui, {}, path)

        local = peer.local() is not None
        canpush = peer.canpush()

        ui.write(_('url: %s\n') % peer.url())
        ui.write(_('local: %s\n') % (_('yes') if local else _('no')))
        ui.write(_('pushable: %s\n') % (_('yes') if canpush else _('no')))

@command('debugpickmergetool',
        [('r', 'rev', '', _('check for files in this revision'), _('REV')),
         ('', 'changedelete', None, _('emulate merging change and delete')),
        ] + cmdutil.walkopts + cmdutil.mergetoolopts,
        _('[PATTERN]...'),
        inferrepo=True)
def debugpickmergetool(ui, repo, *pats, **opts):
    """examine which merge tool is chosen for specified file

    As described in :hg:`help merge-tools`, Mercurial examines
    configurations below in this order to decide which merge tool is
    chosen for specified file.

    1. ``--tool`` option
    2. ``HGMERGE`` environment variable
    3. configurations in ``merge-patterns`` section
    4. configuration of ``ui.merge``
    5. configurations in ``merge-tools`` section
    6. ``hgmerge`` tool (for historical reason only)
    7. default tool for fallback (``:merge`` or ``:prompt``)

    This command writes out examination result in the style below::

        FILE = MERGETOOL

    By default, all files known in the first parent context of the
    working directory are examined. Use file patterns and/or -I/-X
    options to limit target files. -r/--rev is also useful to examine
    files in another context without actual updating to it.

    With --debug, this command shows warning messages while matching
    against ``merge-patterns`` and so on, too. It is recommended to
    use this option with explicit file patterns and/or -I/-X options,
    because this option increases amount of output per file according
    to configurations in hgrc.

    With -v/--verbose, this command shows configurations below at
    first (only if specified).

    - ``--tool`` option
    - ``HGMERGE`` environment variable
    - configuration of ``ui.merge``

    If merge tool is chosen before matching against
    ``merge-patterns``, this command can't show any helpful
    information, even with --debug. In such case, information above is
    useful to know why a merge tool is chosen.
    """
    opts = pycompat.byteskwargs(opts)
    overrides = {}
    if opts['tool']:
        overrides[('ui', 'forcemerge')] = opts['tool']
        ui.note(('with --tool %r\n') % (pycompat.bytestr(opts['tool'])))

    with ui.configoverride(overrides, 'debugmergepatterns'):
        hgmerge = encoding.environ.get("HGMERGE")
        if hgmerge is not None:
            ui.note(('with HGMERGE=%r\n') % (pycompat.bytestr(hgmerge)))
        uimerge = ui.config("ui", "merge")
        if uimerge:
            ui.note(('with ui.merge=%r\n') % (pycompat.bytestr(uimerge)))

        ctx = scmutil.revsingle(repo, opts.get('rev'))
        m = scmutil.match(ctx, pats, opts)
        changedelete = opts['changedelete']
        for path in ctx.walk(m):
            fctx = ctx[path]
            try:
                if not ui.debugflag:
                    ui.pushbuffer(error=True)
                tool, toolpath = filemerge._picktool(repo, ui, path,
                                                     fctx.isbinary(),
                                                     'l' in fctx.flags(),
                                                     changedelete)
            finally:
                if not ui.debugflag:
                    ui.popbuffer()
            ui.write(('%s = %s\n') % (path, tool))

@command('debugpushkey', [], _('REPO NAMESPACE [KEY OLD NEW]'), norepo=True)
def debugpushkey(ui, repopath, namespace, *keyinfo, **opts):
    '''access the pushkey key/value protocol

    With two args, list the keys in the given namespace.

    With five args, set a key to new if it currently is set to old.
    Reports success or failure.
    '''

    target = hg.peer(ui, {}, repopath)
    if keyinfo:
        key, old, new = keyinfo
        with target.commandexecutor() as e:
            r = e.callcommand('pushkey', {
                'namespace': namespace,
                'key': key,
                'old': old,
                'new': new,
            }).result()

        ui.status(pycompat.bytestr(r) + '\n')
        return not r
    else:
        for k, v in sorted(target.listkeys(namespace).iteritems()):
            ui.write("%s\t%s\n" % (stringutil.escapestr(k),
                                   stringutil.escapestr(v)))

@command('debugpvec', [], _('A B'))
def debugpvec(ui, repo, a, b=None):
    ca = scmutil.revsingle(repo, a)
    cb = scmutil.revsingle(repo, b)
    pa = pvec.ctxpvec(ca)
    pb = pvec.ctxpvec(cb)
    if pa == pb:
        rel = "="
    elif pa > pb:
        rel = ">"
    elif pa < pb:
        rel = "<"
    elif pa | pb:
        rel = "|"
    ui.write(_("a: %s\n") % pa)
    ui.write(_("b: %s\n") % pb)
    ui.write(_("depth(a): %d depth(b): %d\n") % (pa._depth, pb._depth))
    ui.write(_("delta: %d hdist: %d distance: %d relation: %s\n") %
             (abs(pa._depth - pb._depth), pvec._hamming(pa._vec, pb._vec),
              pa.distance(pb), rel))

@command('debugrebuilddirstate|debugrebuildstate',
    [('r', 'rev', '', _('revision to rebuild to'), _('REV')),
     ('', 'minimal', None, _('only rebuild files that are inconsistent with '
                             'the working copy parent')),
    ],
    _('[-r REV]'))
def debugrebuilddirstate(ui, repo, rev, **opts):
    """rebuild the dirstate as it would look like for the given revision

    If no revision is specified the first current parent will be used.

    The dirstate will be set to the files of the given revision.
    The actual working directory content or existing dirstate
    information such as adds or removes is not considered.

    ``minimal`` will only rebuild the dirstate status for files that claim to be
    tracked but are not in the parent manifest, or that exist in the parent
    manifest but are not in the dirstate. It will not change adds, removes, or
    modified files that are in the working copy parent.

    One use of this command is to make the next :hg:`status` invocation
    check the actual file content.
    """
    ctx = scmutil.revsingle(repo, rev)
    with repo.wlock():
        dirstate = repo.dirstate
        changedfiles = None
        # See command doc for what minimal does.
        if opts.get(r'minimal'):
            manifestfiles = set(ctx.manifest().keys())
            dirstatefiles = set(dirstate)
            manifestonly = manifestfiles - dirstatefiles
            dsonly = dirstatefiles - manifestfiles
            dsnotadded = set(f for f in dsonly if dirstate[f] != 'a')
            changedfiles = manifestonly | dsnotadded

        dirstate.rebuild(ctx.node(), ctx.manifest(), changedfiles)

@command('debugrebuildfncache', [], '')
def debugrebuildfncache(ui, repo):
    """rebuild the fncache file"""
    repair.rebuildfncache(ui, repo)

@command('debugrename',
    [('r', 'rev', '', _('revision to debug'), _('REV'))],
    _('[-r REV] FILE'))
def debugrename(ui, repo, file1, *pats, **opts):
    """dump rename information"""

    opts = pycompat.byteskwargs(opts)
    ctx = scmutil.revsingle(repo, opts.get('rev'))
    m = scmutil.match(ctx, (file1,) + pats, opts)
    for abs in ctx.walk(m):
        fctx = ctx[abs]
        o = fctx.filelog().renamed(fctx.filenode())
        rel = m.rel(abs)
        if o:
            ui.write(_("%s renamed from %s:%s\n") % (rel, o[0], hex(o[1])))
        else:
            ui.write(_("%s not renamed\n") % rel)

@command('debugrevlog', cmdutil.debugrevlogopts +
    [('d', 'dump', False, _('dump index data'))],
    _('-c|-m|FILE'),
    optionalrepo=True)
def debugrevlog(ui, repo, file_=None, **opts):
    """show data and statistics about a revlog"""
    opts = pycompat.byteskwargs(opts)
    r = cmdutil.openrevlog(repo, 'debugrevlog', file_, opts)

    if opts.get("dump"):
        numrevs = len(r)
        ui.write(("# rev p1rev p2rev start   end deltastart base   p1   p2"
                 " rawsize totalsize compression heads chainlen\n"))
        ts = 0
        heads = set()

        for rev in pycompat.xrange(numrevs):
            dbase = r.deltaparent(rev)
            if dbase == -1:
                dbase = rev
            cbase = r.chainbase(rev)
            clen = r.chainlen(rev)
            p1, p2 = r.parentrevs(rev)
            rs = r.rawsize(rev)
            ts = ts + rs
            heads -= set(r.parentrevs(rev))
            heads.add(rev)
            try:
                compression = ts / r.end(rev)
            except ZeroDivisionError:
                compression = 0
            ui.write("%5d %5d %5d %5d %5d %10d %4d %4d %4d %7d %9d "
                     "%11d %5d %8d\n" %
                     (rev, p1, p2, r.start(rev), r.end(rev),
                      r.start(dbase), r.start(cbase),
                      r.start(p1), r.start(p2),
                      rs, ts, compression, len(heads), clen))
        return 0

    v = r.version
    format = v & 0xFFFF
    flags = []
    gdelta = False
    if v & revlog.FLAG_INLINE_DATA:
        flags.append('inline')
    if v & revlog.FLAG_GENERALDELTA:
        gdelta = True
        flags.append('generaldelta')
    if not flags:
        flags = ['(none)']

    ### tracks merge vs single parent
    nummerges = 0

    ### tracks ways the "delta" are build
    # nodelta
    numempty = 0
    numemptytext = 0
    numemptydelta = 0
    # full file content
    numfull = 0
    # intermediate snapshot against a prior snapshot
    numsemi = 0
    # snapshot count per depth
    numsnapdepth = collections.defaultdict(lambda: 0)
    # delta against previous revision
    numprev = 0
    # delta against first or second parent (not prev)
    nump1 = 0
    nump2 = 0
    # delta against neither prev nor parents
    numother = 0
    # delta against prev that are also first or second parent
    # (details of `numprev`)
    nump1prev = 0
    nump2prev = 0

    # data about delta chain of each revs
    chainlengths = []
    chainbases = []
    chainspans = []

    # data about each revision
    datasize = [None, 0, 0]
    fullsize = [None, 0, 0]
    semisize = [None, 0, 0]
    # snapshot count per depth
    snapsizedepth = collections.defaultdict(lambda: [None, 0, 0])
    deltasize = [None, 0, 0]
    chunktypecounts = {}
    chunktypesizes = {}

    def addsize(size, l):
        if l[0] is None or size < l[0]:
            l[0] = size
        if size > l[1]:
            l[1] = size
        l[2] += size

    numrevs = len(r)
    for rev in pycompat.xrange(numrevs):
        p1, p2 = r.parentrevs(rev)
        delta = r.deltaparent(rev)
        if format > 0:
            addsize(r.rawsize(rev), datasize)
        if p2 != nullrev:
            nummerges += 1
        size = r.length(rev)
        if delta == nullrev:
            chainlengths.append(0)
            chainbases.append(r.start(rev))
            chainspans.append(size)
            if size == 0:
                numempty += 1
                numemptytext += 1
            else:
                numfull += 1
                numsnapdepth[0] += 1
                addsize(size, fullsize)
                addsize(size, snapsizedepth[0])
        else:
            chainlengths.append(chainlengths[delta] + 1)
            baseaddr = chainbases[delta]
            revaddr = r.start(rev)
            chainbases.append(baseaddr)
            chainspans.append((revaddr - baseaddr) + size)
            if size == 0:
                numempty += 1
                numemptydelta += 1
            elif r.issnapshot(rev):
                addsize(size, semisize)
                numsemi += 1
                depth = r.snapshotdepth(rev)
                numsnapdepth[depth] += 1
                addsize(size, snapsizedepth[depth])
            else:
                addsize(size, deltasize)
                if delta == rev - 1:
                    numprev += 1
                    if delta == p1:
                        nump1prev += 1
                    elif delta == p2:
                        nump2prev += 1
                elif delta == p1:
                    nump1 += 1
                elif delta == p2:
                    nump2 += 1
                elif delta != nullrev:
                    numother += 1

        # Obtain data on the raw chunks in the revlog.
        if util.safehasattr(r, '_getsegmentforrevs'):
            segment = r._getsegmentforrevs(rev, rev)[1]
        else:
            segment = r._revlog._getsegmentforrevs(rev, rev)[1]
        if segment:
            chunktype = bytes(segment[0:1])
        else:
            chunktype = 'empty'

        if chunktype not in chunktypecounts:
            chunktypecounts[chunktype] = 0
            chunktypesizes[chunktype] = 0

        chunktypecounts[chunktype] += 1
        chunktypesizes[chunktype] += size

    # Adjust size min value for empty cases
    for size in (datasize, fullsize, semisize, deltasize):
        if size[0] is None:
            size[0] = 0

    numdeltas = numrevs - numfull - numempty - numsemi
    numoprev = numprev - nump1prev - nump2prev
    totalrawsize = datasize[2]
    datasize[2] /= numrevs
    fulltotal = fullsize[2]
    fullsize[2] /= numfull
    semitotal = semisize[2]
    snaptotal = {}
    if numsemi > 0:
        semisize[2] /= numsemi
    for depth in snapsizedepth:
        snaptotal[depth] = snapsizedepth[depth][2]
        snapsizedepth[depth][2] /= numsnapdepth[depth]

    deltatotal = deltasize[2]
    if numdeltas > 0:
        deltasize[2] /= numdeltas
    totalsize = fulltotal + semitotal + deltatotal
    avgchainlen = sum(chainlengths) / numrevs
    maxchainlen = max(chainlengths)
    maxchainspan = max(chainspans)
    compratio = 1
    if totalsize:
        compratio = totalrawsize / totalsize

    basedfmtstr = '%%%dd\n'
    basepcfmtstr = '%%%dd %s(%%5.2f%%%%)\n'

    def dfmtstr(max):
        return basedfmtstr % len(str(max))
    def pcfmtstr(max, padding=0):
        return basepcfmtstr % (len(str(max)), ' ' * padding)

    def pcfmt(value, total):
        if total:
            return (value, 100 * float(value) / total)
        else:
            return value, 100.0

    ui.write(('format : %d\n') % format)
    ui.write(('flags  : %s\n') % ', '.join(flags))

    ui.write('\n')
    fmt = pcfmtstr(totalsize)
    fmt2 = dfmtstr(totalsize)
    ui.write(('revisions     : ') + fmt2 % numrevs)
    ui.write(('    merges    : ') + fmt % pcfmt(nummerges, numrevs))
    ui.write(('    normal    : ') + fmt % pcfmt(numrevs - nummerges, numrevs))
    ui.write(('revisions     : ') + fmt2 % numrevs)
    ui.write(('    empty     : ') + fmt % pcfmt(numempty, numrevs))
    ui.write(('                   text  : ')
             + fmt % pcfmt(numemptytext, numemptytext + numemptydelta))
    ui.write(('                   delta : ')
             + fmt % pcfmt(numemptydelta, numemptytext + numemptydelta))
    ui.write(('    snapshot  : ') + fmt % pcfmt(numfull + numsemi, numrevs))
    for depth in sorted(numsnapdepth):
        ui.write(('      lvl-%-3d :       ' % depth)
                 + fmt % pcfmt(numsnapdepth[depth], numrevs))
    ui.write(('    deltas    : ') + fmt % pcfmt(numdeltas, numrevs))
    ui.write(('revision size : ') + fmt2 % totalsize)
    ui.write(('    snapshot  : ')
             + fmt % pcfmt(fulltotal + semitotal, totalsize))
    for depth in sorted(numsnapdepth):
        ui.write(('      lvl-%-3d :       ' % depth)
                 + fmt % pcfmt(snaptotal[depth], totalsize))
    ui.write(('    deltas    : ') + fmt % pcfmt(deltatotal, totalsize))

    def fmtchunktype(chunktype):
        if chunktype == 'empty':
            return '    %s     : ' % chunktype
        elif chunktype in pycompat.bytestr(string.ascii_letters):
            return '    0x%s (%s)  : ' % (hex(chunktype), chunktype)
        else:
            return '    0x%s      : ' % hex(chunktype)

    ui.write('\n')
    ui.write(('chunks        : ') + fmt2 % numrevs)
    for chunktype in sorted(chunktypecounts):
        ui.write(fmtchunktype(chunktype))
        ui.write(fmt % pcfmt(chunktypecounts[chunktype], numrevs))
    ui.write(('chunks size   : ') + fmt2 % totalsize)
    for chunktype in sorted(chunktypecounts):
        ui.write(fmtchunktype(chunktype))
        ui.write(fmt % pcfmt(chunktypesizes[chunktype], totalsize))

    ui.write('\n')
    fmt = dfmtstr(max(avgchainlen, maxchainlen, maxchainspan, compratio))
    ui.write(('avg chain length  : ') + fmt % avgchainlen)
    ui.write(('max chain length  : ') + fmt % maxchainlen)
    ui.write(('max chain reach   : ') + fmt % maxchainspan)
    ui.write(('compression ratio : ') + fmt % compratio)

    if format > 0:
        ui.write('\n')
        ui.write(('uncompressed data size (min/max/avg) : %d / %d / %d\n')
                 % tuple(datasize))
    ui.write(('full revision size (min/max/avg)     : %d / %d / %d\n')
             % tuple(fullsize))
    ui.write(('inter-snapshot size (min/max/avg)    : %d / %d / %d\n')
             % tuple(semisize))
    for depth in sorted(snapsizedepth):
        if depth == 0:
            continue
        ui.write(('    level-%-3d (min/max/avg)          : %d / %d / %d\n')
                 % ((depth,) + tuple(snapsizedepth[depth])))
    ui.write(('delta size (min/max/avg)             : %d / %d / %d\n')
             % tuple(deltasize))

    if numdeltas > 0:
        ui.write('\n')
        fmt = pcfmtstr(numdeltas)
        fmt2 = pcfmtstr(numdeltas, 4)
        ui.write(('deltas against prev  : ') + fmt % pcfmt(numprev, numdeltas))
        if numprev > 0:
            ui.write(('    where prev = p1  : ') + fmt2 % pcfmt(nump1prev,
                                                              numprev))
            ui.write(('    where prev = p2  : ') + fmt2 % pcfmt(nump2prev,
                                                              numprev))
            ui.write(('    other            : ') + fmt2 % pcfmt(numoprev,
                                                              numprev))
        if gdelta:
            ui.write(('deltas against p1    : ')
                     + fmt % pcfmt(nump1, numdeltas))
            ui.write(('deltas against p2    : ')
                     + fmt % pcfmt(nump2, numdeltas))
            ui.write(('deltas against other : ') + fmt % pcfmt(numother,
                                                             numdeltas))

@command('debugrevlogindex', cmdutil.debugrevlogopts +
    [('f', 'format', 0, _('revlog format'), _('FORMAT'))],
    _('[-f FORMAT] -c|-m|FILE'),
    optionalrepo=True)
def debugrevlogindex(ui, repo, file_=None, **opts):
    """dump the contents of a revlog index"""
    opts = pycompat.byteskwargs(opts)
    r = cmdutil.openrevlog(repo, 'debugrevlogindex', file_, opts)
    format = opts.get('format', 0)
    if format not in (0, 1):
        raise error.Abort(_("unknown format %d") % format)

    if ui.debugflag:
        shortfn = hex
    else:
        shortfn = short

    # There might not be anything in r, so have a sane default
    idlen = 12
    for i in r:
        idlen = len(shortfn(r.node(i)))
        break

    if format == 0:
        if ui.verbose:
            ui.write(("   rev    offset  length linkrev"
                     " %s %s p2\n") % ("nodeid".ljust(idlen),
                                       "p1".ljust(idlen)))
        else:
            ui.write(("   rev linkrev %s %s p2\n") % (
                "nodeid".ljust(idlen), "p1".ljust(idlen)))
    elif format == 1:
        if ui.verbose:
            ui.write(("   rev flag   offset   length     size   link     p1"
                      "     p2 %s\n") % "nodeid".rjust(idlen))
        else:
            ui.write(("   rev flag     size   link     p1     p2 %s\n") %
                     "nodeid".rjust(idlen))

    for i in r:
        node = r.node(i)
        if format == 0:
            try:
                pp = r.parents(node)
            except Exception:
                pp = [nullid, nullid]
            if ui.verbose:
                ui.write("% 6d % 9d % 7d % 7d %s %s %s\n" % (
                        i, r.start(i), r.length(i), r.linkrev(i),
                        shortfn(node), shortfn(pp[0]), shortfn(pp[1])))
            else:
                ui.write("% 6d % 7d %s %s %s\n" % (
                    i, r.linkrev(i), shortfn(node), shortfn(pp[0]),
                    shortfn(pp[1])))
        elif format == 1:
            pr = r.parentrevs(i)
            if ui.verbose:
                ui.write("% 6d %04x % 8d % 8d % 8d % 6d % 6d % 6d %s\n" % (
                        i, r.flags(i), r.start(i), r.length(i), r.rawsize(i),
                        r.linkrev(i), pr[0], pr[1], shortfn(node)))
            else:
                ui.write("% 6d %04x % 8d % 6d % 6d % 6d %s\n" % (
                    i, r.flags(i), r.rawsize(i), r.linkrev(i), pr[0], pr[1],
                    shortfn(node)))

@command('debugrevspec',
    [('', 'optimize', None,
      _('print parsed tree after optimizing (DEPRECATED)')),
     ('', 'show-revs', True, _('print list of result revisions (default)')),
     ('s', 'show-set', None, _('print internal representation of result set')),
     ('p', 'show-stage', [],
      _('print parsed tree at the given stage'), _('NAME')),
     ('', 'no-optimized', False, _('evaluate tree without optimization')),
     ('', 'verify-optimized', False, _('verify optimized result')),
     ],
    ('REVSPEC'))
def debugrevspec(ui, repo, expr, **opts):
    """parse and apply a revision specification

    Use -p/--show-stage option to print the parsed tree at the given stages.
    Use -p all to print tree at every stage.

    Use --no-show-revs option with -s or -p to print only the set
    representation or the parsed tree respectively.

    Use --verify-optimized to compare the optimized result with the unoptimized
    one. Returns 1 if the optimized result differs.
    """
    opts = pycompat.byteskwargs(opts)
    aliases = ui.configitems('revsetalias')
    stages = [
        ('parsed', lambda tree: tree),
        ('expanded', lambda tree: revsetlang.expandaliases(tree, aliases,
                                                           ui.warn)),
        ('concatenated', revsetlang.foldconcat),
        ('analyzed', revsetlang.analyze),
        ('optimized', revsetlang.optimize),
    ]
    if opts['no_optimized']:
        stages = stages[:-1]
    if opts['verify_optimized'] and opts['no_optimized']:
        raise error.Abort(_('cannot use --verify-optimized with '
                            '--no-optimized'))
    stagenames = set(n for n, f in stages)

    showalways = set()
    showchanged = set()
    if ui.verbose and not opts['show_stage']:
        # show parsed tree by --verbose (deprecated)
        showalways.add('parsed')
        showchanged.update(['expanded', 'concatenated'])
        if opts['optimize']:
            showalways.add('optimized')
    if opts['show_stage'] and opts['optimize']:
        raise error.Abort(_('cannot use --optimize with --show-stage'))
    if opts['show_stage'] == ['all']:
        showalways.update(stagenames)
    else:
        for n in opts['show_stage']:
            if n not in stagenames:
                raise error.Abort(_('invalid stage name: %s') % n)
        showalways.update(opts['show_stage'])

    treebystage = {}
    printedtree = None
    tree = revsetlang.parse(expr, lookup=revset.lookupfn(repo))
    for n, f in stages:
        treebystage[n] = tree = f(tree)
        if n in showalways or (n in showchanged and tree != printedtree):
            if opts['show_stage'] or n != 'parsed':
                ui.write(("* %s:\n") % n)
            ui.write(revsetlang.prettyformat(tree), "\n")
            printedtree = tree

    if opts['verify_optimized']:
        arevs = revset.makematcher(treebystage['analyzed'])(repo)
        brevs = revset.makematcher(treebystage['optimized'])(repo)
        if opts['show_set'] or (opts['show_set'] is None and ui.verbose):
            ui.write(("* analyzed set:\n"), stringutil.prettyrepr(arevs), "\n")
            ui.write(("* optimized set:\n"), stringutil.prettyrepr(brevs), "\n")
        arevs = list(arevs)
        brevs = list(brevs)
        if arevs == brevs:
            return 0
        ui.write(('--- analyzed\n'), label='diff.file_a')
        ui.write(('+++ optimized\n'), label='diff.file_b')
        sm = difflib.SequenceMatcher(None, arevs, brevs)
        for tag, alo, ahi, blo, bhi in sm.get_opcodes():
            if tag in ('delete', 'replace'):
                for c in arevs[alo:ahi]:
                    ui.write('-%s\n' % c, label='diff.deleted')
            if tag in ('insert', 'replace'):
                for c in brevs[blo:bhi]:
                    ui.write('+%s\n' % c, label='diff.inserted')
            if tag == 'equal':
                for c in arevs[alo:ahi]:
                    ui.write(' %s\n' % c)
        return 1

    func = revset.makematcher(tree)
    revs = func(repo)
    if opts['show_set'] or (opts['show_set'] is None and ui.verbose):
        ui.write(("* set:\n"), stringutil.prettyrepr(revs), "\n")
    if not opts['show_revs']:
        return
    for c in revs:
        ui.write("%d\n" % c)

@command('debugserve', [
    ('', 'sshstdio', False, _('run an SSH server bound to process handles')),
    ('', 'logiofd', '', _('file descriptor to log server I/O to')),
    ('', 'logiofile', '', _('file to log server I/O to')),
], '')
def debugserve(ui, repo, **opts):
    """run a server with advanced settings

    This command is similar to :hg:`serve`. It exists partially as a
    workaround to the fact that ``hg serve --stdio`` must have specific
    arguments for security reasons.
    """
    opts = pycompat.byteskwargs(opts)

    if not opts['sshstdio']:
        raise error.Abort(_('only --sshstdio is currently supported'))

    logfh = None

    if opts['logiofd'] and opts['logiofile']:
        raise error.Abort(_('cannot use both --logiofd and --logiofile'))

    if opts['logiofd']:
        # Line buffered because output is line based.
        try:
            logfh = os.fdopen(int(opts['logiofd']), r'ab', 1)
        except OSError as e:
            if e.errno != errno.ESPIPE:
                raise
            # can't seek a pipe, so `ab` mode fails on py3
            logfh = os.fdopen(int(opts['logiofd']), r'wb', 1)
    elif opts['logiofile']:
        logfh = open(opts['logiofile'], 'ab', 1)

    s = wireprotoserver.sshserver(ui, repo, logfh=logfh)
    s.serve_forever()

@command('debugsetparents', [], _('REV1 [REV2]'))
def debugsetparents(ui, repo, rev1, rev2=None):
    """manually set the parents of the current working directory

    This is useful for writing repository conversion tools, but should
    be used with care. For example, neither the working directory nor the
    dirstate is updated, so file status may be incorrect after running this
    command.

    Returns 0 on success.
    """

    node1 = scmutil.revsingle(repo, rev1).node()
    node2 = scmutil.revsingle(repo, rev2, 'null').node()

    with repo.wlock():
        repo.setparents(node1, node2)

@command('debugssl', [], '[SOURCE]', optionalrepo=True)
def debugssl(ui, repo, source=None, **opts):
    '''test a secure connection to a server

    This builds the certificate chain for the server on Windows, installing the
    missing intermediates and trusted root via Windows Update if necessary.  It
    does nothing on other platforms.

    If SOURCE is omitted, the 'default' path will be used.  If a URL is given,
    that server is used. See :hg:`help urls` for more information.

    If the update succeeds, retry the original operation.  Otherwise, the cause
    of the SSL error is likely another issue.
    '''
    if not pycompat.iswindows:
        raise error.Abort(_('certificate chain building is only possible on '
                            'Windows'))

    if not source:
        if not repo:
            raise error.Abort(_("there is no Mercurial repository here, and no "
                                "server specified"))
        source = "default"

    source, branches = hg.parseurl(ui.expandpath(source))
    url = util.url(source)
    addr = None

    defaultport = {'https': 443, 'ssh': 22}
    if url.scheme in defaultport:
        try:
            addr = (url.host, int(url.port or defaultport[url.scheme]))
        except ValueError:
            raise error.Abort(_("malformed port number in URL"))
    else:
        raise error.Abort(_("only https and ssh connections are supported"))

    from . import win32

    s = ssl.wrap_socket(socket.socket(), ssl_version=ssl.PROTOCOL_TLS,
                        cert_reqs=ssl.CERT_NONE, ca_certs=None)

    try:
        s.connect(addr)
        cert = s.getpeercert(True)

        ui.status(_('checking the certificate chain for %s\n') % url.host)

        complete = win32.checkcertificatechain(cert, build=False)

        if not complete:
            ui.status(_('certificate chain is incomplete, updating... '))

            if not win32.checkcertificatechain(cert):
                ui.status(_('failed.\n'))
            else:
                ui.status(_('done.\n'))
        else:
            ui.status(_('full certificate chain is available\n'))
    finally:
        s.close()

@command('debugsub',
    [('r', 'rev', '',
     _('revision to check'), _('REV'))],
    _('[-r REV] [REV]'))
def debugsub(ui, repo, rev=None):
    ctx = scmutil.revsingle(repo, rev, None)
    for k, v in sorted(ctx.substate.items()):
        ui.write(('path %s\n') % k)
        ui.write((' source   %s\n') % v[0])
        ui.write((' revision %s\n') % v[1])

@command('debugsuccessorssets',
    [('', 'closest', False, _('return closest successors sets only'))],
    _('[REV]'))
def debugsuccessorssets(ui, repo, *revs, **opts):
    """show set of successors for revision

    A successors set of changeset A is a consistent group of revisions that
    succeed A. It contains non-obsolete changesets only unless closests
    successors set is set.

    In most cases a changeset A has a single successors set containing a single
    successor (changeset A replaced by A').

    A changeset that is made obsolete with no successors are called "pruned".
    Such changesets have no successors sets at all.

    A changeset that has been "split" will have a successors set containing
    more than one successor.

    A changeset that has been rewritten in multiple different ways is called
    "divergent". Such changesets have multiple successor sets (each of which
    may also be split, i.e. have multiple successors).

    Results are displayed as follows::

        <rev1>
            <successors-1A>
        <rev2>
            <successors-2A>
            <successors-2B1> <successors-2B2> <successors-2B3>

    Here rev2 has two possible (i.e. divergent) successors sets. The first
    holds one element, whereas the second holds three (i.e. the changeset has
    been split).
    """
    # passed to successorssets caching computation from one call to another
    cache = {}
    ctx2str = bytes
    node2str = short
    for rev in scmutil.revrange(repo, revs):
        ctx = repo[rev]
        ui.write('%s\n'% ctx2str(ctx))
        for succsset in obsutil.successorssets(repo, ctx.node(),
                                                closest=opts[r'closest'],
                                                cache=cache):
            if succsset:
                ui.write('    ')
                ui.write(node2str(succsset[0]))
                for node in succsset[1:]:
                    ui.write(' ')
                    ui.write(node2str(node))
            ui.write('\n')

@command('debugtemplate',
    [('r', 'rev', [], _('apply template on changesets'), _('REV')),
     ('D', 'define', [], _('define template keyword'), _('KEY=VALUE'))],
    _('[-r REV]... [-D KEY=VALUE]... TEMPLATE'),
    optionalrepo=True)
def debugtemplate(ui, repo, tmpl, **opts):
    """parse and apply a template

    If -r/--rev is given, the template is processed as a log template and
    applied to the given changesets. Otherwise, it is processed as a generic
    template.

    Use --verbose to print the parsed tree.
    """
    revs = None
    if opts[r'rev']:
        if repo is None:
            raise error.RepoError(_('there is no Mercurial repository here '
                                    '(.hg not found)'))
        revs = scmutil.revrange(repo, opts[r'rev'])

    props = {}
    for d in opts[r'define']:
        try:
            k, v = (e.strip() for e in d.split('=', 1))
            if not k or k == 'ui':
                raise ValueError
            props[k] = v
        except ValueError:
            raise error.Abort(_('malformed keyword definition: %s') % d)

    if ui.verbose:
        aliases = ui.configitems('templatealias')
        tree = templater.parse(tmpl)
        ui.note(templater.prettyformat(tree), '\n')
        newtree = templater.expandaliases(tree, aliases)
        if newtree != tree:
            ui.note(("* expanded:\n"), templater.prettyformat(newtree), '\n')

    if revs is None:
        tres = formatter.templateresources(ui, repo)
        t = formatter.maketemplater(ui, tmpl, resources=tres)
        if ui.verbose:
            kwds, funcs = t.symbolsuseddefault()
            ui.write(("* keywords: %s\n") % ', '.join(sorted(kwds)))
            ui.write(("* functions: %s\n") % ', '.join(sorted(funcs)))
        ui.write(t.renderdefault(props))
    else:
        displayer = logcmdutil.maketemplater(ui, repo, tmpl)
        if ui.verbose:
            kwds, funcs = displayer.t.symbolsuseddefault()
            ui.write(("* keywords: %s\n") % ', '.join(sorted(kwds)))
            ui.write(("* functions: %s\n") % ', '.join(sorted(funcs)))
        for r in revs:
            displayer.show(repo[r], **pycompat.strkwargs(props))
        displayer.close()

@command('debuguigetpass', [
    ('p', 'prompt', '', _('prompt text'), _('TEXT')),
], _('[-p TEXT]'), norepo=True)
def debuguigetpass(ui, prompt=''):
    """show prompt to type password"""
    r = ui.getpass(prompt)
    ui.write(('respose: %s\n') % r)

@command('debuguiprompt', [
    ('p', 'prompt', '', _('prompt text'), _('TEXT')),
], _('[-p TEXT]'), norepo=True)
def debuguiprompt(ui, prompt=''):
    """show plain prompt"""
    r = ui.prompt(prompt)
    ui.write(('response: %s\n') % r)

@command('debugupdatecaches', [])
def debugupdatecaches(ui, repo, *pats, **opts):
    """warm all known caches in the repository"""
    with repo.wlock(), repo.lock():
        repo.updatecaches(full=True)

@command('debugupgraderepo', [
    ('o', 'optimize', [], _('extra optimization to perform'), _('NAME')),
    ('', 'run', False, _('performs an upgrade')),
])
def debugupgraderepo(ui, repo, run=False, optimize=None):
    """upgrade a repository to use different features

    If no arguments are specified, the repository is evaluated for upgrade
    and a list of problems and potential optimizations is printed.

    With ``--run``, a repository upgrade is performed. Behavior of the upgrade
    can be influenced via additional arguments. More details will be provided
    by the command output when run without ``--run``.

    During the upgrade, the repository will be locked and no writes will be
    allowed.

    At the end of the upgrade, the repository may not be readable while new
    repository data is swapped in. This window will be as long as it takes to
    rename some directories inside the ``.hg`` directory. On most machines, this
    should complete almost instantaneously and the chances of a consumer being
    unable to access the repository should be low.
    """
    return upgrade.upgraderepo(ui, repo, run=run, optimize=optimize)

@command('debugwalk', cmdutil.walkopts, _('[OPTION]... [FILE]...'),
         inferrepo=True)
def debugwalk(ui, repo, *pats, **opts):
    """show how files match on given patterns"""
    opts = pycompat.byteskwargs(opts)
    m = scmutil.match(repo[None], pats, opts)
    if ui.verbose:
        ui.write(('* matcher:\n'), stringutil.prettyrepr(m), '\n')
    items = list(repo[None].walk(m))
    if not items:
        return
    f = lambda fn: fn
    if ui.configbool('ui', 'slash') and pycompat.ossep != '/':
        f = lambda fn: util.normpath(fn)
    fmt = 'f  %%-%ds  %%-%ds  %%s' % (
        max([len(abs) for abs in items]),
        max([len(m.rel(abs)) for abs in items]))
    for abs in items:
        line = fmt % (abs, f(m.rel(abs)), m.exact(abs) and 'exact' or '')
        ui.write("%s\n" % line.rstrip())

@command('debugwhyunstable', [], _('REV'))
def debugwhyunstable(ui, repo, rev):
    """explain instabilities of a changeset"""
    for entry in obsutil.whyunstable(repo, scmutil.revsingle(repo, rev)):
        dnodes = ''
        if entry.get('divergentnodes'):
            dnodes = ' '.join('%s (%s)' % (ctx.hex(), ctx.phasestr())
                              for ctx in entry['divergentnodes']) + ' '
        ui.write('%s: %s%s %s\n' % (entry['instability'], dnodes,
                                    entry['reason'], entry['node']))

@command('debugwireargs',
    [('', 'three', '', 'three'),
    ('', 'four', '', 'four'),
    ('', 'five', '', 'five'),
    ] + cmdutil.remoteopts,
    _('REPO [OPTIONS]... [ONE [TWO]]'),
    norepo=True)
def debugwireargs(ui, repopath, *vals, **opts):
    opts = pycompat.byteskwargs(opts)
    repo = hg.peer(ui, opts, repopath)
    for opt in cmdutil.remoteopts:
        del opts[opt[1]]
    args = {}
    for k, v in opts.iteritems():
        if v:
            args[k] = v
    args = pycompat.strkwargs(args)
    # run twice to check that we don't mess up the stream for the next command
    res1 = repo.debugwireargs(*vals, **args)
    res2 = repo.debugwireargs(*vals, **args)
    ui.write("%s\n" % res1)
    if res1 != res2:
        ui.warn("%s\n" % res2)

def _parsewirelangblocks(fh):
    activeaction = None
    blocklines = []
    lastindent = 0

    for line in fh:
        line = line.rstrip()
        if not line:
            continue

        if line.startswith(b'#'):
            continue

        if not line.startswith(b' '):
            # New block. Flush previous one.
            if activeaction:
                yield activeaction, blocklines

            activeaction = line
            blocklines = []
            lastindent = 0
            continue

        # Else we start with an indent.

        if not activeaction:
            raise error.Abort(_('indented line outside of block'))

        indent = len(line) - len(line.lstrip())

        # If this line is indented more than the last line, concatenate it.
        if indent > lastindent and blocklines:
            blocklines[-1] += line.lstrip()
        else:
            blocklines.append(line)
            lastindent = indent

    # Flush last block.
    if activeaction:
        yield activeaction, blocklines

@command('debugwireproto',
    [
        ('', 'localssh', False, _('start an SSH server for this repo')),
        ('', 'peer', '', _('construct a specific version of the peer')),
        ('', 'noreadstderr', False, _('do not read from stderr of the remote')),
        ('', 'nologhandshake', False,
         _('do not log I/O related to the peer handshake')),
    ] + cmdutil.remoteopts,
    _('[PATH]'),
    optionalrepo=True)
def debugwireproto(ui, repo, path=None, **opts):
    """send wire protocol commands to a server

    This command can be used to issue wire protocol commands to remote
    peers and to debug the raw data being exchanged.

    ``--localssh`` will start an SSH server against the current repository
    and connect to that. By default, the connection will perform a handshake
    and establish an appropriate peer instance.

    ``--peer`` can be used to bypass the handshake protocol and construct a
    peer instance using the specified class type. Valid values are ``raw``,
    ``http2``, ``ssh1``, and ``ssh2``. ``raw`` instances only allow sending
    raw data payloads and don't support higher-level command actions.

    ``--noreadstderr`` can be used to disable automatic reading from stderr
    of the peer (for SSH connections only). Disabling automatic reading of
    stderr is useful for making output more deterministic.

    Commands are issued via a mini language which is specified via stdin.
    The language consists of individual actions to perform. An action is
    defined by a block. A block is defined as a line with no leading
    space followed by 0 or more lines with leading space. Blocks are
    effectively a high-level command with additional metadata.

    Lines beginning with ``#`` are ignored.

    The following sections denote available actions.

    raw
    ---

    Send raw data to the server.

    The block payload contains the raw data to send as one atomic send
    operation. The data may not actually be delivered in a single system
    call: it depends on the abilities of the transport being used.

    Each line in the block is de-indented and concatenated. Then, that
    value is evaluated as a Python b'' literal. This allows the use of
    backslash escaping, etc.

    raw+
    ----

    Behaves like ``raw`` except flushes output afterwards.

    command <X>
    -----------

    Send a request to run a named command, whose name follows the ``command``
    string.

    Arguments to the command are defined as lines in this block. The format of
    each line is ``<key> <value>``. e.g.::

       command listkeys
           namespace bookmarks

    If the value begins with ``eval:``, it will be interpreted as a Python
    literal expression. Otherwise values are interpreted as Python b'' literals.
    This allows sending complex types and encoding special byte sequences via
    backslash escaping.

    The following arguments have special meaning:

    ``PUSHFILE``
        When defined, the *push* mechanism of the peer will be used instead
        of the static request-response mechanism and the content of the
        file specified in the value of this argument will be sent as the
        command payload.

        This can be used to submit a local bundle file to the remote.

    batchbegin
    ----------

    Instruct the peer to begin a batched send.

    All ``command`` blocks are queued for execution until the next
    ``batchsubmit`` block.

    batchsubmit
    -----------

    Submit previously queued ``command`` blocks as a batch request.

    This action MUST be paired with a ``batchbegin`` action.

    httprequest <method> <path>
    ---------------------------

    (HTTP peer only)

    Send an HTTP request to the peer.

    The HTTP request line follows the ``httprequest`` action. e.g. ``GET /foo``.

    Arguments of the form ``<key>: <value>`` are interpreted as HTTP request
    headers to add to the request. e.g. ``Accept: foo``.

    The following arguments are special:

    ``BODYFILE``
        The content of the file defined as the value to this argument will be
        transferred verbatim as the HTTP request body.

    ``frame <type> <flags> <payload>``
        Send a unified protocol frame as part of the request body.

        All frames will be collected and sent as the body to the HTTP
        request.

    close
    -----

    Close the connection to the server.

    flush
    -----

    Flush data written to the server.

    readavailable
    -------------

    Close the write end of the connection and read all available data from
    the server.

    If the connection to the server encompasses multiple pipes, we poll both
    pipes and read available data.

    readline
    --------

    Read a line of output from the server. If there are multiple output
    pipes, reads only the main pipe.

    ereadline
    ---------

    Like ``readline``, but read from the stderr pipe, if available.

    read <X>
    --------

    ``read()`` N bytes from the server's main output pipe.

    eread <X>
    ---------

    ``read()`` N bytes from the server's stderr pipe, if available.

    Specifying Unified Frame-Based Protocol Frames
    ----------------------------------------------

    It is possible to emit a *Unified Frame-Based Protocol* by using special
    syntax.

    A frame is composed as a type, flags, and payload. These can be parsed
    from a string of the form:

       <request-id> <stream-id> <stream-flags> <type> <flags> <payload>

    ``request-id`` and ``stream-id`` are integers defining the request and
    stream identifiers.

    ``type`` can be an integer value for the frame type or the string name
    of the type. The strings are defined in ``wireprotoframing.py``. e.g.
    ``command-name``.

    ``stream-flags`` and ``flags`` are a ``|`` delimited list of flag
    components. Each component (and there can be just one) can be an integer
    or a flag name for stream flags or frame flags, respectively. Values are
    resolved to integers and then bitwise OR'd together.

    ``payload`` represents the raw frame payload. If it begins with
    ``cbor:``, the following string is evaluated as Python code and the
    resulting object is fed into a CBOR encoder. Otherwise it is interpreted
    as a Python byte string literal.
    """
    opts = pycompat.byteskwargs(opts)

    if opts['localssh'] and not repo:
        raise error.Abort(_('--localssh requires a repository'))

    if opts['peer'] and opts['peer'] not in ('raw', 'http2', 'ssh1', 'ssh2'):
        raise error.Abort(_('invalid value for --peer'),
                          hint=_('valid values are "raw", "ssh1", and "ssh2"'))

    if path and opts['localssh']:
        raise error.Abort(_('cannot specify --localssh with an explicit '
                            'path'))

    if ui.interactive():
        ui.write(_('(waiting for commands on stdin)\n'))

    blocks = list(_parsewirelangblocks(ui.fin))

    proc = None
    stdin = None
    stdout = None
    stderr = None
    opener = None

    if opts['localssh']:
        # We start the SSH server in its own process so there is process
        # separation. This prevents a whole class of potential bugs around
        # shared state from interfering with server operation.
        args = procutil.hgcmd() + [
            '-R', repo.root,
            'debugserve', '--sshstdio',
        ]
        proc = subprocess.Popen(pycompat.rapply(procutil.tonativestr, args),
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                bufsize=0)

        stdin = proc.stdin
        stdout = proc.stdout
        stderr = proc.stderr

        # We turn the pipes into observers so we can log I/O.
        if ui.verbose or opts['peer'] == 'raw':
            stdin = util.makeloggingfileobject(ui, proc.stdin, b'i',
                                               logdata=True)
            stdout = util.makeloggingfileobject(ui, proc.stdout, b'o',
                                                logdata=True)
            stderr = util.makeloggingfileobject(ui, proc.stderr, b'e',
                                                logdata=True)

        # --localssh also implies the peer connection settings.

        url = 'ssh://localserver'
        autoreadstderr = not opts['noreadstderr']

        if opts['peer'] == 'ssh1':
            ui.write(_('creating ssh peer for wire protocol version 1\n'))
            peer = sshpeer.sshv1peer(ui, url, proc, stdin, stdout, stderr,
                                     None, autoreadstderr=autoreadstderr)
        elif opts['peer'] == 'ssh2':
            ui.write(_('creating ssh peer for wire protocol version 2\n'))
            peer = sshpeer.sshv2peer(ui, url, proc, stdin, stdout, stderr,
                                     None, autoreadstderr=autoreadstderr)
        elif opts['peer'] == 'raw':
            ui.write(_('using raw connection to peer\n'))
            peer = None
        else:
            ui.write(_('creating ssh peer from handshake results\n'))
            peer = sshpeer.makepeer(ui, url, proc, stdin, stdout, stderr,
                                    autoreadstderr=autoreadstderr)

    elif path:
        # We bypass hg.peer() so we can proxy the sockets.
        # TODO consider not doing this because we skip
        # ``hg.wirepeersetupfuncs`` and potentially other useful functionality.
        u = util.url(path)
        if u.scheme != 'http':
            raise error.Abort(_('only http:// paths are currently supported'))

        url, authinfo = u.authinfo()
        openerargs = {
            r'useragent': b'Mercurial debugwireproto',
        }

        # Turn pipes/sockets into observers so we can log I/O.
        if ui.verbose:
            openerargs.update({
                r'loggingfh': ui,
                r'loggingname': b's',
                r'loggingopts': {
                    r'logdata': True,
                    r'logdataapis': False,
                },
            })

        if ui.debugflag:
            openerargs[r'loggingopts'][r'logdataapis'] = True

        # Don't send default headers when in raw mode. This allows us to
        # bypass most of the behavior of our URL handling code so we can
        # have near complete control over what's sent on the wire.
        if opts['peer'] == 'raw':
            openerargs[r'sendaccept'] = False

        opener = urlmod.opener(ui, authinfo, **openerargs)

        if opts['peer'] == 'http2':
            ui.write(_('creating http peer for wire protocol version 2\n'))
            # We go through makepeer() because we need an API descriptor for
            # the peer instance to be useful.
            with ui.configoverride({
                ('experimental', 'httppeer.advertise-v2'): True}):
                if opts['nologhandshake']:
                    ui.pushbuffer()

                peer = httppeer.makepeer(ui, path, opener=opener)

                if opts['nologhandshake']:
                    ui.popbuffer()

            if not isinstance(peer, httppeer.httpv2peer):
                raise error.Abort(_('could not instantiate HTTP peer for '
                                    'wire protocol version 2'),
                                  hint=_('the server may not have the feature '
                                         'enabled or is not allowing this '
                                         'client version'))

        elif opts['peer'] == 'raw':
            ui.write(_('using raw connection to peer\n'))
            peer = None
        elif opts['peer']:
            raise error.Abort(_('--peer %s not supported with HTTP peers') %
                              opts['peer'])
        else:
            peer = httppeer.makepeer(ui, path, opener=opener)

        # We /could/ populate stdin/stdout with sock.makefile()...
    else:
        raise error.Abort(_('unsupported connection configuration'))

    batchedcommands = None

    # Now perform actions based on the parsed wire language instructions.
    for action, lines in blocks:
        if action in ('raw', 'raw+'):
            if not stdin:
                raise error.Abort(_('cannot call raw/raw+ on this peer'))

            # Concatenate the data together.
            data = ''.join(l.lstrip() for l in lines)
            data = stringutil.unescapestr(data)
            stdin.write(data)

            if action == 'raw+':
                stdin.flush()
        elif action == 'flush':
            if not stdin:
                raise error.Abort(_('cannot call flush on this peer'))
            stdin.flush()
        elif action.startswith('command'):
            if not peer:
                raise error.Abort(_('cannot send commands unless peer instance '
                                    'is available'))

            command = action.split(' ', 1)[1]

            args = {}
            for line in lines:
                # We need to allow empty values.
                fields = line.lstrip().split(' ', 1)
                if len(fields) == 1:
                    key = fields[0]
                    value = ''
                else:
                    key, value = fields

                if value.startswith('eval:'):
                    value = stringutil.evalpythonliteral(value[5:])
                else:
                    value = stringutil.unescapestr(value)

                args[key] = value

            if batchedcommands is not None:
                batchedcommands.append((command, args))
                continue

            ui.status(_('sending %s command\n') % command)

            if 'PUSHFILE' in args:
                with open(args['PUSHFILE'], r'rb') as fh:
                    del args['PUSHFILE']
                    res, output = peer._callpush(command, fh,
                                                 **pycompat.strkwargs(args))
                    ui.status(_('result: %s\n') % stringutil.escapestr(res))
                    ui.status(_('remote output: %s\n') %
                              stringutil.escapestr(output))
            else:
                with peer.commandexecutor() as e:
                    res = e.callcommand(command, args).result()

                if isinstance(res, wireprotov2peer.commandresponse):
                    val = res.objects()
                    ui.status(_('response: %s\n') %
                              stringutil.pprint(val, bprefix=True, indent=2))
                else:
                    ui.status(_('response: %s\n') %
                              stringutil.pprint(res, bprefix=True, indent=2))

        elif action == 'batchbegin':
            if batchedcommands is not None:
                raise error.Abort(_('nested batchbegin not allowed'))

            batchedcommands = []
        elif action == 'batchsubmit':
            # There is a batching API we could go through. But it would be
            # difficult to normalize requests into function calls. It is easier
            # to bypass this layer and normalize to commands + args.
            ui.status(_('sending batch with %d sub-commands\n') %
                      len(batchedcommands))
            for i, chunk in enumerate(peer._submitbatch(batchedcommands)):
                ui.status(_('response #%d: %s\n') %
                          (i, stringutil.escapestr(chunk)))

            batchedcommands = None

        elif action.startswith('httprequest '):
            if not opener:
                raise error.Abort(_('cannot use httprequest without an HTTP '
                                    'peer'))

            request = action.split(' ', 2)
            if len(request) != 3:
                raise error.Abort(_('invalid httprequest: expected format is '
                                    '"httprequest <method> <path>'))

            method, httppath = request[1:]
            headers = {}
            body = None
            frames = []
            for line in lines:
                line = line.lstrip()
                m = re.match(b'^([a-zA-Z0-9_-]+): (.*)$', line)
                if m:
                    # Headers need to use native strings.
                    key = pycompat.strurl(m.group(1))
                    value = pycompat.strurl(m.group(2))
                    headers[key] = value
                    continue

                if line.startswith(b'BODYFILE '):
                    with open(line.split(b' ', 1), 'rb') as fh:
                        body = fh.read()
                elif line.startswith(b'frame '):
                    frame = wireprotoframing.makeframefromhumanstring(
                        line[len(b'frame '):])

                    frames.append(frame)
                else:
                    raise error.Abort(_('unknown argument to httprequest: %s') %
                                      line)

            url = path + httppath

            if frames:
                body = b''.join(bytes(f) for f in frames)

            req = urlmod.urlreq.request(pycompat.strurl(url), body, headers)

            # urllib.Request insists on using has_data() as a proxy for
            # determining the request method. Override that to use our
            # explicitly requested method.
            req.get_method = lambda: pycompat.sysstr(method)

            try:
                res = opener.open(req)
                body = res.read()
            except util.urlerr.urlerror as e:
                # read() method must be called, but only exists in Python 2
                getattr(e, 'read', lambda: None)()
                continue

            ct = res.headers.get(r'Content-Type')
            if ct == r'application/mercurial-cbor':
                ui.write(_('cbor> %s\n') %
                         stringutil.pprint(cborutil.decodeall(body),
                                           bprefix=True,
                                           indent=2))

        elif action == 'close':
            peer.close()
        elif action == 'readavailable':
            if not stdout or not stderr:
                raise error.Abort(_('readavailable not available on this peer'))

            stdin.close()
            stdout.read()
            stderr.read()

        elif action == 'readline':
            if not stdout:
                raise error.Abort(_('readline not available on this peer'))
            stdout.readline()
        elif action == 'ereadline':
            if not stderr:
                raise error.Abort(_('ereadline not available on this peer'))
            stderr.readline()
        elif action.startswith('read '):
            count = int(action.split(' ', 1)[1])
            if not stdout:
                raise error.Abort(_('read not available on this peer'))
            stdout.read(count)
        elif action.startswith('eread '):
            count = int(action.split(' ', 1)[1])
            if not stderr:
                raise error.Abort(_('eread not available on this peer'))
            stderr.read(count)
        else:
            raise error.Abort(_('unknown action: %s') % action)

    if batchedcommands is not None:
        raise error.Abort(_('unclosed "batchbegin" request'))

    if peer:
        peer.close()

    if proc:
        proc.kill()
