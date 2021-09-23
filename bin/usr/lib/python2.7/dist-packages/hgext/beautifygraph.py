# -*- coding: UTF-8 -*-
# beautifygraph.py - improve graph output by using Unicode characters
#
# Copyright 2018 John Stiles <johnstiles@gmail.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

'''beautify log -G output by using Unicode characters (EXPERIMENTAL)

   A terminal with UTF-8 support and monospace narrow text are required.
'''

from __future__ import absolute_import

from mercurial.i18n import _
from mercurial import (
    encoding,
    extensions,
    graphmod,
    pycompat,
    templatekw,
)

# Note for extension authors: ONLY specify testedwith = 'ships-with-hg-core' for
# extensions which SHIP WITH MERCURIAL. Non-mainline extensions should
# be specifying the version(s) of Mercurial they are tested with, or
# leave the attribute unspecified.
testedwith = 'ships-with-hg-core'

def prettyedge(before, edge, after):
    if edge == '~':
        return '\xE2\x95\xA7' # U+2567 ╧
    if edge == 'X':
        return '\xE2\x95\xB3' # U+2573 ╳
    if edge == '/':
        return '\xE2\x95\xB1' # U+2571 ╱
    if edge == '-':
        return '\xE2\x94\x80' # U+2500 ─
    if edge == '|':
        return '\xE2\x94\x82' # U+2502 │
    if edge == ':':
        return '\xE2\x94\x86' # U+2506 ┆
    if edge == '\\':
        return '\xE2\x95\xB2' # U+2572 ╲
    if edge == '+':
        if before == ' ' and not after  == ' ':
            return '\xE2\x94\x9C' # U+251C ├
        if after  == ' ' and not before == ' ':
            return '\xE2\x94\xA4' # U+2524 ┤
        return '\xE2\x94\xBC' # U+253C ┼
    return edge

def convertedges(line):
    line = ' %s ' % line
    pretty = []
    for idx in pycompat.xrange(len(line) - 2):
        pretty.append(prettyedge(line[idx:idx + 1],
                                 line[idx + 1:idx + 2],
                                 line[idx + 2:idx + 3]))
    return ''.join(pretty)

def getprettygraphnode(orig, *args, **kwargs):
    node = orig(*args, **kwargs)
    if node == 'o':
        return '\xE2\x97\x8B' # U+25CB ○
    if node == '@':
        return '\xE2\x97\x8D' # U+25CD ◍
    if node == '*':
        return '\xE2\x88\x97' # U+2217 ∗
    if node == 'x':
        return '\xE2\x97\x8C' # U+25CC ◌
    if node == '_':
        return '\xE2\x95\xA4' # U+2564 ╤
    return node

def outputprettygraph(orig, ui, graph, *args, **kwargs):
    (edges, text) = zip(*graph)
    graph = zip([convertedges(e) for e in edges], text)
    return orig(ui, graph, *args, **kwargs)

def extsetup(ui):
    if ui.plain('graph'):
        return

    if encoding.encoding != 'UTF-8':
        ui.warn(_('beautifygraph: unsupported encoding, UTF-8 required\n'))
        return

    if r'A' in encoding._wide:
        ui.warn(_('beautifygraph: unsupported terminal settings, '
                  'monospace narrow text required\n'))
        return

    extensions.wrapfunction(graphmod, 'outputgraph', outputprettygraph)
    extensions.wrapfunction(templatekw, 'getgraphnode', getprettygraphnode)
