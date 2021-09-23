# scmutil.py - Mercurial core utility functions
#
#  Copyright Matt Mackall <mpm@selenic.com> and other
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.
from __future__ import absolute_import

from . import repoview

def cachetocopy(srcrepo):
    """return the list of cache file valuable to copy during a clone"""
    # In local clones we're copying all nodes, not just served
    # ones. Therefore copy all branch caches over.
    cachefiles = ['branch2']
    cachefiles += ['branch2-%s' % f for f in repoview.filtertable]
    cachefiles += ['rbc-names-v1', 'rbc-revs-v1']
    cachefiles += ['tags2']
    cachefiles += ['tags2-%s' % f for f in repoview.filtertable]
    cachefiles += ['hgtagsfnodes1']
    return cachefiles
