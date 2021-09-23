# storageutil.py - Storage functionality agnostic of backend implementation.
#
# Copyright 2018 Gregory Szorc <gregory.szorc@gmail.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import hashlib
import re
import struct

from ..i18n import _
from ..node import (
    bin,
    nullid,
    nullrev,
)
from .. import (
    dagop,
    error,
    mdiff,
    pycompat,
)

_nullhash = hashlib.sha1(nullid)

def hashrevisionsha1(text, p1, p2):
    """Compute the SHA-1 for revision data and its parents.

    This hash combines both the current file contents and its history
    in a manner that makes it easy to distinguish nodes with the same
    content in the revision graph.
    """
    # As of now, if one of the parent node is null, p2 is null
    if p2 == nullid:
        # deep copy of a hash is faster than creating one
        s = _nullhash.copy()
        s.update(p1)
    else:
        # none of the parent nodes are nullid
        if p1 < p2:
            a = p1
            b = p2
        else:
            a = p2
            b = p1
        s = hashlib.sha1(a)
        s.update(b)
    s.update(text)
    return s.digest()

METADATA_RE = re.compile(b'\x01\n')

def parsemeta(text):
    """Parse metadata header from revision data.

    Returns a 2-tuple of (metadata, offset), where both can be None if there
    is no metadata.
    """
    # text can be buffer, so we can't use .startswith or .index
    if text[:2] != b'\x01\n':
        return None, None
    s = METADATA_RE.search(text, 2).start()
    mtext = text[2:s]
    meta = {}
    for l in mtext.splitlines():
        k, v = l.split(b': ', 1)
        meta[k] = v
    return meta, s + 2

def packmeta(meta, text):
    """Add metadata to fulltext to produce revision text."""
    keys = sorted(meta)
    metatext = b''.join(b'%s: %s\n' % (k, meta[k]) for k in keys)
    return b'\x01\n%s\x01\n%s' % (metatext, text)

def iscensoredtext(text):
    meta = parsemeta(text)[0]
    return meta and b'censored' in meta

def filtermetadata(text):
    """Extract just the revision data from source text.

    Returns ``text`` unless it has a metadata header, in which case we return
    a new buffer without hte metadata.
    """
    if not text.startswith(b'\x01\n'):
        return text

    offset = text.index(b'\x01\n', 2)
    return text[offset + 2:]

def filerevisioncopied(store, node):
    """Resolve file revision copy metadata.

    Returns ``False`` if the file has no copy metadata. Otherwise a
    2-tuple of the source filename and node.
    """
    if store.parents(node)[0] != nullid:
        return False

    meta = parsemeta(store.revision(node))[0]

    # copy and copyrev occur in pairs. In rare cases due to old bugs,
    # one can occur without the other. So ensure both are present to flag
    # as a copy.
    if meta and b'copy' in meta and b'copyrev' in meta:
        return meta[b'copy'], bin(meta[b'copyrev'])

    return False

def filedataequivalent(store, node, filedata):
    """Determines whether file data is equivalent to a stored node.

    Returns True if the passed file data would hash to the same value
    as a stored revision and False otherwise.

    When a stored revision is censored, filedata must be empty to have
    equivalence.

    When a stored revision has copy metadata, it is ignored as part
    of the compare.
    """

    if filedata.startswith(b'\x01\n'):
        revisiontext = b'\x01\n\x01\n' + filedata
    else:
        revisiontext = filedata

    p1, p2 = store.parents(node)

    computednode = hashrevisionsha1(revisiontext, p1, p2)

    if computednode == node:
        return True

    # Censored files compare against the empty file.
    if store.iscensored(store.rev(node)):
        return filedata == b''

    # Renaming a file produces a different hash, even if the data
    # remains unchanged. Check if that's the case.
    if store.renamed(node):
        return store.read(node) == filedata

    return False

def iterrevs(storelen, start=0, stop=None):
    """Iterate over revision numbers in a store."""
    step = 1

    if stop is not None:
        if start > stop:
            step = -1
        stop += step
        if stop > storelen:
            stop = storelen
    else:
        stop = storelen

    return pycompat.xrange(start, stop, step)

def fileidlookup(store, fileid, identifier):
    """Resolve the file node for a value.

    ``store`` is an object implementing the ``ifileindex`` interface.

    ``fileid`` can be:

    * A 20 byte binary node.
    * An integer revision number
    * A 40 byte hex node.
    * A bytes that can be parsed as an integer representing a revision number.

    ``identifier`` is used to populate ``error.LookupError`` with an identifier
    for the store.

    Raises ``error.LookupError`` on failure.
    """
    if isinstance(fileid, int):
        try:
            return store.node(fileid)
        except IndexError:
            raise error.LookupError('%d' % fileid, identifier,
                                    _('no match found'))

    if len(fileid) == 20:
        try:
            store.rev(fileid)
            return fileid
        except error.LookupError:
            pass

    if len(fileid) == 40:
        try:
            rawnode = bin(fileid)
            store.rev(rawnode)
            return rawnode
        except TypeError:
            pass

    try:
        rev = int(fileid)

        if b'%d' % rev != fileid:
            raise ValueError

        try:
            return store.node(rev)
        except (IndexError, TypeError):
            pass
    except (ValueError, OverflowError):
        pass

    raise error.LookupError(fileid, identifier, _('no match found'))

def resolvestripinfo(minlinkrev, tiprev, headrevs, linkrevfn, parentrevsfn):
    """Resolve information needed to strip revisions.

    Finds the minimum revision number that must be stripped in order to
    strip ``minlinkrev``.

    Returns a 2-tuple of the minimum revision number to do that and a set
    of all revision numbers that have linkrevs that would be broken
    by that strip.

    ``tiprev`` is the current tip-most revision. It is ``len(store) - 1``.
    ``headrevs`` is an iterable of head revisions.
    ``linkrevfn`` is a callable that receives a revision and returns a linked
    revision.
    ``parentrevsfn`` is a callable that receives a revision number and returns
    an iterable of its parent revision numbers.
    """
    brokenrevs = set()
    strippoint = tiprev + 1

    heads = {}
    futurelargelinkrevs = set()
    for head in headrevs:
        headlinkrev = linkrevfn(head)
        heads[head] = headlinkrev
        if headlinkrev >= minlinkrev:
            futurelargelinkrevs.add(headlinkrev)

    # This algorithm involves walking down the rev graph, starting at the
    # heads. Since the revs are topologically sorted according to linkrev,
    # once all head linkrevs are below the minlink, we know there are
    # no more revs that could have a linkrev greater than minlink.
    # So we can stop walking.
    while futurelargelinkrevs:
        strippoint -= 1
        linkrev = heads.pop(strippoint)

        if linkrev < minlinkrev:
            brokenrevs.add(strippoint)
        else:
            futurelargelinkrevs.remove(linkrev)

        for p in parentrevsfn(strippoint):
            if p != nullrev:
                plinkrev = linkrevfn(p)
                heads[p] = plinkrev
                if plinkrev >= minlinkrev:
                    futurelargelinkrevs.add(plinkrev)

    return strippoint, brokenrevs

def emitrevisions(store, nodes, nodesorder, resultcls, deltaparentfn=None,
                  candeltafn=None, rawsizefn=None, revdifffn=None, flagsfn=None,
                  sendfulltext=False,
                  revisiondata=False, assumehaveparentrevisions=False,
                  deltaprevious=False):
    """Generic implementation of ifiledata.emitrevisions().

    Emitting revision data is subtly complex. This function attempts to
    encapsulate all the logic for doing so in a backend-agnostic way.

    ``store``
       Object conforming to ``ifilestorage`` interface.

    ``nodes``
       List of revision nodes whose data to emit.

    ``resultcls``
       A type implementing the ``irevisiondelta`` interface that will be
       constructed and returned.

    ``deltaparentfn`` (optional)
       Callable receiving a revision number and returning the revision number
       of a revision that the internal delta is stored against. This delta
       will be preferred over computing a new arbitrary delta.

       If not defined, a delta will always be computed from raw revision
       data.

    ``candeltafn`` (optional)
       Callable receiving a pair of revision numbers that returns a bool
       indicating whether a delta between them can be produced.

       If not defined, it is assumed that any two revisions can delta with
       each other.

    ``rawsizefn`` (optional)
       Callable receiving a revision number and returning the length of the
       ``store.revision(rev, raw=True)``.

       If not defined, ``len(store.revision(rev, raw=True))`` will be called.

    ``revdifffn`` (optional)
       Callable receiving a pair of revision numbers that returns a delta
       between them.

       If not defined, a delta will be computed by invoking mdiff code
       on ``store.revision()`` results.

       Defining this function allows a precomputed or stored delta to be
       used without having to compute on.

    ``flagsfn`` (optional)
       Callable receiving a revision number and returns the integer flags
       value for it. If not defined, flags value will be 0.

    ``sendfulltext``
       Whether to send fulltext revisions instead of deltas, if allowed.

    ``nodesorder``
    ``revisiondata``
    ``assumehaveparentrevisions``
    ``deltaprevious``
       See ``ifiledata.emitrevisions()`` interface documentation.
    """

    fnode = store.node
    frev = store.rev

    if nodesorder == 'nodes':
        revs = [frev(n) for n in nodes]
    elif nodesorder == 'linear':
        revs = set(frev(n) for n in nodes)
        revs = dagop.linearize(revs, store.parentrevs)
    else: # storage and default
        revs = sorted(frev(n) for n in nodes)

    prevrev = None

    if deltaprevious or assumehaveparentrevisions:
        prevrev = store.parentrevs(revs[0])[0]

    # Set of revs available to delta against.
    available = set()

    for rev in revs:
        if rev == nullrev:
            continue

        node = fnode(rev)
        p1rev, p2rev = store.parentrevs(rev)

        if deltaparentfn:
            deltaparentrev = deltaparentfn(rev)
        else:
            deltaparentrev = nullrev

        # Forced delta against previous mode.
        if deltaprevious:
            baserev = prevrev

        # We're instructed to send fulltext. Honor that.
        elif sendfulltext:
            baserev = nullrev

        # There is a delta in storage. We try to use that because it
        # amounts to effectively copying data from storage and is
        # therefore the fastest.
        elif deltaparentrev != nullrev:
            # Base revision was already emitted in this group. We can
            # always safely use the delta.
            if deltaparentrev in available:
                baserev = deltaparentrev

            # Base revision is a parent that hasn't been emitted already.
            # Use it if we can assume the receiver has the parent revision.
            elif (assumehaveparentrevisions
                  and deltaparentrev in (p1rev, p2rev)):
                baserev = deltaparentrev

            # No guarantee the receiver has the delta parent. Send delta
            # against last revision (if possible), which in the common case
            # should be similar enough to this revision that the delta is
            # reasonable.
            elif prevrev is not None:
                baserev = prevrev
            else:
                baserev = nullrev

        # Storage has a fulltext revision.

        # Let's use the previous revision, which is as good a guess as any.
        # There is definitely room to improve this logic.
        elif prevrev is not None:
            baserev = prevrev
        else:
            baserev = nullrev

        # But we can't actually use our chosen delta base for whatever
        # reason. Reset to fulltext.
        if baserev != nullrev and (candeltafn and not candeltafn(baserev, rev)):
            baserev = nullrev

        revision = None
        delta = None
        baserevisionsize = None

        if revisiondata:
            if store.iscensored(baserev) or store.iscensored(rev):
                try:
                    revision = store.revision(node, raw=True)
                except error.CensoredNodeError as e:
                    revision = e.tombstone

                if baserev != nullrev:
                    if rawsizefn:
                        baserevisionsize = rawsizefn(baserev)
                    else:
                        baserevisionsize = len(store.revision(baserev,
                                                              raw=True))

            elif baserev == nullrev and not deltaprevious:
                revision = store.revision(node, raw=True)
                available.add(rev)
            else:
                if revdifffn:
                    delta = revdifffn(baserev, rev)
                else:
                    delta = mdiff.textdiff(store.revision(baserev, raw=True),
                                           store.revision(rev, raw=True))

                available.add(rev)

        yield resultcls(
            node=node,
            p1node=fnode(p1rev),
            p2node=fnode(p2rev),
            basenode=fnode(baserev),
            flags=flagsfn(rev) if flagsfn else 0,
            baserevisionsize=baserevisionsize,
            revision=revision,
            delta=delta)

        prevrev = rev

def deltaiscensored(delta, baserev, baselenfn):
    """Determine if a delta represents censored revision data.

    ``baserev`` is the base revision this delta is encoded against.
    ``baselenfn`` is a callable receiving a revision number that resolves the
    length of the revision fulltext.

    Returns a bool indicating if the result of the delta represents a censored
    revision.
    """
    # Fragile heuristic: unless new file meta keys are added alphabetically
    # preceding "censored", all censored revisions are prefixed by
    # "\1\ncensored:". A delta producing such a censored revision must be a
    # full-replacement delta, so we inspect the first and only patch in the
    # delta for this prefix.
    hlen = struct.calcsize(">lll")
    if len(delta) <= hlen:
        return False

    oldlen = baselenfn(baserev)
    newlen = len(delta) - hlen
    if delta[:hlen] != mdiff.replacediffheader(oldlen, newlen):
        return False

    add = "\1\ncensored:"
    addlen = len(add)
    return newlen >= addlen and delta[hlen:hlen + addlen] == add
