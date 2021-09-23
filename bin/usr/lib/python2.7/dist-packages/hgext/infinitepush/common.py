# Copyright 2017 Facebook, Inc.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import os

from mercurial.node import hex

from mercurial import (
    error,
    extensions,
    pycompat,
)

def isremotebooksenabled(ui):
    return ('remotenames' in extensions._extensions and
            ui.configbool('remotenames', 'bookmarks'))

def downloadbundle(repo, unknownbinhead):
    index = repo.bundlestore.index
    store = repo.bundlestore.store
    bundleid = index.getbundle(hex(unknownbinhead))
    if bundleid is None:
        raise error.Abort('%s head is not known' % hex(unknownbinhead))
    bundleraw = store.read(bundleid)
    return _makebundlefromraw(bundleraw)

def _makebundlefromraw(data):
    fp = None
    fd, bundlefile = pycompat.mkstemp()
    try:  # guards bundlefile
        try:  # guards fp
            fp = os.fdopen(fd, r'wb')
            fp.write(data)
        finally:
            fp.close()
    except Exception:
        try:
            os.unlink(bundlefile)
        except Exception:
            # we would rather see the original exception
            pass
        raise

    return bundlefile
