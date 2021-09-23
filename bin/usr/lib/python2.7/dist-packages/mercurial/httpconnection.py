# httpconnection.py - urllib2 handler for new http support
#
# Copyright 2005, 2006, 2007, 2008 Matt Mackall <mpm@selenic.com>
# Copyright 2006, 2007 Alexis S. L. Carvalho <alexis@cecm.usp.br>
# Copyright 2006 Vadim Gelfer <vadim.gelfer@gmail.com>
# Copyright 2011 Google, Inc.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import os

from .i18n import _
from . import (
    pycompat,
    util,
)

urlerr = util.urlerr
urlreq = util.urlreq

# moved here from url.py to avoid a cycle
class httpsendfile(object):
    """This is a wrapper around the objects returned by python's "open".

    Its purpose is to send file-like objects via HTTP.
    It do however not define a __len__ attribute because the length
    might be more than Py_ssize_t can handle.
    """

    def __init__(self, ui, *args, **kwargs):
        self.ui = ui
        self._data = open(*args, **kwargs)
        self.seek = self._data.seek
        self.close = self._data.close
        self.write = self._data.write
        self.length = os.fstat(self._data.fileno()).st_size
        self._pos = 0
        # We pass double the max for total because we currently have
        # to send the bundle twice in the case of a server that
        # requires authentication. Since we can't know until we try
        # once whether authentication will be required, just lie to
        # the user and maybe the push succeeds suddenly at 50%.
        self._progress = ui.makeprogress(_('sending'), unit=_('kb'),
                                         total=(self.length // 1024 * 2))

    def read(self, *args, **kwargs):
        ret = self._data.read(*args, **kwargs)
        if not ret:
            self._progress.complete()
            return ret
        self._pos += len(ret)
        self._progress.update(self._pos // 1024)
        return ret

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

# moved here from url.py to avoid a cycle
def readauthforuri(ui, uri, user):
    uri = pycompat.bytesurl(uri)
    # Read configuration
    groups = {}
    for key, val in ui.configitems('auth'):
        if key in ('cookiefile',):
            continue

        if '.' not in key:
            ui.warn(_("ignoring invalid [auth] key '%s'\n") % key)
            continue
        group, setting = key.rsplit('.', 1)
        gdict = groups.setdefault(group, {})
        if setting in ('username', 'cert', 'key'):
            val = util.expandpath(val)
        gdict[setting] = val

    # Find the best match
    scheme, hostpath = uri.split('://', 1)
    bestuser = None
    bestlen = 0
    bestauth = None
    for group, auth in groups.iteritems():
        if user and user != auth.get('username', user):
            # If a username was set in the URI, the entry username
            # must either match it or be unset
            continue
        prefix = auth.get('prefix')
        if not prefix:
            continue
        p = prefix.split('://', 1)
        if len(p) > 1:
            schemes, prefix = [p[0]], p[1]
        else:
            schemes = (auth.get('schemes') or 'https').split()
        if (prefix == '*' or hostpath.startswith(prefix)) and \
            (len(prefix) > bestlen or (len(prefix) == bestlen and \
                not bestuser and 'username' in auth)) \
             and scheme in schemes:
            bestlen = len(prefix)
            bestauth = group, auth
            bestuser = auth.get('username')
            if user and not bestuser:
                auth['username'] = user
    return bestauth
