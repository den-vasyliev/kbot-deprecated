# Copyright 2011 Fog Creek Software
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.
from __future__ import absolute_import

import os
import re

from mercurial.i18n import _

from mercurial import (
    error,
    httppeer,
    util,
    wireprototypes,
    wireprotov1peer,
)

from . import (
    lfutil,
)

urlerr = util.urlerr
urlreq = util.urlreq

LARGEFILES_REQUIRED_MSG = ('\nThis repository uses the largefiles extension.'
                           '\n\nPlease enable it in your Mercurial config '
                           'file.\n')

# these will all be replaced by largefiles.uisetup
ssholdcallstream = None
httpoldcallstream = None

def putlfile(repo, proto, sha):
    '''Server command for putting a largefile into a repository's local store
    and into the user cache.'''
    with proto.mayberedirectstdio() as output:
        path = lfutil.storepath(repo, sha)
        util.makedirs(os.path.dirname(path))
        tmpfp = util.atomictempfile(path, createmode=repo.store.createmode)

        try:
            for p in proto.getpayload():
                tmpfp.write(p)
            tmpfp._fp.seek(0)
            if sha != lfutil.hexsha1(tmpfp._fp):
                raise IOError(0, _('largefile contents do not match hash'))
            tmpfp.close()
            lfutil.linktousercache(repo, sha)
        except IOError as e:
            repo.ui.warn(_('largefiles: failed to put %s into store: %s\n') %
                         (sha, e.strerror))
            return wireprototypes.pushres(
                1, output.getvalue() if output else '')
        finally:
            tmpfp.discard()

    return wireprototypes.pushres(0, output.getvalue() if output else '')

def getlfile(repo, proto, sha):
    '''Server command for retrieving a largefile from the repository-local
    cache or user cache.'''
    filename = lfutil.findfile(repo, sha)
    if not filename:
        raise error.Abort(_('requested largefile %s not present in cache')
                          % sha)
    f = open(filename, 'rb')
    length = os.fstat(f.fileno())[6]

    # Since we can't set an HTTP content-length header here, and
    # Mercurial core provides no way to give the length of a streamres
    # (and reading the entire file into RAM would be ill-advised), we
    # just send the length on the first line of the response, like the
    # ssh proto does for string responses.
    def generator():
        yield '%d\n' % length
        for chunk in util.filechunkiter(f):
            yield chunk
    return wireprototypes.streamreslegacy(gen=generator())

def statlfile(repo, proto, sha):
    '''Server command for checking if a largefile is present - returns '2\n' if
    the largefile is missing, '0\n' if it seems to be in good condition.

    The value 1 is reserved for mismatched checksum, but that is too expensive
    to be verified on every stat and must be caught be running 'hg verify'
    server side.'''
    filename = lfutil.findfile(repo, sha)
    if not filename:
        return wireprototypes.bytesresponse('2\n')
    return wireprototypes.bytesresponse('0\n')

def wirereposetup(ui, repo):
    class lfileswirerepository(repo.__class__):
        def putlfile(self, sha, fd):
            # unfortunately, httprepository._callpush tries to convert its
            # input file-like into a bundle before sending it, so we can't use
            # it ...
            if issubclass(self.__class__, httppeer.httppeer):
                res = self._call('putlfile', data=fd, sha=sha,
                    headers={r'content-type': r'application/mercurial-0.1'})
                try:
                    d, output = res.split('\n', 1)
                    for l in output.splitlines(True):
                        self.ui.warn(_('remote: '), l) # assume l ends with \n
                    return int(d)
                except ValueError:
                    self.ui.warn(_('unexpected putlfile response: %r\n') % res)
                    return 1
            # ... but we can't use sshrepository._call because the data=
            # argument won't get sent, and _callpush does exactly what we want
            # in this case: send the data straight through
            else:
                try:
                    ret, output = self._callpush("putlfile", fd, sha=sha)
                    if ret == "":
                        raise error.ResponseError(_('putlfile failed:'),
                                output)
                    return int(ret)
                except IOError:
                    return 1
                except ValueError:
                    raise error.ResponseError(
                        _('putlfile failed (unexpected response):'), ret)

        def getlfile(self, sha):
            """returns an iterable with the chunks of the file with sha sha"""
            stream = self._callstream("getlfile", sha=sha)
            length = stream.readline()
            try:
                length = int(length)
            except ValueError:
                self._abort(error.ResponseError(_("unexpected response:"),
                                                length))

            # SSH streams will block if reading more than length
            for chunk in util.filechunkiter(stream, limit=length):
                yield chunk
            # HTTP streams must hit the end to process the last empty
            # chunk of Chunked-Encoding so the connection can be reused.
            if issubclass(self.__class__, httppeer.httppeer):
                chunk = stream.read(1)
                if chunk:
                    self._abort(error.ResponseError(_("unexpected response:"),
                                                    chunk))

        @wireprotov1peer.batchable
        def statlfile(self, sha):
            f = wireprotov1peer.future()
            result = {'sha': sha}
            yield result, f
            try:
                yield int(f.value)
            except (ValueError, urlerr.httperror):
                # If the server returns anything but an integer followed by a
                # newline, newline, it's not speaking our language; if we get
                # an HTTP error, we can't be sure the largefile is present;
                # either way, consider it missing.
                yield 2

    repo.__class__ = lfileswirerepository

# advertise the largefiles=serve capability
def _capabilities(orig, repo, proto):
    '''announce largefile server capability'''
    caps = orig(repo, proto)
    caps.append('largefiles=serve')
    return caps

def heads(orig, repo, proto):
    '''Wrap server command - largefile capable clients will know to call
    lheads instead'''
    if lfutil.islfilesrepo(repo):
        return wireprototypes.ooberror(LARGEFILES_REQUIRED_MSG)

    return orig(repo, proto)

def sshrepocallstream(self, cmd, **args):
    if cmd == 'heads' and self.capable('largefiles'):
        cmd = 'lheads'
    if cmd == 'batch' and self.capable('largefiles'):
        args[r'cmds'] = args[r'cmds'].replace('heads ', 'lheads ')
    return ssholdcallstream(self, cmd, **args)

headsre = re.compile(br'(^|;)heads\b')

def httprepocallstream(self, cmd, **args):
    if cmd == 'heads' and self.capable('largefiles'):
        cmd = 'lheads'
    if cmd == 'batch' and self.capable('largefiles'):
        args[r'cmds'] = headsre.sub('lheads', args[r'cmds'])
    return httpoldcallstream(self, cmd, **args)
