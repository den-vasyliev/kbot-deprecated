# httppeer.py - HTTP repository proxy classes for mercurial
#
# Copyright 2005, 2006 Matt Mackall <mpm@selenic.com>
# Copyright 2006 Vadim Gelfer <vadim.gelfer@gmail.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import errno
import io
import os
import socket
import struct
import weakref

from .i18n import _
from . import (
    bundle2,
    error,
    httpconnection,
    pycompat,
    repository,
    statichttprepo,
    url as urlmod,
    util,
    wireprotoframing,
    wireprototypes,
    wireprotov1peer,
    wireprotov2peer,
    wireprotov2server,
)
from .utils import (
    cborutil,
    interfaceutil,
    stringutil,
)

httplib = util.httplib
urlerr = util.urlerr
urlreq = util.urlreq

def encodevalueinheaders(value, header, limit):
    """Encode a string value into multiple HTTP headers.

    ``value`` will be encoded into 1 or more HTTP headers with the names
    ``header-<N>`` where ``<N>`` is an integer starting at 1. Each header
    name + value will be at most ``limit`` bytes long.

    Returns an iterable of 2-tuples consisting of header names and
    values as native strings.
    """
    # HTTP Headers are ASCII. Python 3 requires them to be unicodes,
    # not bytes. This function always takes bytes in as arguments.
    fmt = pycompat.strurl(header) + r'-%s'
    # Note: it is *NOT* a bug that the last bit here is a bytestring
    # and not a unicode: we're just getting the encoded length anyway,
    # and using an r-string to make it portable between Python 2 and 3
    # doesn't work because then the \r is a literal backslash-r
    # instead of a carriage return.
    valuelen = limit - len(fmt % r'000') - len(': \r\n')
    result = []

    n = 0
    for i in pycompat.xrange(0, len(value), valuelen):
        n += 1
        result.append((fmt % str(n), pycompat.strurl(value[i:i + valuelen])))

    return result

class _multifile(object):
    def __init__(self, *fileobjs):
        for f in fileobjs:
            if not util.safehasattr(f, 'length'):
                raise ValueError(
                    '_multifile only supports file objects that '
                    'have a length but this one does not:', type(f), f)
        self._fileobjs = fileobjs
        self._index = 0

    @property
    def length(self):
        return sum(f.length for f in self._fileobjs)

    def read(self, amt=None):
        if amt <= 0:
            return ''.join(f.read() for f in self._fileobjs)
        parts = []
        while amt and self._index < len(self._fileobjs):
            parts.append(self._fileobjs[self._index].read(amt))
            got = len(parts[-1])
            if got < amt:
                self._index += 1
            amt -= got
        return ''.join(parts)

    def seek(self, offset, whence=os.SEEK_SET):
        if whence != os.SEEK_SET:
            raise NotImplementedError(
                '_multifile does not support anything other'
                ' than os.SEEK_SET for whence on seek()')
        if offset != 0:
            raise NotImplementedError(
                '_multifile only supports seeking to start, but that '
                'could be fixed if you need it')
        for f in self._fileobjs:
            f.seek(0)
        self._index = 0

def makev1commandrequest(ui, requestbuilder, caps, capablefn,
                         repobaseurl, cmd, args):
    """Make an HTTP request to run a command for a version 1 client.

    ``caps`` is a set of known server capabilities. The value may be
    None if capabilities are not yet known.

    ``capablefn`` is a function to evaluate a capability.

    ``cmd``, ``args``, and ``data`` define the command, its arguments, and
    raw data to pass to it.
    """
    if cmd == 'pushkey':
        args['data'] = ''
    data = args.pop('data', None)
    headers = args.pop('headers', {})

    ui.debug("sending %s command\n" % cmd)
    q = [('cmd', cmd)]
    headersize = 0
    # Important: don't use self.capable() here or else you end up
    # with infinite recursion when trying to look up capabilities
    # for the first time.
    postargsok = caps is not None and 'httppostargs' in caps

    # Send arguments via POST.
    if postargsok and args:
        strargs = urlreq.urlencode(sorted(args.items()))
        if not data:
            data = strargs
        else:
            if isinstance(data, bytes):
                i = io.BytesIO(data)
                i.length = len(data)
                data = i
            argsio = io.BytesIO(strargs)
            argsio.length = len(strargs)
            data = _multifile(argsio, data)
        headers[r'X-HgArgs-Post'] = len(strargs)
    elif args:
        # Calling self.capable() can infinite loop if we are calling
        # "capabilities". But that command should never accept wire
        # protocol arguments. So this should never happen.
        assert cmd != 'capabilities'
        httpheader = capablefn('httpheader')
        if httpheader:
            headersize = int(httpheader.split(',', 1)[0])

        # Send arguments via HTTP headers.
        if headersize > 0:
            # The headers can typically carry more data than the URL.
            encargs = urlreq.urlencode(sorted(args.items()))
            for header, value in encodevalueinheaders(encargs, 'X-HgArg',
                                                      headersize):
                headers[header] = value
        # Send arguments via query string (Mercurial <1.9).
        else:
            q += sorted(args.items())

    qs = '?%s' % urlreq.urlencode(q)
    cu = "%s%s" % (repobaseurl, qs)
    size = 0
    if util.safehasattr(data, 'length'):
        size = data.length
    elif data is not None:
        size = len(data)
    if data is not None and r'Content-Type' not in headers:
        headers[r'Content-Type'] = r'application/mercurial-0.1'

    # Tell the server we accept application/mercurial-0.2 and multiple
    # compression formats if the server is capable of emitting those
    # payloads.
    # Note: Keep this set empty by default, as client advertisement of
    # protocol parameters should only occur after the handshake.
    protoparams = set()

    mediatypes = set()
    if caps is not None:
        mt = capablefn('httpmediatype')
        if mt:
            protoparams.add('0.1')
            mediatypes = set(mt.split(','))

        protoparams.add('partial-pull')

    if '0.2tx' in mediatypes:
        protoparams.add('0.2')

    if '0.2tx' in mediatypes and capablefn('compression'):
        # We /could/ compare supported compression formats and prune
        # non-mutually supported or error if nothing is mutually supported.
        # For now, send the full list to the server and have it error.
        comps = [e.wireprotosupport().name for e in
                 util.compengines.supportedwireengines(util.CLIENTROLE)]
        protoparams.add('comp=%s' % ','.join(comps))

    if protoparams:
        protoheaders = encodevalueinheaders(' '.join(sorted(protoparams)),
                                            'X-HgProto',
                                            headersize or 1024)
        for header, value in protoheaders:
            headers[header] = value

    varyheaders = []
    for header in headers:
        if header.lower().startswith(r'x-hg'):
            varyheaders.append(header)

    if varyheaders:
        headers[r'Vary'] = r','.join(sorted(varyheaders))

    req = requestbuilder(pycompat.strurl(cu), data, headers)

    if data is not None:
        ui.debug("sending %d bytes\n" % size)
        req.add_unredirected_header(r'Content-Length', r'%d' % size)

    return req, cu, qs

def _reqdata(req):
    """Get request data, if any. If no data, returns None."""
    if pycompat.ispy3:
        return req.data
    if not req.has_data():
        return None
    return req.get_data()

def sendrequest(ui, opener, req):
    """Send a prepared HTTP request.

    Returns the response object.
    """
    dbg = ui.debug
    if (ui.debugflag
        and ui.configbool('devel', 'debug.peer-request')):
        line = 'devel-peer-request: %s\n'
        dbg(line % '%s %s' % (pycompat.bytesurl(req.get_method()),
                              pycompat.bytesurl(req.get_full_url())))
        hgargssize = None

        for header, value in sorted(req.header_items()):
            header = pycompat.bytesurl(header)
            value = pycompat.bytesurl(value)
            if header.startswith('X-hgarg-'):
                if hgargssize is None:
                    hgargssize = 0
                hgargssize += len(value)
            else:
                dbg(line % '  %s %s' % (header, value))

        if hgargssize is not None:
            dbg(line % '  %d bytes of commands arguments in headers'
                % hgargssize)
        data = _reqdata(req)
        if data is not None:
            length = getattr(data, 'length', None)
            if length is None:
                length = len(data)
            dbg(line % '  %d bytes of data' % length)

        start = util.timer()

    res = None
    try:
        res = opener.open(req)
    except urlerr.httperror as inst:
        if inst.code == 401:
            raise error.Abort(_('authorization failed'))
        raise
    except httplib.HTTPException as inst:
        ui.debug('http error requesting %s\n' %
                 util.hidepassword(req.get_full_url()))
        ui.traceback()
        raise IOError(None, inst)
    finally:
        if ui.debugflag and ui.configbool('devel', 'debug.peer-request'):
            code = res.code if res else -1
            dbg(line % '  finished in %.4f seconds (%d)'
                % (util.timer() - start, code))

    # Insert error handlers for common I/O failures.
    urlmod.wrapresponse(res)

    return res

class RedirectedRepoError(error.RepoError):
    def __init__(self, msg, respurl):
        super(RedirectedRepoError, self).__init__(msg)
        self.respurl = respurl

def parsev1commandresponse(ui, baseurl, requrl, qs, resp, compressible,
                           allowcbor=False):
    # record the url we got redirected to
    redirected = False
    respurl = pycompat.bytesurl(resp.geturl())
    if respurl.endswith(qs):
        respurl = respurl[:-len(qs)]
        qsdropped = False
    else:
        qsdropped = True

    if baseurl.rstrip('/') != respurl.rstrip('/'):
        redirected = True
        if not ui.quiet:
            ui.warn(_('real URL is %s\n') % respurl)

    try:
        proto = pycompat.bytesurl(resp.getheader(r'content-type', r''))
    except AttributeError:
        proto = pycompat.bytesurl(resp.headers.get(r'content-type', r''))

    safeurl = util.hidepassword(baseurl)
    if proto.startswith('application/hg-error'):
        raise error.OutOfBandError(resp.read())

    # Pre 1.0 versions of Mercurial used text/plain and
    # application/hg-changegroup. We don't support such old servers.
    if not proto.startswith('application/mercurial-'):
        ui.debug("requested URL: '%s'\n" % util.hidepassword(requrl))
        msg = _("'%s' does not appear to be an hg repository:\n"
                "---%%<--- (%s)\n%s\n---%%<---\n") % (
            safeurl, proto or 'no content-type', resp.read(1024))

        # Some servers may strip the query string from the redirect. We
        # raise a special error type so callers can react to this specially.
        if redirected and qsdropped:
            raise RedirectedRepoError(msg, respurl)
        else:
            raise error.RepoError(msg)

    try:
        subtype = proto.split('-', 1)[1]

        # Unless we end up supporting CBOR in the legacy wire protocol,
        # this should ONLY be encountered for the initial capabilities
        # request during handshake.
        if subtype == 'cbor':
            if allowcbor:
                return respurl, proto, resp
            else:
                raise error.RepoError(_('unexpected CBOR response from '
                                        'server'))

        version_info = tuple([int(n) for n in subtype.split('.')])
    except ValueError:
        raise error.RepoError(_("'%s' sent a broken Content-Type "
                                "header (%s)") % (safeurl, proto))

    # TODO consider switching to a decompression reader that uses
    # generators.
    if version_info == (0, 1):
        if compressible:
            resp = util.compengines['zlib'].decompressorreader(resp)

    elif version_info == (0, 2):
        # application/mercurial-0.2 always identifies the compression
        # engine in the payload header.
        elen = struct.unpack('B', util.readexactly(resp, 1))[0]
        ename = util.readexactly(resp, elen)
        engine = util.compengines.forwiretype(ename)

        resp = engine.decompressorreader(resp)
    else:
        raise error.RepoError(_("'%s' uses newer protocol %s") %
                              (safeurl, subtype))

    return respurl, proto, resp

class httppeer(wireprotov1peer.wirepeer):
    def __init__(self, ui, path, url, opener, requestbuilder, caps):
        self.ui = ui
        self._path = path
        self._url = url
        self._caps = caps
        self._urlopener = opener
        self._requestbuilder = requestbuilder

    def __del__(self):
        for h in self._urlopener.handlers:
            h.close()
            getattr(h, "close_all", lambda: None)()

    # Begin of ipeerconnection interface.

    def url(self):
        return self._path

    def local(self):
        return None

    def peer(self):
        return self

    def canpush(self):
        return True

    def close(self):
        try:
            reqs, sent, recv = (self._urlopener.requestscount,
                                self._urlopener.sentbytescount,
                                self._urlopener.receivedbytescount)
        except AttributeError:
            return
        self.ui.note(_('(sent %d HTTP requests and %d bytes; '
                       'received %d bytes in responses)\n') %
                     (reqs, sent, recv))

    # End of ipeerconnection interface.

    # Begin of ipeercommands interface.

    def capabilities(self):
        return self._caps

    # End of ipeercommands interface.

    def _callstream(self, cmd, _compressible=False, **args):
        args = pycompat.byteskwargs(args)

        req, cu, qs = makev1commandrequest(self.ui, self._requestbuilder,
                                           self._caps, self.capable,
                                           self._url, cmd, args)

        resp = sendrequest(self.ui, self._urlopener, req)

        self._url, ct, resp = parsev1commandresponse(self.ui, self._url, cu, qs,
                                                     resp, _compressible)

        return resp

    def _call(self, cmd, **args):
        fp = self._callstream(cmd, **args)
        try:
            return fp.read()
        finally:
            # if using keepalive, allow connection to be reused
            fp.close()

    def _callpush(self, cmd, cg, **args):
        # have to stream bundle to a temp file because we do not have
        # http 1.1 chunked transfer.

        types = self.capable('unbundle')
        try:
            types = types.split(',')
        except AttributeError:
            # servers older than d1b16a746db6 will send 'unbundle' as a
            # boolean capability. They only support headerless/uncompressed
            # bundles.
            types = [""]
        for x in types:
            if x in bundle2.bundletypes:
                type = x
                break

        tempname = bundle2.writebundle(self.ui, cg, None, type)
        fp = httpconnection.httpsendfile(self.ui, tempname, "rb")
        headers = {r'Content-Type': r'application/mercurial-0.1'}

        try:
            r = self._call(cmd, data=fp, headers=headers, **args)
            vals = r.split('\n', 1)
            if len(vals) < 2:
                raise error.ResponseError(_("unexpected response:"), r)
            return vals
        except urlerr.httperror:
            # Catch and re-raise these so we don't try and treat them
            # like generic socket errors. They lack any values in
            # .args on Python 3 which breaks our socket.error block.
            raise
        except socket.error as err:
            if err.args[0] in (errno.ECONNRESET, errno.EPIPE):
                raise error.Abort(_('push failed: %s') % err.args[1])
            raise error.Abort(err.args[1])
        finally:
            fp.close()
            os.unlink(tempname)

    def _calltwowaystream(self, cmd, fp, **args):
        fh = None
        fp_ = None
        filename = None
        try:
            # dump bundle to disk
            fd, filename = pycompat.mkstemp(prefix="hg-bundle-", suffix=".hg")
            fh = os.fdopen(fd, r"wb")
            d = fp.read(4096)
            while d:
                fh.write(d)
                d = fp.read(4096)
            fh.close()
            # start http push
            fp_ = httpconnection.httpsendfile(self.ui, filename, "rb")
            headers = {r'Content-Type': r'application/mercurial-0.1'}
            return self._callstream(cmd, data=fp_, headers=headers, **args)
        finally:
            if fp_ is not None:
                fp_.close()
            if fh is not None:
                fh.close()
                os.unlink(filename)

    def _callcompressable(self, cmd, **args):
        return self._callstream(cmd, _compressible=True, **args)

    def _abort(self, exception):
        raise exception

def sendv2request(ui, opener, requestbuilder, apiurl, permission, requests,
                  redirect):
    wireprotoframing.populatestreamencoders()

    uiencoders = ui.configlist(b'experimental', b'httppeer.v2-encoder-order')

    if uiencoders:
        encoders = []

        for encoder in uiencoders:
            if encoder not in wireprotoframing.STREAM_ENCODERS:
                ui.warn(_(b'wire protocol version 2 encoder referenced in '
                          b'config (%s) is not known; ignoring\n') % encoder)
            else:
                encoders.append(encoder)

    else:
        encoders = wireprotoframing.STREAM_ENCODERS_ORDER

    reactor = wireprotoframing.clientreactor(ui,
                                             hasmultiplesend=False,
                                             buffersends=True,
                                             clientcontentencoders=encoders)

    handler = wireprotov2peer.clienthandler(ui, reactor,
                                            opener=opener,
                                            requestbuilder=requestbuilder)

    url = '%s/%s' % (apiurl, permission)

    if len(requests) > 1:
        url += '/multirequest'
    else:
        url += '/%s' % requests[0][0]

    ui.debug('sending %d commands\n' % len(requests))
    for command, args, f in requests:
        ui.debug('sending command %s: %s\n' % (
            command, stringutil.pprint(args, indent=2)))
        assert not list(handler.callcommand(command, args, f,
                                            redirect=redirect))

    # TODO stream this.
    body = b''.join(map(bytes, handler.flushcommands()))

    # TODO modify user-agent to reflect v2
    headers = {
        r'Accept': wireprotov2server.FRAMINGTYPE,
        r'Content-Type': wireprotov2server.FRAMINGTYPE,
    }

    req = requestbuilder(pycompat.strurl(url), body, headers)
    req.add_unredirected_header(r'Content-Length', r'%d' % len(body))

    try:
        res = opener.open(req)
    except urlerr.httperror as e:
        if e.code == 401:
            raise error.Abort(_('authorization failed'))

        raise
    except httplib.HTTPException as e:
        ui.traceback()
        raise IOError(None, e)

    return handler, res

class queuedcommandfuture(pycompat.futures.Future):
    """Wraps result() on command futures to trigger submission on call."""

    def result(self, timeout=None):
        if self.done():
            return pycompat.futures.Future.result(self, timeout)

        self._peerexecutor.sendcommands()

        # sendcommands() will restore the original __class__ and self.result
        # will resolve to Future.result.
        return self.result(timeout)

@interfaceutil.implementer(repository.ipeercommandexecutor)
class httpv2executor(object):
    def __init__(self, ui, opener, requestbuilder, apiurl, descriptor,
                 redirect):
        self._ui = ui
        self._opener = opener
        self._requestbuilder = requestbuilder
        self._apiurl = apiurl
        self._descriptor = descriptor
        self._redirect = redirect
        self._sent = False
        self._closed = False
        self._neededpermissions = set()
        self._calls = []
        self._futures = weakref.WeakSet()
        self._responseexecutor = None
        self._responsef = None

    def __enter__(self):
        return self

    def __exit__(self, exctype, excvalue, exctb):
        self.close()

    def callcommand(self, command, args):
        if self._sent:
            raise error.ProgrammingError('callcommand() cannot be used after '
                                         'commands are sent')

        if self._closed:
            raise error.ProgrammingError('callcommand() cannot be used after '
                                         'close()')

        # The service advertises which commands are available. So if we attempt
        # to call an unknown command or pass an unknown argument, we can screen
        # for this.
        if command not in self._descriptor['commands']:
            raise error.ProgrammingError(
                'wire protocol command %s is not available' % command)

        cmdinfo = self._descriptor['commands'][command]
        unknownargs = set(args.keys()) - set(cmdinfo.get('args', {}))

        if unknownargs:
            raise error.ProgrammingError(
                'wire protocol command %s does not accept argument: %s' % (
                    command, ', '.join(sorted(unknownargs))))

        self._neededpermissions |= set(cmdinfo['permissions'])

        # TODO we /could/ also validate types here, since the API descriptor
        # includes types...

        f = pycompat.futures.Future()

        # Monkeypatch it so result() triggers sendcommands(), otherwise result()
        # could deadlock.
        f.__class__ = queuedcommandfuture
        f._peerexecutor = self

        self._futures.add(f)
        self._calls.append((command, args, f))

        return f

    def sendcommands(self):
        if self._sent:
            return

        if not self._calls:
            return

        self._sent = True

        # Unhack any future types so caller sees a clean type and so we
        # break reference cycle.
        for f in self._futures:
            if isinstance(f, queuedcommandfuture):
                f.__class__ = pycompat.futures.Future
                f._peerexecutor = None

        # Mark the future as running and filter out cancelled futures.
        calls = [(command, args, f)
                 for command, args, f in self._calls
                 if f.set_running_or_notify_cancel()]

        # Clear out references, prevent improper object usage.
        self._calls = None

        if not calls:
            return

        permissions = set(self._neededpermissions)

        if 'push' in permissions and 'pull' in permissions:
            permissions.remove('pull')

        if len(permissions) > 1:
            raise error.RepoError(_('cannot make request requiring multiple '
                                    'permissions: %s') %
                                  _(', ').join(sorted(permissions)))

        permission = {
            'push': 'rw',
            'pull': 'ro',
        }[permissions.pop()]

        handler, resp = sendv2request(
            self._ui, self._opener, self._requestbuilder, self._apiurl,
            permission, calls, self._redirect)

        # TODO we probably want to validate the HTTP code, media type, etc.

        self._responseexecutor = pycompat.futures.ThreadPoolExecutor(1)
        self._responsef = self._responseexecutor.submit(self._handleresponse,
                                                        handler, resp)

    def close(self):
        if self._closed:
            return

        self.sendcommands()

        self._closed = True

        if not self._responsef:
            return

        # TODO ^C here may not result in immediate program termination.

        try:
            self._responsef.result()
        finally:
            self._responseexecutor.shutdown(wait=True)
            self._responsef = None
            self._responseexecutor = None

            # If any of our futures are still in progress, mark them as
            # errored, otherwise a result() could wait indefinitely.
            for f in self._futures:
                if not f.done():
                    f.set_exception(error.ResponseError(
                        _('unfulfilled command response')))

            self._futures = None

    def _handleresponse(self, handler, resp):
        # Called in a thread to read the response.

        while handler.readdata(resp):
            pass

@interfaceutil.implementer(repository.ipeerv2)
class httpv2peer(object):
    def __init__(self, ui, repourl, apipath, opener, requestbuilder,
                 apidescriptor):
        self.ui = ui
        self.apidescriptor = apidescriptor

        if repourl.endswith('/'):
            repourl = repourl[:-1]

        self._url = repourl
        self._apipath = apipath
        self._apiurl = '%s/%s' % (repourl, apipath)
        self._opener = opener
        self._requestbuilder = requestbuilder

        self._redirect = wireprotov2peer.supportedredirects(ui, apidescriptor)

    # Start of ipeerconnection.

    def url(self):
        return self._url

    def local(self):
        return None

    def peer(self):
        return self

    def canpush(self):
        # TODO change once implemented.
        return False

    def close(self):
        self.ui.note(_('(sent %d HTTP requests and %d bytes; '
                       'received %d bytes in responses)\n') %
                     (self._opener.requestscount,
                      self._opener.sentbytescount,
                      self._opener.receivedbytescount))

    # End of ipeerconnection.

    # Start of ipeercapabilities.

    def capable(self, name):
        # The capabilities used internally historically map to capabilities
        # advertised from the "capabilities" wire protocol command. However,
        # version 2 of that command works differently.

        # Maps to commands that are available.
        if name in ('branchmap', 'getbundle', 'known', 'lookup', 'pushkey'):
            return True

        # Other concepts.
        if name in ('bundle2'):
            return True

        # Alias command-* to presence of command of that name.
        if name.startswith('command-'):
            return name[len('command-'):] in self.apidescriptor['commands']

        return False

    def requirecap(self, name, purpose):
        if self.capable(name):
            return

        raise error.CapabilityError(
            _('cannot %s; client or remote repository does not support the %r '
              'capability') % (purpose, name))

    # End of ipeercapabilities.

    def _call(self, name, **args):
        with self.commandexecutor() as e:
            return e.callcommand(name, args).result()

    def commandexecutor(self):
        return httpv2executor(self.ui, self._opener, self._requestbuilder,
                              self._apiurl, self.apidescriptor, self._redirect)

# Registry of API service names to metadata about peers that handle it.
#
# The following keys are meaningful:
#
# init
#    Callable receiving (ui, repourl, servicepath, opener, requestbuilder,
#                        apidescriptor) to create a peer.
#
# priority
#    Integer priority for the service. If we could choose from multiple
#    services, we choose the one with the highest priority.
API_PEERS = {
    wireprototypes.HTTP_WIREPROTO_V2: {
        'init': httpv2peer,
        'priority': 50,
    },
}

def performhandshake(ui, url, opener, requestbuilder):
    # The handshake is a request to the capabilities command.

    caps = None
    def capable(x):
        raise error.ProgrammingError('should not be called')

    args = {}

    # The client advertises support for newer protocols by adding an
    # X-HgUpgrade-* header with a list of supported APIs and an
    # X-HgProto-* header advertising which serializing formats it supports.
    # We only support the HTTP version 2 transport and CBOR responses for
    # now.
    advertisev2 = ui.configbool('experimental', 'httppeer.advertise-v2')

    if advertisev2:
        args['headers'] = {
            r'X-HgProto-1': r'cbor',
        }

        args['headers'].update(
            encodevalueinheaders(' '.join(sorted(API_PEERS)),
                                 'X-HgUpgrade',
                                 # We don't know the header limit this early.
                                 # So make it small.
                                 1024))

    req, requrl, qs = makev1commandrequest(ui, requestbuilder, caps,
                                           capable, url, 'capabilities',
                                           args)
    resp = sendrequest(ui, opener, req)

    # The server may redirect us to the repo root, stripping the
    # ?cmd=capabilities query string from the URL. The server would likely
    # return HTML in this case and ``parsev1commandresponse()`` would raise.
    # We catch this special case and re-issue the capabilities request against
    # the new URL.
    #
    # We should ideally not do this, as a redirect that drops the query
    # string from the URL is arguably a server bug. (Garbage in, garbage out).
    # However,  Mercurial clients for several years appeared to handle this
    # issue without behavior degradation. And according to issue 5860, it may
    # be a longstanding bug in some server implementations. So we allow a
    # redirect that drops the query string to "just work."
    try:
        respurl, ct, resp = parsev1commandresponse(ui, url, requrl, qs, resp,
                                                   compressible=False,
                                                   allowcbor=advertisev2)
    except RedirectedRepoError as e:
        req, requrl, qs = makev1commandrequest(ui, requestbuilder, caps,
                                               capable, e.respurl,
                                               'capabilities', args)
        resp = sendrequest(ui, opener, req)
        respurl, ct, resp = parsev1commandresponse(ui, url, requrl, qs, resp,
                                                   compressible=False,
                                                   allowcbor=advertisev2)

    try:
        rawdata = resp.read()
    finally:
        resp.close()

    if not ct.startswith('application/mercurial-'):
        raise error.ProgrammingError('unexpected content-type: %s' % ct)

    if advertisev2:
        if ct == 'application/mercurial-cbor':
            try:
                info = cborutil.decodeall(rawdata)[0]
            except cborutil.CBORDecodeError:
                raise error.Abort(_('error decoding CBOR from remote server'),
                                  hint=_('try again and consider contacting '
                                         'the server operator'))

        # We got a legacy response. That's fine.
        elif ct in ('application/mercurial-0.1', 'application/mercurial-0.2'):
            info = {
                'v1capabilities': set(rawdata.split())
            }

        else:
            raise error.RepoError(
                _('unexpected response type from server: %s') % ct)
    else:
        info = {
            'v1capabilities': set(rawdata.split())
        }

    return respurl, info

def makepeer(ui, path, opener=None, requestbuilder=urlreq.request):
    """Construct an appropriate HTTP peer instance.

    ``opener`` is an ``url.opener`` that should be used to establish
    connections, perform HTTP requests.

    ``requestbuilder`` is the type used for constructing HTTP requests.
    It exists as an argument so extensions can override the default.
    """
    u = util.url(path)
    if u.query or u.fragment:
        raise error.Abort(_('unsupported URL component: "%s"') %
                          (u.query or u.fragment))

    # urllib cannot handle URLs with embedded user or passwd.
    url, authinfo = u.authinfo()
    ui.debug('using %s\n' % url)

    opener = opener or urlmod.opener(ui, authinfo)

    respurl, info = performhandshake(ui, url, opener, requestbuilder)

    # Given the intersection of APIs that both we and the server support,
    # sort by their advertised priority and pick the first one.
    #
    # TODO consider making this request-based and interface driven. For
    # example, the caller could say "I want a peer that does X." It's quite
    # possible that not all peers would do that. Since we know the service
    # capabilities, we could filter out services not meeting the
    # requirements. Possibly by consulting the interfaces defined by the
    # peer type.
    apipeerchoices = set(info.get('apis', {}).keys()) & set(API_PEERS.keys())

    preferredchoices = sorted(apipeerchoices,
                              key=lambda x: API_PEERS[x]['priority'],
                              reverse=True)

    for service in preferredchoices:
        apipath = '%s/%s' % (info['apibase'].rstrip('/'), service)

        return API_PEERS[service]['init'](ui, respurl, apipath, opener,
                                          requestbuilder,
                                          info['apis'][service])

    # Failed to construct an API peer. Fall back to legacy.
    return httppeer(ui, path, respurl, opener, requestbuilder,
                    info['v1capabilities'])

def instance(ui, path, create, intents=None, createopts=None):
    if create:
        raise error.Abort(_('cannot create new http repository'))
    try:
        if path.startswith('https:') and not urlmod.has_https:
            raise error.Abort(_('Python support for SSL and HTTPS '
                                'is not installed'))

        inst = makepeer(ui, path)

        return inst
    except error.RepoError as httpexception:
        try:
            r = statichttprepo.instance(ui, "static-" + path, create)
            ui.note(_('(falling back to static-http)\n'))
            return r
        except error.RepoError:
            raise httpexception # use the original http RepoError instead
