# wireprotov1peer.py - Client-side functionality for wire protocol version 1.
#
# Copyright 2005-2010 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import hashlib
import sys
import weakref

from .i18n import _
from .node import (
    bin,
)
from . import (
    bundle2,
    changegroup as changegroupmod,
    encoding,
    error,
    pushkey as pushkeymod,
    pycompat,
    repository,
    util,
    wireprototypes,
)
from .utils import (
    interfaceutil,
)

urlreq = util.urlreq

def batchable(f):
    '''annotation for batchable methods

    Such methods must implement a coroutine as follows:

    @batchable
    def sample(self, one, two=None):
        # Build list of encoded arguments suitable for your wire protocol:
        encargs = [('one', encode(one),), ('two', encode(two),)]
        # Create future for injection of encoded result:
        encresref = future()
        # Return encoded arguments and future:
        yield encargs, encresref
        # Assuming the future to be filled with the result from the batched
        # request now. Decode it:
        yield decode(encresref.value)

    The decorator returns a function which wraps this coroutine as a plain
    method, but adds the original method as an attribute called "batchable",
    which is used by remotebatch to split the call into separate encoding and
    decoding phases.
    '''
    def plain(*args, **opts):
        batchable = f(*args, **opts)
        encargsorres, encresref = next(batchable)
        if not encresref:
            return encargsorres # a local result in this case
        self = args[0]
        cmd = pycompat.bytesurl(f.__name__)  # ensure cmd is ascii bytestr
        encresref.set(self._submitone(cmd, encargsorres))
        return next(batchable)
    setattr(plain, 'batchable', f)
    setattr(plain, '__name__', f.__name__)
    return plain

class future(object):
    '''placeholder for a value to be set later'''
    def set(self, value):
        if util.safehasattr(self, 'value'):
            raise error.RepoError("future is already set")
        self.value = value

def encodebatchcmds(req):
    """Return a ``cmds`` argument value for the ``batch`` command."""
    escapearg = wireprototypes.escapebatcharg

    cmds = []
    for op, argsdict in req:
        # Old servers didn't properly unescape argument names. So prevent
        # the sending of argument names that may not be decoded properly by
        # servers.
        assert all(escapearg(k) == k for k in argsdict)

        args = ','.join('%s=%s' % (escapearg(k), escapearg(v))
                        for k, v in argsdict.iteritems())
        cmds.append('%s %s' % (op, args))

    return ';'.join(cmds)

class unsentfuture(pycompat.futures.Future):
    """A Future variation to represent an unsent command.

    Because we buffer commands and don't submit them immediately, calling
    ``result()`` on an unsent future could deadlock. Futures for buffered
    commands are represented by this type, which wraps ``result()`` to
    call ``sendcommands()``.
    """

    def result(self, timeout=None):
        if self.done():
            return pycompat.futures.Future.result(self, timeout)

        self._peerexecutor.sendcommands()

        # This looks like it will infinitely recurse. However,
        # sendcommands() should modify __class__. This call serves as a check
        # on that.
        return self.result(timeout)

@interfaceutil.implementer(repository.ipeercommandexecutor)
class peerexecutor(object):
    def __init__(self, peer):
        self._peer = peer
        self._sent = False
        self._closed = False
        self._calls = []
        self._futures = weakref.WeakSet()
        self._responseexecutor = None
        self._responsef = None

    def __enter__(self):
        return self

    def __exit__(self, exctype, excvalee, exctb):
        self.close()

    def callcommand(self, command, args):
        if self._sent:
            raise error.ProgrammingError('callcommand() cannot be used '
                                         'after commands are sent')

        if self._closed:
            raise error.ProgrammingError('callcommand() cannot be used '
                                         'after close()')

        # Commands are dispatched through methods on the peer.
        fn = getattr(self._peer, pycompat.sysstr(command), None)

        if not fn:
            raise error.ProgrammingError(
                'cannot call command %s: method of same name not available '
                'on peer' % command)

        # Commands are either batchable or they aren't. If a command
        # isn't batchable, we send it immediately because the executor
        # can no longer accept new commands after a non-batchable command.
        # If a command is batchable, we queue it for later. But we have
        # to account for the case of a non-batchable command arriving after
        # a batchable one and refuse to service it.

        def addcall():
            f = pycompat.futures.Future()
            self._futures.add(f)
            self._calls.append((command, args, fn, f))
            return f

        if getattr(fn, 'batchable', False):
            f = addcall()

            # But since we don't issue it immediately, we wrap its result()
            # to trigger sending so we avoid deadlocks.
            f.__class__ = unsentfuture
            f._peerexecutor = self
        else:
            if self._calls:
                raise error.ProgrammingError(
                    '%s is not batchable and cannot be called on a command '
                    'executor along with other commands' % command)

            f = addcall()

            # Non-batchable commands can never coexist with another command
            # in this executor. So send the command immediately.
            self.sendcommands()

        return f

    def sendcommands(self):
        if self._sent:
            return

        if not self._calls:
            return

        self._sent = True

        # Unhack any future types so caller seens a clean type and to break
        # cycle between us and futures.
        for f in self._futures:
            if isinstance(f, unsentfuture):
                f.__class__ = pycompat.futures.Future
                f._peerexecutor = None

        calls = self._calls
        # Mainly to destroy references to futures.
        self._calls = None

        # Simple case of a single command. We call it synchronously.
        if len(calls) == 1:
            command, args, fn, f = calls[0]

            # Future was cancelled. Ignore it.
            if not f.set_running_or_notify_cancel():
                return

            try:
                result = fn(**pycompat.strkwargs(args))
            except Exception:
                pycompat.future_set_exception_info(f, sys.exc_info()[1:])
            else:
                f.set_result(result)

            return

        # Batch commands are a bit harder. First, we have to deal with the
        # @batchable coroutine. That's a bit annoying. Furthermore, we also
        # need to preserve streaming. i.e. it should be possible for the
        # futures to resolve as data is coming in off the wire without having
        # to wait for the final byte of the final response. We do this by
        # spinning up a thread to read the responses.

        requests = []
        states = []

        for command, args, fn, f in calls:
            # Future was cancelled. Ignore it.
            if not f.set_running_or_notify_cancel():
                continue

            try:
                batchable = fn.batchable(fn.__self__,
                                         **pycompat.strkwargs(args))
            except Exception:
                pycompat.future_set_exception_info(f, sys.exc_info()[1:])
                return

            # Encoded arguments and future holding remote result.
            try:
                encodedargs, fremote = next(batchable)
            except Exception:
                pycompat.future_set_exception_info(f, sys.exc_info()[1:])
                return

            requests.append((command, encodedargs))
            states.append((command, f, batchable, fremote))

        if not requests:
            return

        # This will emit responses in order they were executed.
        wireresults = self._peer._submitbatch(requests)

        # The use of a thread pool executor here is a bit weird for something
        # that only spins up a single thread. However, thread management is
        # hard and it is easy to encounter race conditions, deadlocks, etc.
        # concurrent.futures already solves these problems and its thread pool
        # executor has minimal overhead. So we use it.
        self._responseexecutor = pycompat.futures.ThreadPoolExecutor(1)
        self._responsef = self._responseexecutor.submit(self._readbatchresponse,
                                                        states, wireresults)

    def close(self):
        self.sendcommands()

        if self._closed:
            return

        self._closed = True

        if not self._responsef:
            return

        # We need to wait on our in-flight response and then shut down the
        # executor once we have a result.
        try:
            self._responsef.result()
        finally:
            self._responseexecutor.shutdown(wait=True)
            self._responsef = None
            self._responseexecutor = None

            # If any of our futures are still in progress, mark them as
            # errored. Otherwise a result() could wait indefinitely.
            for f in self._futures:
                if not f.done():
                    f.set_exception(error.ResponseError(
                        _('unfulfilled batch command response')))

            self._futures = None

    def _readbatchresponse(self, states, wireresults):
        # Executes in a thread to read data off the wire.

        for command, f, batchable, fremote in states:
            # Grab raw result off the wire and teach the internal future
            # about it.
            remoteresult = next(wireresults)
            fremote.set(remoteresult)

            # And ask the coroutine to decode that value.
            try:
                result = next(batchable)
            except Exception:
                pycompat.future_set_exception_info(f, sys.exc_info()[1:])
            else:
                f.set_result(result)

@interfaceutil.implementer(repository.ipeercommands,
                           repository.ipeerlegacycommands)
class wirepeer(repository.peer):
    """Client-side interface for communicating with a peer repository.

    Methods commonly call wire protocol commands of the same name.

    See also httppeer.py and sshpeer.py for protocol-specific
    implementations of this interface.
    """
    def commandexecutor(self):
        return peerexecutor(self)

    # Begin of ipeercommands interface.

    def clonebundles(self):
        self.requirecap('clonebundles', _('clone bundles'))
        return self._call('clonebundles')

    @batchable
    def lookup(self, key):
        self.requirecap('lookup', _('look up remote revision'))
        f = future()
        yield {'key': encoding.fromlocal(key)}, f
        d = f.value
        success, data = d[:-1].split(" ", 1)
        if int(success):
            yield bin(data)
        else:
            self._abort(error.RepoError(data))

    @batchable
    def heads(self):
        f = future()
        yield {}, f
        d = f.value
        try:
            yield wireprototypes.decodelist(d[:-1])
        except ValueError:
            self._abort(error.ResponseError(_("unexpected response:"), d))

    @batchable
    def known(self, nodes):
        f = future()
        yield {'nodes': wireprototypes.encodelist(nodes)}, f
        d = f.value
        try:
            yield [bool(int(b)) for b in pycompat.iterbytestr(d)]
        except ValueError:
            self._abort(error.ResponseError(_("unexpected response:"), d))

    @batchable
    def branchmap(self):
        f = future()
        yield {}, f
        d = f.value
        try:
            branchmap = {}
            for branchpart in d.splitlines():
                branchname, branchheads = branchpart.split(' ', 1)
                branchname = encoding.tolocal(urlreq.unquote(branchname))
                branchheads = wireprototypes.decodelist(branchheads)
                branchmap[branchname] = branchheads
            yield branchmap
        except TypeError:
            self._abort(error.ResponseError(_("unexpected response:"), d))

    @batchable
    def listkeys(self, namespace):
        if not self.capable('pushkey'):
            yield {}, None
        f = future()
        self.ui.debug('preparing listkeys for "%s"\n' % namespace)
        yield {'namespace': encoding.fromlocal(namespace)}, f
        d = f.value
        self.ui.debug('received listkey for "%s": %i bytes\n'
                      % (namespace, len(d)))
        yield pushkeymod.decodekeys(d)

    @batchable
    def pushkey(self, namespace, key, old, new):
        if not self.capable('pushkey'):
            yield False, None
        f = future()
        self.ui.debug('preparing pushkey for "%s:%s"\n' % (namespace, key))
        yield {'namespace': encoding.fromlocal(namespace),
               'key': encoding.fromlocal(key),
               'old': encoding.fromlocal(old),
               'new': encoding.fromlocal(new)}, f
        d = f.value
        d, output = d.split('\n', 1)
        try:
            d = bool(int(d))
        except ValueError:
            raise error.ResponseError(
                _('push failed (unexpected response):'), d)
        for l in output.splitlines(True):
            self.ui.status(_('remote: '), l)
        yield d

    def stream_out(self):
        return self._callstream('stream_out')

    def getbundle(self, source, **kwargs):
        kwargs = pycompat.byteskwargs(kwargs)
        self.requirecap('getbundle', _('look up remote changes'))
        opts = {}
        bundlecaps = kwargs.get('bundlecaps') or set()
        for key, value in kwargs.iteritems():
            if value is None:
                continue
            keytype = wireprototypes.GETBUNDLE_ARGUMENTS.get(key)
            if keytype is None:
                raise error.ProgrammingError(
                    'Unexpectedly None keytype for key %s' % key)
            elif keytype == 'nodes':
                value = wireprototypes.encodelist(value)
            elif keytype == 'csv':
                value = ','.join(value)
            elif keytype == 'scsv':
                value = ','.join(sorted(value))
            elif keytype == 'boolean':
                value = '%i' % bool(value)
            elif keytype != 'plain':
                raise KeyError('unknown getbundle option type %s'
                               % keytype)
            opts[key] = value
        f = self._callcompressable("getbundle", **pycompat.strkwargs(opts))
        if any((cap.startswith('HG2') for cap in bundlecaps)):
            return bundle2.getunbundler(self.ui, f)
        else:
            return changegroupmod.cg1unpacker(f, 'UN')

    def unbundle(self, bundle, heads, url):
        '''Send cg (a readable file-like object representing the
        changegroup to push, typically a chunkbuffer object) to the
        remote server as a bundle.

        When pushing a bundle10 stream, return an integer indicating the
        result of the push (see changegroup.apply()).

        When pushing a bundle20 stream, return a bundle20 stream.

        `url` is the url the client thinks it's pushing to, which is
        visible to hooks.
        '''

        if heads != ['force'] and self.capable('unbundlehash'):
            heads = wireprototypes.encodelist(
                ['hashed', hashlib.sha1(''.join(sorted(heads))).digest()])
        else:
            heads = wireprototypes.encodelist(heads)

        if util.safehasattr(bundle, 'deltaheader'):
            # this a bundle10, do the old style call sequence
            ret, output = self._callpush("unbundle", bundle, heads=heads)
            if ret == "":
                raise error.ResponseError(
                    _('push failed:'), output)
            try:
                ret = int(ret)
            except ValueError:
                raise error.ResponseError(
                    _('push failed (unexpected response):'), ret)

            for l in output.splitlines(True):
                self.ui.status(_('remote: '), l)
        else:
            # bundle2 push. Send a stream, fetch a stream.
            stream = self._calltwowaystream('unbundle', bundle, heads=heads)
            ret = bundle2.getunbundler(self.ui, stream)
        return ret

    # End of ipeercommands interface.

    # Begin of ipeerlegacycommands interface.

    def branches(self, nodes):
        n = wireprototypes.encodelist(nodes)
        d = self._call("branches", nodes=n)
        try:
            br = [tuple(wireprototypes.decodelist(b)) for b in d.splitlines()]
            return br
        except ValueError:
            self._abort(error.ResponseError(_("unexpected response:"), d))

    def between(self, pairs):
        batch = 8 # avoid giant requests
        r = []
        for i in pycompat.xrange(0, len(pairs), batch):
            n = " ".join([wireprototypes.encodelist(p, '-')
                          for p in pairs[i:i + batch]])
            d = self._call("between", pairs=n)
            try:
                r.extend(l and wireprototypes.decodelist(l) or []
                         for l in d.splitlines())
            except ValueError:
                self._abort(error.ResponseError(_("unexpected response:"), d))
        return r

    def changegroup(self, nodes, source):
        n = wireprototypes.encodelist(nodes)
        f = self._callcompressable("changegroup", roots=n)
        return changegroupmod.cg1unpacker(f, 'UN')

    def changegroupsubset(self, bases, heads, source):
        self.requirecap('changegroupsubset', _('look up remote changes'))
        bases = wireprototypes.encodelist(bases)
        heads = wireprototypes.encodelist(heads)
        f = self._callcompressable("changegroupsubset",
                                   bases=bases, heads=heads)
        return changegroupmod.cg1unpacker(f, 'UN')

    # End of ipeerlegacycommands interface.

    def _submitbatch(self, req):
        """run batch request <req> on the server

        Returns an iterator of the raw responses from the server.
        """
        ui = self.ui
        if ui.debugflag and ui.configbool('devel', 'debug.peer-request'):
            ui.debug('devel-peer-request: batched-content\n')
            for op, args in req:
                msg = 'devel-peer-request:    - %s (%d arguments)\n'
                ui.debug(msg % (op, len(args)))

        unescapearg = wireprototypes.unescapebatcharg

        rsp = self._callstream("batch", cmds=encodebatchcmds(req))
        chunk = rsp.read(1024)
        work = [chunk]
        while chunk:
            while ';' not in chunk and chunk:
                chunk = rsp.read(1024)
                work.append(chunk)
            merged = ''.join(work)
            while ';' in merged:
                one, merged = merged.split(';', 1)
                yield unescapearg(one)
            chunk = rsp.read(1024)
            work = [merged, chunk]
        yield unescapearg(''.join(work))

    def _submitone(self, op, args):
        return self._call(op, **pycompat.strkwargs(args))

    def debugwireargs(self, one, two, three=None, four=None, five=None):
        # don't pass optional arguments left at their default value
        opts = {}
        if three is not None:
            opts[r'three'] = three
        if four is not None:
            opts[r'four'] = four
        return self._call('debugwireargs', one=one, two=two, **opts)

    def _call(self, cmd, **args):
        """execute <cmd> on the server

        The command is expected to return a simple string.

        returns the server reply as a string."""
        raise NotImplementedError()

    def _callstream(self, cmd, **args):
        """execute <cmd> on the server

        The command is expected to return a stream. Note that if the
        command doesn't return a stream, _callstream behaves
        differently for ssh and http peers.

        returns the server reply as a file like object.
        """
        raise NotImplementedError()

    def _callcompressable(self, cmd, **args):
        """execute <cmd> on the server

        The command is expected to return a stream.

        The stream may have been compressed in some implementations. This
        function takes care of the decompression. This is the only difference
        with _callstream.

        returns the server reply as a file like object.
        """
        raise NotImplementedError()

    def _callpush(self, cmd, fp, **args):
        """execute a <cmd> on server

        The command is expected to be related to a push. Push has a special
        return method.

        returns the server reply as a (ret, output) tuple. ret is either
        empty (error) or a stringified int.
        """
        raise NotImplementedError()

    def _calltwowaystream(self, cmd, fp, **args):
        """execute <cmd> on server

        The command will send a stream to the server and get a stream in reply.
        """
        raise NotImplementedError()

    def _abort(self, exception):
        """clearly abort the wire protocol connection and raise the exception
        """
        raise NotImplementedError()
