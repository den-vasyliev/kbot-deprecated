# Copyright 21 May 2005 - (c) 2005 Jake Edge <jake@edge2.net>
# Copyright 2005-2007 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import collections
import contextlib
import hashlib

from .i18n import _
from .node import (
    hex,
    nullid,
)
from . import (
    discovery,
    encoding,
    error,
    match as matchmod,
    narrowspec,
    pycompat,
    streamclone,
    util,
    wireprotoframing,
    wireprototypes,
)
from .utils import (
    cborutil,
    interfaceutil,
    stringutil,
)

FRAMINGTYPE = b'application/mercurial-exp-framing-0006'

HTTP_WIREPROTO_V2 = wireprototypes.HTTP_WIREPROTO_V2

COMMANDS = wireprototypes.commanddict()

# Value inserted into cache key computation function. Change the value to
# force new cache keys for every command request. This should be done when
# there is a change to how caching works, etc.
GLOBAL_CACHE_VERSION = 1

def handlehttpv2request(rctx, req, res, checkperm, urlparts):
    from .hgweb import common as hgwebcommon

    # URL space looks like: <permissions>/<command>, where <permission> can
    # be ``ro`` or ``rw`` to signal read-only or read-write, respectively.

    # Root URL does nothing meaningful... yet.
    if not urlparts:
        res.status = b'200 OK'
        res.headers[b'Content-Type'] = b'text/plain'
        res.setbodybytes(_('HTTP version 2 API handler'))
        return

    if len(urlparts) == 1:
        res.status = b'404 Not Found'
        res.headers[b'Content-Type'] = b'text/plain'
        res.setbodybytes(_('do not know how to process %s\n') %
                         req.dispatchpath)
        return

    permission, command = urlparts[0:2]

    if permission not in (b'ro', b'rw'):
        res.status = b'404 Not Found'
        res.headers[b'Content-Type'] = b'text/plain'
        res.setbodybytes(_('unknown permission: %s') % permission)
        return

    if req.method != 'POST':
        res.status = b'405 Method Not Allowed'
        res.headers[b'Allow'] = b'POST'
        res.setbodybytes(_('commands require POST requests'))
        return

    # At some point we'll want to use our own API instead of recycling the
    # behavior of version 1 of the wire protocol...
    # TODO return reasonable responses - not responses that overload the
    # HTTP status line message for error reporting.
    try:
        checkperm(rctx, req, 'pull' if permission == b'ro' else 'push')
    except hgwebcommon.ErrorResponse as e:
        res.status = hgwebcommon.statusmessage(e.code, pycompat.bytestr(e))
        for k, v in e.headers:
            res.headers[k] = v
        res.setbodybytes('permission denied')
        return

    # We have a special endpoint to reflect the request back at the client.
    if command == b'debugreflect':
        _processhttpv2reflectrequest(rctx.repo.ui, rctx.repo, req, res)
        return

    # Extra commands that we handle that aren't really wire protocol
    # commands. Think extra hard before making this hackery available to
    # extension.
    extracommands = {'multirequest'}

    if command not in COMMANDS and command not in extracommands:
        res.status = b'404 Not Found'
        res.headers[b'Content-Type'] = b'text/plain'
        res.setbodybytes(_('unknown wire protocol command: %s\n') % command)
        return

    repo = rctx.repo
    ui = repo.ui

    proto = httpv2protocolhandler(req, ui)

    if (not COMMANDS.commandavailable(command, proto)
        and command not in extracommands):
        res.status = b'404 Not Found'
        res.headers[b'Content-Type'] = b'text/plain'
        res.setbodybytes(_('invalid wire protocol command: %s') % command)
        return

    # TODO consider cases where proxies may add additional Accept headers.
    if req.headers.get(b'Accept') != FRAMINGTYPE:
        res.status = b'406 Not Acceptable'
        res.headers[b'Content-Type'] = b'text/plain'
        res.setbodybytes(_('client MUST specify Accept header with value: %s\n')
                           % FRAMINGTYPE)
        return

    if req.headers.get(b'Content-Type') != FRAMINGTYPE:
        res.status = b'415 Unsupported Media Type'
        # TODO we should send a response with appropriate media type,
        # since client does Accept it.
        res.headers[b'Content-Type'] = b'text/plain'
        res.setbodybytes(_('client MUST send Content-Type header with '
                           'value: %s\n') % FRAMINGTYPE)
        return

    _processhttpv2request(ui, repo, req, res, permission, command, proto)

def _processhttpv2reflectrequest(ui, repo, req, res):
    """Reads unified frame protocol request and dumps out state to client.

    This special endpoint can be used to help debug the wire protocol.

    Instead of routing the request through the normal dispatch mechanism,
    we instead read all frames, decode them, and feed them into our state
    tracker. We then dump the log of all that activity back out to the
    client.
    """
    import json

    # Reflection APIs have a history of being abused, accidentally disclosing
    # sensitive data, etc. So we have a config knob.
    if not ui.configbool('experimental', 'web.api.debugreflect'):
        res.status = b'404 Not Found'
        res.headers[b'Content-Type'] = b'text/plain'
        res.setbodybytes(_('debugreflect service not available'))
        return

    # We assume we have a unified framing protocol request body.

    reactor = wireprotoframing.serverreactor(ui)
    states = []

    while True:
        frame = wireprotoframing.readframe(req.bodyfh)

        if not frame:
            states.append(b'received: <no frame>')
            break

        states.append(b'received: %d %d %d %s' % (frame.typeid, frame.flags,
                                                  frame.requestid,
                                                  frame.payload))

        action, meta = reactor.onframerecv(frame)
        states.append(json.dumps((action, meta), sort_keys=True,
                                 separators=(', ', ': ')))

    action, meta = reactor.oninputeof()
    meta['action'] = action
    states.append(json.dumps(meta, sort_keys=True, separators=(', ',': ')))

    res.status = b'200 OK'
    res.headers[b'Content-Type'] = b'text/plain'
    res.setbodybytes(b'\n'.join(states))

def _processhttpv2request(ui, repo, req, res, authedperm, reqcommand, proto):
    """Post-validation handler for HTTPv2 requests.

    Called when the HTTP request contains unified frame-based protocol
    frames for evaluation.
    """
    # TODO Some HTTP clients are full duplex and can receive data before
    # the entire request is transmitted. Figure out a way to indicate support
    # for that so we can opt into full duplex mode.
    reactor = wireprotoframing.serverreactor(ui, deferoutput=True)
    seencommand = False

    outstream = None

    while True:
        frame = wireprotoframing.readframe(req.bodyfh)
        if not frame:
            break

        action, meta = reactor.onframerecv(frame)

        if action == 'wantframe':
            # Need more data before we can do anything.
            continue
        elif action == 'runcommand':
            # Defer creating output stream because we need to wait for
            # protocol settings frames so proper encoding can be applied.
            if not outstream:
                outstream = reactor.makeoutputstream()

            sentoutput = _httpv2runcommand(ui, repo, req, res, authedperm,
                                           reqcommand, reactor, outstream,
                                           meta, issubsequent=seencommand)

            if sentoutput:
                return

            seencommand = True

        elif action == 'error':
            # TODO define proper error mechanism.
            res.status = b'200 OK'
            res.headers[b'Content-Type'] = b'text/plain'
            res.setbodybytes(meta['message'] + b'\n')
            return
        else:
            raise error.ProgrammingError(
                'unhandled action from frame processor: %s' % action)

    action, meta = reactor.oninputeof()
    if action == 'sendframes':
        # We assume we haven't started sending the response yet. If we're
        # wrong, the response type will raise an exception.
        res.status = b'200 OK'
        res.headers[b'Content-Type'] = FRAMINGTYPE
        res.setbodygen(meta['framegen'])
    elif action == 'noop':
        pass
    else:
        raise error.ProgrammingError('unhandled action from frame processor: %s'
                                     % action)

def _httpv2runcommand(ui, repo, req, res, authedperm, reqcommand, reactor,
                      outstream, command, issubsequent):
    """Dispatch a wire protocol command made from HTTPv2 requests.

    The authenticated permission (``authedperm``) along with the original
    command from the URL (``reqcommand``) are passed in.
    """
    # We already validated that the session has permissions to perform the
    # actions in ``authedperm``. In the unified frame protocol, the canonical
    # command to run is expressed in a frame. However, the URL also requested
    # to run a specific command. We need to be careful that the command we
    # run doesn't have permissions requirements greater than what was granted
    # by ``authedperm``.
    #
    # Our rule for this is we only allow one command per HTTP request and
    # that command must match the command in the URL. However, we make
    # an exception for the ``multirequest`` URL. This URL is allowed to
    # execute multiple commands. We double check permissions of each command
    # as it is invoked to ensure there is no privilege escalation.
    # TODO consider allowing multiple commands to regular command URLs
    # iff each command is the same.

    proto = httpv2protocolhandler(req, ui, args=command['args'])

    if reqcommand == b'multirequest':
        if not COMMANDS.commandavailable(command['command'], proto):
            # TODO proper error mechanism
            res.status = b'200 OK'
            res.headers[b'Content-Type'] = b'text/plain'
            res.setbodybytes(_('wire protocol command not available: %s') %
                             command['command'])
            return True

        # TODO don't use assert here, since it may be elided by -O.
        assert authedperm in (b'ro', b'rw')
        wirecommand = COMMANDS[command['command']]
        assert wirecommand.permission in ('push', 'pull')

        if authedperm == b'ro' and wirecommand.permission != 'pull':
            # TODO proper error mechanism
            res.status = b'403 Forbidden'
            res.headers[b'Content-Type'] = b'text/plain'
            res.setbodybytes(_('insufficient permissions to execute '
                               'command: %s') % command['command'])
            return True

        # TODO should we also call checkperm() here? Maybe not if we're going
        # to overhaul that API. The granted scope from the URL check should
        # be good enough.

    else:
        # Don't allow multiple commands outside of ``multirequest`` URL.
        if issubsequent:
            # TODO proper error mechanism
            res.status = b'200 OK'
            res.headers[b'Content-Type'] = b'text/plain'
            res.setbodybytes(_('multiple commands cannot be issued to this '
                               'URL'))
            return True

        if reqcommand != command['command']:
            # TODO define proper error mechanism
            res.status = b'200 OK'
            res.headers[b'Content-Type'] = b'text/plain'
            res.setbodybytes(_('command in frame must match command in URL'))
            return True

    res.status = b'200 OK'
    res.headers[b'Content-Type'] = FRAMINGTYPE

    try:
        objs = dispatch(repo, proto, command['command'], command['redirect'])

        action, meta = reactor.oncommandresponsereadyobjects(
            outstream, command['requestid'], objs)

    except error.WireprotoCommandError as e:
        action, meta = reactor.oncommanderror(
            outstream, command['requestid'], e.message, e.messageargs)

    except Exception as e:
        action, meta = reactor.onservererror(
            outstream, command['requestid'],
            _('exception when invoking command: %s') %
            stringutil.forcebytestr(e))

    if action == 'sendframes':
        res.setbodygen(meta['framegen'])
        return True
    elif action == 'noop':
        return False
    else:
        raise error.ProgrammingError('unhandled event from reactor: %s' %
                                     action)

def getdispatchrepo(repo, proto, command):
    return repo.filtered('served')

def dispatch(repo, proto, command, redirect):
    """Run a wire protocol command.

    Returns an iterable of objects that will be sent to the client.
    """
    repo = getdispatchrepo(repo, proto, command)

    entry = COMMANDS[command]
    func = entry.func
    spec = entry.args

    args = proto.getargs(spec)

    # There is some duplicate boilerplate code here for calling the command and
    # emitting objects. It is either that or a lot of indented code that looks
    # like a pyramid (since there are a lot of code paths that result in not
    # using the cacher).
    callcommand = lambda: func(repo, proto, **pycompat.strkwargs(args))

    # Request is not cacheable. Don't bother instantiating a cacher.
    if not entry.cachekeyfn:
        for o in callcommand():
            yield o
        return

    if redirect:
        redirecttargets = redirect[b'targets']
        redirecthashes = redirect[b'hashes']
    else:
        redirecttargets = []
        redirecthashes = []

    cacher = makeresponsecacher(repo, proto, command, args,
                                cborutil.streamencode,
                                redirecttargets=redirecttargets,
                                redirecthashes=redirecthashes)

    # But we have no cacher. Do default handling.
    if not cacher:
        for o in callcommand():
            yield o
        return

    with cacher:
        cachekey = entry.cachekeyfn(repo, proto, cacher, **args)

        # No cache key or the cacher doesn't like it. Do default handling.
        if cachekey is None or not cacher.setcachekey(cachekey):
            for o in callcommand():
                yield o
            return

        # Serve it from the cache, if possible.
        cached = cacher.lookup()

        if cached:
            for o in cached['objs']:
                yield o
            return

        # Else call the command and feed its output into the cacher, allowing
        # the cacher to buffer/mutate objects as it desires.
        for o in callcommand():
            for o in cacher.onobject(o):
                yield o

        for o in cacher.onfinished():
            yield o

@interfaceutil.implementer(wireprototypes.baseprotocolhandler)
class httpv2protocolhandler(object):
    def __init__(self, req, ui, args=None):
        self._req = req
        self._ui = ui
        self._args = args

    @property
    def name(self):
        return HTTP_WIREPROTO_V2

    def getargs(self, args):
        # First look for args that were passed but aren't registered on this
        # command.
        extra = set(self._args) - set(args)
        if extra:
            raise error.WireprotoCommandError(
                'unsupported argument to command: %s' %
                ', '.join(sorted(extra)))

        # And look for required arguments that are missing.
        missing = {a for a in args if args[a]['required']} - set(self._args)

        if missing:
            raise error.WireprotoCommandError(
                'missing required arguments: %s' % ', '.join(sorted(missing)))

        # Now derive the arguments to pass to the command, taking into
        # account the arguments specified by the client.
        data = {}
        for k, meta in sorted(args.items()):
            # This argument wasn't passed by the client.
            if k not in self._args:
                data[k] = meta['default']()
                continue

            v = self._args[k]

            # Sets may be expressed as lists. Silently normalize.
            if meta['type'] == 'set' and isinstance(v, list):
                v = set(v)

            # TODO consider more/stronger type validation.

            data[k] = v

        return data

    def getprotocaps(self):
        # Protocol capabilities are currently not implemented for HTTP V2.
        return set()

    def getpayload(self):
        raise NotImplementedError

    @contextlib.contextmanager
    def mayberedirectstdio(self):
        raise NotImplementedError

    def client(self):
        raise NotImplementedError

    def addcapabilities(self, repo, caps):
        return caps

    def checkperm(self, perm):
        raise NotImplementedError

def httpv2apidescriptor(req, repo):
    proto = httpv2protocolhandler(req, repo.ui)

    return _capabilitiesv2(repo, proto)

def _capabilitiesv2(repo, proto):
    """Obtain the set of capabilities for version 2 transports.

    These capabilities are distinct from the capabilities for version 1
    transports.
    """
    caps = {
        'commands': {},
        'framingmediatypes': [FRAMINGTYPE],
        'pathfilterprefixes': set(narrowspec.VALID_PREFIXES),
    }

    for command, entry in COMMANDS.items():
        args = {}

        for arg, meta in entry.args.items():
            args[arg] = {
                # TODO should this be a normalized type using CBOR's
                # terminology?
                b'type': meta['type'],
                b'required': meta['required'],
            }

            if not meta['required']:
                args[arg][b'default'] = meta['default']()

            if meta['validvalues']:
                args[arg][b'validvalues'] = meta['validvalues']

        # TODO this type of check should be defined in a per-command callback.
        if (command == b'rawstorefiledata'
            and not streamclone.allowservergeneration(repo)):
            continue

        caps['commands'][command] = {
            'args': args,
            'permissions': [entry.permission],
        }

        if entry.extracapabilitiesfn:
            extracaps = entry.extracapabilitiesfn(repo, proto)
            caps['commands'][command].update(extracaps)

    caps['rawrepoformats'] = sorted(repo.requirements &
                                    repo.supportedformats)

    targets = getadvertisedredirecttargets(repo, proto)
    if targets:
        caps[b'redirect'] = {
            b'targets': [],
            b'hashes': [b'sha256', b'sha1'],
        }

        for target in targets:
            entry = {
                b'name': target['name'],
                b'protocol': target['protocol'],
                b'uris': target['uris'],
            }

            for key in ('snirequired', 'tlsversions'):
                if key in target:
                    entry[key] = target[key]

            caps[b'redirect'][b'targets'].append(entry)

    return proto.addcapabilities(repo, caps)

def getadvertisedredirecttargets(repo, proto):
    """Obtain a list of content redirect targets.

    Returns a list containing potential redirect targets that will be
    advertised in capabilities data. Each dict MUST have the following
    keys:

    name
       The name of this redirect target. This is the identifier clients use
       to refer to a target. It is transferred as part of every command
       request.

    protocol
       Network protocol used by this target. Typically this is the string
       in front of the ``://`` in a URL. e.g. ``https``.

    uris
       List of representative URIs for this target. Clients can use the
       URIs to test parsing for compatibility or for ordering preference
       for which target to use.

    The following optional keys are recognized:

    snirequired
       Bool indicating if Server Name Indication (SNI) is required to
       connect to this target.

    tlsversions
       List of bytes indicating which TLS versions are supported by this
       target.

    By default, clients reflect the target order advertised by servers
    and servers will use the first client-advertised target when picking
    a redirect target. So targets should be advertised in the order the
    server prefers they be used.
    """
    return []

def wireprotocommand(name, args=None, permission='push', cachekeyfn=None,
                     extracapabilitiesfn=None):
    """Decorator to declare a wire protocol command.

    ``name`` is the name of the wire protocol command being provided.

    ``args`` is a dict defining arguments accepted by the command. Keys are
    the argument name. Values are dicts with the following keys:

       ``type``
          The argument data type. Must be one of the following string
          literals: ``bytes``, ``int``, ``list``, ``dict``, ``set``,
          or ``bool``.

       ``default``
          A callable returning the default value for this argument. If not
          specified, ``None`` will be the default value.

       ``example``
          An example value for this argument.

       ``validvalues``
          Set of recognized values for this argument.

    ``permission`` defines the permission type needed to run this command.
    Can be ``push`` or ``pull``. These roughly map to read-write and read-only,
    respectively. Default is to assume command requires ``push`` permissions
    because otherwise commands not declaring their permissions could modify
    a repository that is supposed to be read-only.

    ``cachekeyfn`` defines an optional callable that can derive the
    cache key for this request.

    ``extracapabilitiesfn`` defines an optional callable that defines extra
    command capabilities/parameters that are advertised next to the command
    in the capabilities data structure describing the server. The callable
    receives as arguments the repository and protocol objects. It returns
    a dict of extra fields to add to the command descriptor.

    Wire protocol commands are generators of objects to be serialized and
    sent to the client.

    If a command raises an uncaught exception, this will be translated into
    a command error.

    All commands can opt in to being cacheable by defining a function
    (``cachekeyfn``) that is called to derive a cache key. This function
    receives the same arguments as the command itself plus a ``cacher``
    argument containing the active cacher for the request and returns a bytes
    containing the key in a cache the response to this command may be cached
    under.
    """
    transports = {k for k, v in wireprototypes.TRANSPORTS.items()
                  if v['version'] == 2}

    if permission not in ('push', 'pull'):
        raise error.ProgrammingError('invalid wire protocol permission; '
                                     'got %s; expected "push" or "pull"' %
                                     permission)

    if args is None:
        args = {}

    if not isinstance(args, dict):
        raise error.ProgrammingError('arguments for version 2 commands '
                                     'must be declared as dicts')

    for arg, meta in args.items():
        if arg == '*':
            raise error.ProgrammingError('* argument name not allowed on '
                                         'version 2 commands')

        if not isinstance(meta, dict):
            raise error.ProgrammingError('arguments for version 2 commands '
                                         'must declare metadata as a dict')

        if 'type' not in meta:
            raise error.ProgrammingError('%s argument for command %s does not '
                                         'declare type field' % (arg, name))

        if meta['type'] not in ('bytes', 'int', 'list', 'dict', 'set', 'bool'):
            raise error.ProgrammingError('%s argument for command %s has '
                                         'illegal type: %s' % (arg, name,
                                                               meta['type']))

        if 'example' not in meta:
            raise error.ProgrammingError('%s argument for command %s does not '
                                         'declare example field' % (arg, name))

        meta['required'] = 'default' not in meta

        meta.setdefault('default', lambda: None)
        meta.setdefault('validvalues', None)

    def register(func):
        if name in COMMANDS:
            raise error.ProgrammingError('%s command already registered '
                                         'for version 2' % name)

        COMMANDS[name] = wireprototypes.commandentry(
            func, args=args, transports=transports, permission=permission,
            cachekeyfn=cachekeyfn, extracapabilitiesfn=extracapabilitiesfn)

        return func

    return register

def makecommandcachekeyfn(command, localversion=None, allargs=False):
    """Construct a cache key derivation function with common features.

    By default, the cache key is a hash of:

    * The command name.
    * A global cache version number.
    * A local cache version number (passed via ``localversion``).
    * All the arguments passed to the command.
    * The media type used.
    * Wire protocol version string.
    * The repository path.
    """
    if not allargs:
        raise error.ProgrammingError('only allargs=True is currently supported')

    if localversion is None:
        raise error.ProgrammingError('must set localversion argument value')

    def cachekeyfn(repo, proto, cacher, **args):
        spec = COMMANDS[command]

        # Commands that mutate the repo can not be cached.
        if spec.permission == 'push':
            return None

        # TODO config option to disable caching.

        # Our key derivation strategy is to construct a data structure
        # holding everything that could influence cacheability and to hash
        # the CBOR representation of that. Using CBOR seems like it might
        # be overkill. However, simpler hashing mechanisms are prone to
        # duplicate input issues. e.g. if you just concatenate two values,
        # "foo"+"bar" is identical to "fo"+"obar". Using CBOR provides
        # "padding" between values and prevents these problems.

        # Seed the hash with various data.
        state = {
            # To invalidate all cache keys.
            b'globalversion': GLOBAL_CACHE_VERSION,
            # More granular cache key invalidation.
            b'localversion': localversion,
            # Cache keys are segmented by command.
            b'command': pycompat.sysbytes(command),
            # Throw in the media type and API version strings so changes
            # to exchange semantics invalid cache.
            b'mediatype': FRAMINGTYPE,
            b'version': HTTP_WIREPROTO_V2,
            # So same requests for different repos don't share cache keys.
            b'repo': repo.root,
        }

        # The arguments passed to us will have already been normalized.
        # Default values will be set, etc. This is important because it
        # means that it doesn't matter if clients send an explicit argument
        # or rely on the default value: it will all normalize to the same
        # set of arguments on the server and therefore the same cache key.
        #
        # Arguments by their very nature must support being encoded to CBOR.
        # And the CBOR encoder is deterministic. So we hash the arguments
        # by feeding the CBOR of their representation into the hasher.
        if allargs:
            state[b'args'] = pycompat.byteskwargs(args)

        cacher.adjustcachekeystate(state)

        hasher = hashlib.sha1()
        for chunk in cborutil.streamencode(state):
            hasher.update(chunk)

        return pycompat.sysbytes(hasher.hexdigest())

    return cachekeyfn

def makeresponsecacher(repo, proto, command, args, objencoderfn,
                       redirecttargets, redirecthashes):
    """Construct a cacher for a cacheable command.

    Returns an ``iwireprotocolcommandcacher`` instance.

    Extensions can monkeypatch this function to provide custom caching
    backends.
    """
    return None

def resolvenodes(repo, revisions):
    """Resolve nodes from a revisions specifier data structure."""
    cl = repo.changelog
    clhasnode = cl.hasnode

    seen = set()
    nodes = []

    if not isinstance(revisions, list):
        raise error.WireprotoCommandError('revisions must be defined as an '
                                          'array')

    for spec in revisions:
        if b'type' not in spec:
            raise error.WireprotoCommandError(
                'type key not present in revision specifier')

        typ = spec[b'type']

        if typ == b'changesetexplicit':
            if b'nodes' not in spec:
                raise error.WireprotoCommandError(
                    'nodes key not present in changesetexplicit revision '
                    'specifier')

            for node in spec[b'nodes']:
                if node not in seen:
                    nodes.append(node)
                    seen.add(node)

        elif typ == b'changesetexplicitdepth':
            for key in (b'nodes', b'depth'):
                if key not in spec:
                    raise error.WireprotoCommandError(
                        '%s key not present in changesetexplicitdepth revision '
                        'specifier', (key,))

            for rev in repo.revs(b'ancestors(%ln, %d)', spec[b'nodes'],
                                 spec[b'depth'] - 1):
                node = cl.node(rev)

                if node not in seen:
                    nodes.append(node)
                    seen.add(node)

        elif typ == b'changesetdagrange':
            for key in (b'roots', b'heads'):
                if key not in spec:
                    raise error.WireprotoCommandError(
                        '%s key not present in changesetdagrange revision '
                        'specifier', (key,))

            if not spec[b'heads']:
                raise error.WireprotoCommandError(
                    'heads key in changesetdagrange cannot be empty')

            if spec[b'roots']:
                common = [n for n in spec[b'roots'] if clhasnode(n)]
            else:
                common = [nullid]

            for n in discovery.outgoing(repo, common, spec[b'heads']).missing:
                if n not in seen:
                    nodes.append(n)
                    seen.add(n)

        else:
            raise error.WireprotoCommandError(
                'unknown revision specifier type: %s', (typ,))

    return nodes

@wireprotocommand('branchmap', permission='pull')
def branchmapv2(repo, proto):
    yield {encoding.fromlocal(k): v
           for k, v in repo.branchmap().iteritems()}

@wireprotocommand('capabilities', permission='pull')
def capabilitiesv2(repo, proto):
    yield _capabilitiesv2(repo, proto)

@wireprotocommand(
    'changesetdata',
    args={
        'revisions': {
            'type': 'list',
            'example': [{
                b'type': b'changesetexplicit',
                b'nodes': [b'abcdef...'],
            }],
        },
        'fields': {
            'type': 'set',
            'default': set,
            'example': {b'parents', b'revision'},
            'validvalues': {b'bookmarks', b'parents', b'phase', b'revision'},
        },
    },
    permission='pull')
def changesetdata(repo, proto, revisions, fields):
    # TODO look for unknown fields and abort when they can't be serviced.
    # This could probably be validated by dispatcher using validvalues.

    cl = repo.changelog
    outgoing = resolvenodes(repo, revisions)
    publishing = repo.publishing()

    if outgoing:
        repo.hook('preoutgoing', throw=True, source='serve')

    yield {
        b'totalitems': len(outgoing),
    }

    # The phases of nodes already transferred to the client may have changed
    # since the client last requested data. We send phase-only records
    # for these revisions, if requested.
    # TODO actually do this. We'll probably want to emit phase heads
    # in the ancestry set of the outgoing revisions. This will ensure
    # that phase updates within that set are seen.
    if b'phase' in fields:
        pass

    nodebookmarks = {}
    for mark, node in repo._bookmarks.items():
        nodebookmarks.setdefault(node, set()).add(mark)

    # It is already topologically sorted by revision number.
    for node in outgoing:
        d = {
            b'node': node,
        }

        if b'parents' in fields:
            d[b'parents'] = cl.parents(node)

        if b'phase' in fields:
            if publishing:
                d[b'phase'] = b'public'
            else:
                ctx = repo[node]
                d[b'phase'] = ctx.phasestr()

        if b'bookmarks' in fields and node in nodebookmarks:
            d[b'bookmarks'] = sorted(nodebookmarks[node])
            del nodebookmarks[node]

        followingmeta = []
        followingdata = []

        if b'revision' in fields:
            revisiondata = cl.revision(node, raw=True)
            followingmeta.append((b'revision', len(revisiondata)))
            followingdata.append(revisiondata)

        # TODO make it possible for extensions to wrap a function or register
        # a handler to service custom fields.

        if followingmeta:
            d[b'fieldsfollowing'] = followingmeta

        yield d

        for extra in followingdata:
            yield extra

    # If requested, send bookmarks from nodes that didn't have revision
    # data sent so receiver is aware of any bookmark updates.
    if b'bookmarks' in fields:
        for node, marks in sorted(nodebookmarks.iteritems()):
            yield {
                b'node': node,
                b'bookmarks': sorted(marks),
            }

class FileAccessError(Exception):
    """Represents an error accessing a specific file."""

    def __init__(self, path, msg, args):
        self.path = path
        self.msg = msg
        self.args = args

def getfilestore(repo, proto, path):
    """Obtain a file storage object for use with wire protocol.

    Exists as a standalone function so extensions can monkeypatch to add
    access control.
    """
    # This seems to work even if the file doesn't exist. So catch
    # "empty" files and return an error.
    fl = repo.file(path)

    if not len(fl):
        raise FileAccessError(path, 'unknown file: %s', (path,))

    return fl

def emitfilerevisions(repo, path, revisions, fields):
    clnode = repo.changelog.node

    for revision in revisions:
        d = {
            b'node': revision.node,
        }

        if b'parents' in fields:
            d[b'parents'] = [revision.p1node, revision.p2node]

        if b'linknode' in fields:
            # TODO by creating the filectx against a specific file revision
            # instead of changeset, linkrev() is always used. This is wrong for
            # cases where linkrev() may refer to a hidden changeset. We need an
            # API for performing linkrev adjustment that takes this into
            # account.
            fctx = repo.filectx(path, fileid=revision.node)
            d[b'linknode'] = clnode(fctx.introrev())

        followingmeta = []
        followingdata = []

        if b'revision' in fields:
            if revision.revision is not None:
                followingmeta.append((b'revision', len(revision.revision)))
                followingdata.append(revision.revision)
            else:
                d[b'deltabasenode'] = revision.basenode
                followingmeta.append((b'delta', len(revision.delta)))
                followingdata.append(revision.delta)

        if followingmeta:
            d[b'fieldsfollowing'] = followingmeta

        yield d

        for extra in followingdata:
            yield extra

def makefilematcher(repo, pathfilter):
    """Construct a matcher from a path filter dict."""

    # Validate values.
    if pathfilter:
        for key in (b'include', b'exclude'):
            for pattern in pathfilter.get(key, []):
                if not pattern.startswith((b'path:', b'rootfilesin:')):
                    raise error.WireprotoCommandError(
                        '%s pattern must begin with `path:` or `rootfilesin:`; '
                        'got %s', (key, pattern))

    if pathfilter:
        matcher = matchmod.match(repo.root, b'',
                                 include=pathfilter.get(b'include', []),
                                 exclude=pathfilter.get(b'exclude', []))
    else:
        matcher = matchmod.match(repo.root, b'')

    # Requested patterns could include files not in the local store. So
    # filter those out.
    return matchmod.intersectmatchers(repo.narrowmatch(), matcher)

@wireprotocommand(
    'filedata',
    args={
        'haveparents': {
            'type': 'bool',
            'default': lambda: False,
            'example': True,
        },
        'nodes': {
            'type': 'list',
            'example': [b'0123456...'],
        },
        'fields': {
            'type': 'set',
            'default': set,
            'example': {b'parents', b'revision'},
            'validvalues': {b'parents', b'revision', b'linknode'},
        },
        'path': {
            'type': 'bytes',
            'example': b'foo.txt',
        }
    },
    permission='pull',
    # TODO censoring a file revision won't invalidate the cache.
    # Figure out a way to take censoring into account when deriving
    # the cache key.
    cachekeyfn=makecommandcachekeyfn('filedata', 1, allargs=True))
def filedata(repo, proto, haveparents, nodes, fields, path):
    # TODO this API allows access to file revisions that are attached to
    # secret changesets. filesdata does not have this problem. Maybe this
    # API should be deleted?

    try:
        # Extensions may wish to access the protocol handler.
        store = getfilestore(repo, proto, path)
    except FileAccessError as e:
        raise error.WireprotoCommandError(e.msg, e.args)

    # Validate requested nodes.
    for node in nodes:
        try:
            store.rev(node)
        except error.LookupError:
            raise error.WireprotoCommandError('unknown file node: %s',
                                              (hex(node),))

    revisions = store.emitrevisions(nodes,
                                    revisiondata=b'revision' in fields,
                                    assumehaveparentrevisions=haveparents)

    yield {
        b'totalitems': len(nodes),
    }

    for o in emitfilerevisions(repo, path, revisions, fields):
        yield o

def filesdatacapabilities(repo, proto):
    batchsize = repo.ui.configint(
        b'experimental', b'server.filesdata.recommended-batch-size')
    return {
        b'recommendedbatchsize': batchsize,
    }

@wireprotocommand(
    'filesdata',
    args={
        'haveparents': {
            'type': 'bool',
            'default': lambda: False,
            'example': True,
        },
        'fields': {
            'type': 'set',
            'default': set,
            'example': {b'parents', b'revision'},
            'validvalues': {b'firstchangeset', b'linknode', b'parents',
                            b'revision'},
        },
        'pathfilter': {
            'type': 'dict',
            'default': lambda: None,
            'example': {b'include': [b'path:tests']},
        },
        'revisions': {
            'type': 'list',
            'example': [{
                b'type': b'changesetexplicit',
                b'nodes': [b'abcdef...'],
            }],
        },
    },
    permission='pull',
    # TODO censoring a file revision won't invalidate the cache.
    # Figure out a way to take censoring into account when deriving
    # the cache key.
    cachekeyfn=makecommandcachekeyfn('filesdata', 1, allargs=True),
    extracapabilitiesfn=filesdatacapabilities)
def filesdata(repo, proto, haveparents, fields, pathfilter, revisions):
    # TODO This should operate on a repo that exposes obsolete changesets. There
    # is a race between a client making a push that obsoletes a changeset and
    # another client fetching files data for that changeset. If a client has a
    # changeset, it should probably be allowed to access files data for that
    # changeset.

    cl = repo.changelog
    outgoing = resolvenodes(repo, revisions)
    filematcher = makefilematcher(repo, pathfilter)

    # Figure out what needs to be emitted.
    changedpaths = set()
    fnodes = collections.defaultdict(set)

    for node in outgoing:
        ctx = repo[node]
        changedpaths.update(ctx.files())

    changedpaths = sorted(p for p in changedpaths if filematcher(p))

    # If ancestors are known, we send file revisions having a linkrev in the
    # outgoing set of changeset revisions.
    if haveparents:
        outgoingclrevs = set(cl.rev(n) for n in outgoing)

        for path in changedpaths:
            try:
                store = getfilestore(repo, proto, path)
            except FileAccessError as e:
                raise error.WireprotoCommandError(e.msg, e.args)

            for rev in store:
                linkrev = store.linkrev(rev)

                if linkrev in outgoingclrevs:
                    fnodes[path].add(store.node(rev))

    # If ancestors aren't known, we walk the manifests and send all
    # encountered file revisions.
    else:
        for node in outgoing:
            mctx = repo[node].manifestctx()

            for path, fnode in mctx.read().items():
                if filematcher(path):
                    fnodes[path].add(fnode)

    yield {
        b'totalpaths': len(fnodes),
        b'totalitems': sum(len(v) for v in fnodes.values())
    }

    for path, filenodes in sorted(fnodes.items()):
        try:
            store = getfilestore(repo, proto, path)
        except FileAccessError as e:
            raise error.WireprotoCommandError(e.msg, e.args)

        yield {
            b'path': path,
            b'totalitems': len(filenodes),
        }

        revisions = store.emitrevisions(filenodes,
                                        revisiondata=b'revision' in fields,
                                        assumehaveparentrevisions=haveparents)

        for o in emitfilerevisions(repo, path, revisions, fields):
            yield o

@wireprotocommand(
    'heads',
    args={
        'publiconly': {
            'type': 'bool',
            'default': lambda: False,
            'example': False,
        },
    },
    permission='pull')
def headsv2(repo, proto, publiconly):
    if publiconly:
        repo = repo.filtered('immutable')

    yield repo.heads()

@wireprotocommand(
    'known',
    args={
        'nodes': {
            'type': 'list',
            'default': list,
            'example': [b'deadbeef'],
        },
    },
    permission='pull')
def knownv2(repo, proto, nodes):
    result = b''.join(b'1' if n else b'0' for n in repo.known(nodes))
    yield result

@wireprotocommand(
    'listkeys',
    args={
        'namespace': {
            'type': 'bytes',
            'example': b'ns',
        },
    },
    permission='pull')
def listkeysv2(repo, proto, namespace):
    keys = repo.listkeys(encoding.tolocal(namespace))
    keys = {encoding.fromlocal(k): encoding.fromlocal(v)
            for k, v in keys.iteritems()}

    yield keys

@wireprotocommand(
    'lookup',
    args={
        'key': {
            'type': 'bytes',
            'example': b'foo',
        },
    },
    permission='pull')
def lookupv2(repo, proto, key):
    key = encoding.tolocal(key)

    # TODO handle exception.
    node = repo.lookup(key)

    yield node

def manifestdatacapabilities(repo, proto):
    batchsize = repo.ui.configint(
        b'experimental', b'server.manifestdata.recommended-batch-size')

    return {
        b'recommendedbatchsize': batchsize,
    }

@wireprotocommand(
    'manifestdata',
    args={
        'nodes': {
            'type': 'list',
            'example': [b'0123456...'],
        },
        'haveparents': {
            'type': 'bool',
            'default': lambda: False,
            'example': True,
        },
        'fields': {
            'type': 'set',
            'default': set,
            'example': {b'parents', b'revision'},
            'validvalues': {b'parents', b'revision'},
        },
        'tree': {
            'type': 'bytes',
            'example': b'',
        },
    },
    permission='pull',
    cachekeyfn=makecommandcachekeyfn('manifestdata', 1, allargs=True),
    extracapabilitiesfn=manifestdatacapabilities)
def manifestdata(repo, proto, haveparents, nodes, fields, tree):
    store = repo.manifestlog.getstorage(tree)

    # Validate the node is known and abort on unknown revisions.
    for node in nodes:
        try:
            store.rev(node)
        except error.LookupError:
            raise error.WireprotoCommandError(
                'unknown node: %s', (node,))

    revisions = store.emitrevisions(nodes,
                                    revisiondata=b'revision' in fields,
                                    assumehaveparentrevisions=haveparents)

    yield {
        b'totalitems': len(nodes),
    }

    for revision in revisions:
        d = {
            b'node': revision.node,
        }

        if b'parents' in fields:
            d[b'parents'] = [revision.p1node, revision.p2node]

        followingmeta = []
        followingdata = []

        if b'revision' in fields:
            if revision.revision is not None:
                followingmeta.append((b'revision', len(revision.revision)))
                followingdata.append(revision.revision)
            else:
                d[b'deltabasenode'] = revision.basenode
                followingmeta.append((b'delta', len(revision.delta)))
                followingdata.append(revision.delta)

        if followingmeta:
            d[b'fieldsfollowing'] = followingmeta

        yield d

        for extra in followingdata:
            yield extra

@wireprotocommand(
    'pushkey',
    args={
        'namespace': {
            'type': 'bytes',
            'example': b'ns',
        },
        'key': {
            'type': 'bytes',
            'example': b'key',
        },
        'old': {
            'type': 'bytes',
            'example': b'old',
        },
        'new': {
            'type': 'bytes',
            'example': 'new',
        },
    },
    permission='push')
def pushkeyv2(repo, proto, namespace, key, old, new):
    # TODO handle ui output redirection
    yield repo.pushkey(encoding.tolocal(namespace),
                       encoding.tolocal(key),
                       encoding.tolocal(old),
                       encoding.tolocal(new))


@wireprotocommand(
    'rawstorefiledata',
    args={
        'files': {
            'type': 'list',
            'example': [b'changelog', b'manifestlog'],
        },
        'pathfilter': {
            'type': 'list',
            'default': lambda: None,
            'example': {b'include': [b'path:tests']},
        },
    },
    permission='pull')
def rawstorefiledata(repo, proto, files, pathfilter):
    if not streamclone.allowservergeneration(repo):
        raise error.WireprotoCommandError(b'stream clone is disabled')

    # TODO support dynamically advertising what store files "sets" are
    # available. For now, we support changelog, manifestlog, and files.
    files = set(files)
    allowedfiles = {b'changelog', b'manifestlog'}

    unsupported = files - allowedfiles
    if unsupported:
        raise error.WireprotoCommandError(b'unknown file type: %s',
                                          (b', '.join(sorted(unsupported)),))

    with repo.lock():
        topfiles = list(repo.store.topfiles())

    sendfiles = []
    totalsize = 0

    # TODO this is a bunch of storage layer interface abstractions because
    # it assumes revlogs.
    for name, encodedname, size in topfiles:
        if b'changelog' in files and name.startswith(b'00changelog'):
            pass
        elif b'manifestlog' in files and name.startswith(b'00manifest'):
            pass
        else:
            continue

        sendfiles.append((b'store', name, size))
        totalsize += size

    yield {
        b'filecount': len(sendfiles),
        b'totalsize': totalsize,
    }

    for location, name, size in sendfiles:
        yield {
            b'location': location,
            b'path': name,
            b'size': size,
        }

        # We have to use a closure for this to ensure the context manager is
        # closed only after sending the final chunk.
        def getfiledata():
            with repo.svfs(name, 'rb', auditpath=False) as fh:
                for chunk in util.filechunkiter(fh, limit=size):
                    yield chunk

        yield wireprototypes.indefinitebytestringresponse(
            getfiledata())
