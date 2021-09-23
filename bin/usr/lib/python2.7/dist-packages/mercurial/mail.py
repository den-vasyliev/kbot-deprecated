# mail.py - mail sending bits for mercurial
#
# Copyright 2006 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import email
import email.charset
import email.header
import email.message
import email.parser
import io
import os
import smtplib
import socket
import time

from .i18n import _
from . import (
    encoding,
    error,
    pycompat,
    sslutil,
    util,
)
from .utils import (
    procutil,
    stringutil,
)

class STARTTLS(smtplib.SMTP):
    '''Derived class to verify the peer certificate for STARTTLS.

    This class allows to pass any keyword arguments to SSL socket creation.
    '''
    def __init__(self, ui, host=None, **kwargs):
        smtplib.SMTP.__init__(self, **kwargs)
        self._ui = ui
        self._host = host

    def starttls(self, keyfile=None, certfile=None):
        if not self.has_extn("starttls"):
            msg = "STARTTLS extension not supported by server"
            raise smtplib.SMTPException(msg)
        (resp, reply) = self.docmd("STARTTLS")
        if resp == 220:
            self.sock = sslutil.wrapsocket(self.sock, keyfile, certfile,
                                           ui=self._ui,
                                           serverhostname=self._host)
            self.file = smtplib.SSLFakeFile(self.sock)
            self.helo_resp = None
            self.ehlo_resp = None
            self.esmtp_features = {}
            self.does_esmtp = 0
        return (resp, reply)

class SMTPS(smtplib.SMTP):
    '''Derived class to verify the peer certificate for SMTPS.

    This class allows to pass any keyword arguments to SSL socket creation.
    '''
    def __init__(self, ui, keyfile=None, certfile=None, host=None,
                 **kwargs):
        self.keyfile = keyfile
        self.certfile = certfile
        smtplib.SMTP.__init__(self, **kwargs)
        self._host = host
        self.default_port = smtplib.SMTP_SSL_PORT
        self._ui = ui

    def _get_socket(self, host, port, timeout):
        if self.debuglevel > 0:
            self._ui.debug('connect: %r\n' % ((host, port),))
        new_socket = socket.create_connection((host, port), timeout)
        new_socket = sslutil.wrapsocket(new_socket,
                                        self.keyfile, self.certfile,
                                        ui=self._ui,
                                        serverhostname=self._host)
        self.file = new_socket.makefile(r'rb')
        return new_socket

def _pyhastls():
    """Returns true iff Python has TLS support, false otherwise."""
    try:
        import ssl
        getattr(ssl, 'HAS_TLS', False)
        return True
    except ImportError:
        return False

def _smtp(ui):
    '''build an smtp connection and return a function to send mail'''
    local_hostname = ui.config('smtp', 'local_hostname')
    tls = ui.config('smtp', 'tls')
    # backward compatible: when tls = true, we use starttls.
    starttls = tls == 'starttls' or stringutil.parsebool(tls)
    smtps = tls == 'smtps'
    if (starttls or smtps) and not _pyhastls():
        raise error.Abort(_("can't use TLS: Python SSL support not installed"))
    mailhost = ui.config('smtp', 'host')
    if not mailhost:
        raise error.Abort(_('smtp.host not configured - cannot send mail'))
    if smtps:
        ui.note(_('(using smtps)\n'))
        s = SMTPS(ui, local_hostname=local_hostname, host=mailhost)
    elif starttls:
        s = STARTTLS(ui, local_hostname=local_hostname, host=mailhost)
    else:
        s = smtplib.SMTP(local_hostname=local_hostname)
    if smtps:
        defaultport = 465
    else:
        defaultport = 25
    mailport = util.getport(ui.config('smtp', 'port', defaultport))
    ui.note(_('sending mail: smtp host %s, port %d\n') %
            (mailhost, mailport))
    s.connect(host=mailhost, port=mailport)
    if starttls:
        ui.note(_('(using starttls)\n'))
        s.ehlo()
        s.starttls()
        s.ehlo()
    if starttls or smtps:
        ui.note(_('(verifying remote certificate)\n'))
        sslutil.validatesocket(s.sock)
    username = ui.config('smtp', 'username')
    password = ui.config('smtp', 'password')
    if username and not password:
        password = ui.getpass()
    if username and password:
        ui.note(_('(authenticating to mail server as %s)\n') %
                  (username))
        try:
            s.login(username, password)
        except smtplib.SMTPException as inst:
            raise error.Abort(inst)

    def send(sender, recipients, msg):
        try:
            return s.sendmail(sender, recipients, msg)
        except smtplib.SMTPRecipientsRefused as inst:
            recipients = [r[1] for r in inst.recipients.values()]
            raise error.Abort('\n' + '\n'.join(recipients))
        except smtplib.SMTPException as inst:
            raise error.Abort(inst)

    return send

def _sendmail(ui, sender, recipients, msg):
    '''send mail using sendmail.'''
    program = ui.config('email', 'method')
    stremail = lambda x: stringutil.email(encoding.strtolocal(x))
    cmdline = '%s -f %s %s' % (program, stremail(sender),
                               ' '.join(map(stremail, recipients)))
    ui.note(_('sending mail: %s\n') % cmdline)
    fp = procutil.popen(cmdline, 'wb')
    fp.write(util.tonativeeol(msg))
    ret = fp.close()
    if ret:
        raise error.Abort('%s %s' % (
            os.path.basename(program.split(None, 1)[0]),
            procutil.explainexit(ret)))

def _mbox(mbox, sender, recipients, msg):
    '''write mails to mbox'''
    fp = open(mbox, 'ab+')
    # Should be time.asctime(), but Windows prints 2-characters day
    # of month instead of one. Make them print the same thing.
    date = time.strftime(r'%a %b %d %H:%M:%S %Y', time.localtime())
    fp.write('From %s %s\n' % (encoding.strtolocal(sender),
                               encoding.strtolocal(date)))
    fp.write(msg)
    fp.write('\n\n')
    fp.close()

def connect(ui, mbox=None):
    '''make a mail connection. return a function to send mail.
    call as sendmail(sender, list-of-recipients, msg).'''
    if mbox:
        open(mbox, 'wb').close()
        return lambda s, r, m: _mbox(mbox, s, r, m)
    if ui.config('email', 'method') == 'smtp':
        return _smtp(ui)
    return lambda s, r, m: _sendmail(ui, s, r, m)

def sendmail(ui, sender, recipients, msg, mbox=None):
    send = connect(ui, mbox=mbox)
    return send(sender, recipients, msg)

def validateconfig(ui):
    '''determine if we have enough config data to try sending email.'''
    method = ui.config('email', 'method')
    if method == 'smtp':
        if not ui.config('smtp', 'host'):
            raise error.Abort(_('smtp specified as email transport, '
                               'but no smtp host configured'))
    else:
        if not procutil.findexe(method):
            raise error.Abort(_('%r specified as email transport, '
                               'but not in PATH') % method)

def codec2iana(cs):
    ''''''
    cs = pycompat.sysbytes(email.charset.Charset(cs).input_charset.lower())

    # "latin1" normalizes to "iso8859-1", standard calls for "iso-8859-1"
    if cs.startswith("iso") and not cs.startswith("iso-"):
        return "iso-" + cs[3:]
    return cs

def mimetextpatch(s, subtype='plain', display=False):
    '''Return MIME message suitable for a patch.
    Charset will be detected by first trying to decode as us-ascii, then utf-8,
    and finally the global encodings. If all those fail, fall back to
    ISO-8859-1, an encoding with that allows all byte sequences.
    Transfer encodings will be used if necessary.'''

    cs = ['us-ascii', 'utf-8', encoding.encoding, encoding.fallbackencoding]
    if display:
        cs = ['us-ascii']
    for charset in cs:
        try:
            s.decode(pycompat.sysstr(charset))
            return mimetextqp(s, subtype, codec2iana(charset))
        except UnicodeDecodeError:
            pass

    return mimetextqp(s, subtype, "iso-8859-1")

def mimetextqp(body, subtype, charset):
    '''Return MIME message.
    Quoted-printable transfer encoding will be used if necessary.
    '''
    cs = email.charset.Charset(charset)
    msg = email.message.Message()
    msg.set_type(pycompat.sysstr('text/' + subtype))

    for line in body.splitlines():
        if len(line) > 950:
            cs.body_encoding = email.charset.QP
            break

    msg.set_payload(body, cs)

    return msg

def _charsets(ui):
    '''Obtains charsets to send mail parts not containing patches.'''
    charsets = [cs.lower() for cs in ui.configlist('email', 'charsets')]
    fallbacks = [encoding.fallbackencoding.lower(),
                 encoding.encoding.lower(), 'utf-8']
    for cs in fallbacks: # find unique charsets while keeping order
        if cs not in charsets:
            charsets.append(cs)
    return [cs for cs in charsets if not cs.endswith('ascii')]

def _encode(ui, s, charsets):
    '''Returns (converted) string, charset tuple.
    Finds out best charset by cycling through sendcharsets in descending
    order. Tries both encoding and fallbackencoding for input. Only as
    last resort send as is in fake ascii.
    Caveat: Do not use for mail parts containing patches!'''
    sendcharsets = charsets or _charsets(ui)
    if not isinstance(s, bytes):
        # We have unicode data, which we need to try and encode to
        # some reasonable-ish encoding. Try the encodings the user
        # wants, and fall back to garbage-in-ascii.
        for ocs in sendcharsets:
            try:
                return s.encode(pycompat.sysstr(ocs)), ocs
            except UnicodeEncodeError:
                pass
            except LookupError:
                ui.warn(_('ignoring invalid sendcharset: %s\n') % ocs)
        else:
            # Everything failed, ascii-armor what we've got and send it.
            return s.encode('ascii', 'backslashreplace')
    # We have a bytes of unknown encoding. We'll try and guess a valid
    # encoding, falling back to pretending we had ascii even though we
    # know that's wrong.
    try:
        s.decode('ascii')
    except UnicodeDecodeError:
        for ics in (encoding.encoding, encoding.fallbackencoding):
            try:
                u = s.decode(ics)
            except UnicodeDecodeError:
                continue
            for ocs in sendcharsets:
                try:
                    return u.encode(pycompat.sysstr(ocs)), ocs
                except UnicodeEncodeError:
                    pass
                except LookupError:
                    ui.warn(_('ignoring invalid sendcharset: %s\n') % ocs)
    # if ascii, or all conversion attempts fail, send (broken) ascii
    return s, 'us-ascii'

def headencode(ui, s, charsets=None, display=False):
    '''Returns RFC-2047 compliant header from given string.'''
    if not display:
        # split into words?
        s, cs = _encode(ui, s, charsets)
        return str(email.header.Header(s, cs))
    return s

def _addressencode(ui, name, addr, charsets=None):
    assert isinstance(addr, bytes)
    name = headencode(ui, name, charsets)
    try:
        acc, dom = addr.split('@')
        acc.decode('ascii')
        dom = dom.decode(pycompat.sysstr(encoding.encoding)).encode('idna')
        addr = '%s@%s' % (acc, dom)
    except UnicodeDecodeError:
        raise error.Abort(_('invalid email address: %s') % addr)
    except ValueError:
        try:
            # too strict?
            addr.decode('ascii')
        except UnicodeDecodeError:
            raise error.Abort(_('invalid local address: %s') % addr)
    return pycompat.bytesurl(
        email.utils.formataddr((name, encoding.strfromlocal(addr))))

def addressencode(ui, address, charsets=None, display=False):
    '''Turns address into RFC-2047 compliant header.'''
    if display or not address:
        return address or ''
    name, addr = email.utils.parseaddr(encoding.strfromlocal(address))
    return _addressencode(ui, name, encoding.strtolocal(addr), charsets)

def addrlistencode(ui, addrs, charsets=None, display=False):
    '''Turns a list of addresses into a list of RFC-2047 compliant headers.
    A single element of input list may contain multiple addresses, but output
    always has one address per item'''
    for a in addrs:
        assert isinstance(a, bytes), (r'%r unexpectedly not a bytestr' % a)
    if display:
        return [a.strip() for a in addrs if a.strip()]

    result = []
    for name, addr in email.utils.getaddresses(
            [encoding.strfromlocal(a) for a in addrs]):
        if name or addr:
            r = _addressencode(ui, name, encoding.strtolocal(addr), charsets)
            result.append(r)
    return result

def mimeencode(ui, s, charsets=None, display=False):
    '''creates mime text object, encodes it if needed, and sets
    charset and transfer-encoding accordingly.'''
    cs = 'us-ascii'
    if not display:
        s, cs = _encode(ui, s, charsets)
    return mimetextqp(s, 'plain', cs)

if pycompat.ispy3:
    def parse(fp):
        ep = email.parser.Parser()
        # disable the "universal newlines" mode, which isn't binary safe.
        # I have no idea if ascii/surrogateescape is correct, but that's
        # what the standard Python email parser does.
        fp = io.TextIOWrapper(fp, encoding=r'ascii',
                              errors=r'surrogateescape', newline=chr(10))
        try:
            return ep.parse(fp)
        finally:
            fp.detach()
else:
    def parse(fp):
        ep = email.parser.Parser()
        return ep.parse(fp)

def headdecode(s):
    '''Decodes RFC-2047 header'''
    uparts = []
    for part, charset in email.header.decode_header(s):
        if charset is not None:
            try:
                uparts.append(part.decode(charset))
                continue
            except UnicodeDecodeError:
                pass
        # On Python 3, decode_header() may return either bytes or unicode
        # depending on whether the header has =?<charset>? or not
        if isinstance(part, type(u'')):
            uparts.append(part)
            continue
        try:
            uparts.append(part.decode('UTF-8'))
            continue
        except UnicodeDecodeError:
            pass
        uparts.append(part.decode('ISO-8859-1'))
    return encoding.unitolocal(u' '.join(uparts))
