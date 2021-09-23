# util.py - Mercurial utility functions relative to dates
#
#  Copyright 2018 Boris Feld <boris.feld@octobus.net>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import, print_function

import calendar
import datetime
import time

from ..i18n import _
from .. import (
    encoding,
    error,
    pycompat,
)

# used by parsedate
defaultdateformats = (
    '%Y-%m-%dT%H:%M:%S', # the 'real' ISO8601
    '%Y-%m-%dT%H:%M',    #   without seconds
    '%Y-%m-%dT%H%M%S',   # another awful but legal variant without :
    '%Y-%m-%dT%H%M',     #   without seconds
    '%Y-%m-%d %H:%M:%S', # our common legal variant
    '%Y-%m-%d %H:%M',    #   without seconds
    '%Y-%m-%d %H%M%S',   # without :
    '%Y-%m-%d %H%M',     #   without seconds
    '%Y-%m-%d %I:%M:%S%p',
    '%Y-%m-%d %H:%M',
    '%Y-%m-%d %I:%M%p',
    '%Y-%m-%d',
    '%m-%d',
    '%m/%d',
    '%m/%d/%y',
    '%m/%d/%Y',
    '%a %b %d %H:%M:%S %Y',
    '%a %b %d %I:%M:%S%p %Y',
    '%a, %d %b %Y %H:%M:%S',        #  GNU coreutils "/bin/date --rfc-2822"
    '%b %d %H:%M:%S %Y',
    '%b %d %I:%M:%S%p %Y',
    '%b %d %H:%M:%S',
    '%b %d %I:%M:%S%p',
    '%b %d %H:%M',
    '%b %d %I:%M%p',
    '%b %d %Y',
    '%b %d',
    '%H:%M:%S',
    '%I:%M:%S%p',
    '%H:%M',
    '%I:%M%p',
)

extendeddateformats = defaultdateformats + (
    "%Y",
    "%Y-%m",
    "%b",
    "%b %Y",
)

def makedate(timestamp=None):
    '''Return a unix timestamp (or the current time) as a (unixtime,
    offset) tuple based off the local timezone.'''
    if timestamp is None:
        timestamp = time.time()
    if timestamp < 0:
        hint = _("check your clock")
        raise error.Abort(_("negative timestamp: %d") % timestamp, hint=hint)
    delta = (datetime.datetime.utcfromtimestamp(timestamp) -
             datetime.datetime.fromtimestamp(timestamp))
    tz = delta.days * 86400 + delta.seconds
    return timestamp, tz

def datestr(date=None, format='%a %b %d %H:%M:%S %Y %1%2'):
    """represent a (unixtime, offset) tuple as a localized time.
    unixtime is seconds since the epoch, and offset is the time zone's
    number of seconds away from UTC.

    >>> datestr((0, 0))
    'Thu Jan 01 00:00:00 1970 +0000'
    >>> datestr((42, 0))
    'Thu Jan 01 00:00:42 1970 +0000'
    >>> datestr((-42, 0))
    'Wed Dec 31 23:59:18 1969 +0000'
    >>> datestr((0x7fffffff, 0))
    'Tue Jan 19 03:14:07 2038 +0000'
    >>> datestr((-0x80000000, 0))
    'Fri Dec 13 20:45:52 1901 +0000'
    """
    t, tz = date or makedate()
    if "%1" in format or "%2" in format or "%z" in format:
        sign = (tz > 0) and "-" or "+"
        minutes = abs(tz) // 60
        q, r = divmod(minutes, 60)
        format = format.replace("%z", "%1%2")
        format = format.replace("%1", "%c%02d" % (sign, q))
        format = format.replace("%2", "%02d" % r)
    d = t - tz
    if d > 0x7fffffff:
        d = 0x7fffffff
    elif d < -0x80000000:
        d = -0x80000000
    # Never use time.gmtime() and datetime.datetime.fromtimestamp()
    # because they use the gmtime() system call which is buggy on Windows
    # for negative values.
    t = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=d)
    s = encoding.strtolocal(t.strftime(encoding.strfromlocal(format)))
    return s

def shortdate(date=None):
    """turn (timestamp, tzoff) tuple into iso 8631 date."""
    return datestr(date, format='%Y-%m-%d')

def parsetimezone(s):
    """find a trailing timezone, if any, in string, and return a
       (offset, remainder) pair"""
    s = pycompat.bytestr(s)

    if s.endswith("GMT") or s.endswith("UTC"):
        return 0, s[:-3].rstrip()

    # Unix-style timezones [+-]hhmm
    if len(s) >= 5 and s[-5] in "+-" and s[-4:].isdigit():
        sign = (s[-5] == "+") and 1 or -1
        hours = int(s[-4:-2])
        minutes = int(s[-2:])
        return -sign * (hours * 60 + minutes) * 60, s[:-5].rstrip()

    # ISO8601 trailing Z
    if s.endswith("Z") and s[-2:-1].isdigit():
        return 0, s[:-1]

    # ISO8601-style [+-]hh:mm
    if (len(s) >= 6 and s[-6] in "+-" and s[-3] == ":" and
        s[-5:-3].isdigit() and s[-2:].isdigit()):
        sign = (s[-6] == "+") and 1 or -1
        hours = int(s[-5:-3])
        minutes = int(s[-2:])
        return -sign * (hours * 60 + minutes) * 60, s[:-6]

    return None, s

def strdate(string, format, defaults=None):
    """parse a localized time string and return a (unixtime, offset) tuple.
    if the string cannot be parsed, ValueError is raised."""
    if defaults is None:
        defaults = {}

    # NOTE: unixtime = localunixtime + offset
    offset, date = parsetimezone(string)

    # add missing elements from defaults
    usenow = False # default to using biased defaults
    for part in ("S", "M", "HI", "d", "mb", "yY"): # decreasing specificity
        part = pycompat.bytestr(part)
        found = [True for p in part if ("%"+p) in format]
        if not found:
            date += "@" + defaults[part][usenow]
            format += "@%" + part[0]
        else:
            # We've found a specific time element, less specific time
            # elements are relative to today
            usenow = True

    timetuple = time.strptime(encoding.strfromlocal(date),
                              encoding.strfromlocal(format))
    localunixtime = int(calendar.timegm(timetuple))
    if offset is None:
        # local timezone
        unixtime = int(time.mktime(timetuple))
        offset = unixtime - localunixtime
    else:
        unixtime = localunixtime + offset
    return unixtime, offset

def parsedate(date, formats=None, bias=None):
    """parse a localized date/time and return a (unixtime, offset) tuple.

    The date may be a "unixtime offset" string or in one of the specified
    formats. If the date already is a (unixtime, offset) tuple, it is returned.

    >>> parsedate(b' today ') == parsedate(
    ...     datetime.date.today().strftime('%b %d').encode('ascii'))
    True
    >>> parsedate(b'yesterday ') == parsedate(
    ...     (datetime.date.today() - datetime.timedelta(days=1)
    ...      ).strftime('%b %d').encode('ascii'))
    True
    >>> now, tz = makedate()
    >>> strnow, strtz = parsedate(b'now')
    >>> (strnow - now) < 1
    True
    >>> tz == strtz
    True
    """
    if bias is None:
        bias = {}
    if not date:
        return 0, 0
    if isinstance(date, tuple) and len(date) == 2:
        return date
    if not formats:
        formats = defaultdateformats
    date = date.strip()

    if date == 'now' or date == _('now'):
        return makedate()
    if date == 'today' or date == _('today'):
        date = datetime.date.today().strftime(r'%b %d')
        date = encoding.strtolocal(date)
    elif date == 'yesterday' or date == _('yesterday'):
        date = (datetime.date.today() -
                datetime.timedelta(days=1)).strftime(r'%b %d')
        date = encoding.strtolocal(date)

    try:
        when, offset = map(int, date.split(' '))
    except ValueError:
        # fill out defaults
        now = makedate()
        defaults = {}
        for part in ("d", "mb", "yY", "HI", "M", "S"):
            # this piece is for rounding the specific end of unknowns
            b = bias.get(part)
            if b is None:
                if part[0:1] in "HMS":
                    b = "00"
                else:
                    b = "0"

            # this piece is for matching the generic end to today's date
            n = datestr(now, "%" + part[0:1])

            defaults[part] = (b, n)

        for format in formats:
            try:
                when, offset = strdate(date, format, defaults)
            except (ValueError, OverflowError):
                pass
            else:
                break
        else:
            raise error.ParseError(
                _('invalid date: %r') % pycompat.bytestr(date))
    # validate explicit (probably user-specified) date and
    # time zone offset. values must fit in signed 32 bits for
    # current 32-bit linux runtimes. timezones go from UTC-12
    # to UTC+14
    if when < -0x80000000 or when > 0x7fffffff:
        raise error.ParseError(_('date exceeds 32 bits: %d') % when)
    if offset < -50400 or offset > 43200:
        raise error.ParseError(_('impossible time zone offset: %d') % offset)
    return when, offset

def matchdate(date):
    """Return a function that matches a given date match specifier

    Formats include:

    '{date}' match a given date to the accuracy provided

    '<{date}' on or before a given date

    '>{date}' on or after a given date

    >>> p1 = parsedate(b"10:29:59")
    >>> p2 = parsedate(b"10:30:00")
    >>> p3 = parsedate(b"10:30:59")
    >>> p4 = parsedate(b"10:31:00")
    >>> p5 = parsedate(b"Sep 15 10:30:00 1999")
    >>> f = matchdate(b"10:30")
    >>> f(p1[0])
    False
    >>> f(p2[0])
    True
    >>> f(p3[0])
    True
    >>> f(p4[0])
    False
    >>> f(p5[0])
    False
    """

    def lower(date):
        d = {'mb': "1", 'd': "1"}
        return parsedate(date, extendeddateformats, d)[0]

    def upper(date):
        d = {'mb': "12", 'HI': "23", 'M': "59", 'S': "59"}
        for days in ("31", "30", "29"):
            try:
                d["d"] = days
                return parsedate(date, extendeddateformats, d)[0]
            except error.ParseError:
                pass
        d["d"] = "28"
        return parsedate(date, extendeddateformats, d)[0]

    date = date.strip()

    if not date:
        raise error.Abort(_("dates cannot consist entirely of whitespace"))
    elif date[0:1] == b"<":
        if not date[1:]:
            raise error.Abort(_("invalid day spec, use '<DATE'"))
        when = upper(date[1:])
        return lambda x: x <= when
    elif date[0:1] == b">":
        if not date[1:]:
            raise error.Abort(_("invalid day spec, use '>DATE'"))
        when = lower(date[1:])
        return lambda x: x >= when
    elif date[0:1] == b"-":
        try:
            days = int(date[1:])
        except ValueError:
            raise error.Abort(_("invalid day spec: %s") % date[1:])
        if days < 0:
            raise error.Abort(_("%s must be nonnegative (see 'hg help dates')")
                % date[1:])
        when = makedate()[0] - days * 3600 * 24
        return lambda x: x >= when
    elif b" to " in date:
        a, b = date.split(b" to ")
        start, stop = lower(a), upper(b)
        return lambda x: x >= start and x <= stop
    else:
        start, stop = lower(date), upper(date)
        return lambda x: x >= start and x <= stop
