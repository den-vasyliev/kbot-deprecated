# progress.py progress bars related code
#
# Copyright (C) 2010 Augie Fackler <durin42@gmail.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import errno
import threading
import time

from .i18n import _
from . import encoding

def spacejoin(*args):
    return ' '.join(s for s in args if s)

def shouldprint(ui):
    return not (ui.quiet or ui.plain('progress')) and (
        ui._isatty(ui.ferr) or ui.configbool('progress', 'assume-tty'))

def fmtremaining(seconds):
    """format a number of remaining seconds in human readable way

    This will properly display seconds, minutes, hours, days if needed"""
    if seconds < 60:
        # i18n: format XX seconds as "XXs"
        return _("%02ds") % (seconds)
    minutes = seconds // 60
    if minutes < 60:
        seconds -= minutes * 60
        # i18n: format X minutes and YY seconds as "XmYYs"
        return _("%dm%02ds") % (minutes, seconds)
    # we're going to ignore seconds in this case
    minutes += 1
    hours = minutes // 60
    minutes -= hours * 60
    if hours < 30:
        # i18n: format X hours and YY minutes as "XhYYm"
        return _("%dh%02dm") % (hours, minutes)
    # we're going to ignore minutes in this case
    hours += 1
    days = hours // 24
    hours -= days * 24
    if days < 15:
        # i18n: format X days and YY hours as "XdYYh"
        return _("%dd%02dh") % (days, hours)
    # we're going to ignore hours in this case
    days += 1
    weeks = days // 7
    days -= weeks * 7
    if weeks < 55:
        # i18n: format X weeks and YY days as "XwYYd"
        return _("%dw%02dd") % (weeks, days)
    # we're going to ignore days and treat a year as 52 weeks
    weeks += 1
    years = weeks // 52
    weeks -= years * 52
    # i18n: format X years and YY weeks as "XyYYw"
    return _("%dy%02dw") % (years, weeks)

# file_write() and file_flush() of Python 2 do not restart on EINTR if
# the file is attached to a "slow" device (e.g. a terminal) and raise
# IOError. We cannot know how many bytes would be written by file_write(),
# but a progress text is known to be short enough to be written by a
# single write() syscall, so we can just retry file_write() with the whole
# text. (issue5532)
#
# This should be a short-term workaround. We'll need to fix every occurrence
# of write() to a terminal or pipe.
def _eintrretry(func, *args):
    while True:
        try:
            return func(*args)
        except IOError as err:
            if err.errno == errno.EINTR:
                continue
            raise

class progbar(object):
    def __init__(self, ui):
        self.ui = ui
        self._refreshlock = threading.Lock()
        self.resetstate()

    def resetstate(self):
        self.topics = []
        self.topicstates = {}
        self.starttimes = {}
        self.startvals = {}
        self.printed = False
        self.lastprint = time.time() + float(self.ui.config(
            'progress', 'delay'))
        self.curtopic = None
        self.lasttopic = None
        self.indetcount = 0
        self.refresh = float(self.ui.config(
            'progress', 'refresh'))
        self.changedelay = max(3 * self.refresh,
                               float(self.ui.config(
                                   'progress', 'changedelay')))
        self.order = self.ui.configlist('progress', 'format')
        self.estimateinterval = self.ui.configwith(
            float, 'progress', 'estimateinterval')

    def show(self, now, topic, pos, item, unit, total):
        if not shouldprint(self.ui):
            return
        termwidth = self.width()
        self.printed = True
        head = ''
        needprogress = False
        tail = ''
        for indicator in self.order:
            add = ''
            if indicator == 'topic':
                add = topic
            elif indicator == 'number':
                if total:
                    add = b'%*d/%d' % (len(str(total)), pos, total)
                else:
                    add = b'%d' % pos
            elif indicator.startswith('item') and item:
                slice = 'end'
                if '-' in indicator:
                    wid = int(indicator.split('-')[1])
                elif '+' in indicator:
                    slice = 'beginning'
                    wid = int(indicator.split('+')[1])
                else:
                    wid = 20
                if slice == 'end':
                    add = encoding.trim(item, wid, leftside=True)
                else:
                    add = encoding.trim(item, wid)
                add += (wid - encoding.colwidth(add)) * ' '
            elif indicator == 'bar':
                add = ''
                needprogress = True
            elif indicator == 'unit' and unit:
                add = unit
            elif indicator == 'estimate':
                add = self.estimate(topic, pos, total, now)
            elif indicator == 'speed':
                add = self.speed(topic, pos, unit, now)
            if not needprogress:
                head = spacejoin(head, add)
            else:
                tail = spacejoin(tail, add)
        if needprogress:
            used = 0
            if head:
                used += encoding.colwidth(head) + 1
            if tail:
                used += encoding.colwidth(tail) + 1
            progwidth = termwidth - used - 3
            if total and pos <= total:
                amt = pos * progwidth // total
                bar = '=' * (amt - 1)
                if amt > 0:
                    bar += '>'
                bar += ' ' * (progwidth - amt)
            else:
                progwidth -= 3
                self.indetcount += 1
                # mod the count by twice the width so we can make the
                # cursor bounce between the right and left sides
                amt = self.indetcount % (2 * progwidth)
                amt -= progwidth
                bar = (' ' * int(progwidth - abs(amt)) + '<=>' +
                       ' ' * int(abs(amt)))
            prog = ''.join(('[', bar, ']'))
            out = spacejoin(head, prog, tail)
        else:
            out = spacejoin(head, tail)
        self._writeerr('\r' + encoding.trim(out, termwidth))
        self.lasttopic = topic
        self._flusherr()

    def clear(self):
        if not self.printed or not self.lastprint or not shouldprint(self.ui):
            return
        self._writeerr('\r%s\r' % (' ' * self.width()))
        if self.printed:
            # force immediate re-paint of progress bar
            self.lastprint = 0

    def complete(self):
        if not shouldprint(self.ui):
            return
        if self.ui.configbool('progress', 'clear-complete'):
            self.clear()
        else:
            self._writeerr('\n')
        self._flusherr()

    def _flusherr(self):
        _eintrretry(self.ui.ferr.flush)

    def _writeerr(self, msg):
        _eintrretry(self.ui.ferr.write, msg)

    def width(self):
        tw = self.ui.termwidth()
        return min(int(self.ui.config('progress', 'width', default=tw)), tw)

    def estimate(self, topic, pos, total, now):
        if total is None:
            return ''
        initialpos = self.startvals[topic]
        target = total - initialpos
        delta = pos - initialpos
        if delta > 0:
            elapsed = now - self.starttimes[topic]
            seconds = (elapsed * (target - delta)) // delta + 1
            return fmtremaining(seconds)
        return ''

    def speed(self, topic, pos, unit, now):
        initialpos = self.startvals[topic]
        delta = pos - initialpos
        elapsed = now - self.starttimes[topic]
        if elapsed > 0:
            return _('%d %s/sec') % (delta / elapsed, unit)
        return ''

    def _oktoprint(self, now):
        '''Check if conditions are met to print - e.g. changedelay elapsed'''
        if (self.lasttopic is None # first time we printed
            # not a topic change
            or self.curtopic == self.lasttopic
            # it's been long enough we should print anyway
            or now - self.lastprint >= self.changedelay):
            return True
        else:
            return False

    def _calibrateestimate(self, topic, now, pos):
        '''Adjust starttimes and startvals for topic so ETA works better

        If progress is non-linear (ex. get much slower in the last minute),
        it's more friendly to only use a recent time span for ETA and speed
        calculation.

            [======================================>       ]
                                             ^^^^^^^
                           estimateinterval, only use this for estimation
        '''
        interval = self.estimateinterval
        if interval <= 0:
            return
        elapsed = now - self.starttimes[topic]
        if elapsed > interval:
            delta = pos - self.startvals[topic]
            newdelta = delta * interval / elapsed
            # If a stall happens temporarily, ETA could change dramatically
            # frequently. This is to avoid such dramatical change and make ETA
            # smoother.
            if newdelta < 0.1:
                return
            self.startvals[topic] = pos - newdelta
            self.starttimes[topic] = now - interval

    def progress(self, topic, pos, item='', unit='', total=None):
        if pos is None:
            self.closetopic(topic)
            return
        now = time.time()
        with self._refreshlock:
            if topic not in self.topics:
                self.starttimes[topic] = now
                self.startvals[topic] = pos
                self.topics.append(topic)
            self.topicstates[topic] = pos, item, unit, total
            self.curtopic = topic
            self._calibrateestimate(topic, now, pos)
            if now - self.lastprint >= self.refresh and self.topics:
                if self._oktoprint(now):
                    self.lastprint = now
                    self.show(now, topic, *self.topicstates[topic])

    def closetopic(self, topic):
        with self._refreshlock:
            self.starttimes.pop(topic, None)
            self.startvals.pop(topic, None)
            self.topicstates.pop(topic, None)
            # reset the progress bar if this is the outermost topic
            if self.topics and self.topics[0] == topic and self.printed:
                self.complete()
                self.resetstate()
            # truncate the list of topics assuming all topics within
            # this one are also closed
            if topic in self.topics:
                self.topics = self.topics[:self.topics.index(topic)]
                # reset the last topic to the one we just unwound to,
                # so that higher-level topics will be stickier than
                # lower-level topics
                if self.topics:
                    self.lasttopic = self.topics[-1]
                else:
                    self.lasttopic = None
