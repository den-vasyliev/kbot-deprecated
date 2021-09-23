# match.py - filename matching
#
#  Copyright 2008, 2009 Matt Mackall <mpm@selenic.com> and others
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import, print_function

import copy
import itertools
import os
import re

from .i18n import _
from . import (
    encoding,
    error,
    pathutil,
    pycompat,
    util,
)
from .utils import (
    stringutil,
)

allpatternkinds = ('re', 'glob', 'path', 'relglob', 'relpath', 'relre',
                   'listfile', 'listfile0', 'set', 'include', 'subinclude',
                   'rootfilesin')
cwdrelativepatternkinds = ('relpath', 'glob')

propertycache = util.propertycache

def _rematcher(regex):
    '''compile the regexp with the best available regexp engine and return a
    matcher function'''
    m = util.re.compile(regex)
    try:
        # slightly faster, provided by facebook's re2 bindings
        return m.test_match
    except AttributeError:
        return m.match

def _expandsets(root, cwd, kindpats, ctx, listsubrepos, badfn):
    '''Returns the kindpats list with the 'set' patterns expanded to matchers'''
    matchers = []
    other = []

    for kind, pat, source in kindpats:
        if kind == 'set':
            if ctx is None:
                raise error.ProgrammingError("fileset expression with no "
                                             "context")
            matchers.append(ctx.matchfileset(pat, badfn=badfn))

            if listsubrepos:
                for subpath in ctx.substate:
                    sm = ctx.sub(subpath).matchfileset(pat, badfn=badfn)
                    pm = prefixdirmatcher(root, cwd, subpath, sm, badfn=badfn)
                    matchers.append(pm)

            continue
        other.append((kind, pat, source))
    return matchers, other

def _expandsubinclude(kindpats, root):
    '''Returns the list of subinclude matcher args and the kindpats without the
    subincludes in it.'''
    relmatchers = []
    other = []

    for kind, pat, source in kindpats:
        if kind == 'subinclude':
            sourceroot = pathutil.dirname(util.normpath(source))
            pat = util.pconvert(pat)
            path = pathutil.join(sourceroot, pat)

            newroot = pathutil.dirname(path)
            matcherargs = (newroot, '', [], ['include:%s' % path])

            prefix = pathutil.canonpath(root, root, newroot)
            if prefix:
                prefix += '/'
            relmatchers.append((prefix, matcherargs))
        else:
            other.append((kind, pat, source))

    return relmatchers, other

def _kindpatsalwaysmatch(kindpats):
    """"Checks whether the kindspats match everything, as e.g.
    'relpath:.' does.
    """
    for kind, pat, source in kindpats:
        if pat != '' or kind not in ['relpath', 'glob']:
            return False
    return True

def _buildkindpatsmatcher(matchercls, root, cwd, kindpats, ctx=None,
                          listsubrepos=False, badfn=None):
    matchers = []
    fms, kindpats = _expandsets(root, cwd, kindpats, ctx=ctx,
                                listsubrepos=listsubrepos, badfn=badfn)
    if kindpats:
        m = matchercls(root, cwd, kindpats, listsubrepos=listsubrepos,
                       badfn=badfn)
        matchers.append(m)
    if fms:
        matchers.extend(fms)
    if not matchers:
        return nevermatcher(root, cwd, badfn=badfn)
    if len(matchers) == 1:
        return matchers[0]
    return unionmatcher(matchers)

def match(root, cwd, patterns=None, include=None, exclude=None, default='glob',
          exact=False, auditor=None, ctx=None, listsubrepos=False, warn=None,
          badfn=None, icasefs=False):
    """build an object to match a set of file patterns

    arguments:
    root - the canonical root of the tree you're matching against
    cwd - the current working directory, if relevant
    patterns - patterns to find
    include - patterns to include (unless they are excluded)
    exclude - patterns to exclude (even if they are included)
    default - if a pattern in patterns has no explicit type, assume this one
    exact - patterns are actually filenames (include/exclude still apply)
    warn - optional function used for printing warnings
    badfn - optional bad() callback for this matcher instead of the default
    icasefs - make a matcher for wdir on case insensitive filesystems, which
        normalizes the given patterns to the case in the filesystem

    a pattern is one of:
    'glob:<glob>' - a glob relative to cwd
    're:<regexp>' - a regular expression
    'path:<path>' - a path relative to repository root, which is matched
                    recursively
    'rootfilesin:<path>' - a path relative to repository root, which is
                    matched non-recursively (will not match subdirectories)
    'relglob:<glob>' - an unrooted glob (*.c matches C files in all dirs)
    'relpath:<path>' - a path relative to cwd
    'relre:<regexp>' - a regexp that needn't match the start of a name
    'set:<fileset>' - a fileset expression
    'include:<path>' - a file of patterns to read and include
    'subinclude:<path>' - a file of patterns to match against files under
                          the same directory
    '<something>' - a pattern of the specified default type
    """
    normalize = _donormalize
    if icasefs:
        if exact:
            raise error.ProgrammingError("a case-insensitive exact matcher "
                                         "doesn't make sense")
        dirstate = ctx.repo().dirstate
        dsnormalize = dirstate.normalize

        def normalize(patterns, default, root, cwd, auditor, warn):
            kp = _donormalize(patterns, default, root, cwd, auditor, warn)
            kindpats = []
            for kind, pats, source in kp:
                if kind not in ('re', 'relre'):  # regex can't be normalized
                    p = pats
                    pats = dsnormalize(pats)

                    # Preserve the original to handle a case only rename.
                    if p != pats and p in dirstate:
                        kindpats.append((kind, p, source))

                kindpats.append((kind, pats, source))
            return kindpats

    if exact:
        m = exactmatcher(root, cwd, patterns, badfn)
    elif patterns:
        kindpats = normalize(patterns, default, root, cwd, auditor, warn)
        if _kindpatsalwaysmatch(kindpats):
            m = alwaysmatcher(root, cwd, badfn, relativeuipath=True)
        else:
            m = _buildkindpatsmatcher(patternmatcher, root, cwd, kindpats,
                                      ctx=ctx, listsubrepos=listsubrepos,
                                      badfn=badfn)
    else:
        # It's a little strange that no patterns means to match everything.
        # Consider changing this to match nothing (probably using nevermatcher).
        m = alwaysmatcher(root, cwd, badfn)

    if include:
        kindpats = normalize(include, 'glob', root, cwd, auditor, warn)
        im = _buildkindpatsmatcher(includematcher, root, cwd, kindpats, ctx=ctx,
                                   listsubrepos=listsubrepos, badfn=None)
        m = intersectmatchers(m, im)
    if exclude:
        kindpats = normalize(exclude, 'glob', root, cwd, auditor, warn)
        em = _buildkindpatsmatcher(includematcher, root, cwd, kindpats, ctx=ctx,
                                   listsubrepos=listsubrepos, badfn=None)
        m = differencematcher(m, em)
    return m

def exact(root, cwd, files, badfn=None):
    return exactmatcher(root, cwd, files, badfn=badfn)

def always(root, cwd):
    return alwaysmatcher(root, cwd)

def never(root, cwd):
    return nevermatcher(root, cwd)

def badmatch(match, badfn):
    """Make a copy of the given matcher, replacing its bad method with the given
    one.
    """
    m = copy.copy(match)
    m.bad = badfn
    return m

def _donormalize(patterns, default, root, cwd, auditor, warn):
    '''Convert 'kind:pat' from the patterns list to tuples with kind and
    normalized and rooted patterns and with listfiles expanded.'''
    kindpats = []
    for kind, pat in [_patsplit(p, default) for p in patterns]:
        if kind in cwdrelativepatternkinds:
            pat = pathutil.canonpath(root, cwd, pat, auditor)
        elif kind in ('relglob', 'path', 'rootfilesin'):
            pat = util.normpath(pat)
        elif kind in ('listfile', 'listfile0'):
            try:
                files = util.readfile(pat)
                if kind == 'listfile0':
                    files = files.split('\0')
                else:
                    files = files.splitlines()
                files = [f for f in files if f]
            except EnvironmentError:
                raise error.Abort(_("unable to read file list (%s)") % pat)
            for k, p, source in _donormalize(files, default, root, cwd,
                                             auditor, warn):
                kindpats.append((k, p, pat))
            continue
        elif kind == 'include':
            try:
                fullpath = os.path.join(root, util.localpath(pat))
                includepats = readpatternfile(fullpath, warn)
                for k, p, source in _donormalize(includepats, default,
                                                 root, cwd, auditor, warn):
                    kindpats.append((k, p, source or pat))
            except error.Abort as inst:
                raise error.Abort('%s: %s' % (pat, inst[0]))
            except IOError as inst:
                if warn:
                    warn(_("skipping unreadable pattern file '%s': %s\n") %
                         (pat, stringutil.forcebytestr(inst.strerror)))
            continue
        # else: re or relre - which cannot be normalized
        kindpats.append((kind, pat, ''))
    return kindpats

class basematcher(object):

    def __init__(self, root, cwd, badfn=None, relativeuipath=True):
        self._root = root
        self._cwd = cwd
        if badfn is not None:
            self.bad = badfn
        self._relativeuipath = relativeuipath

    def __call__(self, fn):
        return self.matchfn(fn)
    def __iter__(self):
        for f in self._files:
            yield f
    # Callbacks related to how the matcher is used by dirstate.walk.
    # Subscribers to these events must monkeypatch the matcher object.
    def bad(self, f, msg):
        '''Callback from dirstate.walk for each explicit file that can't be
        found/accessed, with an error message.'''

    # If an explicitdir is set, it will be called when an explicitly listed
    # directory is visited.
    explicitdir = None

    # If an traversedir is set, it will be called when a directory discovered
    # by recursive traversal is visited.
    traversedir = None

    def abs(self, f):
        '''Convert a repo path back to path that is relative to the root of the
        matcher.'''
        return f

    def rel(self, f):
        '''Convert repo path back to path that is relative to cwd of matcher.'''
        return util.pathto(self._root, self._cwd, f)

    def uipath(self, f):
        '''Convert repo path to a display path.  If patterns or -I/-X were used
        to create this matcher, the display path will be relative to cwd.
        Otherwise it is relative to the root of the repo.'''
        return (self._relativeuipath and self.rel(f)) or self.abs(f)

    @propertycache
    def _files(self):
        return []

    def files(self):
        '''Explicitly listed files or patterns or roots:
        if no patterns or .always(): empty list,
        if exact: list exact files,
        if not .anypats(): list all files and dirs,
        else: optimal roots'''
        return self._files

    @propertycache
    def _fileset(self):
        return set(self._files)

    def exact(self, f):
        '''Returns True if f is in .files().'''
        return f in self._fileset

    def matchfn(self, f):
        return False

    def visitdir(self, dir):
        '''Decides whether a directory should be visited based on whether it
        has potential matches in it or one of its subdirectories. This is
        based on the match's primary, included, and excluded patterns.

        Returns the string 'all' if the given directory and all subdirectories
        should be visited. Otherwise returns True or False indicating whether
        the given directory should be visited.
        '''
        return True

    def visitchildrenset(self, dir):
        '''Decides whether a directory should be visited based on whether it
        has potential matches in it or one of its subdirectories, and
        potentially lists which subdirectories of that directory should be
        visited. This is based on the match's primary, included, and excluded
        patterns.

        This function is very similar to 'visitdir', and the following mapping
        can be applied:

             visitdir | visitchildrenlist
            ----------+-------------------
             False    | set()
             'all'    | 'all'
             True     | 'this' OR non-empty set of subdirs -or files- to visit

        Example:
          Assume matchers ['path:foo/bar', 'rootfilesin:qux'], we would return
          the following values (assuming the implementation of visitchildrenset
          is capable of recognizing this; some implementations are not).

          '.' -> {'foo', 'qux'}
          'baz' -> set()
          'foo' -> {'bar'}
          # Ideally this would be 'all', but since the prefix nature of matchers
          # is applied to the entire matcher, we have to downgrade this to
          # 'this' due to the non-prefix 'rootfilesin'-kind matcher being mixed
          # in.
          'foo/bar' -> 'this'
          'qux' -> 'this'

        Important:
          Most matchers do not know if they're representing files or
          directories. They see ['path:dir/f'] and don't know whether 'f' is a
          file or a directory, so visitchildrenset('dir') for most matchers will
          return {'f'}, but if the matcher knows it's a file (like exactmatcher
          does), it may return 'this'. Do not rely on the return being a set
          indicating that there are no files in this dir to investigate (or
          equivalently that if there are files to investigate in 'dir' that it
          will always return 'this').
        '''
        return 'this'

    def always(self):
        '''Matcher will match everything and .files() will be empty --
        optimization might be possible.'''
        return False

    def isexact(self):
        '''Matcher will match exactly the list of files in .files() --
        optimization might be possible.'''
        return False

    def prefix(self):
        '''Matcher will match the paths in .files() recursively --
        optimization might be possible.'''
        return False

    def anypats(self):
        '''None of .always(), .isexact(), and .prefix() is true --
        optimizations will be difficult.'''
        return not self.always() and not self.isexact() and not self.prefix()

class alwaysmatcher(basematcher):
    '''Matches everything.'''

    def __init__(self, root, cwd, badfn=None, relativeuipath=False):
        super(alwaysmatcher, self).__init__(root, cwd, badfn,
                                            relativeuipath=relativeuipath)

    def always(self):
        return True

    def matchfn(self, f):
        return True

    def visitdir(self, dir):
        return 'all'

    def visitchildrenset(self, dir):
        return 'all'

    def __repr__(self):
        return r'<alwaysmatcher>'

class nevermatcher(basematcher):
    '''Matches nothing.'''

    def __init__(self, root, cwd, badfn=None):
        super(nevermatcher, self).__init__(root, cwd, badfn)

    # It's a little weird to say that the nevermatcher is an exact matcher
    # or a prefix matcher, but it seems to make sense to let callers take
    # fast paths based on either. There will be no exact matches, nor any
    # prefixes (files() returns []), so fast paths iterating over them should
    # be efficient (and correct).
    def isexact(self):
        return True

    def prefix(self):
        return True

    def visitdir(self, dir):
        return False

    def visitchildrenset(self, dir):
        return set()

    def __repr__(self):
        return r'<nevermatcher>'

class predicatematcher(basematcher):
    """A matcher adapter for a simple boolean function"""

    def __init__(self, root, cwd, predfn, predrepr=None, badfn=None):
        super(predicatematcher, self).__init__(root, cwd, badfn)
        self.matchfn = predfn
        self._predrepr = predrepr

    @encoding.strmethod
    def __repr__(self):
        s = (stringutil.buildrepr(self._predrepr)
             or pycompat.byterepr(self.matchfn))
        return '<predicatenmatcher pred=%s>' % s

class patternmatcher(basematcher):

    def __init__(self, root, cwd, kindpats, listsubrepos=False, badfn=None):
        super(patternmatcher, self).__init__(root, cwd, badfn)

        self._files = _explicitfiles(kindpats)
        self._prefix = _prefix(kindpats)
        self._pats, self.matchfn = _buildmatch(kindpats, '$', listsubrepos,
                                               root)

    @propertycache
    def _dirs(self):
        return set(util.dirs(self._fileset)) | {'.'}

    def visitdir(self, dir):
        if self._prefix and dir in self._fileset:
            return 'all'
        return ('.' in self._fileset or
                dir in self._fileset or
                dir in self._dirs or
                any(parentdir in self._fileset
                    for parentdir in util.finddirs(dir)))

    def visitchildrenset(self, dir):
        ret = self.visitdir(dir)
        if ret is True:
            return 'this'
        elif not ret:
            return set()
        assert ret == 'all'
        return 'all'

    def prefix(self):
        return self._prefix

    @encoding.strmethod
    def __repr__(self):
        return ('<patternmatcher patterns=%r>' % pycompat.bytestr(self._pats))

# This is basically a reimplementation of util.dirs that stores the children
# instead of just a count of them, plus a small optional optimization to avoid
# some directories we don't need.
class _dirchildren(object):
    def __init__(self, paths, onlyinclude=None):
        self._dirs = {}
        self._onlyinclude = onlyinclude or []
        addpath = self.addpath
        for f in paths:
            addpath(f)

    def addpath(self, path):
        if path == '.':
            return
        dirs = self._dirs
        findsplitdirs = _dirchildren._findsplitdirs
        for d, b in findsplitdirs(path):
            if d not in self._onlyinclude:
                continue
            dirs.setdefault(d, set()).add(b)

    @staticmethod
    def _findsplitdirs(path):
        # yields (dirname, basename) tuples, walking back to the root.  This is
        # very similar to util.finddirs, except:
        #  - produces a (dirname, basename) tuple, not just 'dirname'
        #  - includes root dir
        # Unlike manifest._splittopdir, this does not suffix `dirname` with a
        # slash, and produces '.' for the root instead of ''.
        oldpos = len(path)
        pos = path.rfind('/')
        while pos != -1:
            yield path[:pos], path[pos + 1:oldpos]
            oldpos = pos
            pos = path.rfind('/', 0, pos)
        yield '.', path[:oldpos]

    def get(self, path):
        return self._dirs.get(path, set())

class includematcher(basematcher):

    def __init__(self, root, cwd, kindpats, listsubrepos=False, badfn=None):
        super(includematcher, self).__init__(root, cwd, badfn)

        self._pats, self.matchfn = _buildmatch(kindpats, '(?:/|$)',
                                               listsubrepos, root)
        self._prefix = _prefix(kindpats)
        roots, dirs, parents = _rootsdirsandparents(kindpats)
        # roots are directories which are recursively included.
        self._roots = set(roots)
        # dirs are directories which are non-recursively included.
        self._dirs = set(dirs)
        # parents are directories which are non-recursively included because
        # they are needed to get to items in _dirs or _roots.
        self._parents = set(parents)

    def visitdir(self, dir):
        if self._prefix and dir in self._roots:
            return 'all'
        return ('.' in self._roots or
                dir in self._roots or
                dir in self._dirs or
                dir in self._parents or
                any(parentdir in self._roots
                    for parentdir in util.finddirs(dir)))

    @propertycache
    def _allparentschildren(self):
        # It may seem odd that we add dirs, roots, and parents, and then
        # restrict to only parents. This is to catch the case of:
        #   dirs = ['foo/bar']
        #   parents = ['foo']
        # if we asked for the children of 'foo', but had only added
        # self._parents, we wouldn't be able to respond ['bar'].
        return _dirchildren(
                itertools.chain(self._dirs, self._roots, self._parents),
                onlyinclude=self._parents)

    def visitchildrenset(self, dir):
        if self._prefix and dir in self._roots:
            return 'all'
        # Note: this does *not* include the 'dir in self._parents' case from
        # visitdir, that's handled below.
        if ('.' in self._roots or
            dir in self._roots or
            dir in self._dirs or
            any(parentdir in self._roots
                for parentdir in util.finddirs(dir))):
            return 'this'

        if dir in self._parents:
            return self._allparentschildren.get(dir) or set()
        return set()

    @encoding.strmethod
    def __repr__(self):
        return ('<includematcher includes=%r>' % pycompat.bytestr(self._pats))

class exactmatcher(basematcher):
    '''Matches the input files exactly. They are interpreted as paths, not
    patterns (so no kind-prefixes).
    '''

    def __init__(self, root, cwd, files, badfn=None):
        super(exactmatcher, self).__init__(root, cwd, badfn)

        if isinstance(files, list):
            self._files = files
        else:
            self._files = list(files)

    matchfn = basematcher.exact

    @propertycache
    def _dirs(self):
        return set(util.dirs(self._fileset)) | {'.'}

    def visitdir(self, dir):
        return dir in self._dirs

    def visitchildrenset(self, dir):
        if not self._fileset or dir not in self._dirs:
            return set()

        candidates = self._fileset | self._dirs - {'.'}
        if dir != '.':
            d = dir + '/'
            candidates = set(c[len(d):] for c in candidates if
                             c.startswith(d))
        # self._dirs includes all of the directories, recursively, so if
        # we're attempting to match foo/bar/baz.txt, it'll have '.', 'foo',
        # 'foo/bar' in it. Thus we can safely ignore a candidate that has a
        # '/' in it, indicating a it's for a subdir-of-a-subdir; the
        # immediate subdir will be in there without a slash.
        ret = {c for c in candidates if '/' not in c}
        # We really do not expect ret to be empty, since that would imply that
        # there's something in _dirs that didn't have a file in _fileset.
        assert ret
        return ret

    def isexact(self):
        return True

    @encoding.strmethod
    def __repr__(self):
        return ('<exactmatcher files=%r>' % self._files)

class differencematcher(basematcher):
    '''Composes two matchers by matching if the first matches and the second
    does not.

    The second matcher's non-matching-attributes (root, cwd, bad, explicitdir,
    traversedir) are ignored.
    '''
    def __init__(self, m1, m2):
        super(differencematcher, self).__init__(m1._root, m1._cwd)
        self._m1 = m1
        self._m2 = m2
        self.bad = m1.bad
        self.explicitdir = m1.explicitdir
        self.traversedir = m1.traversedir

    def matchfn(self, f):
        return self._m1(f) and not self._m2(f)

    @propertycache
    def _files(self):
        if self.isexact():
            return [f for f in self._m1.files() if self(f)]
        # If m1 is not an exact matcher, we can't easily figure out the set of
        # files, because its files() are not always files. For example, if
        # m1 is "path:dir" and m2 is "rootfileins:.", we don't
        # want to remove "dir" from the set even though it would match m2,
        # because the "dir" in m1 may not be a file.
        return self._m1.files()

    def visitdir(self, dir):
        if self._m2.visitdir(dir) == 'all':
            return False
        return bool(self._m1.visitdir(dir))

    def visitchildrenset(self, dir):
        m2_set = self._m2.visitchildrenset(dir)
        if m2_set == 'all':
            return set()
        m1_set = self._m1.visitchildrenset(dir)
        # Possible values for m1: 'all', 'this', set(...), set()
        # Possible values for m2:        'this', set(...), set()
        # If m2 has nothing under here that we care about, return m1, even if
        # it's 'all'. This is a change in behavior from visitdir, which would
        # return True, not 'all', for some reason.
        if not m2_set:
            return m1_set
        if m1_set in ['all', 'this']:
            # Never return 'all' here if m2_set is any kind of non-empty (either
            # 'this' or set(foo)), since m2 might return set() for a
            # subdirectory.
            return 'this'
        # Possible values for m1:         set(...), set()
        # Possible values for m2: 'this', set(...)
        # We ignore m2's set results. They're possibly incorrect:
        #  m1 = path:dir/subdir, m2=rootfilesin:dir, visitchildrenset('.'):
        #    m1 returns {'dir'}, m2 returns {'dir'}, if we subtracted we'd
        #    return set(), which is *not* correct, we still need to visit 'dir'!
        return m1_set

    def isexact(self):
        return self._m1.isexact()

    @encoding.strmethod
    def __repr__(self):
        return ('<differencematcher m1=%r, m2=%r>' % (self._m1, self._m2))

def intersectmatchers(m1, m2):
    '''Composes two matchers by matching if both of them match.

    The second matcher's non-matching-attributes (root, cwd, bad, explicitdir,
    traversedir) are ignored.
    '''
    if m1 is None or m2 is None:
        return m1 or m2
    if m1.always():
        m = copy.copy(m2)
        # TODO: Consider encapsulating these things in a class so there's only
        # one thing to copy from m1.
        m.bad = m1.bad
        m.explicitdir = m1.explicitdir
        m.traversedir = m1.traversedir
        m.abs = m1.abs
        m.rel = m1.rel
        m._relativeuipath |= m1._relativeuipath
        return m
    if m2.always():
        m = copy.copy(m1)
        m._relativeuipath |= m2._relativeuipath
        return m
    return intersectionmatcher(m1, m2)

class intersectionmatcher(basematcher):
    def __init__(self, m1, m2):
        super(intersectionmatcher, self).__init__(m1._root, m1._cwd)
        self._m1 = m1
        self._m2 = m2
        self.bad = m1.bad
        self.explicitdir = m1.explicitdir
        self.traversedir = m1.traversedir

    @propertycache
    def _files(self):
        if self.isexact():
            m1, m2 = self._m1, self._m2
            if not m1.isexact():
                m1, m2 = m2, m1
            return [f for f in m1.files() if m2(f)]
        # It neither m1 nor m2 is an exact matcher, we can't easily intersect
        # the set of files, because their files() are not always files. For
        # example, if intersecting a matcher "-I glob:foo.txt" with matcher of
        # "path:dir2", we don't want to remove "dir2" from the set.
        return self._m1.files() + self._m2.files()

    def matchfn(self, f):
        return self._m1(f) and self._m2(f)

    def visitdir(self, dir):
        visit1 = self._m1.visitdir(dir)
        if visit1 == 'all':
            return self._m2.visitdir(dir)
        # bool() because visit1=True + visit2='all' should not be 'all'
        return bool(visit1 and self._m2.visitdir(dir))

    def visitchildrenset(self, dir):
        m1_set = self._m1.visitchildrenset(dir)
        if not m1_set:
            return set()
        m2_set = self._m2.visitchildrenset(dir)
        if not m2_set:
            return set()

        if m1_set == 'all':
            return m2_set
        elif m2_set == 'all':
            return m1_set

        if m1_set == 'this' or m2_set == 'this':
            return 'this'

        assert isinstance(m1_set, set) and isinstance(m2_set, set)
        return m1_set.intersection(m2_set)

    def always(self):
        return self._m1.always() and self._m2.always()

    def isexact(self):
        return self._m1.isexact() or self._m2.isexact()

    @encoding.strmethod
    def __repr__(self):
        return ('<intersectionmatcher m1=%r, m2=%r>' % (self._m1, self._m2))

class subdirmatcher(basematcher):
    """Adapt a matcher to work on a subdirectory only.

    The paths are remapped to remove/insert the path as needed:

    >>> from . import pycompat
    >>> m1 = match(b'root', b'', [b'a.txt', b'sub/b.txt'])
    >>> m2 = subdirmatcher(b'sub', m1)
    >>> bool(m2(b'a.txt'))
    False
    >>> bool(m2(b'b.txt'))
    True
    >>> bool(m2.matchfn(b'a.txt'))
    False
    >>> bool(m2.matchfn(b'b.txt'))
    True
    >>> m2.files()
    ['b.txt']
    >>> m2.exact(b'b.txt')
    True
    >>> util.pconvert(m2.rel(b'b.txt'))
    'sub/b.txt'
    >>> def bad(f, msg):
    ...     print(pycompat.sysstr(b"%s: %s" % (f, msg)))
    >>> m1.bad = bad
    >>> m2.bad(b'x.txt', b'No such file')
    sub/x.txt: No such file
    >>> m2.abs(b'c.txt')
    'sub/c.txt'
    """

    def __init__(self, path, matcher):
        super(subdirmatcher, self).__init__(matcher._root, matcher._cwd)
        self._path = path
        self._matcher = matcher
        self._always = matcher.always()

        self._files = [f[len(path) + 1:] for f in matcher._files
                       if f.startswith(path + "/")]

        # If the parent repo had a path to this subrepo and the matcher is
        # a prefix matcher, this submatcher always matches.
        if matcher.prefix():
            self._always = any(f == path for f in matcher._files)

    def bad(self, f, msg):
        self._matcher.bad(self._path + "/" + f, msg)

    def abs(self, f):
        return self._matcher.abs(self._path + "/" + f)

    def rel(self, f):
        return self._matcher.rel(self._path + "/" + f)

    def uipath(self, f):
        return self._matcher.uipath(self._path + "/" + f)

    def matchfn(self, f):
        # Some information is lost in the superclass's constructor, so we
        # can not accurately create the matching function for the subdirectory
        # from the inputs. Instead, we override matchfn() and visitdir() to
        # call the original matcher with the subdirectory path prepended.
        return self._matcher.matchfn(self._path + "/" + f)

    def visitdir(self, dir):
        if dir == '.':
            dir = self._path
        else:
            dir = self._path + "/" + dir
        return self._matcher.visitdir(dir)

    def visitchildrenset(self, dir):
        if dir == '.':
            dir = self._path
        else:
            dir = self._path + "/" + dir
        return self._matcher.visitchildrenset(dir)

    def always(self):
        return self._always

    def prefix(self):
        return self._matcher.prefix() and not self._always

    @encoding.strmethod
    def __repr__(self):
        return ('<subdirmatcher path=%r, matcher=%r>' %
                (self._path, self._matcher))

class prefixdirmatcher(basematcher):
    """Adapt a matcher to work on a parent directory.

    The matcher's non-matching-attributes (root, cwd, bad, explicitdir,
    traversedir) are ignored.

    The prefix path should usually be the relative path from the root of
    this matcher to the root of the wrapped matcher.

    >>> m1 = match(util.localpath(b'root/d/e'), b'f', [b'../a.txt', b'b.txt'])
    >>> m2 = prefixdirmatcher(b'root', b'd/e/f', b'd/e', m1)
    >>> bool(m2(b'a.txt'),)
    False
    >>> bool(m2(b'd/e/a.txt'))
    True
    >>> bool(m2(b'd/e/b.txt'))
    False
    >>> m2.files()
    ['d/e/a.txt', 'd/e/f/b.txt']
    >>> m2.exact(b'd/e/a.txt')
    True
    >>> m2.visitdir(b'd')
    True
    >>> m2.visitdir(b'd/e')
    True
    >>> m2.visitdir(b'd/e/f')
    True
    >>> m2.visitdir(b'd/e/g')
    False
    >>> m2.visitdir(b'd/ef')
    False
    """

    def __init__(self, root, cwd, path, matcher, badfn=None):
        super(prefixdirmatcher, self).__init__(root, cwd, badfn)
        if not path:
            raise error.ProgrammingError('prefix path must not be empty')
        self._path = path
        self._pathprefix = path + '/'
        self._matcher = matcher

    @propertycache
    def _files(self):
        return [self._pathprefix + f for f in self._matcher._files]

    def matchfn(self, f):
        if not f.startswith(self._pathprefix):
            return False
        return self._matcher.matchfn(f[len(self._pathprefix):])

    @propertycache
    def _pathdirs(self):
        return set(util.finddirs(self._path)) | {'.'}

    def visitdir(self, dir):
        if dir == self._path:
            return self._matcher.visitdir('.')
        if dir.startswith(self._pathprefix):
            return self._matcher.visitdir(dir[len(self._pathprefix):])
        return dir in self._pathdirs

    def visitchildrenset(self, dir):
        if dir == self._path:
            return self._matcher.visitchildrenset('.')
        if dir.startswith(self._pathprefix):
            return self._matcher.visitchildrenset(dir[len(self._pathprefix):])
        if dir in self._pathdirs:
            return 'this'
        return set()

    def isexact(self):
        return self._matcher.isexact()

    def prefix(self):
        return self._matcher.prefix()

    @encoding.strmethod
    def __repr__(self):
        return ('<prefixdirmatcher path=%r, matcher=%r>'
                % (pycompat.bytestr(self._path), self._matcher))

class unionmatcher(basematcher):
    """A matcher that is the union of several matchers.

    The non-matching-attributes (root, cwd, bad, explicitdir, traversedir) are
    taken from the first matcher.
    """

    def __init__(self, matchers):
        m1 = matchers[0]
        super(unionmatcher, self).__init__(m1._root, m1._cwd)
        self.explicitdir = m1.explicitdir
        self.traversedir = m1.traversedir
        self._matchers = matchers

    def matchfn(self, f):
        for match in self._matchers:
            if match(f):
                return True
        return False

    def visitdir(self, dir):
        r = False
        for m in self._matchers:
            v = m.visitdir(dir)
            if v == 'all':
                return v
            r |= v
        return r

    def visitchildrenset(self, dir):
        r = set()
        this = False
        for m in self._matchers:
            v = m.visitchildrenset(dir)
            if not v:
                continue
            if v == 'all':
                return v
            if this or v == 'this':
                this = True
                # don't break, we might have an 'all' in here.
                continue
            assert isinstance(v, set)
            r = r.union(v)
        if this:
            return 'this'
        return r

    @encoding.strmethod
    def __repr__(self):
        return ('<unionmatcher matchers=%r>' % self._matchers)

def patkind(pattern, default=None):
    '''If pattern is 'kind:pat' with a known kind, return kind.'''
    return _patsplit(pattern, default)[0]

def _patsplit(pattern, default):
    """Split a string into the optional pattern kind prefix and the actual
    pattern."""
    if ':' in pattern:
        kind, pat = pattern.split(':', 1)
        if kind in allpatternkinds:
            return kind, pat
    return default, pattern

def _globre(pat):
    r'''Convert an extended glob string to a regexp string.

    >>> from . import pycompat
    >>> def bprint(s):
    ...     print(pycompat.sysstr(s))
    >>> bprint(_globre(br'?'))
    .
    >>> bprint(_globre(br'*'))
    [^/]*
    >>> bprint(_globre(br'**'))
    .*
    >>> bprint(_globre(br'**/a'))
    (?:.*/)?a
    >>> bprint(_globre(br'a/**/b'))
    a/(?:.*/)?b
    >>> bprint(_globre(br'[a*?!^][^b][!c]'))
    [a*?!^][\^b][^c]
    >>> bprint(_globre(br'{a,b}'))
    (?:a|b)
    >>> bprint(_globre(br'.\*\?'))
    \.\*\?
    '''
    i, n = 0, len(pat)
    res = ''
    group = 0
    escape = util.stringutil.reescape
    def peek():
        return i < n and pat[i:i + 1]
    while i < n:
        c = pat[i:i + 1]
        i += 1
        if c not in '*?[{},\\':
            res += escape(c)
        elif c == '*':
            if peek() == '*':
                i += 1
                if peek() == '/':
                    i += 1
                    res += '(?:.*/)?'
                else:
                    res += '.*'
            else:
                res += '[^/]*'
        elif c == '?':
            res += '.'
        elif c == '[':
            j = i
            if j < n and pat[j:j + 1] in '!]':
                j += 1
            while j < n and pat[j:j + 1] != ']':
                j += 1
            if j >= n:
                res += '\\['
            else:
                stuff = pat[i:j].replace('\\','\\\\')
                i = j + 1
                if stuff[0:1] == '!':
                    stuff = '^' + stuff[1:]
                elif stuff[0:1] == '^':
                    stuff = '\\' + stuff
                res = '%s[%s]' % (res, stuff)
        elif c == '{':
            group += 1
            res += '(?:'
        elif c == '}' and group:
            res += ')'
            group -= 1
        elif c == ',' and group:
            res += '|'
        elif c == '\\':
            p = peek()
            if p:
                i += 1
                res += escape(p)
            else:
                res += escape(c)
        else:
            res += escape(c)
    return res

def _regex(kind, pat, globsuffix):
    '''Convert a (normalized) pattern of any kind into a regular expression.
    globsuffix is appended to the regexp of globs.'''
    if not pat:
        return ''
    if kind == 're':
        return pat
    if kind in ('path', 'relpath'):
        if pat == '.':
            return ''
        return util.stringutil.reescape(pat) + '(?:/|$)'
    if kind == 'rootfilesin':
        if pat == '.':
            escaped = ''
        else:
            # Pattern is a directory name.
            escaped = util.stringutil.reescape(pat) + '/'
        # Anything after the pattern must be a non-directory.
        return escaped + '[^/]+$'
    if kind == 'relglob':
        return '(?:|.*/)' + _globre(pat) + globsuffix
    if kind == 'relre':
        if pat.startswith('^'):
            return pat
        return '.*' + pat
    if kind == 'glob':
        return _globre(pat) + globsuffix
    raise error.ProgrammingError('not a regex pattern: %s:%s' % (kind, pat))

def _buildmatch(kindpats, globsuffix, listsubrepos, root):
    '''Return regexp string and a matcher function for kindpats.
    globsuffix is appended to the regexp of globs.'''
    matchfuncs = []

    subincludes, kindpats = _expandsubinclude(kindpats, root)
    if subincludes:
        submatchers = {}
        def matchsubinclude(f):
            for prefix, matcherargs in subincludes:
                if f.startswith(prefix):
                    mf = submatchers.get(prefix)
                    if mf is None:
                        mf = match(*matcherargs)
                        submatchers[prefix] = mf

                    if mf(f[len(prefix):]):
                        return True
            return False
        matchfuncs.append(matchsubinclude)

    regex = ''
    if kindpats:
        if all(k == 'rootfilesin' for k, p, s in kindpats):
            dirs = {p for k, p, s in kindpats}
            def mf(f):
                i = f.rfind('/')
                if i >= 0:
                    dir = f[:i]
                else:
                    dir = '.'
                return dir in dirs
            regex = b'rootfilesin: %s' % stringutil.pprint(list(sorted(dirs)))
            matchfuncs.append(mf)
        else:
            regex, mf = _buildregexmatch(kindpats, globsuffix)
            matchfuncs.append(mf)

    if len(matchfuncs) == 1:
        return regex, matchfuncs[0]
    else:
        return regex, lambda f: any(mf(f) for mf in matchfuncs)

def _buildregexmatch(kindpats, globsuffix):
    """Build a match function from a list of kinds and kindpats,
    return regexp string and a matcher function."""
    try:
        regex = '(?:%s)' % '|'.join([_regex(k, p, globsuffix)
                                     for (k, p, s) in kindpats])
        if len(regex) > 20000:
            raise OverflowError
        return regex, _rematcher(regex)
    except OverflowError:
        # We're using a Python with a tiny regex engine and we
        # made it explode, so we'll divide the pattern list in two
        # until it works
        l = len(kindpats)
        if l < 2:
            raise
        regexa, a = _buildregexmatch(kindpats[:l//2], globsuffix)
        regexb, b = _buildregexmatch(kindpats[l//2:], globsuffix)
        return regex, lambda s: a(s) or b(s)
    except re.error:
        for k, p, s in kindpats:
            try:
                _rematcher('(?:%s)' % _regex(k, p, globsuffix))
            except re.error:
                if s:
                    raise error.Abort(_("%s: invalid pattern (%s): %s") %
                                     (s, k, p))
                else:
                    raise error.Abort(_("invalid pattern (%s): %s") % (k, p))
        raise error.Abort(_("invalid pattern"))

def _patternrootsanddirs(kindpats):
    '''Returns roots and directories corresponding to each pattern.

    This calculates the roots and directories exactly matching the patterns and
    returns a tuple of (roots, dirs) for each. It does not return other
    directories which may also need to be considered, like the parent
    directories.
    '''
    r = []
    d = []
    for kind, pat, source in kindpats:
        if kind == 'glob': # find the non-glob prefix
            root = []
            for p in pat.split('/'):
                if '[' in p or '{' in p or '*' in p or '?' in p:
                    break
                root.append(p)
            r.append('/'.join(root) or '.')
        elif kind in ('relpath', 'path'):
            r.append(pat or '.')
        elif kind in ('rootfilesin',):
            d.append(pat or '.')
        else: # relglob, re, relre
            r.append('.')
    return r, d

def _roots(kindpats):
    '''Returns root directories to match recursively from the given patterns.'''
    roots, dirs = _patternrootsanddirs(kindpats)
    return roots

def _rootsdirsandparents(kindpats):
    '''Returns roots and exact directories from patterns.

    `roots` are directories to match recursively, `dirs` should
    be matched non-recursively, and `parents` are the implicitly required
    directories to walk to items in either roots or dirs.

    Returns a tuple of (roots, dirs, parents).

    >>> _rootsdirsandparents(
    ...     [(b'glob', b'g/h/*', b''), (b'glob', b'g/h', b''),
    ...      (b'glob', b'g*', b'')])
    (['g/h', 'g/h', '.'], [], ['g', '.'])
    >>> _rootsdirsandparents(
    ...     [(b'rootfilesin', b'g/h', b''), (b'rootfilesin', b'', b'')])
    ([], ['g/h', '.'], ['g', '.'])
    >>> _rootsdirsandparents(
    ...     [(b'relpath', b'r', b''), (b'path', b'p/p', b''),
    ...      (b'path', b'', b'')])
    (['r', 'p/p', '.'], [], ['p', '.'])
    >>> _rootsdirsandparents(
    ...     [(b'relglob', b'rg*', b''), (b're', b're/', b''),
    ...      (b'relre', b'rr', b'')])
    (['.', '.', '.'], [], ['.'])
    '''
    r, d = _patternrootsanddirs(kindpats)

    p = []
    # Append the parents as non-recursive/exact directories, since they must be
    # scanned to get to either the roots or the other exact directories.
    p.extend(util.dirs(d))
    p.extend(util.dirs(r))
    # util.dirs() does not include the root directory, so add it manually
    p.append('.')

    # FIXME: all uses of this function convert these to sets, do so before
    # returning.
    # FIXME: all uses of this function do not need anything in 'roots' and
    # 'dirs' to also be in 'parents', consider removing them before returning.
    return r, d, p

def _explicitfiles(kindpats):
    '''Returns the potential explicit filenames from the patterns.

    >>> _explicitfiles([(b'path', b'foo/bar', b'')])
    ['foo/bar']
    >>> _explicitfiles([(b'rootfilesin', b'foo/bar', b'')])
    []
    '''
    # Keep only the pattern kinds where one can specify filenames (vs only
    # directory names).
    filable = [kp for kp in kindpats if kp[0] not in ('rootfilesin',)]
    return _roots(filable)

def _prefix(kindpats):
    '''Whether all the patterns match a prefix (i.e. recursively)'''
    for kind, pat, source in kindpats:
        if kind not in ('path', 'relpath'):
            return False
    return True

_commentre = None

def readpatternfile(filepath, warn, sourceinfo=False):
    '''parse a pattern file, returning a list of
    patterns. These patterns should be given to compile()
    to be validated and converted into a match function.

    trailing white space is dropped.
    the escape character is backslash.
    comments start with #.
    empty lines are skipped.

    lines can be of the following formats:

    syntax: regexp # defaults following lines to non-rooted regexps
    syntax: glob   # defaults following lines to non-rooted globs
    re:pattern     # non-rooted regular expression
    glob:pattern   # non-rooted glob
    pattern        # pattern of the current default type

    if sourceinfo is set, returns a list of tuples:
    (pattern, lineno, originalline). This is useful to debug ignore patterns.
    '''

    syntaxes = {'re': 'relre:', 'regexp': 'relre:', 'glob': 'relglob:',
                'include': 'include', 'subinclude': 'subinclude'}
    syntax = 'relre:'
    patterns = []

    fp = open(filepath, 'rb')
    for lineno, line in enumerate(util.iterfile(fp), start=1):
        if "#" in line:
            global _commentre
            if not _commentre:
                _commentre = util.re.compile(br'((?:^|[^\\])(?:\\\\)*)#.*')
            # remove comments prefixed by an even number of escapes
            m = _commentre.search(line)
            if m:
                line = line[:m.end(1)]
            # fixup properly escaped comments that survived the above
            line = line.replace("\\#", "#")
        line = line.rstrip()
        if not line:
            continue

        if line.startswith('syntax:'):
            s = line[7:].strip()
            try:
                syntax = syntaxes[s]
            except KeyError:
                if warn:
                    warn(_("%s: ignoring invalid syntax '%s'\n") %
                         (filepath, s))
            continue

        linesyntax = syntax
        for s, rels in syntaxes.iteritems():
            if line.startswith(rels):
                linesyntax = rels
                line = line[len(rels):]
                break
            elif line.startswith(s+':'):
                linesyntax = rels
                line = line[len(s) + 1:]
                break
        if sourceinfo:
            patterns.append((linesyntax + line, lineno, line))
        else:
            patterns.append(linesyntax + line)
    fp.close()
    return patterns
