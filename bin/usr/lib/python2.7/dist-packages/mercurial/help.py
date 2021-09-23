# help.py - help data for mercurial
#
# Copyright 2006 Matt Mackall <mpm@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import absolute_import

import itertools
import os
import re
import textwrap

from .i18n import (
    _,
    gettext,
)
from . import (
    cmdutil,
    encoding,
    error,
    extensions,
    fancyopts,
    filemerge,
    fileset,
    minirst,
    pycompat,
    registrar,
    revset,
    templatefilters,
    templatefuncs,
    templatekw,
    ui as uimod,
    util,
)
from .hgweb import (
    webcommands,
)

_exclkeywords = {
    "(ADVANCED)",
    "(DEPRECATED)",
    "(EXPERIMENTAL)",
    # i18n: "(ADVANCED)" is a keyword, must be translated consistently
    _("(ADVANCED)"),
    # i18n: "(DEPRECATED)" is a keyword, must be translated consistently
    _("(DEPRECATED)"),
    # i18n: "(EXPERIMENTAL)" is a keyword, must be translated consistently
    _("(EXPERIMENTAL)"),
}

# The order in which command categories will be displayed.
# Extensions with custom categories should insert them into this list
# after/before the appropriate item, rather than replacing the list or
# assuming absolute positions.
CATEGORY_ORDER = [
    registrar.command.CATEGORY_REPO_CREATION,
    registrar.command.CATEGORY_REMOTE_REPO_MANAGEMENT,
    registrar.command.CATEGORY_COMMITTING,
    registrar.command.CATEGORY_CHANGE_MANAGEMENT,
    registrar.command.CATEGORY_CHANGE_ORGANIZATION,
    registrar.command.CATEGORY_FILE_CONTENTS,
    registrar.command.CATEGORY_CHANGE_NAVIGATION ,
    registrar.command.CATEGORY_WORKING_DIRECTORY,
    registrar.command.CATEGORY_IMPORT_EXPORT,
    registrar.command.CATEGORY_MAINTENANCE,
    registrar.command.CATEGORY_HELP,
    registrar.command.CATEGORY_MISC,
    registrar.command.CATEGORY_NONE,
]

# Human-readable category names. These are translated.
# Extensions with custom categories should add their names here.
CATEGORY_NAMES = {
    registrar.command.CATEGORY_REPO_CREATION: 'Repository creation',
    registrar.command.CATEGORY_REMOTE_REPO_MANAGEMENT:
        'Remote repository management',
    registrar.command.CATEGORY_COMMITTING: 'Change creation',
    registrar.command.CATEGORY_CHANGE_NAVIGATION: 'Change navigation',
    registrar.command.CATEGORY_CHANGE_MANAGEMENT: 'Change manipulation',
    registrar.command.CATEGORY_CHANGE_ORGANIZATION: 'Change organization',
    registrar.command.CATEGORY_WORKING_DIRECTORY:
        'Working directory management',
    registrar.command.CATEGORY_FILE_CONTENTS: 'File content management',
    registrar.command.CATEGORY_IMPORT_EXPORT: 'Change import/export',
    registrar.command.CATEGORY_MAINTENANCE: 'Repository maintenance',
    registrar.command.CATEGORY_HELP: 'Help',
    registrar.command.CATEGORY_MISC: 'Miscellaneous commands',
    registrar.command.CATEGORY_NONE: 'Uncategorized commands',
}

# Topic categories.
TOPIC_CATEGORY_IDS = 'ids'
TOPIC_CATEGORY_OUTPUT = 'output'
TOPIC_CATEGORY_CONFIG = 'config'
TOPIC_CATEGORY_CONCEPTS = 'concepts'
TOPIC_CATEGORY_MISC = 'misc'
TOPIC_CATEGORY_NONE = 'none'

# The order in which topic categories will be displayed.
# Extensions with custom categories should insert them into this list
# after/before the appropriate item, rather than replacing the list or
# assuming absolute positions.
TOPIC_CATEGORY_ORDER = [
    TOPIC_CATEGORY_IDS,
    TOPIC_CATEGORY_OUTPUT,
    TOPIC_CATEGORY_CONFIG,
    TOPIC_CATEGORY_CONCEPTS,
    TOPIC_CATEGORY_MISC,
    TOPIC_CATEGORY_NONE,
]

# Human-readable topic category names. These are translated.
TOPIC_CATEGORY_NAMES = {
    TOPIC_CATEGORY_IDS: 'Mercurial identifiers',
    TOPIC_CATEGORY_OUTPUT: 'Mercurial output',
    TOPIC_CATEGORY_CONFIG: 'Mercurial configuration',
    TOPIC_CATEGORY_CONCEPTS: 'Concepts',
    TOPIC_CATEGORY_MISC: 'Miscellaneous',
    TOPIC_CATEGORY_NONE: 'Uncategorized topics',
    TOPIC_CATEGORY_NONE: 'Uncategorized topics',
}

def listexts(header, exts, indent=1, showdeprecated=False):
    '''return a text listing of the given extensions'''
    rst = []
    if exts:
        for name, desc in sorted(exts.iteritems()):
            if not showdeprecated and any(w in desc for w in _exclkeywords):
                continue
            rst.append('%s:%s: %s\n' % (' ' * indent, name, desc))
    if rst:
        rst.insert(0, '\n%s\n\n' % header)
    return rst

def extshelp(ui):
    rst = loaddoc('extensions')(ui).splitlines(True)
    rst.extend(listexts(
        _('enabled extensions:'), extensions.enabled(), showdeprecated=True))
    rst.extend(listexts(_('disabled extensions:'), extensions.disabled(),
                        showdeprecated=ui.verbose))
    doc = ''.join(rst)
    return doc

def optrst(header, options, verbose):
    data = []
    multioccur = False
    for option in options:
        if len(option) == 5:
            shortopt, longopt, default, desc, optlabel = option
        else:
            shortopt, longopt, default, desc = option
            optlabel = _("VALUE") # default label

        if not verbose and any(w in desc for w in _exclkeywords):
            continue

        so = ''
        if shortopt:
            so = '-' + shortopt
        lo = '--' + longopt

        if isinstance(default, fancyopts.customopt):
            default = default.getdefaultvalue()
        if default and not callable(default):
            # default is of unknown type, and in Python 2 we abused
            # the %s-shows-repr property to handle integers etc. To
            # match that behavior on Python 3, we do str(default) and
            # then convert it to bytes.
            desc += _(" (default: %s)") % pycompat.bytestr(default)

        if isinstance(default, list):
            lo += " %s [+]" % optlabel
            multioccur = True
        elif (default is not None) and not isinstance(default, bool):
            lo += " %s" % optlabel

        data.append((so, lo, desc))

    if multioccur:
        header += (_(" ([+] can be repeated)"))

    rst = ['\n%s:\n\n' % header]
    rst.extend(minirst.maketable(data, 1))

    return ''.join(rst)

def indicateomitted(rst, omitted, notomitted=None):
    rst.append('\n\n.. container:: omitted\n\n    %s\n\n' % omitted)
    if notomitted:
        rst.append('\n\n.. container:: notomitted\n\n    %s\n\n' % notomitted)

def filtercmd(ui, cmd, kw, doc):
    if not ui.debugflag and cmd.startswith("debug") and kw != "debug":
        return True
    if not ui.verbose and doc and any(w in doc for w in _exclkeywords):
        return True
    return False

def topicmatch(ui, commands, kw):
    """Return help topics matching kw.

    Returns {'section': [(name, summary), ...], ...} where section is
    one of topics, commands, extensions, or extensioncommands.
    """
    kw = encoding.lower(kw)
    def lowercontains(container):
        return kw in encoding.lower(container)  # translated in helptable
    results = {'topics': [],
               'commands': [],
               'extensions': [],
               'extensioncommands': [],
               }
    for topic in helptable:
        names, header, doc = topic[0:3]
        # Old extensions may use a str as doc.
        if (sum(map(lowercontains, names))
            or lowercontains(header)
            or (callable(doc) and lowercontains(doc(ui)))):
            results['topics'].append((names[0], header))
    for cmd, entry in commands.table.iteritems():
        if len(entry) == 3:
            summary = entry[2]
        else:
            summary = ''
        # translate docs *before* searching there
        docs = _(pycompat.getdoc(entry[0])) or ''
        if kw in cmd or lowercontains(summary) or lowercontains(docs):
            doclines = docs.splitlines()
            if doclines:
                summary = doclines[0]
            cmdname = cmdutil.parsealiases(cmd)[0]
            if filtercmd(ui, cmdname, kw, docs):
                continue
            results['commands'].append((cmdname, summary))
    for name, docs in itertools.chain(
        extensions.enabled(False).iteritems(),
        extensions.disabled().iteritems()):
        if not docs:
            continue
        name = name.rpartition('.')[-1]
        if lowercontains(name) or lowercontains(docs):
            # extension docs are already translated
            results['extensions'].append((name, docs.splitlines()[0]))
        try:
            mod = extensions.load(ui, name, '')
        except ImportError:
            # debug message would be printed in extensions.load()
            continue
        for cmd, entry in getattr(mod, 'cmdtable', {}).iteritems():
            if kw in cmd or (len(entry) > 2 and lowercontains(entry[2])):
                cmdname = cmdutil.parsealiases(cmd)[0]
                cmddoc = pycompat.getdoc(entry[0])
                if cmddoc:
                    cmddoc = gettext(cmddoc).splitlines()[0]
                else:
                    cmddoc = _('(no help text available)')
                if filtercmd(ui, cmdname, kw, cmddoc):
                    continue
                results['extensioncommands'].append((cmdname, cmddoc))
    return results

def loaddoc(topic, subdir=None):
    """Return a delayed loader for help/topic.txt."""

    def loader(ui):
        docdir = os.path.join(util.datapath, 'help')
        if subdir:
            docdir = os.path.join(docdir, subdir)
        path = os.path.join(docdir, topic + ".txt")
        doc = gettext(util.readfile(path))
        for rewriter in helphooks.get(topic, []):
            doc = rewriter(ui, topic, doc)
        return doc

    return loader

internalstable = sorted([
    (['bundle2'], _('Bundle2'),
     loaddoc('bundle2', subdir='internals')),
    (['bundles'], _('Bundles'),
     loaddoc('bundles', subdir='internals')),
    (['cbor'], _('CBOR'),
     loaddoc('cbor', subdir='internals')),
    (['censor'], _('Censor'),
     loaddoc('censor', subdir='internals')),
    (['changegroups'], _('Changegroups'),
     loaddoc('changegroups', subdir='internals')),
    (['config'], _('Config Registrar'),
     loaddoc('config', subdir='internals')),
    (['requirements'], _('Repository Requirements'),
     loaddoc('requirements', subdir='internals')),
    (['revlogs'], _('Revision Logs'),
     loaddoc('revlogs', subdir='internals')),
    (['wireprotocol'], _('Wire Protocol'),
     loaddoc('wireprotocol', subdir='internals')),
    (['wireprotocolrpc'], _('Wire Protocol RPC'),
     loaddoc('wireprotocolrpc', subdir='internals')),
    (['wireprotocolv2'], _('Wire Protocol Version 2'),
     loaddoc('wireprotocolv2', subdir='internals')),
])

def internalshelp(ui):
    """Generate the index for the "internals" topic."""
    lines = ['To access a subtopic, use "hg help internals.{subtopic-name}"\n',
             '\n']
    for names, header, doc in internalstable:
        lines.append(' :%s: %s\n' % (names[0], header))

    return ''.join(lines)

helptable = sorted([
    (['bundlespec'], _("Bundle File Formats"), loaddoc('bundlespec'),
     TOPIC_CATEGORY_CONCEPTS),
    (['color'], _("Colorizing Outputs"), loaddoc('color'),
     TOPIC_CATEGORY_OUTPUT),
    (["config", "hgrc"], _("Configuration Files"), loaddoc('config'),
     TOPIC_CATEGORY_CONFIG),
    (['deprecated'], _("Deprecated Features"), loaddoc('deprecated'),
     TOPIC_CATEGORY_MISC),
    (["dates"], _("Date Formats"), loaddoc('dates'), TOPIC_CATEGORY_OUTPUT),
    (["flags"], _("Command-line flags"), loaddoc('flags'),
     TOPIC_CATEGORY_CONFIG),
    (["patterns"], _("File Name Patterns"), loaddoc('patterns'),
     TOPIC_CATEGORY_IDS),
    (['environment', 'env'], _('Environment Variables'),
     loaddoc('environment'), TOPIC_CATEGORY_CONFIG),
    (['revisions', 'revs', 'revsets', 'revset', 'multirevs', 'mrevs'],
      _('Specifying Revisions'), loaddoc('revisions'), TOPIC_CATEGORY_IDS),
    (['filesets', 'fileset'], _("Specifying File Sets"), loaddoc('filesets'),
     TOPIC_CATEGORY_IDS),
    (['diffs'], _('Diff Formats'), loaddoc('diffs'), TOPIC_CATEGORY_OUTPUT),
    (['merge-tools', 'mergetools', 'mergetool'], _('Merge Tools'),
     loaddoc('merge-tools'), TOPIC_CATEGORY_CONFIG),
    (['templating', 'templates', 'template', 'style'], _('Template Usage'),
     loaddoc('templates'), TOPIC_CATEGORY_OUTPUT),
    (['urls'], _('URL Paths'), loaddoc('urls'), TOPIC_CATEGORY_IDS),
    (["extensions"], _("Using Additional Features"), extshelp,
     TOPIC_CATEGORY_CONFIG),
    (["subrepos", "subrepo"], _("Subrepositories"), loaddoc('subrepos'),
     TOPIC_CATEGORY_CONCEPTS),
    (["hgweb"], _("Configuring hgweb"), loaddoc('hgweb'),
     TOPIC_CATEGORY_CONFIG),
    (["glossary"], _("Glossary"), loaddoc('glossary'), TOPIC_CATEGORY_CONCEPTS),
    (["hgignore", "ignore"], _("Syntax for Mercurial Ignore Files"),
     loaddoc('hgignore'), TOPIC_CATEGORY_IDS),
    (["phases"], _("Working with Phases"), loaddoc('phases'),
     TOPIC_CATEGORY_CONCEPTS),
    (['scripting'], _('Using Mercurial from scripts and automation'),
     loaddoc('scripting'), TOPIC_CATEGORY_MISC),
    (['internals'], _("Technical implementation topics"), internalshelp,
     TOPIC_CATEGORY_MISC),
    (['pager'], _("Pager Support"), loaddoc('pager'), TOPIC_CATEGORY_CONFIG),
])

# Maps topics with sub-topics to a list of their sub-topics.
subtopics = {
    'internals': internalstable,
}

# Map topics to lists of callable taking the current topic help and
# returning the updated version
helphooks = {}

def addtopichook(topic, rewriter):
    helphooks.setdefault(topic, []).append(rewriter)

def makeitemsdoc(ui, topic, doc, marker, items, dedent=False):
    """Extract docstring from the items key to function mapping, build a
    single documentation block and use it to overwrite the marker in doc.
    """
    entries = []
    for name in sorted(items):
        text = (pycompat.getdoc(items[name]) or '').rstrip()
        if (not text
            or not ui.verbose and any(w in text for w in _exclkeywords)):
            continue
        text = gettext(text)
        if dedent:
            # Abuse latin1 to use textwrap.dedent() on bytes.
            text = textwrap.dedent(text.decode('latin1')).encode('latin1')
        lines = text.splitlines()
        doclines = [(lines[0])]
        for l in lines[1:]:
            # Stop once we find some Python doctest
            if l.strip().startswith('>>>'):
                break
            if dedent:
                doclines.append(l.rstrip())
            else:
                doclines.append('  ' + l.strip())
        entries.append('\n'.join(doclines))
    entries = '\n\n'.join(entries)
    return doc.replace(marker, entries)

def addtopicsymbols(topic, marker, symbols, dedent=False):
    def add(ui, topic, doc):
        return makeitemsdoc(ui, topic, doc, marker, symbols, dedent=dedent)
    addtopichook(topic, add)

addtopicsymbols('bundlespec', '.. bundlecompressionmarker',
                util.bundlecompressiontopics())
addtopicsymbols('filesets', '.. predicatesmarker', fileset.symbols)
addtopicsymbols('merge-tools', '.. internaltoolsmarker',
                filemerge.internalsdoc)
addtopicsymbols('revisions', '.. predicatesmarker', revset.symbols)
addtopicsymbols('templates', '.. keywordsmarker', templatekw.keywords)
addtopicsymbols('templates', '.. filtersmarker', templatefilters.filters)
addtopicsymbols('templates', '.. functionsmarker', templatefuncs.funcs)
addtopicsymbols('hgweb', '.. webcommandsmarker', webcommands.commands,
                dedent=True)

def inserttweakrc(ui, topic, doc):
    marker = '.. tweakdefaultsmarker'
    repl = uimod.tweakrc
    def sub(m):
        lines = [m.group(1) + s for s in repl.splitlines()]
        return '\n'.join(lines)
    return re.sub(br'( *)%s' % re.escape(marker), sub, doc)

addtopichook('config', inserttweakrc)

def help_(ui, commands, name, unknowncmd=False, full=True, subtopic=None,
          **opts):
    '''
    Generate the help for 'name' as unformatted restructured text. If
    'name' is None, describe the commands available.
    '''

    opts = pycompat.byteskwargs(opts)

    def helpcmd(name, subtopic=None):
        try:
            aliases, entry = cmdutil.findcmd(name, commands.table,
                                             strict=unknowncmd)
        except error.AmbiguousCommand as inst:
            # py3 fix: except vars can't be used outside the scope of the
            # except block, nor can be used inside a lambda. python issue4617
            prefix = inst.args[0]
            select = lambda c: cmdutil.parsealiases(c)[0].startswith(prefix)
            rst = helplist(select)
            return rst

        rst = []

        # check if it's an invalid alias and display its error if it is
        if getattr(entry[0], 'badalias', None):
            rst.append(entry[0].badalias + '\n')
            if entry[0].unknowncmd:
                try:
                    rst.extend(helpextcmd(entry[0].cmdname))
                except error.UnknownCommand:
                    pass
            return rst

        # synopsis
        if len(entry) > 2:
            if entry[2].startswith('hg'):
                rst.append("%s\n" % entry[2])
            else:
                rst.append('hg %s %s\n' % (aliases[0], entry[2]))
        else:
            rst.append('hg %s\n' % aliases[0])
        # aliases
        if full and not ui.quiet and len(aliases) > 1:
            rst.append(_("\naliases: %s\n") % ', '.join(aliases[1:]))
        rst.append('\n')

        # description
        doc = gettext(pycompat.getdoc(entry[0]))
        if not doc:
            doc = _("(no help text available)")
        if util.safehasattr(entry[0], 'definition'):  # aliased command
            source = entry[0].source
            if entry[0].definition.startswith('!'):  # shell alias
                doc = (_('shell alias for: %s\n\n%s\n\ndefined by: %s\n') %
                       (entry[0].definition[1:], doc, source))
            else:
                doc = (_('alias for: hg %s\n\n%s\n\ndefined by: %s\n') %
                       (entry[0].definition, doc, source))
        doc = doc.splitlines(True)
        if ui.quiet or not full:
            rst.append(doc[0])
        else:
            rst.extend(doc)
        rst.append('\n')

        # check if this command shadows a non-trivial (multi-line)
        # extension help text
        try:
            mod = extensions.find(name)
            doc = gettext(pycompat.getdoc(mod)) or ''
            if '\n' in doc.strip():
                msg = _("(use 'hg help -e %s' to show help for "
                        "the %s extension)") % (name, name)
                rst.append('\n%s\n' % msg)
        except KeyError:
            pass

        # options
        if not ui.quiet and entry[1]:
            rst.append(optrst(_("options"), entry[1], ui.verbose))

        if ui.verbose:
            rst.append(optrst(_("global options"),
                              commands.globalopts, ui.verbose))

        if not ui.verbose:
            if not full:
                rst.append(_("\n(use 'hg %s -h' to show more help)\n")
                           % name)
            elif not ui.quiet:
                rst.append(_('\n(some details hidden, use --verbose '
                               'to show complete help)'))

        return rst

    def helplist(select=None, **opts):
        # Category -> list of commands
        cats = {}
        # Command -> short description
        h = {}
        # Command -> string showing synonyms
        syns = {}
        for c, e in commands.table.iteritems():
            fs = cmdutil.parsealiases(c)
            f = fs[0]
            syns[f] = ', '.join(fs)
            func = e[0]
            if select and not select(f):
                continue
            if (not select and name != 'shortlist' and
                func.__module__ != commands.__name__):
                continue
            if name == "shortlist":
                if not getattr(func, 'helpbasic', False):
                    continue
            doc = pycompat.getdoc(func)
            if filtercmd(ui, f, name, doc):
                continue
            doc = gettext(doc)
            if not doc:
                doc = _("(no help text available)")
            h[f] = doc.splitlines()[0].rstrip()

            cat = getattr(func, 'helpcategory', None) or (
                registrar.command.CATEGORY_NONE)
            cats.setdefault(cat, []).append(f)

        rst = []
        if not h:
            if not ui.quiet:
                rst.append(_('no commands defined\n'))
            return rst

        # Output top header.
        if not ui.quiet:
            if name == "shortlist":
                rst.append(_('basic commands:\n\n'))
            elif name == "debug":
                rst.append(_('debug commands (internal and unsupported):\n\n'))
            else:
                rst.append(_('list of commands:\n'))

        def appendcmds(cmds):
            cmds = sorted(cmds)
            for c in cmds:
                if ui.verbose:
                    rst.append(" :%s: %s\n" % (syns[c], h[c]))
                else:
                    rst.append(' :%s: %s\n' % (c, h[c]))

        if name in ('shortlist', 'debug'):
            # List without categories.
            appendcmds(h)
        else:
            # Check that all categories have an order.
            missing_order = set(cats.keys()) - set(CATEGORY_ORDER)
            if missing_order:
                ui.develwarn('help categories missing from CATEGORY_ORDER: %s' %
                             missing_order)

            # List per category.
            for cat in CATEGORY_ORDER:
                catfns = cats.get(cat, [])
                if catfns:
                    if len(cats) > 1:
                        catname = gettext(CATEGORY_NAMES[cat])
                        rst.append("\n%s:\n" % catname)
                    rst.append("\n")
                    appendcmds(catfns)

        ex = opts.get
        anyopts = (ex(r'keyword') or not (ex(r'command') or ex(r'extension')))
        if not name and anyopts:
            exts = listexts(_('enabled extensions:'), extensions.enabled())
            if exts:
                rst.append('\n')
                rst.extend(exts)

            rst.append(_("\nadditional help topics:\n"))
            # Group commands by category.
            topiccats = {}
            for topic in helptable:
                names, header, doc = topic[0:3]
                if len(topic) > 3 and topic[3]:
                    category = topic[3]
                else:
                    category = TOPIC_CATEGORY_NONE

                topiccats.setdefault(category, []).append((names[0], header))

            # Check that all categories have an order.
            missing_order = set(topiccats.keys()) - set(TOPIC_CATEGORY_ORDER)
            if missing_order:
                ui.develwarn(
                    'help categories missing from TOPIC_CATEGORY_ORDER: %s' %
                    missing_order)

            # Output topics per category.
            for cat in TOPIC_CATEGORY_ORDER:
                topics = topiccats.get(cat, [])
                if topics:
                    if len(topiccats) > 1:
                        catname = gettext(TOPIC_CATEGORY_NAMES[cat])
                        rst.append("\n%s:\n" % catname)
                    rst.append("\n")
                    for t, desc in topics:
                        rst.append(" :%s: %s\n" % (t, desc))

        if ui.quiet:
            pass
        elif ui.verbose:
            rst.append('\n%s\n' % optrst(_("global options"),
                                         commands.globalopts, ui.verbose))
            if name == 'shortlist':
                rst.append(_("\n(use 'hg help' for the full list "
                             "of commands)\n"))
        else:
            if name == 'shortlist':
                rst.append(_("\n(use 'hg help' for the full list of commands "
                             "or 'hg -v' for details)\n"))
            elif name and not full:
                rst.append(_("\n(use 'hg help %s' to show the full help "
                             "text)\n") % name)
            elif name and syns and name in syns.keys():
                rst.append(_("\n(use 'hg help -v -e %s' to show built-in "
                             "aliases and global options)\n") % name)
            else:
                rst.append(_("\n(use 'hg help -v%s' to show built-in aliases "
                             "and global options)\n")
                           % (name and " " + name or ""))
        return rst

    def helptopic(name, subtopic=None):
        # Look for sub-topic entry first.
        header, doc = None, None
        if subtopic and name in subtopics:
            for names, header, doc in subtopics[name]:
                if subtopic in names:
                    break

        if not header:
            for topic in helptable:
                names, header, doc = topic[0:3]
                if name in names:
                    break
            else:
                raise error.UnknownCommand(name)

        rst = [minirst.section(header)]

        # description
        if not doc:
            rst.append("    %s\n" % _("(no help text available)"))
        if callable(doc):
            rst += ["    %s\n" % l for l in doc(ui).splitlines()]

        if not ui.verbose:
            omitted = _('(some details hidden, use --verbose'
                         ' to show complete help)')
            indicateomitted(rst, omitted)

        try:
            cmdutil.findcmd(name, commands.table)
            rst.append(_("\nuse 'hg help -c %s' to see help for "
                       "the %s command\n") % (name, name))
        except error.UnknownCommand:
            pass
        return rst

    def helpext(name, subtopic=None):
        try:
            mod = extensions.find(name)
            doc = gettext(pycompat.getdoc(mod)) or _('no help text available')
        except KeyError:
            mod = None
            doc = extensions.disabledext(name)
            if not doc:
                raise error.UnknownCommand(name)

        if '\n' not in doc:
            head, tail = doc, ""
        else:
            head, tail = doc.split('\n', 1)
        rst = [_('%s extension - %s\n\n') % (name.rpartition('.')[-1], head)]
        if tail:
            rst.extend(tail.splitlines(True))
            rst.append('\n')

        if not ui.verbose:
            omitted = _('(some details hidden, use --verbose'
                         ' to show complete help)')
            indicateomitted(rst, omitted)

        if mod:
            try:
                ct = mod.cmdtable
            except AttributeError:
                ct = {}
            modcmds = set([c.partition('|')[0] for c in ct])
            rst.extend(helplist(modcmds.__contains__))
        else:
            rst.append(_("(use 'hg help extensions' for information on enabling"
                       " extensions)\n"))
        return rst

    def helpextcmd(name, subtopic=None):
        cmd, ext, doc = extensions.disabledcmd(ui, name,
                                               ui.configbool('ui', 'strict'))
        doc = doc.splitlines()[0]

        rst = listexts(_("'%s' is provided by the following "
                              "extension:") % cmd, {ext: doc}, indent=4,
                       showdeprecated=True)
        rst.append('\n')
        rst.append(_("(use 'hg help extensions' for information on enabling "
                   "extensions)\n"))
        return rst


    rst = []
    kw = opts.get('keyword')
    if kw or name is None and any(opts[o] for o in opts):
        matches = topicmatch(ui, commands, name or '')
        helpareas = []
        if opts.get('extension'):
            helpareas += [('extensions', _('Extensions'))]
        if opts.get('command'):
            helpareas += [('commands', _('Commands'))]
        if not helpareas:
            helpareas = [('topics', _('Topics')),
                         ('commands', _('Commands')),
                         ('extensions', _('Extensions')),
                         ('extensioncommands', _('Extension Commands'))]
        for t, title in helpareas:
            if matches[t]:
                rst.append('%s:\n\n' % title)
                rst.extend(minirst.maketable(sorted(matches[t]), 1))
                rst.append('\n')
        if not rst:
            msg = _('no matches')
            hint = _("try 'hg help' for a list of topics")
            raise error.Abort(msg, hint=hint)
    elif name and name != 'shortlist':
        queries = []
        if unknowncmd:
            queries += [helpextcmd]
        if opts.get('extension'):
            queries += [helpext]
        if opts.get('command'):
            queries += [helpcmd]
        if not queries:
            queries = (helptopic, helpcmd, helpext, helpextcmd)
        for f in queries:
            try:
                rst = f(name, subtopic)
                break
            except error.UnknownCommand:
                pass
        else:
            if unknowncmd:
                raise error.UnknownCommand(name)
            else:
                msg = _('no such help topic: %s') % name
                hint = _("try 'hg help --keyword %s'") % name
                raise error.Abort(msg, hint=hint)
    else:
        # program name
        if not ui.quiet:
            rst = [_("Mercurial Distributed SCM\n"), '\n']
        rst.extend(helplist(None, **pycompat.strkwargs(opts)))

    return ''.join(rst)

def formattedhelp(ui, commands, fullname, keep=None, unknowncmd=False,
                  full=True, **opts):
    """get help for a given topic (as a dotted name) as rendered rst

    Either returns the rendered help text or raises an exception.
    """
    if keep is None:
        keep = []
    else:
        keep = list(keep) # make a copy so we can mutate this later

    # <fullname> := <name>[.<subtopic][.<section>]
    name = subtopic = section = None
    if fullname is not None:
        nameparts = fullname.split('.')
        name = nameparts.pop(0)
        if nameparts and name in subtopics:
            subtopic = nameparts.pop(0)
        if nameparts:
            section = encoding.lower('.'.join(nameparts))

    textwidth = ui.configint('ui', 'textwidth')
    termwidth = ui.termwidth() - 2
    if textwidth <= 0 or termwidth < textwidth:
        textwidth = termwidth
    text = help_(ui, commands, name,
                 subtopic=subtopic, unknowncmd=unknowncmd, full=full, **opts)

    blocks, pruned = minirst.parse(text, keep=keep)
    if 'verbose' in pruned:
        keep.append('omitted')
    else:
        keep.append('notomitted')
    blocks, pruned = minirst.parse(text, keep=keep)
    if section:
        blocks = minirst.filtersections(blocks, section)

    # We could have been given a weird ".foo" section without a name
    # to look for, or we could have simply failed to found "foo.bar"
    # because bar isn't a section of foo
    if section and not (blocks and name):
        raise error.Abort(_("help section not found: %s") % fullname)

    return minirst.formatplain(blocks, textwidth)
