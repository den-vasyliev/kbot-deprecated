# state.py - writing and reading state files in Mercurial
#
# Copyright 2018 Pulkit Goyal <pulkitmgoyal@gmail.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

"""
This file contains class to wrap the state for commands and other
related logic.

All the data related to the command state is stored as dictionary in the object.
The class has methods using which the data can be stored to disk in a file under
.hg/ directory.

We store the data on disk in cbor, for which we use the third party cbor library
to serialize and deserialize data.
"""

from __future__ import absolute_import

from . import (
    error,
    util,
)
from .utils import (
    cborutil,
)

class cmdstate(object):
    """a wrapper class to store the state of commands like `rebase`, `graft`,
    `histedit`, `shelve` etc. Extensions can also use this to write state files.

    All the data for the state is stored in the form of key-value pairs in a
    dictionary.

    The class object can write all the data to a file in .hg/ directory and
    can populate the object data reading that file.

    Uses cbor to serialize and deserialize data while writing and reading from
    disk.
    """

    def __init__(self, repo, fname):
        """ repo is the repo object
        fname is the file name in which data should be stored in .hg directory
        """
        self._repo = repo
        self.fname = fname

    def read(self):
        """read the existing state file and return a dict of data stored"""
        return self._read()

    def save(self, version, data):
        """write all the state data stored to .hg/<filename> file

        we use third-party library cbor to serialize data to write in the file.
        """
        if not isinstance(version, int):
            raise error.ProgrammingError("version of state file should be"
                                         " an integer")

        with self._repo.vfs(self.fname, 'wb', atomictemp=True) as fp:
            fp.write('%d\n' % version)
            for chunk in cborutil.streamencode(data):
                fp.write(chunk)

    def _read(self):
        """reads the state file and returns a dictionary which contain
        data in the same format as it was before storing"""
        with self._repo.vfs(self.fname, 'rb') as fp:
            try:
                int(fp.readline())
            except ValueError:
                raise error.CorruptedState("unknown version of state file"
                                           " found")

            return cborutil.decodeall(fp.read())[0]

    def delete(self):
        """drop the state file if exists"""
        util.unlinkpath(self._repo.vfs.join(self.fname), ignoremissing=True)

    def exists(self):
        """check whether the state file exists or not"""
        return self._repo.vfs.exists(self.fname)
