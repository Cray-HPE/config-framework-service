#
# MIT License
#
# (C) Copyright 2019-2025 Hewlett Packard Enterprise Development LP
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#

"""
CFS Database Exceptions
"""

from .defs import DB_BUSY_SECONDS
from .typing import DatabaseNames, DbKey


class DBError(Exception):
    """
    Parent class for CFS DB exceptions
    """
    def __init__(self, db_name: DatabaseNames) -> None:
        self.db_name = db_name
        super().__init__(self.__str__())


class DBNoEntryError(DBError):
    """
    This exception is raised when the DB tries to do a get
    and the entry is not found.
    """
    def __init__(self, db_name: DatabaseNames, key: DbKey) -> None:
        self.key: str = key if isinstance(key, str) else key.decode('utf-8')
        super().__init__(db_name)

    def __str__(self) -> str:
        return f"No entry for '{self.key}' in '{self.db_name}' database"


class DBTooBusyError(DBError):
    """
    This exception is raised when the DB is unable to get the database lock
    within DB_BUSY_SECONDS.
    """
    def __str__(self) -> str:
        return f"Could not acquire '{self.db_name}' database lock within {DB_BUSY_SECONDS} seconds"
