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

from collections.abc import Generator, Iterable
import logging
from typing import Optional

import redis
import ujson as json

from .decorators import convert_db_watch_errors
from .defs import (
                    DATABASES,
                    DB_HOST,
                    DB_PORT
                  )
from .exceptions import DBNoEntryError, DBTooBusyError
from .typing import (
                        DatabaseNames,
                        DataFilter,
                        DbEntry,
                        DbIdentifier,
                        DbKey,
                        DeletionHandler,
                        JsonDict,
                        UpdateHandler
                    )


LOGGER = logging.getLogger(__name__)


class DBWrapper:
    """A wrapper around a Redis database connection

    The handles creating the Redis client and provides REST-like methods for
    modifying json data in the database.

    Because the underlying Redis client is threadsafe, this class is as well,
    and can be safely shared by multiple threads.
    """

    def __init__(self, db: DbIdentifier) -> None:
        self.db_id = self._get_db_id(db)
        self.db_name: DatabaseNames = DATABASES[self.db_id]
        self.client = self._get_client()

    def __contains__(self, key: DbKey) -> bool:
        return self.client.exists(key)

    @classmethod
    def _get_db_id(cls, db: DbIdentifier) -> int:
        """Converts a db name to the id used by Redis."""
        return db if isinstance(db, int) else DATABASES.index(db)

    def _get_client(self) -> redis.client.Redis:
        """Create a connection with the database."""
        LOGGER.debug("Creating database connection"
                     "host: %s port: %s database: %s",
                     DB_HOST, DB_PORT, self.db_id)
        try:
            return redis.Redis(host=DB_HOST, port=DB_PORT, db=self.db_id, protocol=3)
        except Exception as err:
            LOGGER.error("Failed to connect to database %s : %s",
                         self.db_id, err)
            raise

    def no_entry_exception(self, key: DbKey) -> DBNoEntryError:
        """
        Helper method for creating a DBNoEntryError for this database
        """
        return DBNoEntryError(self.db_name, key)

    def too_busy_exception(self) -> DBTooBusyError:
        """
        Helper method for creating a DBTooBusyError for this database
        """
        return DBTooBusyError(self.db_name)

    @convert_db_watch_errors
    def get(self, key: DbKey) -> Optional[DbEntry]:
        """
        Get the data for the given key from the database, and return it.
        Raises DBNoEntryError if the entry does not exist.
        """
        datastr = self.client.get(key)
        if not datastr:
            raise self.no_entry_exception(key)
        data = json.loads(datastr)
        if data is None:
            raise self.no_entry_exception(key)
        return data

    @convert_db_watch_errors
    def get_delete(self, key: DbKey) -> DbEntry:
        """
        Get the data for the given key from the database, and delete it from the DB.
        Returns the data. Raises DBNoEntryError if the entry does not exist.
        """
        datastr = self.client.getdel(key)
        if not datastr:
            raise self.no_entry_exception(key)
        data = json.loads(datastr)
        if data is None:
            raise self.no_entry_exception(key)
        return data

    def get_keys(self, start_after_key: Optional[str] = None) -> list[str]:
        """
        Returns a sorted list of all keys (as str) in the database.
        If start_after_key is specified, only keys lexically after the
        specified key will be returned.
        """
        # Redis SCAN operations can produce duplicate results.  Using a set fixes this.
        # Using count=500 significantly improves the performance, by limiting the number
        # of network calls to the database
        keys = { key.decode('utf-8') for key in self.client.scan_iter(count=500) }
        if start_after_key is None:
            return sorted(keys)
        # Add the start_after_key to the set, as it may not be in there already
        keys.add(start_after_key)
        # Make a sorted list from the set
        sorted_keys = sorted(keys)
        # Find the index of start_after_key
        i = sorted_keys.index(start_after_key)
        # Return the list starting after that index
        return sorted_keys[i+1:]

    def iter_values(self, start_after_key: Optional[str] = None) -> Generator[DbEntry, None, None]:
        """
        Iterate through every item in the database. Parse each item as JSON and yield it.
        If start_after_key is specified, skip any keys that are lexically <= the specified key.
        """
        all_keys = self.get_keys(start_after_key=start_after_key)
        while all_keys:
            for datastr in self.client.mget(all_keys[:500]):
                data = json.loads(datastr) if datastr else None
                if data is None:
                    # If datastr is empty/None, that means that the entry was
                    # deleted after the key was returned by the mget call.
                    # In that case, we just skip it.
                    continue
                yield data
            all_keys = all_keys[500:]

    @convert_db_watch_errors
    def get_all(
        self, limit: int = 0,
        after_id: Optional[str] = None,
        data_filters: Optional[Iterable[DataFilter]] = None
    ) -> tuple[list[DbEntry], bool]:
        """Get an array of data for all keys."""

        if not data_filters:
            data_filters = []
        limit = max(limit, 0)
        page_full = False
        next_page_exists = False
        data_page = []
        for data in self.iter_values(after_id):
            # Data filtering happens here rather than after; due to
            # paging/memory constraints, we can't load all data and then filter on the results.
            if data_filters and not all(data_filter(data) for data_filter in data_filters):
                # If there are data filters specified, and this data does not match all of them,
                # then skip it
                continue
            # This means either there are no data filters, or there are data filters and this
            # data matches all of them.
            if page_full:
                next_page_exists = True
                break
            data_page.append(data)
            if limit and len(data_page) >= limit:
                page_full = True
        return data_page, next_page_exists

    @convert_db_watch_errors
    def put(self, key: DbKey, new_data: DbEntry) -> DbEntry:
        """
        Put data into the database, replacing any old data.
        """
        datastr = json.dumps(new_data)
        self.client.set(key, datastr)
        return new_data

    @convert_db_watch_errors
    def patch(
        self, key: DbKey,
        new_data: DbEntry,
        update_handler: Optional[UpdateHandler] = None
    ) -> DbEntry:
        """
        Patch data in the database.
        update_handler provides a way to operate on the full patched data.
        """
        data_str = self.client.get(key)
        data = json.loads(data_str)
        data = self._update(data, new_data)
        if update_handler:
            data = update_handler(data)
        data_str = json.dumps(data)
        self.client.set(key, data_str)
        return data

    @convert_db_watch_errors
    def patch_all_entries(
        self, data_filter: DataFilter,
        patch: JsonDict,
        update_handler: Optional[UpdateHandler] = None
    ) -> Generator[tuple[str, DbEntry], None, None]:
        """
        Patch multiple resources in the database.
        For each entry that is patched, yield a tuple of its key and its data.
        """
        for key in self.get_keys():
            data_str = self.client.get(key)
            data = json.loads(data_str)
            # Data filtering happens here rather than after due to paging/memory constraints;
            # we can't load all data and then filter on the results
            if data_filter and not data_filter(data):
                # This data does not match our filter, so skip it
                continue
            data = self._update(data, patch)
            if update_handler:
                data = update_handler(data)
            data_str = json.dumps(data)
            self.client.set(key, data_str)
            yield (key, data)

    @convert_db_watch_errors
    def patch_all(
        self, data_filter: DataFilter,
        patch: JsonDict,
        update_handler: Optional[UpdateHandler] = None
    ) -> list[str]:
        """Patch multiple resources in the database."""
        return [ k for k, _ in self.patch_all_entries(data_filter=data_filter,
                                                      patch=patch,
                                                      update_handler=update_handler) ]

    def _update(self, data: JsonDict, new_data: JsonDict) -> JsonDict:
        """Recursively patches JSON to allow sub-fields to be patched."""
        for k, v in new_data.items():
            if isinstance(v, dict):
                data[k] = self._update(data.get(k, {}), v)
            else:
                data[k] = v
        return data

    @convert_db_watch_errors
    def delete(self, key: DbKey) -> None:
        """
        Deletes data from the database.
        This is just a wrapper for the get_delete method,
        but it discards the return value.
        """
        self.get_delete(key)

    @convert_db_watch_errors
    def delete_all(
        self, data_filter: DataFilter,
        deletion_handler: Optional[DeletionHandler] = None
    ) -> list[str]:
        """Delete multiple resources in the database."""
        deleted_id_list = []
        for key in self.get_keys():
            data_str = self.client.get(key)
            data = json.loads(data_str)
            if data_filter and not data_filter(data):
                # This data does not match our filter, so skip it
                continue
            self.client.delete(key)
            if deletion_handler:
                deletion_handler(data)
            deleted_id_list.append(key)
        return deleted_id_list

    def info(self) -> dict:
        """Returns the database info."""
        return self.client.info()


def get_wrapper(db: DbIdentifier) -> DBWrapper:
    """Returns a database object."""
    return DBWrapper(db)
