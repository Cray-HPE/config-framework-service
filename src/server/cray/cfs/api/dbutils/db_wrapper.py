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
import itertools
import logging
import time
from typing import Optional

import redis
import ujson as json

from .decorators import convert_db_watch_errors
from .defs import (
                    DATABASES,
                    DB_BATCH_SIZE,
                    DB_BUSY_SECONDS,
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

    def _patch(
        self, key: DbKey,
        patch_data: DbEntry,
        update_handler: Optional[UpdateHandler]
    ) -> DbEntry:
        """
        Helper function for patch, which tries to apply the patch inside a Redis pipeline.
        Returns the updated entry data if successful.
        The pipeline will raise a Redis WatchError otherwise.
        """
        with self.client.pipeline() as pipe:
            # Mark this key for monitoring before we retrieve its data,
            # so that we know if it changes underneath us.
            pipe.watch(key)

            # Because we have not yet called pipe.multi(), this DB
            # get call will execute immediately and return the data.
            data_str = pipe.get(key)

            # Raise an exception if no or null entry.
            if not data_str:
                raise self.no_entry_exception(key)
            data = json.loads(data_str)
            # Apply the patch_data to the current data
            data = self._update(data, patch_data)

            # Call the update handler, if one was specified
            if update_handler:
                data = update_handler(data)

            # Encode the updated data as a JSON string
            data_str = json.dumps(data)

            # Begin our transaction.
            pipe.multi()

            # Because this is after the pipe.multi() call, the following
            # set command is not executed immediately, and instead is
            # queued up. The return value from this call is not the
            # return value for the actual DB call.
            pipe.set(key, data_str)

            # Calling pipe.execute() does one of two things:
            # 1. If the key we are watching has not changed since we started
            #    watching it, then our DB set operation will be executed.
            #    The return value of pipe.execute() is a list of the responses
            #    from all of the queued DB commands (in our case, just a single
            #    command -- the set). We don't really care about the response.
            #    If the set failed, Redis would raise an exception, and that's
            #    all we're really concerned about.
            # 2. If they key we are watching DID change, then the queued DB
            #    operations (the set, in our case) are aborted and a Redis WatchError
            #    exception will be raised, and the caller will have to decide how
            #    to deal with it.
            pipe.execute()

        # If we get here, it means no exception was raised by pipe.execute, so we
        # should return the updated data.
        return data

    @convert_db_watch_errors
    def patch(
        self, key: DbKey,
        patch_data: DbEntry,
        update_handler: Optional[UpdateHandler] = None
    ) -> DbEntry:
        """
        Patch data in the database.
        update_handler provides a way to operate on the full patched data.
        """
        # Set the time after which we will perform no more DB retries.
        # Note that this is not the same as it being a hard timeout. If no Redis
        # WatchErrors are raised after this time limit has been passed, then the method
        # will run to completion, regardless of how long it takes. This is solely
        # present to avoid a method which endlessly keeps retrying.
        no_retries_after: float = time.time() + DB_BUSY_SECONDS

        # The loop condition is a simple True because the logic for exiting the loop is
        # contained inside the loop itself.
        while True:
            try:
                # Call a helper function to try and patch the entry. If it is successful,
                # it will return the updated data, which we will return to our caller.
                # If it is unsuccessful, an exception will be raised.
                return self._patch(key, patch_data, update_handler)
            except redis.exceptions.WatchError as err:
                # This means the entry changed values while the helper function was
                # trying to patch it.
                if time.time() > no_retries_after:
                    # We are past the last allowed retry time, so re-raise the exception
                    raise err

                # We are not past the time limit, so just log a warning and we'll go back to the
                # top of the loop.
                LOGGER.warning("Key '%s' changed (%s); retrying", key, err)

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

    def _delete_batch(self,
        keys: Iterable[str],
        data_filter: DataFilter,
        keys_done: set[str]
    ) -> tuple[list[str], list[DbEntry]]:
        """
        Helper for delete_all method
        Given a list of keys, tries to delete all of the keys
        which exist in the database and which meet the specified
        filter (if one is specified).

        Before attempting the delete, as keys are excluded based on
        the above criteria, they are added to 'keys_done'.

        If the delete fails because of database changes that occur,
        then a Redis WatchError will be raised, which the caller must
        handle as they see fit.

        If the delete succeeds, this function returns two lists:
        1. A list of database keys that were deleted
        2. The data entries for each key in #1

        If no keys meet the criteria for deletion, then this function
        will return two empty lists.
        """
        keys_to_delete: list[str] = []
        data_to_delete: list[DbEntry] = []
        with self.client.pipeline() as pipe:
            # Start by watching all of the keys in this batch
            # This has to be done before we call mget for them.
            pipe.watch(*keys)

            # Retrieve all of the specified keys from the database
            # All database calls to pipe will be executed immediately,
            # since we have not yet called pipe.multi()
            data_str_list = pipe.mget(*keys)

            for key, data_str in zip(keys, data_str_list):
                if not data_str:
                    # Already empty, no need to delete it
                    keys_done.add(key)
                    continue
                data = json.loads(data_str)
                if data_filter and not data_filter(data):
                    # This data does not match our filter, so skip it
                    keys_done.add(key)
                    continue
                # This key should be deleted
                keys_to_delete.append(key)
                data_to_delete.append(data)

            # For the keys that we are not going to delete, the above loop
            # adds them to the keys_done set, so we don't check them again on
            # future iterations (if we have to re-try because of database changes)
            # We would also like to stop watching them, but unfortunately that is
            # not possible.

            if keys_to_delete:
                # Begin our transaction
                # After this call to pipe.multi(), the database calls are NOT executed immediately.
                pipe.multi()

                # Queue the DB command to delete the specified keys
                pipe.delete(*keys_to_delete)

                # Execute the pipeline
                #
                # At this point, if any entries still being watched have been changed since we
                # started watching them (including being created or deleted), then a Redis
                # WatchError exception will be raised and the delete command will not be executed.
                # The caller of this function will include logic that decides how to handle this.
                #
                # Instead, if none of those entries has changed, then Redis will atomically execute
                # all of the queued database commands. The return value of the pipe.execute()
                # call is a list with the return values for all of the queued DB commands. In this
                # case, that is just the delete call, and we do not care about its return value.
                # If the delete failed, it would raise an exception, and that's all we really care
                # about.
                pipe.execute()

        # If we get here, it means that either the pipe executed successfully, or (if we had no
        # keys to be deleted in this batch) it was not executed at all. Either way, return the
        # list of keys and data that we deleted (which will both be empty, in the latter case).
        return keys_to_delete, data_to_delete


    @convert_db_watch_errors
    def delete_all(
        self, data_filter: DataFilter,
        deletion_handler: Optional[DeletionHandler] = None
    ) -> list[str]:
        """
        Delete multiple resources in the database.
        Raises DBTooBusyError if unable to complete the deletes in time.
        """
        # Set the time after which we will perform no more DB retries.
        # Note that this is not the same as it being a hard timeout. If no Redis
        # WatchErrors are raised after this time limit has been passed, then the method
        # will run to completion, regardless of how long it takes. This is solely
        # present to avoid a method which endlessly keeps retrying.
        no_retries_after: float = time.time() + DB_BUSY_SECONDS

        # keys_left starts being set to all of the keys in the database.
        # We will remove keys from it as we process them (either by deleting
        # them or determining that they do not need to be deleted).
        keys_left: list[str] = self.get_keys()

        # deleted_keys is where we record the keys that we delete, so they
        # can be returned to the caller
        deleted_keys: list[str] = []

        # The delete_all method has a big loop at its heart.
        # The keys_done variable is used to keep track of which keys have been processed
        # in the current iteration of the loop.
        keys_done: set[str] = set()

        # Keep looping until either we have processed all of the keys in our keys_left list -- we
        # do not worry about keys that are created after this method starts running.
        #
        # The DB busy scenario is handled by an exception being raised, so it bypasses the
        # regular loop logic.
        while keys_left:
            if keys_done:
                # keys_done is non-empty, which means that some keys were processed on
                # the previous iteration of the loop. So we remove those from our list of
                # remaining keys
                keys_left = [ k for k in keys_left if k not in keys_done ]

                # If that removed the final keys from the list, exit the loop
                if not keys_left:
                    break

                # Clear the keys_done set, so it is empty to start this iteration
                keys_done.clear()

            # The main work in the loop is enclosed in this try/except block.
            # This is to catch Redis WatchErrors, which are raised when a change to the database
            # caused one of our delete operations to abort. Any other exceptions that arise are
            # not handled at this layer.
            try:
                # Process the keys in batches, rather than all at once.
                # See defs.py for details on DB_BATCH_SIZE.
                for key_batch in itertools.batched(keys_left, DB_BATCH_SIZE):
                    # Call our helper function on this batch of keys
                    batch_deleted_keys, batch_deleted_data = self._delete_batch(key_batch,
                                                                                data_filter,
                                                                                keys_done)
                    # If we get here, it means the deletes completed successfully.
                    # If any of these keys did not need to be deleted, then the helper
                    # function has already added them to the keys_done list.

                    # We also add the deleted keys to our done list.
                    keys_done.update(batch_deleted_keys)

                    # And add them to our list of deleted keys
                    deleted_keys.extend(batch_deleted_keys)

                    # If there is no deletion_handler, we can go to the next batch
                    if not deletion_handler:
                        continue

                    # If there is a deletion_handler, apply it to the entries that
                    # were deleted
                    for data in batch_deleted_data:
                        deletion_handler(data)
            except redis.exceptions.WatchError as err:
                # This means one of the keys changed values between when we filtered it and when
                # we went to delete it.
                if time.time() > no_retries_after:
                    # We are past the last allowed retry time, so re-raise the exception
                    raise err

                # We are not past the time limit, so just log a warning and we'll go back to the
                # top of the loop.
                LOGGER.warning("Key changed (%s); retrying", err)

        # If we get here, it means we ended up processing all of the keys in our starting list.
        # So return the IDs of the ones we deleted.
        # Sort the list, to preserve the previous behavior of this function.
        return sorted(deleted_keys)

    def info(self) -> dict:
        """Returns the database info."""
        return self.client.info()


def get_wrapper(db: DbIdentifier) -> DBWrapper:
    """Returns a database object."""
    return DBWrapper(db)
