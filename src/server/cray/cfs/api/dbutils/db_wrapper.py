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

from collections.abc import Collection, Generator, Iterable, Sequence
import copy
import itertools
import logging
import time
from typing import Optional

import redis
import ujson as json

from .conversions import patch_dict
from .decorators import convert_db_watch_errors, redis_pipeline
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
                        EntryChecker,
                        JsonDict,
                        PatchHandler,
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

    def get_keys(self, *, start_after_key: Optional[str] = None) -> list[str]:
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

    def iter_values(self, *, start_after_key: Optional[str] = None) -> Generator[DbEntry, None, None]:
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
        self,
        *,
        limit: int = 0,
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
        for data in self.iter_values(start_after_key=after_id):
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
    def put_if_not_set(self, key: DbKey, new_data: DbEntry) -> bool:
        """
        Put data into the database only if the entry does not already exist.
        If the entry already exists, does nothing.
        Returns True if the entry did not exist (so we set it).
        Returns False otherwise.
        """
        datastr = json.dumps(new_data)
        # setnx returns 1 if the data was set, 0 otherwise
        return bool(self.client.setnx(key, datastr))

    @redis_pipeline
    def _patch(self,
        pipe: redis.client.Pipeline,
        *,
        key: DbKey,
        patch_data: DbEntry,
        update_handler: Optional[UpdateHandler],
        patch_handler: PatchHandler,
        default_entry: Optional[DbEntry]
    ) -> DbEntry:
        """
        Helper function for patch, which tries to apply the patch inside a Redis pipeline.
        Returns the updated entry data if successful.
        The pipeline will raise a Redis WatchError otherwise.

        Note:
        The pipe argument does not exist as far as the caller of this method is concerned.
        That argument is automatically provided by the @redis_pipeline wrapper.
        """
        # Mark this key for monitoring before we retrieve its data,
        # so that we know if it changes underneath us.
        pipe.watch(key)

        # Because we have not yet called pipe.multi(), this DB
        # get call will execute immediately and return the data.
        data_str = pipe.get(key)

        if data_str:
            orig_data = json.loads(data_str)

            # Start by making a copy of the data, so that we can compare
            # the final patched version to it, and see if it has been changed
            # (this is necessary because many of the CFS patching functions change the
            # data in place, as well as returning it)
            new_data = copy.deepcopy(orig_data)

            # Apply the patch_data to the current data
            new_data = patch_handler(new_data, patch_data)
        elif default_entry is not None:
            # A default entry was specified, so we will apply the patch
            # on that.
            orig_data = None
            new_data = patch_handler(default_entry, patch_data)
        else:
            # No default entry specified
            # Raise an exception if no or null entry.
            raise self.no_entry_exception(key)

        # Call the update handler, if one was specified
        if update_handler:
            new_data = update_handler(new_data)

        # If the data has not changed, no need to continue
        if orig_data == new_data:
            return new_data

        # Encode the updated data as a JSON string
        data_str = json.dumps(new_data)

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
        return new_data

    @convert_db_watch_errors
    def patch(
        self,
        key: DbKey,
        patch_data: DbEntry,
        *,
        update_handler: Optional[UpdateHandler] = None,
        patch_handler: PatchHandler = patch_dict,
        default_entry: Optional[DbEntry] = None
    ) -> DbEntry:
        """
        Patch data in the database.
        If the entry does not exist in the DB:
            If default_entry is None, then DBNoEntryError is raised.
            Otherwise, the patch is applied on top of the specified default entry
            (and written to the DB).
        patch_handler provides an option to specify a non-default patch function.
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
                #
                # At the time of this writing, pylint is not clever enough to understand
                # the function signature mutation performed by the @redis_pipeline
                # decorator, and so it falsely reports that we are missing an argument
                # here.
                # pylint: disable=no-value-for-parameter
                return self._patch(key=key,
                                   patch_data=patch_data,
                                   update_handler=update_handler,
                                   patch_handler=patch_handler,
                                   default_entry=default_entry)
            except redis.exceptions.WatchError as err:
                # This means the entry changed values while the helper function was
                # trying to patch it.
                if time.time() > no_retries_after:
                    # We are past the last allowed retry time, so re-raise the exception
                    raise err

                # We are not past the time limit, so just log a warning and we'll go back to the
                # top of the loop.
                LOGGER.warning("Key '%s' changed (%s); retrying", key, err)

    @redis_pipeline
    def _patch_batch(
        self,
        pipe: redis.client.Pipeline,
        *,
        keys: Collection[str],
        data_filter: DataFilter,
        patch: JsonDict,
        update_handler: Optional[UpdateHandler],
        patch_handler: PatchHandler,
        keys_done: set[str]
    ) -> dict[str, DbEntry]:
        """
        Helper for patch_all_entries method

        Note:
        The pipe argument does not exist as far as the caller of this method is concerned.
        That argument is automatically provided by the @redis_pipeline wrapper.

        Given a list of keys, tries to apply the specified patch to all of the keys
        which exist in the database and which meet the specified filter.

        Before attempting the patch, as keys are excluded based on
        the above criteria, they are added to 'keys_done'.

        If the patch fails because of database changes that occur,
        then a Redis WatchError will be raised, which the caller must
        handle as they see fit.

        If the patch succeeds, this function returns a mapping from keys to the
        corresponding patched entry data

        If no keys meet the criteria for patching, then this function
        will return an empty dict.
        """
        # Mapping from keys to the updated data
        patched_data_map: dict[str, DbEntry] = {}

        # If no keys were specified we're done already
        if not keys:
            return patched_data_map

        # Mapping from keys to the updated JSON-encoded data strings
        patched_datastr_map: dict[str, str] = {}

        # Start by watching all of the keys in this batch
        # This has to be done before we call mget for them.
        pipe.watch(*keys)

        # Retrieve all of the specified keys from the database
        # All database calls to pipe will be executed immediately,
        # since we have not yet called pipe.multi()
        data_str_list: list[None|str] = pipe.mget(*keys)

        for key, data_str in zip(keys, data_str_list):
            if not data_str:
                # Already empty, cannot patch it
                keys_done.add(key)
                continue
            orig_data = json.loads(data_str)
            # Data filtering happens here rather than after due to paging/memory constraints;
            # we can't load all data and then filter on the results
            if data_filter and not data_filter(orig_data):
                # This data does not match our filter, so skip it
                keys_done.add(key)
                continue

            # This key should be patched

            # Start by making a copy of the data, so that we can compare
            # the final patched version to it, and see if it has been changed
            # (this is necessary because many of the CFS patching functions change the
            # data in place, as well as returning it)
            new_data = copy.deepcopy(orig_data)

            # Apply the patch to the current data,
            # and call the update_handler
            new_data = patch_handler(new_data, patch)
            if update_handler:
                new_data = update_handler(new_data)

            # If this did not actually change the entry, then there is no need to
            # actually patch it
            if new_data == orig_data:
                keys_done.add(key)
                continue

            patched_data_map[key] = new_data
            patched_datastr_map[key] = json.dumps(new_data)

        if patched_datastr_map:
            # Begin our transaction
            # After this call to pipe.multi(), the database calls are NOT executed immediately.
            pipe.multi()

            # Queue the DB command to updated the specified keys with the patched data
            pipe.mset(patched_datastr_map)

            # Execute the pipeline
            #
            # At this point, if any entries still being watched have been changed since we
            # started watching them (including being created or deleted), then a Redis
            # WatchError exception will be raised and the mset command will not be executed.
            # The caller of this function will include logic that decides how to handle this.
            #
            # Instead, if none of those entries has changed, then Redis will atomically execute
            # all of the queued database commands. The return value of the pipe.execute()
            # call is a list with the return values for all of the queued DB commands. In this
            # case, that is just the mset call, and we do not care about its return value.
            # If the mset failed, it would raise an exception, and that's all we really care
            # about.
            pipe.execute()

        # If we get here, it means that either the pipe executed successfully, or (if we had no
        # keys to be patched in this batch) it was not executed at all. Either way, add the
        # patched keys (if any) to the done list, and then return the patched data map.
        keys_done.update(patched_data_map)
        return patched_data_map

    @convert_db_watch_errors
    def patch_all_return_entries(
        self,
        data_filter: DataFilter,
        patch: JsonDict,
        *,
        update_handler: Optional[UpdateHandler] = None,
        patch_handler: PatchHandler = patch_dict
    ) -> list[DbEntry]:
        """
        Patch multiple resources in the database.
        Returns a list of the patched entries (sorted by key order).
        Raises DBTooBusyError if unable to complete the patches in time.
        """
        # Set the time after which we will perform no more DB retries.
        # Note that this is not the same as it being a hard timeout. If no Redis
        # WatchErrors are raised after this time limit has been passed, then the method
        # will run to completion, regardless of how long it takes. This is solely
        # present to avoid a method which endlessly keeps retrying.
        no_retries_after: float = time.time() + DB_BUSY_SECONDS

        # keys_left starts being set to all of the keys in the database.
        # We will remove keys from it as we process them (either by patching
        # them or determining that they do not need to be patched).
        keys_left: list[str] = self.get_keys()

        # Mapping from keys to the updated data
        patched_data_map: dict[str, DbEntry] = {}

        # The patch_all_entries method has a big loop at its heart.
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
            # caused one of our patch operations to abort. Any other exceptions that arise are
            # not handled at this layer.
            try:
                # Process the keys in batches, rather than all at once.
                # See defs.py for details on DB_BATCH_SIZE.
                for key_batch in itertools.batched(keys_left, DB_BATCH_SIZE):
                    # Call our helper function on this batch of keys
                    # At the time of this writing, pylint is not clever enough to understand
                    # the function signature mutation performed by the @redis_pipeline
                    # decorator, and so it falsely reports that we are missing an argument
                    # here.
                    # pylint: disable=no-value-for-parameter
                    batch_patched_data_map = self._patch_batch(keys=key_batch,
                                                               data_filter=data_filter,
                                                               patch=patch,
                                                               update_handler=update_handler,
                                                               patch_handler=patch_handler,
                                                               keys_done=keys_done)
                    # If we get here, it means the patches (if any) completed successfully.
                    # The helper function will have already updated keys_done with any
                    # keys that did not need to be patched, and any keys that were patched.

                    # Update our master patched data map from this batch
                    patched_data_map.update(batch_patched_data_map)
            except redis.exceptions.WatchError as err:
                # This means one of the keys changed values between when we filtered it and when
                # we went to update the DB with the patched data.
                if time.time() > no_retries_after:
                    # We are past the last allowed retry time, so re-raise the exception
                    raise err

                # We are not past the time limit, so just log a warning and we'll go back to the
                # top of the loop.
                LOGGER.warning("Key changed (%s); retrying", err)

        # If we get here, it means we ended up processing all of the keys in our starting list.
        # So return a list of the patched data.
        # Sort the list by key, to preserve the previous behavior of this function.
        return [ patched_data_map[key] for key in sorted(patched_data_map) ]

    @convert_db_watch_errors
    def patch_all_return_keys(
        self,
        data_filter: DataFilter,
        patch: JsonDict,
        *,
        update_handler: Optional[UpdateHandler] = None,
        patch_handler: PatchHandler = patch_dict
    ) -> list[str]:
        """
        Patch multiple resources in the database.
        Return list of the IDs of the patched entries.
        Raises DBTooBusyError if unable to complete the patches in time.
        """
        # Set the time after which we will perform no more DB retries.
        # Note that this is not the same as it being a hard timeout. If no Redis
        # WatchErrors are raised after this time limit has been passed, then the method
        # will run to completion, regardless of how long it takes. This is solely
        # present to avoid a method which endlessly keeps retrying.
        no_retries_after: float = time.time() + DB_BUSY_SECONDS

        # keys_left starts being set to all of the keys in the database.
        # We will remove keys from it as we process them (either by patching
        # them or determining that they do not need to be patched).
        keys_left: list[str] = self.get_keys()

        # List of keys whose entries have been patched
        patched_ids: list[str] = []

        # The patch_all_entries method has a big loop at its heart.
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
            # caused one of our patch operations to abort. Any other exceptions that arise are
            # not handled at this layer.
            try:
                # Process the keys in batches, rather than all at once.
                # See defs.py for details on DB_BATCH_SIZE.
                for key_batch in itertools.batched(keys_left, DB_BATCH_SIZE):
                    # Call our helper function on this batch of keys
                    # At the time of this writing, pylint is not clever enough to understand
                    # the function signature mutation performed by the @redis_pipeline
                    # decorator, and so it falsely reports that we are missing an argument
                    # here.
                    # pylint: disable=no-value-for-parameter
                    batch_patched_data_map = self._patch_batch(keys=key_batch,
                                                               data_filter=data_filter,
                                                               patch=patch,
                                                               update_handler=update_handler,
                                                               patch_handler=patch_handler,
                                                               keys_done=keys_done)
                    # If we get here, it means the patches (if any) completed successfully.
                    # The helper function will have already updated keys_done with any
                    # keys that did not need to be patched, and any keys that were patched.

                    # Update patched_ids from this batch
                    # This appends the keys of batch_patched_data_map to the end of the patched_ids list.
                    patched_ids.extend(batch_patched_data_map)
            except redis.exceptions.WatchError as err:
                # This means one of the keys changed values between when we filtered it and when
                # we went to update the DB with the patched data.
                if time.time() > no_retries_after:
                    # We are past the last allowed retry time, so re-raise the exception
                    raise err

                # We are not past the time limit, so just log a warning and we'll go back to the
                # top of the loop.
                LOGGER.warning("Key changed (%s); retrying", err)

        # If we get here, it means we ended up processing all of the keys in our starting list.
        # So return a list of the patched ids.
        # Sort the list, to preserve the previous behavior of this function.
        return sorted(patched_ids)


    def _patch_list_load_entry_data(
        self,
        pipe: redis.client.Pipeline,
        *,
        unique_keys: Sequence[str]
    ) -> dict[str, DbEntry]:
        """
        Helper function for _patch_list

        Retrieves the entries for the specified keys from the database.
        If any of them do not exist, raise a DBNoEntryError
        Otherwise, JSON decode them, and return a dict mapping the keys to
        the decoded data.
        """
        # Retrieve all of the specified keys from the database
        # All database calls to pipe will be executed immediately,
        # since _patch_list has not yet called pipe.multi()
        data_str_list: list[None|str] = pipe.mget(*unique_keys)
        data_str_map: dict[str, str] = {}
        for key, data_str in zip(unique_keys, data_str_list):
            # To preserve the existing CFS behavior, if any entries are missing
            # in the database, we will raise an exception for the first one we find
            if not data_str:
                raise self.no_entry_exception(key)
            # This means data_str must be a str value
            data_str_map[key] = data_str

        # Return a mapping from the keys to the decoded JSON data
        return { key: json.loads(data_str)
                 for key, data_str in data_str_map.items() }

    @redis_pipeline
    def _patch_list(
        self,
        pipe: redis.client.Pipeline,
        *,
        key_patch_tuples: Sequence[tuple[str, DbEntry]],
        update_handler: Optional[UpdateHandler],
        patch_handler: PatchHandler
    ) -> list[tuple[str, DbEntry]]:
        """
        Helper for patch_list method

        Note:
        The pipe argument does not exist as far as the caller of this method is concerned.
        That argument is automatically provided by the @redis_pipeline wrapper.

        Given a list of (key, patch for that entry) tuples, apply the specified patch to the
        specified key for every tuple in the list.

        If any of the entries does not exist in the DB, none of the patches are applied, and
        DBNoEntryError is raised for the first missing entry that is identified.

        If the patch fails because of database changes that occur,
        then a Redis WatchError will be raised, which the caller must
        handle as they see fit.

        If the patch succeeds, this function returns a list of tuples that corresponds to
        the input list of tuples. The first element of each tuple will be the same as the
        input list tuples -- the DB key. The second element of each tuple will be the patched
        entry data AT THAT POINT. Emphasis because it is possible that the same key is listed
        twice. In that case, the first instance of that key in the output will show that entry
        after the first patch was applied, the second instance will show the entry after the
        second patch was applied, and so on. The final instance for any given DB key in the
        output reflects the value of that DB entry after all patches have been applied.

        Note that in the case that multiple patches are applied to the same key, the data is only
        written to the database once, with the final patched value. None of the intermediate
        patched values are written to the database.
        """
        # List of output tuples:
        key_patched_tuples: list[tuple[str, DbEntry]] = []

        # Extract the keys from the tuples
        all_keys = [ key for key, _ in key_patch_tuples ]

        # Create a list of the unique key values (since the same key could be in multiple tuples).
        # Sort this list in order of the earliest occurance in the input tuples list, to preserve
        # the existing CFS behavior.
        unique_keys: list[str] = sorted(set(all_keys), key=all_keys.index)

        # Start by watching all of the keys
        # This has to be done before we call mget for them.
        pipe.watch(*unique_keys)

        # Load the data for the keys from the database, and JSON decode it
        orig_data_map = self._patch_list_load_entry_data(pipe, unique_keys=unique_keys)
        patched_data_map = orig_data_map.copy()

        for key, patch in key_patch_tuples:
            # Apply the patch to the current data,
            # and call the update_handler
            orig_data = copy.deepcopy(patched_data_map[key])
            new_data = patch_handler(orig_data, patch)
            if update_handler:
                new_data = update_handler(new_data)

            patched_data_map[key] = new_data
            key_patched_tuples.append((key, new_data))

        # Make a new map which consists of all DB data that has actually changed
        # as a result of the patch, encoded into JSON data strings
        patched_datastr_map = { key: json.dumps(patched_data)
                                for key, patched_data in patched_data_map.items()
                                if patched_data != orig_data[key] }

        if patched_datastr_map:
            # Begin our transaction
            # After this call to pipe.multi(), the database calls are NOT executed immediately.
            pipe.multi()

            # Queue the DB command to updated the specified keys with the patched data
            pipe.mset(patched_datastr_map)

            # Execute the pipeline
            #
            # At this point, if any entries still being watched have been changed since we
            # started watching them (including being created or deleted), then a Redis
            # WatchError exception will be raised and the mset command will not be executed.
            # The caller of this function will include logic that decides how to handle this.
            #
            # Instead, if none of those entries has changed, then Redis will atomically execute
            # all of the queued database commands. The return value of the pipe.execute()
            # call is a list with the return values for all of the queued DB commands. In this
            # case, that is just the mset call, and we do not care about its return value.
            # If the mset failed, it would raise an exception, and that's all we really care
            # about.
            pipe.execute()

        # If we get here, it means that either the pipe executed successfully, or none of the
        # patches actually resulted in DB data changing. Either way, return.
        return key_patched_tuples

    @convert_db_watch_errors
    def patch_list(
        self,
        key_patch_tuples: Sequence[tuple[str, JsonDict]],
        *,
        update_handler: Optional[UpdateHandler] = None,
        patch_handler: PatchHandler = patch_dict
    ) -> list[tuple[str, DbEntry]]:
        """
        Input is a list of tuples: (DB key, patch for that entry)
        The specified patch is applied to the specified DB entry, for all tuples.
        Returns a list of tuples of the entries that were patched. The tuples consist of the
        DB key and the associated patched data.
        If any of the entries does not exist in the DB, none of the patches are applied, and
        DBNoEntryError is raised.
        Raises DBTooBusyError if unable to complete the patches in time.
        """
        # Set the time after which we will perform no more DB retries.
        # Note that this is not the same as it being a hard timeout. If no Redis
        # WatchErrors are raised after this time limit has been passed, then the method
        # will run to completion, regardless of how long it takes. This is solely
        # present to avoid a method which endlessly keeps retrying.
        no_retries_after: float = time.time() + DB_BUSY_SECONDS

        # Keep looping until the patch succeeds or we run out of time.
        #
        # The stop conditions are handled inside the loop, so the check condition here
        # is just True
        while True:
            # The main work in the loop is enclosed in this try/except block.
            # This is to catch Redis WatchErrors, which are raised when a change to the database
            # caused our patch operation to abort. Any other exceptions that arise are
            # not handled at this layer.
            try:
                # Because the patch data is being sent to us as a list, it should be short enough
                # that we do not require batch processing. This also means we can guarantee that
                # either the entire patch is applied or none of it.
                #
                # Call our helper function to patch the data and return the result
                # At the time of this writing, pylint is not clever enough to understand
                # the function signature mutation performed by the @redis_pipeline
                # decorator, and so it falsely reports that we are missing an argument
                # here.
                # pylint: disable=no-value-for-parameter
                return self._patch_list(key_patch_tuples=key_patch_tuples,
                                        update_handler=update_handler,
                                        patch_handler=patch_handler)
            except redis.exceptions.WatchError as err:
                # This means one of the keys changed values between when we filtered it and when
                # we went to update the DB with the patched data.
                if time.time() > no_retries_after:
                    # We are past the last allowed retry time, so re-raise the exception
                    raise err

                # We are not past the time limit, so just log a warning and we'll go back to the
                # top of the loop.
                LOGGER.warning("Key changed (%s); retrying", err)

    @convert_db_watch_errors
    def delete(self, key: DbKey) -> None:
        """
        Deletes data from the database.
        This is just a wrapper for the get_delete method,
        but it discards the return value.
        """
        self.get_delete(key)

    @redis_pipeline
    def _conditional_delete(
        self,
        pipe: redis.client.Pipeline,
        *,
        key: DbKey,
        deletion_checker: EntryChecker
    ) -> bool:
        """
        Helper for conditional_delete method

        Note:
        The pipe argument does not exist as far as the caller of this method is concerned.
        That argument is automatically provided by the @redis_pipeline wrapper.

        Does the exact procedure described in the conditional_delete docstring,
        just inside a Redis pipeline
        """
        # Mark this key for monitoring before we retrieve its data,
        # so that we know if it changes underneath us.
        pipe.watch(key)

        # Because we have not yet called pipe.multi(), this DB
        # get call will execute immediately and return the data.
        data_str = pipe.get(key)

        # Raise an exception if no or null entry.
        if not data_str:
            raise self.no_entry_exception(key)

        # Decode the data
        data = json.loads(data_str)

        # Call our checker
        if not deletion_checker(data):
            # It says not to delete this, so return False
            return False

        # This means we should delete it

        # Begin our transaction.
        pipe.multi()

        # Because this is after the pipe.multi() call, the following
        # delete command is not executed immediately, and instead is
        # queued up. The return value from this call is not the
        # return value for the actual DB call.
        pipe.delete(key)

        # Calling pipe.execute() does one of two things:
        # 1. If the key we are watching has not changed since we started
        #    watching it, then our DB set operation will be executed.
        #    The return value of pipe.execute() is a list of the responses
        #    from all of the queued DB commands (in our case, just a single
        #    command -- the delete). We don't really care about the response.
        #    If the delete failed, Redis would raise an exception, and that's
        #    all we're really concerned about.
        # 2. If they key we are watching DID change, then the queued DB
        #    operations (the delete, in our case) are aborted and a Redis WatchError
        #    exception will be raised, and the caller will have to decide how
        #    to deal with it.
        pipe.execute()

        # If we get here, it means no exception was raised by pipe.execute, so we
        # should return True to indicate that we deleted the entry
        return True


    @convert_db_watch_errors
    def conditional_delete(self, key: DbKey, deletion_checker: EntryChecker) -> bool:
        """
        Retrieve the current entry data.
        If the entry does not exist, raise DBNoEntryError.
        Otherwise, call delete_check with the entry data.
        If delete_check returns True, delete the entry.
        If delete_check returns False, do not delete the entry.

        Returns:
        True if we deleted the entry,
        False otherwise
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
                # Call a helper function to try and delete the entry. If it is successful,
                # it will return True (meaning it deleted it) or False (meaning the check
                # function said it should not be deleted). Either way, we just return what
                # it gives us. (If the entry does not exist, it will raise a DBNoEntryError)
                #
                # At the time of this writing, pylint is not clever enough to understand
                # the function signature mutation performed by the @redis_pipeline
                # decorator, and so it falsely reports that we are missing an argument
                # here.
                # pylint: disable=no-value-for-parameter
                return self._conditional_delete(key=key,
                                                deletion_checker=deletion_checker)
            except redis.exceptions.WatchError as err:
                # This means the entry changed values while the helper function was
                # trying to delete it.
                if time.time() > no_retries_after:
                    # We are past the last allowed retry time, so re-raise the exception
                    raise err

                # We are not past the time limit, so just log a warning and we'll go back to the
                # top of the loop.
                LOGGER.warning("Key '%s' changed (%s); retrying", key, err)


    @redis_pipeline
    def _delete_batch(
        self,
        pipe: redis.client.Pipeline,
        *,
        keys: Collection[str],
        data_filter: DataFilter,
        keys_done: set[str]
    ) -> dict[str, DbEntry]:
        """
        Helper for delete_all method

        Note:
        The pipe argument does not exist as far as the caller of this method is concerned.
        That argument is automatically provided by the @redis_pipeline wrapper.

        Given a list of keys, tries to delete all of the keys
        which exist in the database and which meet the specified
        filter (if one is specified).

        Before attempting the delete, as keys are excluded based on
        the above criteria, they are added to 'keys_done'.

        If the delete fails because of database changes that occur,
        then a Redis WatchError will be raised, which the caller must
        handle as they see fit.

        If the delete succeeds, this function returns a mapping from keys to the
        deleted entry data.

        If no keys meet the criteria for deletion, then this function
        will return an empty dict.
        """
        # Mapping from keys to their deleted entry data
        deleted_data_map: dict[str, DbEntry] = {}

        # If no keys are specified as input, we are done
        if not keys:
            return deleted_data_map

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
            deleted_data_map[key] = data

        # For the keys that we are not going to delete, the above loop
        # adds them to the keys_done set, so we don't check them again on
        # future iterations (if we have to re-try because of database changes)
        # We would also like to stop watching them, but unfortunately that is
        # not possible.

        if deleted_data_map:
            # Begin our transaction
            # After this call to pipe.multi(), the database calls are NOT executed immediately.
            pipe.multi()

            # Queue the DB command to delete the specified keys
            pipe.delete(*deleted_data_map)

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
        # keys to be deleted in this batch) it was not executed at all. Either way, add the
        # deleted keys (if any) to keys_done and then return the list of keys and data that we
        # deleted.
        keys_done.update(deleted_data_map)
        return deleted_data_map

    @convert_db_watch_errors
    def delete_all(
        self,
        data_filter: DataFilter,
        *,
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
                    # At the time of this writing, pylint is not clever enough to understand
                    # the function signature mutation performed by the @redis_pipeline
                    # decorator, and so it falsely reports that we are missing an argument
                    # here.
                    # pylint: disable=no-value-for-parameter
                    batch_deleted_data_map = self._delete_batch(keys=key_batch,
                                                                data_filter=data_filter,
                                                                keys_done=keys_done)
                    # If we get here, it means the deletes completed successfully.
                    # The helper function will have updated keys_done with the keys that
                    # were deleted and the keys that did not need to be deleted.

                    # Add the deleted keys to our master list of deleted keys
                    deleted_keys.extend(batch_deleted_data_map)

                    # If there is no deletion_handler, we can go to the next batch
                    if not deletion_handler:
                        continue

                    # If there is a deletion_handler, apply it to the entries that
                    # were deleted
                    for data in batch_deleted_data_map.values():
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
