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

from collections.abc import Callable, Generator, Iterable
import functools
import logging
from typing import Any, Literal, Optional, ParamSpec, TypeVar

import connexion
from connexion.lifecycle import ConnexionResponse as CxResponse
from kubernetes import config, client
from kubernetes.config.config_exception import ConfigException
import redis
import ujson as json

from cray.cfs.api.models.base_model import Model as BaseModel

# Definitions for type hinting
type JsonData = bool | str | None | int | float | list[JsonData] | dict[str, JsonData]
type JsonDict = dict[str, JsonData]
type JsonList = list[JsonData]
# All CFS database entries are dicts that are stored in JSON.
type DbEntry = JsonDict
type DbKey = str | bytes
DatabaseNames = Literal["options", "sessions", "components", "configurations", "sources"]
type DbIdentifier = DatabaseNames | int
type DataFilter = Callable[[DbEntry], bool]
type UpdateHandler = Callable[[DbEntry], DbEntry]
type DeletionHandler = Callable[[DbEntry], None]

LOGGER = logging.getLogger(__name__)
DATABASES: list[DatabaseNames] = ["options",
                                  "sessions",
                                  "components",
                                  "configurations",
                                  "sources"]  # Index is the db id.

try:
    config.load_incluster_config()
except ConfigException:  # pragma: no cover
    config.load_kube_config()  # Development

_api_client = client.ApiClient()
k8ssvcs = client.CoreV1Api(_api_client)
svc_obj = k8ssvcs.read_namespaced_service("cray-cfs-api-db", "services")
DB_HOST = svc_obj.spec.cluster_ip
DB_PORT = 6379


class DBWrapper:
    """A wrapper around a Redis database connection

    The handles creating the Redis client and provides REST-like methods for
    modifying json data in the database.

    Because the underlying Redis client is threadsafe, this class is as well,
    and can be safely shared by multiple threads.
    """

    def __init__(self, db: DbIdentifier) -> None:
        db_id = self._get_db_id(db)
        self.client = self._get_client(db_id)

    def __contains__(self, key: DbKey) -> bool:
        return self.client.exists(key)

    def _get_db_id(self, db: DbIdentifier) -> int:
        """Converts a db name to the id used by Redis."""
        return db if isinstance(db, int) else DATABASES.index(db)

    def _get_client(self, db_id: int) -> redis.client.Redis:
        """Create a connection with the database."""
        LOGGER.debug("Creating database connection"
                     "host: %s port: %s database: %s",
                     DB_HOST, DB_PORT, db_id)
        try:
            return redis.Redis(host=DB_HOST, port=DB_PORT, db=db_id, protocol=3)
        except Exception as err:
            LOGGER.error("Failed to connect to database %s : %s",
                         db_id, err)
            raise

    # The following methods act like REST calls for single items
    def get(self, key: DbKey) -> Optional[DbEntry]:
        """Get the data for the given key, or None if the entry does not exist."""
        datastr = self.client.get(key)
        return json.loads(datastr) if datastr else None

    def get_delete(self, key: DbKey) -> Optional[DbEntry]:
        """
        Get the data for the given key from the database, and delete it from the DB.
        Returns the data (or None if the entry does not exist).
        """
        datastr = self.client.getdel(key)
        return json.loads(datastr) if datastr else None

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

    def put(self, key: DbKey, new_data: DbEntry) -> Optional[DbEntry]:
        """Put data into the database, replacing any old data."""
        datastr = json.dumps(new_data)
        self.client.set(key, datastr)
        return self.get(key)

    def patch(
        self, key: DbKey,
        new_data: DbEntry,
        update_handler: Optional[UpdateHandler] = None
    ) -> Optional[DbEntry]:
        """
        Patch data in the database.
        update_handler provides a way to operate on the full patched data
        """
        data_str = self.client.get(key)
        data = json.loads(data_str)
        data = self._update(data, new_data)
        if update_handler:
            data = update_handler(data)
        data_str = json.dumps(data)
        self.client.set(key, data_str)
        return self.get(key)

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

    def delete(self, key: DbKey) -> None:
        """Deletes data from the database."""
        self.client.delete(key)

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


P = ParamSpec('P')
R = TypeVar('R')

def redis_error_handler(func: Callable[P, R]) -> Callable[P, R|CxResponse]:
    """Decorator for returning better errors if Redis is unreachable"""

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R | CxResponse:
        # Our get/patch functions don't take body, but the **kwargs
        # in the arguments to this wrapper cause it to get passed.
        kwargs.pop('body', None)
        try:
            return func(*args, **kwargs)
        except redis.exceptions.ConnectionError as e:
            LOGGER.error('Unable to connect to the Redis database: %s', e)
            return connexion.problem(
                status=503,
                title='Unable to connect to the Redis database',
                detail=str(e))

    return wrapper


def get_wrapper(db: DbIdentifier) -> DBWrapper:
    """Returns a database object."""
    return DBWrapper(db)


def convert_data_to_v2(data: JsonDict, model_type: BaseModel) -> JsonDict:
    """
    When exporting from a model with to_dict, all keys are in snake_case.  However the model
        contains the information on the keys in the api spec.  This gives the ability to make the
        data match the given model/spec, which is useful when translating between the v2 and v3 api
    Data must start in the v3 format exported by model().to_dict()
    """
    result = {}
    model = model_type()
    for attribute, attribute_key in model.attribute_map.items():
        if attribute in data:
            data_type = model.openapi_types[attribute]
            result[attribute_key] = _convert_data_to_v2(data[attribute], data_type)
    return result


def _convert_data_to_v2(data: JsonDict, data_type: Any) -> JsonDict:
    if not isinstance(data_type, type):
        # Special case where the data_type is a "typing" object.  e.g typing.Dict
        if not data:
            return data
        if data_type.__origin__ == list:
            return [_convert_data_to_v2(item_data, data_type.__args__[0])
                    for item_data in data]
        if data_type.__origin__ == dict:
            return {key: _convert_data_to_v2(item_data, data_type.__args__[1])
                    for key, item_data in data.items()}
    elif issubclass(data_type, BaseModel):
        if not data:
            data = {}
        return convert_data_to_v2(data, data_type)
    return data


def convert_data_from_v2(data: JsonDict, model_type: BaseModel) -> JsonDict:
    """
    When exporting from a model with to_dict, all keys are in snake_case.  However the model
        contains the information on the keys in the api spec.  This gives the ability to make the
        data match the given model/spec, which is useful when translating between the v2 and v3 api
    Data must start in the v3 format exported by model().to_dict()
    """
    result = {}
    model = model_type()
    for attribute_key, attribute in model.attribute_map.items():
        if attribute in data:
            data_type = model.openapi_types[attribute_key]
            result[attribute_key] = _convert_data_from_v2(data[attribute], data_type)
    return result


def _convert_data_from_v2(data: JsonDict, data_type: Any) -> JsonDict:
    if not isinstance(data_type, type):
        # Special case where the data_type is a "typing" object.  e.g typing.Dict
        if not data:
            return data
        if data_type.__origin__ == list:
            return [_convert_data_from_v2(item_data, data_type.__args__[0])
                    for item_data in data]
        if data_type.__origin__ == dict:
            return {key: _convert_data_from_v2(item_data, data_type.__args__[1])
                    for key, item_data in data.items()}
    elif issubclass(data_type, BaseModel):
        if not data:
            data = {}
        return convert_data_from_v2(data, data_type)
    return data
