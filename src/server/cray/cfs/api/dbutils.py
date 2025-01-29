#
# MIT License
#
# (C) Copyright 2019-2024 Hewlett Packard Enterprise Development LP
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
import connexion
import ujson as json
import logging
import redis
from typing import Optional

from kubernetes import config, client
from kubernetes.config.config_exception import ConfigException

from cray.cfs.api.models.base_model import Model as BaseModel

LOGGER = logging.getLogger(__name__)
DATABASES = ["options", "sessions", "components", "configurations", "sources"]  # Index is the db id.

try:
    config.load_incluster_config()
except ConfigException:  # pragma: no cover
    config.load_kube_config()  # Development

_api_client = client.ApiClient()
k8ssvcs = client.CoreV1Api(_api_client)
svc_obj = k8ssvcs.read_namespaced_service("cray-cfs-api-db", "services")
DB_HOST = svc_obj.spec.cluster_ip
DB_PORT = 6379


class DBWrapper():
    """A wrapper around a Redis database connection

    The handles creating the Redis client and provides REST-like methods for
    modifying json data in the database.

    Because the underlying Redis client is threadsafe, this class is as well,
    and can be safely shared by multiple threads.
    """

    def __init__(self, db):
        db_id = self._get_db_id(db)
        self.client = self._get_client(db_id)

    def __contains__(self, key):
        return self.client.exists(key)

    def _get_db_id(self, db):
        """Converts a db name to the id used by Redis."""
        if isinstance(db, int):
            return db
        else:
            return DATABASES.index(db)

    def _get_client(self, db_id):
        """Create a connection with the database."""
        try:
            LOGGER.debug("Creating database connection"
                         "host: %s port: %s database: %s",
                         DB_HOST, DB_PORT, db_id)
            return redis.Redis(host=DB_HOST, port=DB_PORT, db=db_id)
        except Exception as err:
            LOGGER.error("Failed to connect to database %s : %s",
                         db_id, err)
            raise

    # The following methods act like REST calls for single items
    def get(self, key):
        """Get the data for the given key."""
        datastr = self.client.get(key)
        if not datastr:
            return None
        data = json.loads(datastr)
        return data


    def iter_values(self, start_after_key: Optional[str] = None):
        """
        Iterate through every item in the database. Parse each item as JSON and yield it.
        If start_after_key is specified, skip any keys that are lexically <= the specified key.
        """
        all_keys = sorted({k.decode() for k in self.client.scan_iter()})
        if start_after_key is not None:
            all_keys = [k for k in all_keys if k > start_after_key]
        while all_keys:
            for datastr in self.client.mget(all_keys[:500]):
                yield json.loads(datastr) if datastr else None
            all_keys = all_keys[500:]


    def get_all(self, limit=0, after_id=None, data_filter=None):
        """Get an array of data for all keys."""

        if limit < 0:
            limit = 0
        page_full = False
        next_page_exists = False
        data_page = []
        for data in self.iter_values(after_id):
            if not data_filter or data_filter(data):
                # filtering happens in get_all rather than after due to paging/memory constraints
                #   we can't load all data and then filter on the results
                if page_full:
                    next_page_exists = True
                    break
                else:
                    data_page.append(data)
                    if limit and len(data_page) >= limit:
                        page_full = True
        return data_page, next_page_exists


    def get_keys(self):
        keys = set()
        for key in self.client.scan_iter():
            keys.add(key)
        # Sorting the keys guarantees a consistent order when paging
        sorted_keys = sorted(list(keys))
        return sorted_keys

    def put(self, key, new_data):
        """Put data into the database, replacing any old data."""
        datastr = json.dumps(new_data)
        self.client.set(key, datastr)
        return self.get(key)

    def patch(self, key, new_data, update_handler=None):
        """Patch data in the database."""
        """update_handler provides a way to operate on the full patched data"""
        data_str = self.client.get(key)
        data = json.loads(data_str)
        data = self._update(data, new_data)
        if update_handler:
            data = update_handler(data)
        data_str = json.dumps(data)
        self.client.set(key, data_str)
        data = self.get(key)
        return data

    def patch_all(self, data_filter, patch, update_handler=None):
        """Patch multiple resources in the database."""
        # Redis SCAN operations can produce duplicate results.  Using a set fixes this.
        keys = set()
        for key in self.client.scan_iter():
            keys.add(key)
        # Sorting the keys guarantees a consistent order when paging
        sorted_keys = sorted(list(keys))
        patched_id_list = []
        for key in sorted_keys:
            data_str = self.client.get(key)
            data = json.loads(data_str)
            if not data_filter or data_filter(data):
                # filtering happens in get_all rather than after due to paging/memory constraints
                #   we can't load all data and then filter on the results
                data = self._update(data, patch)
                if update_handler:
                    data = update_handler(data)
                data_str = json.dumps(data)
                self.client.set(key, data_str)
                # Decode the key into a UTF-8 string, so the list will be JSON serializable
                patched_id_list.append(key.decode('utf-8'))
        return patched_id_list

    def _update(self, data, new_data):
        """Recursively patches JSON to allow sub-fields to be patched."""
        for k, v in new_data.items():
            if isinstance(v, dict):
                data[k] = self._update(data.get(k, {}), v)
            else:
                data[k] = v
        return data

    def delete(self, key):
        """Deletes data from the database."""
        self.client.delete(key)

    def delete_all(self, data_filter, deletion_handler=None):
        """Delete multiple resources in the database."""
        # Redis SCAN operations can produce duplicate results.  Using a set fixes this.
        keys = set()
        for key in self.client.scan_iter():
            keys.add(key)
        # Sorting the keys guarantees a consistent order when paging
        sorted_keys = sorted(list(keys))

        deleted_id_list = []
        for key in sorted_keys:
            data_str = self.client.get(key)
            data = json.loads(data_str)
            if not data_filter or data_filter(data):
                self.client.delete(key)
                if deletion_handler:
                    deletion_handler(data)
                # Decode the key into a UTF-8 string, so the list will be JSON serializable
                deleted_id_list.append(key.decode('utf-8'))
        return deleted_id_list

    def info(self):
        """Returns the database info."""
        return self.client.info()


def redis_error_handler(func):
    """Decorator for returning better errors if Redis is unreachable"""
    def wrapper(*args, **kwargs):
        try:
            if 'body' in kwargs:
                # Our get/patch functions don't take body, but the **kwargs
                # in the arguments to this wrapper cause it to get passed.
                del kwargs['body']
            return func(*args, **kwargs)
        except redis.exceptions.ConnectionError as e:
            LOGGER.error('Unable to connect to the Redis database: {}'.format(e))
            return connexion.problem(
                status=503, title='Unable to connect to the Redis database',
                detail=str(e))
    return wrapper


def get_wrapper(db):
    """Returns a database object."""
    return DBWrapper(db)


def convert_data_to_v2(data, model_type):
    """
    When exporting from a model with to_dict, all keys are in snake_case.  However the model contains the information
        on the keys in the api spec.  This gives the ability to make the data match the given model/spec, which
        is useful when translating between the v2 and v3 api.
    Data must start in the v3 format exported by model().to_dict()
    """
    result = {}
    model = model_type()
    for attribute, attribute_key in model.attribute_map.items():
        if attribute in data:
            data_type = model.openapi_types[attribute]
            result[attribute_key] = _convert_data_to_v2(data[attribute], data_type)
    return result


def _convert_data_to_v2(data, data_type):
    if not isinstance(data_type, type):
        # Special case where the data_type is a "typing" object.  e.g typing.Dict
        if not data:
            return data
        elif data_type.__origin__ == list:
            return [_convert_data_to_v2(item_data, data_type.__args__[0])
                    for item_data in data]
        elif data_type.__origin__ == dict:
            return {key: _convert_data_to_v2(item_data, data_type.__args__[1])
                    for key, item_data in data.items()}
    elif issubclass(data_type, BaseModel):
        if not data:
            data = {}
        return convert_data_to_v2(data, data_type)
    return data


def convert_data_from_v2(data, model_type):
    """
    When exporting from a model with to_dict, all keys are in snake_case.  However the model contains the information
        on the keys in the api spec.  This gives the ability to make the data match the given model/spec, which
        is useful when translating between the v2 and v3 api.
    Data must start in the v3 format exported by model().to_dict()
    """
    result = {}
    model = model_type()
    for attribute_key, attribute in model.attribute_map.items():
        if attribute in data:
            data_type = model.openapi_types[attribute_key]
            result[attribute_key] = _convert_data_from_v2(data[attribute], data_type)
    return result


def _convert_data_from_v2(data, data_type):
    if not isinstance(data_type, type):
        # Special case where the data_type is a "typing" object.  e.g typing.Dict
        if not data:
            return data
        elif data_type.__origin__ == list:
            return [_convert_data_from_v2(item_data, data_type.__args__[0])
                    for item_data in data]
        elif data_type.__origin__ == dict:
            return {key: _convert_data_from_v2(item_data, data_type.__args__[1])
                    for key, item_data in data.items()}
    elif issubclass(data_type, BaseModel):
        if not data:
            data = {}
        return convert_data_from_v2(data, data_type)
    return data
