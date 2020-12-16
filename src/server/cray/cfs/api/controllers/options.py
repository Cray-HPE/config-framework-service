# Copyright 2020 Hewlett Packard Enterprise Development LP

import logging
import connexion


from cray.cfs.api import dbutils
from cray.cfs.api.models.v1_options import V1Options
from cray.cfs.api.models.v2_options import V2Options

LOGGER = logging.getLogger('cray.cfs.api.controllers.options')
DB = dbutils.get_wrapper(db='options')
# We store all options as json under this key so that the data format is
# similar to other data stored in the database, and to make retrieval of all
# options simpler
OPTIONS_KEY = 'options'
DEFAULTS = {
    'defaultCloneUrl': 'https://api-gw-service-nmn.local/vcs/cray/config-management.git',  # noqa: E501
    'defaultPlaybook': 'site.yml',
    'defaultAnsibleConfig': 'cfs-default-ansible-cfg',
}


def _init(namespace='services'):
    """ Cleanup old options """
    data = DB.get(OPTIONS_KEY)
    if not data:
        return
    # Cleanup
    to_delete = []
    all_options = set(V1Options().attribute_map.values()) | set(V2Options().attribute_map.values())
    for key in data:
        if key not in all_options:
            to_delete.append(key)
    for key in to_delete:
        del data[key]
    DB.put(OPTIONS_KEY, data)


@dbutils.redis_error_handler
def get_options():
    """Used by the GET /options API operation"""
    LOGGER.debug("GET /options invoked get_options")
    data = get_options_data()
    to_delete = []
    for key in data:
        if key not in V1Options().attribute_map.values():
            to_delete.append(key)
    for key in to_delete:
        del data[key]
    return data, 200


def get_options_data():
    return _check_defaults(DB.get(OPTIONS_KEY))


def _check_defaults(data):
    """Adds defaults to the options data if they don't exist"""
    put = False
    if not data:
        data = {}
        put = True
    for key in DEFAULTS:
        if key not in data:
            data[key] = DEFAULTS[key]
            put = True
    if put:
        return DB.put(OPTIONS_KEY, data)
    return data


@dbutils.redis_error_handler
def patch_options():
    """Used by the PATCH /options API operation"""
    LOGGER.debug("PATCH /options invoked patch_options")
    try:
        data = connexion.request.get_json()
    except Exception as err:
        return connexion.problem(
            status=400, title="Error parsing the data provided.",
            detail=str(err))
    if OPTIONS_KEY not in DB:
        DB.put(OPTIONS_KEY, {})
    return DB.patch(OPTIONS_KEY, data), 200


# V2 #
@dbutils.redis_error_handler
def get_options_v2():
    """Used by the GET /options API operation"""
    LOGGER.debug("GET /options invoked get_options")
    data = get_options_data()
    to_delete = []
    for key in data:
        if key not in V2Options().attribute_map.values():
            to_delete.append(key)
    for key in to_delete:
        del data[key]
    return data, 200


patch_options_v2 = patch_options


class Options():
    """Helper class for other endpoints that need access to options"""
    def get_option(self, key, type):
        if not hasattr(self, 'options'):
            self.options = get_options_data()
        return type(self.options[key])

    @property
    def batcher_check_interval(self):
        return self.get_option('batcherCheckInterval', int)

    @property
    def batch_size(self):
        return self.get_option('batchSize', int)

    @property
    def batch_window(self):
        return self.get_option('batchWindow', int)

    @property
    def default_batcher_retry_policy(self):
        return self.get_option('defaultBatcherRetryPolicy', int)

    @property
    def default_clone_url(self):
        return self.get_option('defaultCloneUrl', str)

    @property
    def default_playbook(self):
        return self.get_option('defaultPlaybook', str)
