#
# MIT License
#
# (C) Copyright 2020-2023 Hewlett Packard Enterprise Development LP
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
import logging
import connexion
import threading
import time

from cray.cfs.api import dbutils
from cray.cfs.api.models.v2_options import V2Options
from cray.cfs.api.models.v3_options import V3Options

LOGGER = logging.getLogger('cray.cfs.api.controllers.options')
DB = dbutils.get_wrapper(db='options')
# We store all options as json under this key so that the data format is
# similar to other data stored in the database, and to make retrieval of all
# options simpler
OPTIONS_KEY = 'options'
DEFAULTS = {
    'default_playbook': 'site.yml',
    'default_ansible_config': 'cfs-default-ansible-cfg',
    'logging_level': 'INFO',
    'default_page_size': 1000,
}


def _init():
    # Start options refresh
    options_refresh = threading.Thread(target=periodically_refresh_options, args=())
    options_refresh.start()


def cleanup_old_options():
    data = DB.get(OPTIONS_KEY)
    if not data:
        return
    # Cleanup
    model_data = V2Options.from_dict(data).to_dict() | V3Options.from_dict(data).to_dict()
    clean_data = {k: v for k, v in model_data.items() if v is not None}
    DB.put(OPTIONS_KEY, clean_data)


@dbutils.redis_error_handler
def get_options_v2():
    """Used by the GET /options API operation"""
    LOGGER.debug("GET /options invoked get_options")
    data = get_options_data()
    response = convert_options_to_v2(data)
    return response, 200


@dbutils.redis_error_handler
def get_options_v3():
    """Used by the GET /options API operation"""
    LOGGER.debug("GET /options invoked get_options")
    response = get_options_data()
    return response, 200


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
def patch_options_v2():
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
    data = dbutils.convert_data_from_v2(data, V2Options)
    result = DB.patch(OPTIONS_KEY, data)
    return dbutils.convert_data_to_v2(result, V2Options), 200


@dbutils.redis_error_handler
def patch_options_v3():
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


class Options:
    """Helper class for other endpoints that need access to options"""
    def __new__(cls):
        """This override makes the class a singleton"""
        if not hasattr(cls, 'instance'):
            cls.instance = super(Options, cls).__new__(cls)
            cls.instance.__init__()
        return cls.instance

    def __init__(self):
        self.options = None

    def refresh(self):
        self.options = get_options_data()

    def get_option(self, key, data_type, default=None):
        if not self.options:
            self.refresh()
        try:
            return data_type(self.options[key])
        except KeyError as e:
            if default is not None:
                LOGGER.warning(
                    'Option {} has not been initialized.  Defaulting to {}'.format(key, default))
                return default
            else:
                LOGGER.error('Option {} has not been initialized.'.format(key))
                raise e

    @property
    def batcher_check_interval(self):
        return self.get_option('batcher_check_interval', int, default=60)

    @property
    def batch_size(self):
        return self.get_option('batch_size', int, default=100)

    @property
    def batch_window(self):
        return self.get_option('batch_window', int, default=60)

    @property
    def default_ansible_config(self):
        return self.get_option('default_ansible_config', str, default='cfs-default-ansible-cfg')

    @property
    def default_batcher_retry_policy(self):
        return self.get_option('default_batcher_retry_policy', int, default=1)

    @property
    def default_playbook(self):
        return self.get_option('default_playbook', str)

    @property
    def default_page_size(self):
        return self.get_option('default_page_size', int, default=1000)

    @property
    def logging_level(self):
        return self.get_option('logging_level', str)


def update_log_level(new_level_str):
    new_level = logging.getLevelName(new_level_str.upper())
    current_level = LOGGER.getEffectiveLevel()
    if current_level != new_level:
        LOGGER.log(current_level, 'Changing logging level from {} to {}'.format(
            logging.getLevelName(current_level), logging.getLevelName(new_level)))
        logger = logging.getLogger()
        logger.setLevel(new_level)
        LOGGER.log(new_level, 'Logging level changed from {} to {}'.format(
            logging.getLevelName(current_level), logging.getLevelName(new_level)))


def periodically_refresh_options():
    """Caching and refreshing options saves time during calls"""
    options = Options()
    while True:
        try:
            options.refresh()
            if options.logging_level:
                update_log_level(options.logging_level)
        except Exception as e:
            LOGGER.debug(e)
        time.sleep(2)


def convert_options_to_v2(data):
    data = dbutils.convert_data_to_v2(data, V2Options)
    return data


def defaults(**default_kwargs):
    """
    Allows controller functions to specify parameters that have defaults stored in options
    """
    def wrap(f):
        def wrapped_f(*args, **kwargs):
            options = Options()
            options.refresh()
            for key in default_kwargs:
                if key not in kwargs:
                    kwargs[key] = getattr(options, default_kwargs[key])
            return f(*args, **kwargs)
        return wrapped_f
    return wrap
