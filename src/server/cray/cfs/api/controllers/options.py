#
# MIT License
#
# (C) Copyright 2020-2025 Hewlett Packard Enterprise Development LP
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

from collections.abc import Callable
import functools
import logging
import threading
from typing import overload, Literal, NewType

import connexion
from connexion.lifecycle import ConnexionResponse as CxResponse

from cray.cfs.api import dbutils
from cray.cfs.api.dbutils import JsonData, JsonDict
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
    'include_ara_links': True,
}

# Prevent multiple threads from updating the log level at the same time
# (mainly to avoid noise in the log)
LogLevelUpdateLock = threading.Lock()

# Rudimentary type hint definitions

V2OptionsData = NewType("V2OptionsData", JsonDict)
V2OptionsPatch = NewType("V2OptionsPatch", JsonDict)
V3OptionsData = NewType("V3OptionsData", JsonDict)
V3OptionsPatch = NewType("V3OptionsPatch", JsonDict)

# Even though it does not follow convention, a successful patch request to
# both the CFS V2 and V3 endpoints results in a 200 status code
type V2PatchOptionsResponse = tuple[V2OptionsData, Literal[200]] | CxResponse
type V3PatchOptionsResponse = tuple[V3OptionsData, Literal[200]] | CxResponse


def _init():
    """
    Called by cray.cfs.api.__main__ on server startup
    """
    cleanup_old_options()
    update_server_log_level()


def patch_options_db(
    patch_data: V3OptionsPatch,
    **kwargs
) -> V3OptionsData:
    """
    By specifying a copy of the defaults dictionary as the default_entry, this
    means that if the options data is not in the DB, the default values will be
    written to the DB.
    """
    return DB.patch(OPTIONS_KEY, patch_data, default_entry=DEFAULTS.copy(), **kwargs)


def cleanup_old_options() -> None:
    """
    For this patch call, no patch data is provided, because our cleanup function
    does not need it. But DB.patch requires patch_data to be specified, so
    we pass an empty dictionary as the patch data.
    We also do not care about the return value here -- we just want to initialize
    (if needed) and clean (if needed) the options data.
    """
    patch_options_db({}, patch_handler=_cleanup_old_options)


def _cleanup_old_options(options_data: JsonDict,
                         _: JsonDict) -> V3OptionsData:
    """
    The second argument is not used, and is only present to be compatible with
    the expected interface for a patching function.
    """
    if not options_data:
        # Nothing to clean up in an empty dict
        return options_data
    model_data = V2Options.from_dict(options_data).to_dict() | V3Options.from_dict(options_data).to_dict()
    clean_data = {k: v for k, v in model_data.items() if v is not None}
    return clean_data


def refresh_options_update_loglevel[**P, R](func: Callable[P, R]) -> Callable[P, R]:
    """
    This is a decorator to put around all API controller functions (so that it runs on
    all entrypoints into the server, other than initial startup). It simply calls
    update_server_log_level() before calling the function. It does not change the
    signature of the function.
    """

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        update_server_log_level()
        return func(*args, **kwargs)

    return wrapper


@dbutils.redis_error_handler
@refresh_options_update_loglevel
def get_options_v2():
    """Used by the GET /options API operation"""
    LOGGER.debug("GET /v2/options invoked get_options_v2")
    data = get_options_data()
    response = convert_options_to_v2(data)
    return response, 200


@dbutils.redis_error_handler
@refresh_options_update_loglevel
def get_options_v3():
    """Used by the GET /options API operation"""
    LOGGER.debug("GET /v3/options invoked get_options_v3")
    response = get_options_data()
    return response, 200


def get_options_data():
    """
    We pass an empty dict as the patch data, because our patch handler doesn't
    need it.
    We specify an empty dict as a default entry to guarantee that we will get back
    options data (rather than DBNoEntryError)
    This effectively is a very fancy GET operation in the case where all of the options
    are already present in the DB
    """
    return patch_options_db({}, patch_handler=_set_defaults)


def _set_defaults(options_data: V3OptionsData, _: JsonDict) -> V3OptionsData:
    """
    Adds defaults to the options data if they don't exist
    The second argument is not used, and is only present to be compatible with
    the expected interface for a patching function.
    """
    for key, value in DEFAULTS.items():
        if key not in options_data:
            options_data[key] = value
    return options_data


@dbutils.redis_error_handler
@refresh_options_update_loglevel
def patch_options_v2() -> V2PatchOptionsResponse:
    """Used by the PATCH /options API operation"""
    LOGGER.debug("PATCH /v2/options invoked patch_options_v2")
    try:
        v2_patch: V2OptionsPatch = connexion.request.get_json()
    except Exception as err:
        return connexion.problem(
            status=400, title="Error parsing the data provided.",
            detail=str(err))
    v3_patch = convert_options_from_v2(v2_patch)
    new_v3_data = _patch_options(v3_patch)
    return convert_options_to_v2(new_v3_data), 200


@dbutils.redis_error_handler
@refresh_options_update_loglevel
def patch_options_v3() -> V3PatchOptionsResponse:
    """Used by the PATCH /options API operation"""
    LOGGER.debug("PATCH /v3/options invoked patch_options_v3")
    try:
        v3_patch: V3OptionsPatch = connexion.request.get_json()
    except Exception as err:
        return connexion.problem(
            status=400, title="Error parsing the data provided.",
            detail=str(err))
    return _patch_options(v3_patch), 200


def _patch_options(v3_patch: V3OptionsPatch) -> V3OptionsData:
    """
    Helper function to do the work of applying a V3 patch, since
    this is also used by the V2 patch process.
    """
    new_v3_data = patch_options_db(v3_patch)
    if "logging_level" in v3_patch:
        update_server_log_level()
    return new_v3_data


class Options:
    """Helper class for other endpoints that need access to options"""
    _create_lock = threading.Lock()

    def __new__(cls):
        """This override makes the class a singleton"""
        if not hasattr(cls, 'instance'):
            # Make sure that no other thread has beaten us to the punch
            with cls._create_lock:
                if not hasattr(cls, 'instance'):
                    new_instance = super(Options, cls).__new__(cls)
                    new_instance.__init__(_initialize=True)
                    # Only assign to cls.instance after all work has been done, to ensure
                    # no other threads access it prematurely
                    cls.instance = new_instance
        return cls.instance

    def __init__(self, _initialize: bool=False):
        """
        We only want this singleton to be initialized once
        """
        if _initialize:
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
                    'Option %s has not been initialized.  Defaulting to %s', key, default)
                return default
            LOGGER.error('Option %s has not been initialized.', key)
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

    @property
    def include_ara_links(self):
        return self.get_option('include_ara_links', bool, True)

    @property
    def additional_inventory_source(self):
        return self.get_option('additional_inventory_source', str, default="")


def do_update_log_level(current_level_int: int, new_level_int: int, new_level_str: str) -> None:
    """
    Change the logging level of the current process to the specified new level
    """
    current_level_str = logging.getLevelName(current_level_int)
    LOGGER.log(current_level_int, 'Changing logging level from %s to %s',
               current_level_str, new_level_str)
    logging.getLogger().setLevel(new_level_int)
    LOGGER.log(new_level_int, 'Logging level changed from %s to %s',
               current_level_str, new_level_str)


def update_server_log_level() -> Options:
    """
    Refresh CFS options and update the log level for this process, if needed.
    Returns the refreshed options data, in case the caller wants it.
    """
    options = Options()
    options.refresh()
    desired_level_str = options.logging_level.upper()
    desired_level_int = logging.getLevelName(desired_level_str)
    current_level_int = LOGGER.getEffectiveLevel()
    if current_level_int == desired_level_int:
        # No update needed
        return options
    # Take a lock to prevent multiple threads from doing this
    with LogLevelUpdateLock:
        if current_level_int != desired_level_int:
            do_update_log_level(current_level_int, desired_level_int, desired_level_str)
    return options

@overload
def convert_options_to_v2(v3_data: V3OptionsData) -> V2OptionsData: ...

@overload
def convert_options_to_v2(v3_data: V3OptionsPatch) -> V2OptionsPatch: ...

def convert_options_to_v2(v3_data: V3OptionsData|V3OptionsPatch) -> V2OptionsData|V2OptionsPatch:
    return dbutils.convert_data_to_v2(v3_data, V2Options)


@overload
def convert_options_from_v2(v2_data: V2OptionsData) -> V3OptionsData: ...

@overload
def convert_options_from_v2(v2_data: V2OptionsPatch) -> V3OptionsPatch: ...

def convert_options_from_v2(v2_data: V2OptionsData|V2OptionsPatch) -> V3OptionsData|V3OptionsPatch:
    return dbutils.convert_data_from_v2(v2_data, V2Options)


def defaults(**default_kwargs):
    """
    Allows controller functions to specify parameters that have defaults stored in options.
    It also calls update_server_log_level
    """
    def wrap(f):
        @functools.wraps(f)
        def wrapped_f(*args, **kwargs):
            options = Options()
            options.refresh()
            for key, value in default_kwargs.items():
                if key not in kwargs:
                    kwargs[key] = getattr(options, value)
            return f(*args, **kwargs)
        return wrapped_f
    return wrap
