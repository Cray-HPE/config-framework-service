#
# MIT License
#
# (C) Copyright 2022-2024, 2026 Hewlett Packard Enterprise Development LP
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
# Cray-provided base controllers for the Configuration Framework Service


import logging

import yaml

from cray.cfs.api import dbutils
from cray.cfs.api.controllers import options
from cray.cfs.api.models.version import Version

LOGGER = logging.getLogger('cray.cfs.api.controllers.versions')


def calc_version() -> Version:
    # parse open API spec file from docker image or local repository
    openapispec_f = '/app/lib/server/cray/cfs/api/openapi/openapi.yaml'
    f = None
    try:
        f = open(openapispec_f, 'r')
    except IOError as e:
        LOGGER.debug('error opening openapi.yaml file: %s' % e)

    openapispec_map = yaml.safe_load(f)
    f.close()
    major, minor, patch = openapispec_map['info']['version'].split('.')
    return Version(
        major=major,
        minor=minor,
        patch=patch,
    )


def _get_version():
    LOGGER.debug('in _get_version')
    return calc_version(), 200


@dbutils.redis_error_handler
@options.refresh_options_update_loglevel
def get_version():
    """Used by the GET / API operation"""
    LOGGER.debug("GET /versions invoked get_versions")
    return _get_version()


@dbutils.redis_error_handler
@options.refresh_options_update_loglevel
def get_versions():
    """Used by the GET /versions API operation"""
    LOGGER.debug("GET /versions invoked get_versions")
    return _get_version()


@dbutils.redis_error_handler
@options.refresh_options_update_loglevel
def get_versions_v2():
    """Used by the GET /v2 API operation"""
    LOGGER.debug("GET /v2 invoked get_versions_v2")
    return _get_version()


@dbutils.redis_error_handler
@options.refresh_options_update_loglevel
def get_versions_v3():
    """Used by the GET /v3 API operation"""
    LOGGER.debug("GET /v3 invoked get_versions_v3")
    return _get_version()
