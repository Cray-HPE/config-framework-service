#
# MIT License
#
# (C) Copyright 2023-2024 Hewlett Packard Enterprise Development LP
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
from collections.abc import Container
import connexion
from datetime import datetime
from functools import partial
#import jsonref
import logging
import uuid
import urllib.parse

from cray.cfs.api import dbutils
from cray.cfs.api.controllers import configurations
from cray.cfs.api.controllers import options

from cray.cfs.api.vault_utils import put_secret as put_vault_secret
from cray.cfs.api.vault_utils import delete_secret as delete_vault_secret

LOGGER = logging.getLogger('cray.cfs.api.controllers.sources')
DB = dbutils.get_wrapper(db='sources')
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

#with open("/app/api/openapi.json", "rt") as jsonspec:
#    OAS_JSON = jsonref.replace_refs(jsonref.load(jsonspec))

@dbutils.redis_error_handler
@options.defaults(limit="default_page_size")
def get_sources_v3(in_use=None, limit=1, after_id=""):
    """Used by the GET /sources API operation"""
    LOGGER.info("GET /sources invoked get_sources_v3")
    called_parameters = locals()
    LOGGER.info("GET /sources 1")
    sources_data, next_page_exists = _get_sources_data(in_use=in_use, limit=limit, after_id=after_id)
    LOGGER.info("GET /sources 2")
    response = {"sources": sources_data, "next": None}
    if next_page_exists:
        next_data = called_parameters
        LOGGER.info("GET /sources 3")
        next_data["after_id"] = sources_data[-1]["name"]
        LOGGER.info("GET /sources 4")
        response["next"] = next_data
        LOGGER.info("GET /sources 5")
    LOGGER.info("GET /sources 6")
    return response, 200


@options.defaults(limit="default_page_size")
def _get_sources_data(in_use=None, limit=1, after_id=""):
    # CASMCMS-9197: Only specify a filter if we are actually filtering
    if in_use is not None:
        source_filter = partial(_source_filter, in_use=in_use, in_use_list=_get_in_use_list())
    else:
        source_filter = None
    source_data_page, next_page_exists = DB.get_all(limit=limit, after_id=after_id, data_filter=source_filter)
    return source_data_page, next_page_exists


def _source_filter(source_data: dict, in_use: bool, in_use_list: Container[str]) -> bool:
    """
    If in_use is true:
        Returns True if the name of the specified source is in in_use_list,
        Returns False otherwise

    If in_use is false:
        Returns True if the name of the specified source is NOT in in_use_list,
        Returns False otherwise
    """
    return (source_data["name"] in in_use_list) == in_use


def _get_in_use_list():
    in_use_list = set()
    source = options.Options().additional_inventory_source
    if source:
        in_use_list.add(source)
    for configuration in _iter_configurations_data():
        source = configuration.get("additional_inventory_source", "")
        if source and type(source) == str:
            in_use_list.add(source)
        for layer in configuration.get("layers"):
            source = layer.get("source", "")
            if source:
                in_use_list.add(source)
    return list(in_use_list)


def _iter_configurations_data():
    next_parameters = { }
    while True:
        data, _ = configurations.get_configurations_v3(**next_parameters)
        for component in data["configurations"]:
            yield component
        next_parameters = data["next"]
        if not next_parameters:
            break


@dbutils.redis_error_handler
def get_source_v3(source_id):
    """Used by the GET /sources/{source_id} API operation"""
    LOGGER.debug(f"GET /sources/{source_id} invoked get_source_v3")
    source_id = urllib.parse.unquote(source_id)
    if source_id not in DB:
        return connexion.problem(
            status=404, title="Source not found",
            detail="Source {} could not be found".format(source_id))
    return DB.get(source_id), 200

#def set_schema_defaults(schema: dict, data: dict) -> None:
#    for prop_name, prop_data in schema["properties"].items():
#        if prop_name in data:
#            if prop_data["type"] == "object":
#                set_schema_defaults(schema=prop_data, data=data[prop_name])
#            continue
#        if "default" in prop_data:
#            data[prop_name] = prop_data["default"]


@dbutils.redis_error_handler
def post_source_v3():
    """Used by the POST /sources/ API operation"""
    LOGGER.debug("POST /sources/ invoked post_source_v3")
    try:
        data = connexion.request.get_json()
    except Exception as err:
        return connexion.problem(
            status=400, title="Error parsing the data provided.",
            detail=str(err))

    # CASMCMS-9196: connexion does not fill in default values for parameters in the request
    # body. So here we set the default value for authentication_method, if needed. Note that
    # connexion DOES validate that the request is valid, so we know that the credentials field
    # is present.
    if "authentication_method" not in data["credentials"]:
        data["credentials"]["authentication_method"] = "password"

    #LOGGER.info("data before setting defaults = %s", str(data))
    #set_schema_defaults(schema=OAS_JSON["components"]["schemas"]["V3SourceCreateData"], data=data)
    #LOGGER.info("data after setting defaults = %s", str(data))

    error = _validate_source(data)
    if error:
        return error
    data = _set_auto_fields(data)

    if not data.get("name"):
        data["name"] = data["clone_url"]

    if data["name"] in DB:
        return connexion.problem(
            detail="A source with the name {} already exists".format(data["name"]),
            status=409,
            title="Conflicting source name"
        )

    data = _update_credentials_secret(data)
    return DB.put(data.get("name"), data), 201


@dbutils.redis_error_handler
def patch_source_v3(source_id):
    """Used by the PATCH /sources/{source_id} API operation"""
    LOGGER.debug(f"PATCH /sources/{source_id} invoked patch_source_v3")
    source_id = urllib.parse.unquote(source_id)
    if source_id not in DB:
        return connexion.problem(
            status=404, title="Source not found.",
            detail="Source {} could not be found".format(source_id))
    try:
        data = connexion.request.get_json()
    except Exception as err:
        return connexion.problem(
            status=400, title="Error parsing the data provided.",
            detail=str(err))

    error = _validate_source(data)
    if error:
        return error
    data = _set_auto_fields(data)

    response_data = DB.patch(source_id, data, update_handler=_update_credentials_secret)
    return response_data, 200


def _validate_source(source):
    source_credentials = source.get("credentials")
    if source_credentials:
        source_credentials_type = source_credentials.get("authentication_method")
        if not source_credentials_type or source_credentials_type == "password":
            if not (source_credentials.get("username") and source_credentials.get("password")):
                return connexion.problem(
                    status=400, title="Invalid credentials",
                    detail="Both username and password must be provided for password authentication credentials")
    return None


def _update_credentials_secret(source):
    source_credentials = source.get("credentials")
    if not source_credentials:
        return source
    secret_name = source_credentials.get("secret_name")
    if not secret_name:
        secret_name = f"cfs-source-credentials-{uuid.uuid4()}"
        source["credentials"]["secret_name"] = secret_name
    authentication_method = source_credentials.get("authentication_method")
    if authentication_method == "password" and source_credentials.get("username") and source_credentials.get("password"):
        secret_data = {"username": source_credentials["username"],
                       "password": source_credentials["password"]}
        put_vault_secret(secret_name, secret_data)
    source = _clean_credentials_data(source)
    return source


def _clean_credentials_data(data):
    credentials = data.get("credentials")
    if not credentials:
        return data
    clean_credentials = {}
    for field in ["authentication_method", "secret_name"]:
        clean_credentials[field] = credentials[field]
    data["credentials"] = clean_credentials
    return data


@dbutils.redis_error_handler
def delete_source_v3(source_id):
    """Used by the DELETE /sources/{source_id} API operation"""
    LOGGER.debug(f"DELETE /sources/{source_id} invoked delete_source_v3")
    source_id = urllib.parse.unquote(source_id)
    if source_id not in DB:
        return connexion.problem(
            status=404, title="Source not found",
            detail="Source {} could not be found".format(source_id))
    if source_id in _get_in_use_list():
        return connexion.problem(
            status=400, title="Source is in use.",
            detail="Source {} is referenced by some configurations".format(source_id))
    source = DB.get(source_id)
    source_credentials = source.get("credentials", {})
    if source_credentials and source_credentials.get("secret_name"):
        delete_vault_secret(source_credentials["secret_name"])
    return DB.delete(source_id), 204


def _set_auto_fields(data):
    data = _set_last_updated(data)
    return data


def _set_last_updated(data):
    data['last_updated'] = datetime.now().strftime(TIME_FORMAT)
    return data
