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

from collections.abc import Container
from datetime import datetime
from functools import partial
import logging
import os
import subprocess
import tempfile

import connexion

from cray.cfs.api import dbutils
from cray.cfs.api.controllers import components, options, sources
from cray.cfs.api.k8s_utils import get_configmap as get_kubernetes_configmap
from cray.cfs.api.models.v2_configuration import V2Configuration # noqa: E501
from cray.cfs.api.vault_utils import get_secret as get_vault_secret
from cray.cfs.utils.multitenancy import get_tenant_from_header, reject_invalid_tenant

LOGGER = logging.getLogger('cray.cfs.api.controllers.configurations')
DB = dbutils.get_wrapper(db='configurations')
SOURCES_DB = dbutils.get_wrapper(db='sources')
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

def _get_filtered_configurations(tenant):
    response = DB.get_all()
    if tenant:
        response = [r for r in response if _matches_filter(r, tenant)]
    return response


def _matches_filter(data, tenant):
    if tenant and tenant != data.get("tenant_name"):
        return False
    return True

# Common Multitenancy specific connection responses
TENANT_FORBIDDEN_OPERATION = connexion.problem(
    status=403, title="Forbidden operation.",
    detail="Tenant does not own the requested resources and is forbidden from making changes."
)
IMMUTABLE_TENANT_NAME_FIELD = connexion.problem(
    status=403, title="Forbidden operation.",
    detail="Modification to existing field 'tenant_name' is not permitted."
)

@dbutils.redis_error_handler
@options.refresh_options_update_loglevel
def get_configurations_v2(in_use=None):
    """Used by the GET /configurations API operation"""
    LOGGER.debug("GET /v2/configurations invoked get_configurations_v2")
    configurations_data, next_page_exists = _get_configurations_data(in_use=in_use)
    if next_page_exists:
        return connexion.problem(
            status=400, title="The response size is too large",
            detail="The response size exceeds the default_page_size.  "
                   "Use the v3 API to page through the results.")
    return (
        [convert_configuration_to_v2(configuration)for configuration in configurations_data],
        200
    )


@dbutils.redis_error_handler
@reject_invalid_tenant
@options.refresh_options_update_loglevel
@options.defaults(limit="default_page_size")
def get_configurations_v3(in_use=None, limit=1, after_id=""):
    """Used by the GET /configurations API operation"""
    LOGGER.debug("GET /v3/configurations invoked get_configurations_v3")
    called_parameters = locals()
    tenant = get_tenant_from_header() or None
    configurations_data, next_page_exists = _get_configurations_data(in_use=in_use, limit=limit,
                                                                     after_id=after_id,
                                                                     tenant=tenant)
    response = {"configurations": configurations_data, "next": None}
    if next_page_exists:
        next_data = called_parameters
        next_data["after_id"] = configurations_data[-1]["name"]
        response["next"] = next_data
    return response, 200


@options.defaults(limit="default_page_size")
def _get_configurations_data(in_use=None, limit=1, after_id="", tenant=None):
    data_filters = []
    # CASMCMS-9197: Only specify a filter if we are actually filtering
    if in_use is not None:
        data_filters.append(partial(_configuration_filter, in_use=in_use,
                                    in_use_list=_get_in_use_list()))
    if tenant:
        # In the event a tenant is not set, the super administrator should be able to view
        # configurations owned by ALL tenants. As such, we only reduce the effective set of
        # configurations down when a tenant admin is requesting.
        data_filters.append(partial(_tenancy_filter, tenant=tenant))
    configuration_data_page, next_page_exists = DB.get_all(limit=limit, after_id=after_id,
                                                           data_filters=data_filters)
    return configuration_data_page, next_page_exists


def _configuration_filter(configuration_data: dict, in_use: bool,
                          in_use_list: Container[str]) -> bool:
    """
    The purpose of this function is to filter CFS configurations that are referenced by any
    defined component.

    If in_use is true:
        Returns True if the name of the specified configuration is in in_use_list,
        Returns False otherwise

    If in_use is false:
        Returns True if the name of the specified configuration is NOT in in_use_list,
        Returns False otherwise
    """
    return (configuration_data["name"] in in_use_list) == in_use


def _tenancy_filter(configuration_data: dict, tenant: str) -> bool:
    """
    The purpose of this function is to reduce the total number of configurations to just those
    owned by an individual tenant.
    """
    return configuration_data.get('tenant_name', '') == tenant


def _get_in_use_list():
    in_use_list = set()
    for component in _iter_components_data():
        desired_state = component.get('desired_state', '')
        if desired_state and isinstance(desired_state, str):
            in_use_list.add(desired_state)
    return list(in_use_list)


def _iter_components_data():
    next_parameters = {}
    while True:
        data, _ = components.get_components_v3(**next_parameters)
        yield from data["components"]
        next_parameters = data["next"]
        if not next_parameters:
            break

def _config_in_use(config_name: str) -> bool:
    data, _ = components.get_components_v3(config_name=config_name, limit=1)
    if data["components"]:
        return True
    return False

@dbutils.redis_error_handler
@options.refresh_options_update_loglevel
def get_configuration_v2(configuration_id):
    """Used by the GET /configurations/{configuration_id} API operation"""
    LOGGER.debug("GET /v2/configurations/%s invoked get_configuration_v2", configuration_id)
    if configuration_id not in DB:
        return connexion.problem(
            status=404, title="Configuration not found",
            detail=f"Configuration {configuration_id} could not be found")
    return convert_configuration_to_v2(DB.get(configuration_id)), 200


@dbutils.redis_error_handler
@reject_invalid_tenant
@options.refresh_options_update_loglevel
def get_configuration_v3(configuration_id):
    """Used by the GET /configurations/{configuration_id} API operation"""
    LOGGER.debug("GET /v3/configurations/%s invoked get_configuration_v3", configuration_id)
    if configuration_id not in DB:
        return connexion.problem(status=404, title="Configuration not found",
                                 detail=f"Configuration {configuration_id} could not be found")
    configuration_data = DB.get(configuration_id)
    tenant = get_tenant_from_header() or None
    if all([tenant,
            tenant != configuration_data.get('tenant_name', '')]):
        return TENANT_FORBIDDEN_OPERATION
    return DB.get(configuration_id), 200


@dbutils.redis_error_handler
@options.refresh_options_update_loglevel
def put_configuration_v2(configuration_id):
    """Used by the PUT /configurations/{configuration_id} API operation"""
    LOGGER.debug("PUT /v2/configurations/%s invoked put_configuration_v2", configuration_id)
    try:
        data = connexion.request.get_json()
        data = convert_configuration_to_v3(data)
    except Exception as err:
        return connexion.problem(
            status=400, title="Error parsing the data provided.",
            detail=str(err))

    for layer in iter_layers(data, include_additional_inventory=True):
        if 'branch' in layer and 'commit' in layer:
            return connexion.problem(
                status=400, title="Error handling error branches",
                detail='Only branch or commit should be specified for each layer, not both.')

    try:
        data = _set_auto_fields(data)
    except BranchConversionException as e:
        return connexion.problem(
            status=400, title="Error converting branch name to commit",
            detail=str(e))

    layer_keys = set()
    for layer in iter_layers(data, include_additional_inventory=False):
        layer_key = (layer.get('clone_url'), layer.get('playbook'))
        if layer_key in layer_keys:
            return connexion.problem(
                status=400, title="Error with conflicting layers",
                detail='Two or more layers apply the same playbook from the same repo, '
                       'but have different commit ids.')
        layer_keys.add(layer_key)

    data['name'] = configuration_id
    return convert_configuration_to_v2(DB.put(configuration_id, data)), 200


@dbutils.redis_error_handler
@reject_invalid_tenant
@options.refresh_options_update_loglevel
def put_configuration_v3(configuration_id, drop_branches=False):
    """Used by the PUT /configurations/{configuration_id} API operation"""
    LOGGER.debug("PUT /v3/configurations/%s invoked put_configuration_v3", configuration_id)
    try:
        data = connexion.request.get_json()
    except Exception as err:
        return connexion.problem(
            status=400, title="Error parsing the data provided.",
            detail=str(err))

    # If the put request comes from a specific tenant, make note of it in the record -- we're
    # going to use it in subsequent data puts and permission checks.
    requesting_tenant = get_tenant_from_header() or None

    # If the configuration already exists, and the configuration is not owned by the requesting put
    # tenant, then we cannot allow them to overwrite the existing data for this key.
    existing_configuration = DB.get(configuration_id) or {}
    LOGGER.debug("Requesting Tenant: '%s'; Existing Configuration: '%s'", requesting_tenant,
                 existing_configuration)
    if requesting_tenant is not None:
        if all([existing_configuration,
                existing_configuration.get('tenant_name', None) != requesting_tenant]):
            return TENANT_FORBIDDEN_OPERATION
        if data.get('tenant_name', None) not in set(['', None, requesting_tenant]):
            return IMMUTABLE_TENANT_NAME_FIELD
        data['tenant_name'] = requesting_tenant
    else:
        # The global admin is requesting the change; they can do everything, including putting over
        # other people's stuff. This block is split out specifically for this comment, which is why
        # we have it even though it is just a pass.
        pass

    for layer in iter_layers(data, include_additional_inventory=True):
        if 'clone_url' in layer and 'source' in layer:
            return connexion.problem(
                status=400, title="Error handling source",
                detail='Only source or clone_url should be specified for each layer, not both.')
        if 'clone_url' not in layer and 'source' not in layer:
            return connexion.problem(
                status=400, title="Error handling source",
                detail='Either source or clone_url must be specified for each layer.')
        if 'branch' in layer and 'commit' in layer:
            return connexion.problem(
                status=400, title="Error handling branches",
                detail='Only branch or commit should be specified for each layer, not both.')
        if layer.get("source") and layer.get("source") not in SOURCES_DB:
            return connexion.problem(
                status=400, title="Source does not exist",
                detail=f"The source {layer['source']} does not exist.")

    try:
        data = _set_auto_fields(data)
    except BranchConversionException as e:
        return connexion.problem(
            status=400, title="Error converting branch name to commit",
            detail=str(e))

    layer_keys = set()
    for layer in iter_layers(data, include_additional_inventory=False):
        layer_key = (layer.get('clone_url', layer.get('source')), layer.get('playbook'))
        if layer_key in layer_keys:
            return connexion.problem(
                status=400, title="Error with conflicting layers",
                detail='Two or more layers apply the same playbook from the same repo, '
                       'but have different commit ids.')
        layer_keys.add(layer_key)

    if drop_branches:
        for layer in iter_layers(data, include_additional_inventory=True):
            layer.pop("branch", None)

    data['name'] = configuration_id
    return DB.put(configuration_id, data), 200


@dbutils.redis_error_handler
@options.refresh_options_update_loglevel
def patch_configuration_v2(configuration_id):
    """Used by the PATCH /configurations/{configuration_id} API operation"""
    LOGGER.debug("PATCH /v2/configurations/%s invoked patch_configuration_v2", configuration_id)
    if configuration_id not in DB:
        return connexion.problem(
            status=404, title="Configuration not found",
            detail=f"Configuration {configuration_id} could not be found")
    data = DB.get(configuration_id)
    try:
        data = _set_auto_fields(data)
    except BranchConversionException as e:
        return connexion.problem(
            status=400, title="Error converting branch name to commit",
            detail=str(e))

    return convert_configuration_to_v2(DB.put(configuration_id, data)), 200


@dbutils.redis_error_handler
@reject_invalid_tenant
@options.refresh_options_update_loglevel
def patch_configuration_v3(configuration_id):
    """Used by the PATCH /configurations/{configuration_id} API operation"""
    LOGGER.debug("PATCH /v3/configurations/%s invoked patch_configuration_v3", configuration_id)
    if configuration_id not in DB:
        return connexion.problem(
            status=404, title="Configuration not found",
            detail=f"Configuration {configuration_id} could not be found")
    data = DB.get(configuration_id)

    tenant = get_tenant_from_header() or None
    if all([tenant,
            tenant != data.get('tenant_name', '')]):
        return TENANT_FORBIDDEN_OPERATION

    try:
        data = _set_auto_fields(data)
    except BranchConversionException as e:
        return connexion.problem(
            status=400, title="Error converting branch name to commit",
            detail=str(e))

    return DB.put(configuration_id, data), 200


@dbutils.redis_error_handler
@options.refresh_options_update_loglevel
def delete_configuration_v2(configuration_id):
    """Used by the DELETE /configurations/{configuration_id} API operation"""
    LOGGER.debug("DELETE /v2/configurations/%s invoked delete_configuration_v2", configuration_id)
    if configuration_id not in DB:
        return connexion.problem(
            status=404, title="Configuration not found",
            detail=f"Configuration {configuration_id} could not be found")
    if _config_in_use(configuration_id):
        return connexion.problem(
            status=400, title="Configuration is in use.",
            detail=f"Configuration {configuration_id} is referenced by the desired state of "
                   "some components")
    return DB.delete(configuration_id), 204


@dbutils.redis_error_handler
@reject_invalid_tenant
@options.refresh_options_update_loglevel
def delete_configuration_v3(configuration_id):
    """Used by the DELETE /configurations/{configuration_id} API operation"""
    LOGGER.debug("DELETE /v3/configurations/%s invoked delete_configuration_v3", configuration_id)
    if configuration_id not in DB:
        return connexion.problem(
            status=404, title="Configuration not found",
            detail=f"Configuration {configuration_id} could not be found")
    if _config_in_use(configuration_id):
        return connexion.problem(
            status=400, title="Configuration is in use.",
            detail=f"Configuration {configuration_id} is referenced by the desired state of "
                   "some components")
    # If the put request comes from a specific tenant, make note of it in the record -- we're going
    # to use it in subsequent data puts and permission checks.
    requesting_tenant = get_tenant_from_header() or None
    # If the configuration already exists, and the tenant is not owned by the requesting delete
    # tenant, then we cannot allow them to overwrite the existing data for this key.
    existing_configuration = DB.get(configuration_id) or {}
    if all([requesting_tenant is not None,
            existing_configuration.get('tenant_name', '') != requesting_tenant]):
        return TENANT_FORBIDDEN_OPERATION
    return DB.delete(configuration_id), 204


def iter_layers(config_data, include_additional_inventory=True):
    yield from config_data.get('layers')
    if include_additional_inventory and (add_inv := config_data.get("additional_inventory")):
        yield add_inv


def _set_auto_fields(data):
    data = _set_last_updated(data)
    try:
        data = _convert_branches_to_commits(data)
    except BranchConversionException as e:
        LOGGER.error("Error converting branch name to commit: %s", e)
        raise
    except Exception as e:
        LOGGER.exception("Unexpected error converting branch name to commit: %s", e)
        raise BranchConversionException(e) from e
    return data


def _set_last_updated(data):
    data['last_updated'] = datetime.now().strftime(TIME_FORMAT)
    return data


class BranchConversionException(Exception):
    pass


def _convert_branches_to_commits(data):
    for layer in iter_layers(data, include_additional_inventory=True):
        if 'branch' in layer:
            branch = layer.get('branch')
            if 'source' in layer:
                source, _ = sources.get_source_v3(layer.get('source'))
                clone_url = source['clone_url']
            else:
                source = None
                clone_url = layer.get('clone_url')
            layer['commit'] = _get_commit_id(clone_url, branch, source=source)
    return data


def _get_commit_id(repo_url, branch, source=None):
    """
    Given a branch and git url, returns the commit id at the top of that branch

    Args:
      repo_url: The cloneUrl to pass to the CFS session
      branch: The branch to pass to the CFS session

    Returns:
      commit: A commit id for the given branch

    Raises:
      BranchConversionException -- for errors encountered calling git
    """
    split_url = repo_url.split('/')
    repo_name = split_url[-1].split('.')[0]
    with tempfile.TemporaryDirectory(dir='/tmp') as tmp_dir:
        repo_dir = os.path.join(tmp_dir, repo_name)
        try:
            username, password = _get_git_credentials(source)
        except Exception as e:
            LOGGER.error("Error retrieving git credentials: %s", e)
            raise
        ssl_info = _get_ssl_info(source, tmp_dir)
        creds_url = ''.join([split_url[0], '//', username, ':', password, '@', split_url[2]])
        creds_file_name = os.path.join(tmp_dir, '.git-credentials')
        with open(creds_file_name, 'w') as creds_file:
            creds_file.write(creds_url)

        config_command = 'git config --file .gitconfig credential.helper store'.split()
        clone_command = f'git clone {repo_url}'.split()
        checkout_command = f'git checkout {branch}'.split()
        parse_command = 'git rev-parse HEAD'.split()
        try:
            # Setting HOME lets us keep the .git-credentials file in the temp directory rather
            # than the HOME shared by all threads/calls.
            subprocess.check_call(config_command, cwd=tmp_dir,
                                  env={'HOME': tmp_dir, 'GIT_SSL_CAINFO': ssl_info},
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.check_call(clone_command, cwd=tmp_dir,
                                  env={'HOME': tmp_dir, 'GIT_SSL_CAINFO': ssl_info},
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.check_call(checkout_command, cwd=repo_dir,
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            output = subprocess.check_output(parse_command, cwd=repo_dir)
        except subprocess.CalledProcessError as e:
            raise BranchConversionException(
                f"Failed interacting with the specified clone_url: {e}") from e
    commit = output.decode("utf-8").strip()
    LOGGER.info('Translated git branch %s to commit %s', branch, commit)
    return commit


def _get_git_credentials(source=None):
    if not source:
        username = os.environ['VCS_USERNAME'].strip()
        password = os.environ['VCS_PASSWORD'].strip()
        return username, password
    source_credentials = source["credentials"]
    secret_name = source_credentials["secret_name"]
    try:
        secret = get_vault_secret(secret_name)
    except Exception as e:
        raise BranchConversionException(f"Error loading Vault secret: {e}") from e
    try:
        username = secret["username"]
        password = secret["password"]
    except Exception as e:
        raise BranchConversionException(
            f"Error reading username and password from secret: {e}") from e
    return username, password


def _get_ssl_info(source=None, tmp_dir=""):
    if not source or not (cert_info := source.get("ca_cert")):
        return os.environ['GIT_SSL_CAINFO']
    configmap_name = cert_info["configmap_name"]
    configmap_namespace = cert_info.get("configmap_namespace")
    if configmap_namespace:
        response = get_kubernetes_configmap(configmap_name, configmap_namespace)
    else:
        response = get_kubernetes_configmap(configmap_name)
    data = response.data
    file_name = list(data.keys())[0]
    file_path = os.path.join(tmp_dir, file_name)
    with open(file_path, 'w') as f:
        f.write(data[file_name])
    return file_path


class Configurations:
    """Helper class for other endpoints that need access to configurations"""

    def __init__(self):
        # Some callers call the get_config method without checking if the configuration name is
        # set. If it is not set, calling the database will always just return None, so we can
        # save ourselves the network traffic of a database call here.
        self.configs = { "": None, None: None }

    def get_config(self, key):
        if key not in self.configs:
            self.configs[key] = DB.get(key)
        return self.configs[key]


def convert_configuration_to_v2(data):
    data = dbutils.convert_data_to_v2(data, V2Configuration)
    return data


def convert_configuration_to_v3(data):
    data = dbutils.convert_data_from_v2(data, V2Configuration)
    return data
