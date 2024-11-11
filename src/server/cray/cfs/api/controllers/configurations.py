#
# MIT License
#
# (C) Copyright 2020-2024 Hewlett Packard Enterprise Development LP
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
import logging
import os
import subprocess
import tempfile

from cray.cfs.api import dbutils
from cray.cfs.api.controllers import components
from cray.cfs.api.controllers import options
from cray.cfs.api.controllers import sources
from cray.cfs.api.k8s_utils import get_configmap as get_kubernetes_configmap
from cray.cfs.api.vault_utils import get_secret as get_vault_secret
from cray.cfs.api.models.v2_configuration import V2Configuration # noqa: E501

LOGGER = logging.getLogger('cray.cfs.api.controllers.configurations')
DB = dbutils.get_wrapper(db='configurations')
SOURCES_DB = dbutils.get_wrapper(db='sources')
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


@dbutils.redis_error_handler
def get_configurations_v2(in_use=None):
    """Used by the GET /configurations API operation"""
    LOGGER.debug("GET /configurations invoked get_configurations")
    configurations_data, next_page_exists = _get_configurations_data(in_use=in_use)
    if next_page_exists:
        return connexion.problem(
            status=400, title="The response size is too large",
            detail="The response size exceeds the default_page_size.  Use the v3 API to page through the results.")
    return [convert_configuration_to_v2(configuration) for configuration in configurations_data], 200


@dbutils.redis_error_handler
@options.defaults(limit="default_page_size")
def get_configurations_v3(in_use=None, limit=1, after_id=""):
    """Used by the GET /configurations API operation"""
    LOGGER.debug("GET /configurations invoked get_configurations")
    called_parameters = locals()
    configurations_data, next_page_exists = _get_configurations_data(in_use=in_use, limit=limit, after_id=after_id)
    response = {"configurations": configurations_data, "next": None}
    if next_page_exists:
        next_data = called_parameters
        next_data["after_id"] = configurations_data[-1]["name"]
        response["next"] = next_data
    return response, 200


@options.defaults(limit="default_page_size")
def _get_configurations_data(in_use=None, limit=1, after_id=""):
    # CASMCMS-9197: Only specify a filter if we are actually filtering
    if in_use is not None:
        configuration_filter = partial(_configuration_filter, in_use=in_use, in_use_list=_get_in_use_list())
    else:
        configuration_filter = None
    configuration_data_page, next_page_exists = DB.get_all(limit=limit, after_id=after_id, data_filter=configuration_filter)
    return configuration_data_page, next_page_exists


def _configuration_filter(configuration_data: dict, in_use: bool, in_use_list: Container[str]) -> bool:
    """
    If in_use is true:
        Returns True if the name of the specified configuration is in in_use_list,
        Returns False otherwise

    If in_use is false:
        Returns True if the name of the specified configuration is NOT in in_use_list,
        Returns False otherwise
    """
    return (configuration_data["name"] in in_use_list) == in_use


def _get_in_use_list():
    in_use_list = set()
    for component in _iter_components_data():
        desired_state = component.get('desired_state', '')
        if desired_state and type(desired_state) == str:
            in_use_list.add(desired_state)
    return list(in_use_list)


def _iter_components_data():
    next_parameters = {}
    while True:
        data, _ = components.get_components_v3(**next_parameters)
        for component in data["components"]:
            yield component
        next_parameters = data["next"]
        if not next_parameters:
            break


@dbutils.redis_error_handler
def get_configuration_v2(configuration_id):
    """Used by the GET /configurations/{configuration_id} API operation"""
    LOGGER.debug("GET /configurations/id invoked get_configuration")
    if configuration_id not in DB:
        return connexion.problem(
            status=404, title="Configuration not found",
            detail="Configuration {} could not be found".format(configuration_id))
    return convert_configuration_to_v2(DB.get(configuration_id)), 200


@dbutils.redis_error_handler
def get_configuration_v3(configuration_id):
    """Used by the GET /configurations/{configuration_id} API operation"""
    LOGGER.debug("GET /configurations/id invoked get_configuration")
    if configuration_id not in DB:
        return connexion.problem(
            status=404, title="Configuration not found",
            detail="Configuration {} could not be found".format(configuration_id))
    return DB.get(configuration_id), 200


@dbutils.redis_error_handler
def put_configuration_v2(configuration_id):
    """Used by the PUT /configurations/{configuration_id} API operation"""
    LOGGER.debug("PUT /configurations/id invoked put_configuration")
    try:
        data = connexion.request.get_json()
        data = convert_configuration_to_v3(data)
    except Exception as err:
        return connexion.problem(
            status=400, title="Error parsing the data provided.",
            detail=str(err))

    for layer in _iter_layers(data, include_additional_inventory=True):
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
    for layer in _iter_layers(data, include_additional_inventory=False):
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
def put_configuration_v3(configuration_id, drop_branches=False):
    """Used by the PUT /configurations/{configuration_id} API operation"""
    LOGGER.debug("PUT /configurations/id invoked put_configuration")
    try:
        data = connexion.request.get_json()
    except Exception as err:
        return connexion.problem(
            status=400, title="Error parsing the data provided.",
            detail=str(err))

    for layer in _iter_layers(data, include_additional_inventory=True):
        if 'clone_url' in layer and 'source' in layer:
            return connexion.problem(
                status=400, title="Error handling source",
                detail='Only source or clone_url should be specified for each layer, not both.')
        if 'clone_url' not in layer and 'source' not in layer:
            return connexion.problem(
                status=400, title="Error handling source",
                detail='Either source or clone_url must be specified for each layer.')
        if layer.get("source") and layer.get("source") not in SOURCES_DB:
            return connexion.problem(
                status=400, title="Source does not exist",
                detail=f"The source {layer['source']} does not exist.")
        if 'branch' in layer and 'commit' in layer:
            return connexion.problem(
                status=400, title="Error handling branches",
                detail='Only branch or commit should be specified for each layer, not both.')

    try:
        data = _set_auto_fields(data)
    except BranchConversionException as e:
        return connexion.problem(
            status=400, title="Error converting branch name to commit",
            detail=str(e))

    layer_keys = set()
    for layer in _iter_layers(data, include_additional_inventory=False):
        layer_key = (layer.get('clone_url', layer.get('source')), layer.get('playbook'))
        if layer_key in layer_keys:
            return connexion.problem(
                status=400, title="Error with conflicting layers",
                detail='Two or more layers apply the same playbook from the same repo, '
                       'but have different commit ids.')
        layer_keys.add(layer_key)

    if drop_branches:
        for layer in _iter_layers(data, include_additional_inventory=True):
            if "branch" in layer:
                del(layer["branch"])

    data['name'] = configuration_id
    return DB.put(configuration_id, data), 200


@dbutils.redis_error_handler
def patch_configuration_v2(configuration_id):
    """Used by the PATCH /configurations/{configuration_id} API operation"""
    LOGGER.debug("PATCH /configurations/id invoked put_configuration")
    if configuration_id not in DB:
        return connexion.problem(
            status=404, title="Configuration not found",
            detail="Configuration {} could not be found".format(configuration_id))
    data = DB.get(configuration_id)
    try:
        data = _set_auto_fields(data)
    except BranchConversionException as e:
        return connexion.problem(
            status=400, title="Error converting branch name to commit",
            detail=str(e))

    return convert_configuration_to_v2(DB.put(configuration_id, data)), 200


@dbutils.redis_error_handler
def patch_configuration_v3(configuration_id):
    """Used by the PATCH /configurations/{configuration_id} API operation"""
    LOGGER.debug("PATCH /configurations/id invoked put_configuration")
    if configuration_id not in DB:
        return connexion.problem(
            status=404, title="Configuration not found",
            detail="Configuration {} could not be found".format(configuration_id))
    data = DB.get(configuration_id)

    try:
        data = _set_auto_fields(data)
    except BranchConversionException as e:
        return connexion.problem(
            status=400, title="Error converting branch name to commit",
            detail=str(e))

    return DB.put(configuration_id, data), 200


@dbutils.redis_error_handler
def delete_configuration_v2(configuration_id):
    """Used by the DELETE /configurations/{configuration_id} API operation"""
    LOGGER.debug("DELETE /configurations/id invoked delete_configuration")
    if configuration_id not in DB:
        return connexion.problem(
            status=404, title="Configuration not found",
            detail="Configuration {} could not be found".format(configuration_id))
    if configuration_id in _get_in_use_list():
        return connexion.problem(
            status=400, title="Configuration is in use.",
            detail="Configuration {} is referenced by the desired state of"
                   "some components".format(configuration_id))
    return DB.delete(configuration_id), 204


@dbutils.redis_error_handler
def delete_configuration_v3(configuration_id):
    """Used by the DELETE /configurations/{configuration_id} API operation"""
    LOGGER.debug("DELETE /configurations/id invoked delete_configuration")
    if configuration_id not in DB:
        return connexion.problem(
            status=404, title="Configuration not found",
            detail="Configuration {} could not be found".format(configuration_id))
    if configuration_id in _get_in_use_list():
        return connexion.problem(
            status=400, title="Configuration is in use.",
            detail="Configuration {} is referenced by the desired state of"
                   "some components".format(configuration_id))
    return DB.delete(configuration_id), 204


def _iter_layers(config_data, include_additional_inventory=True):
    if include_additional_inventory and config_data.get("additional_inventory"):
        for layer in (config_data.get('layers') + [config_data.get('additional_inventory')]):
            yield layer
    else:
        for layer in config_data.get('layers'):
            yield layer


def _set_auto_fields(data):
    data = _set_last_updated(data)
    try:
        data = _convert_branches_to_commits(data)
    except BranchConversionException as e:
        LOGGER.error(f"Error converting branch name to commit: {e}")
        raise
    except Exception as e:
        LOGGER.exception(f"Unexpected error converting branch name to commit: {e}")
        raise BranchConversionException(e) from e
    return data


def _set_last_updated(data):
    data['last_updated'] = datetime.now().strftime(TIME_FORMAT)
    return data


class BranchConversionException(Exception):
    pass


def _convert_branches_to_commits(data):
    for layer in _iter_layers(data, include_additional_inventory=True):
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
    with tempfile.TemporaryDirectory(dir='/tmp') as tmp_dir:
        repo_name = repo_url.split('/')[-1].split('.')[0]
        repo_dir = os.path.join(tmp_dir, repo_name)

        split_url = repo_url.split('/')
        try:
            username, password = _get_git_credentials(source)
        except Exception as e:
            LOGGER.error(f"Error retrieving git credentials: {e}")
            raise
        ssl_info = _get_ssl_info(source, tmp_dir)
        creds_url = ''.join([split_url[0], '//', username, ':', password, '@', split_url[2]])
        creds_file_name = os.path.join(tmp_dir, '.git-credentials')
        with open(creds_file_name, 'w') as creds_file:
            creds_file.write(creds_url)

        config_command = 'git config --file .gitconfig credential.helper store'.split()
        clone_command = 'git clone {}'.format(repo_url).split()
        checkout_command = 'git checkout {}'.format(branch).split()
        parse_command = 'git rev-parse HEAD'.split()
        try:
            # Setting HOME lets us keep the .git-credentials file in the temp directory rather than the
            # HOME shared by all threads/calls.
            subprocess.check_call(config_command, cwd=tmp_dir,
                                  env={'HOME': tmp_dir, 'GIT_SSL_CAINFO': ssl_info},
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.check_call(clone_command, cwd=tmp_dir,
                                  env={'HOME': tmp_dir, 'GIT_SSL_CAINFO': ssl_info},
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.check_call(checkout_command, cwd=repo_dir,
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            output = subprocess.check_output(parse_command, cwd=repo_dir)
            commit = output.decode("utf-8").strip()
            LOGGER.info('Translated git branch {} to commit {}'.format(branch, commit))
            return commit
        except subprocess.CalledProcessError as e:
            raise BranchConversionException(f"Failed interacting with the specified clone_url: {e}") from e


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
        raise BranchConversionException(f"Error reading username and password from secret: {e}") from e
    return username, password


def _get_ssl_info(source=None, tmp_dir=""):
    if source and source.get("ca_cert"):
        cert_info = source.get("ca_cert")
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
    else:
        return os.environ['GIT_SSL_CAINFO']


class Configurations(object):
    def __init__(self):
        # Some callers call the get_config method without checking if the configuration name is
        # set. If it is not set, calling the database will always just return None, so we can
        # save ourselves the network traffic of a database call here.
        self.configs = { "": None, None: None }

    """Helper class for other endpoints that need access to configurations"""
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
