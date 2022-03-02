#
# MIT License
#
# (C) Copyright 2020-2022 Hewlett Packard Enterprise Development LP
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

import connexion
from copy import deepcopy
from datetime import datetime
import logging

from cray.cfs.api import dbutils
from cray.cfs.api.controllers import options
from cray.cfs.api.controllers import configurations

LOGGER = logging.getLogger('cray.cfs.api.controllers.components')
DB = dbutils.get_wrapper(db='components')
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

STATUS_UNCONFIGURED = 0
STATUS_FAILED = 1
STATUS_PENDING = 2
STATUS_CONFIGURED = 3
STATUS_DEPRECATED = 4

STATUS = {
    STATUS_UNCONFIGURED: 'unconfigured',
    STATUS_FAILED: 'failed',
    STATUS_PENDING: 'pending',
    STATUS_CONFIGURED: 'configured',
    STATUS_DEPRECATED: 'config_deprecated',
}


@dbutils.redis_error_handler
def get_components(ids="", status="", enabled=None, config_name="", config_details=False,
                   tags=""):
    """Used by the GET /components API operation

    Allows filtering using a comma seperated list of ids.
    """
    LOGGER.debug("GET /components invoked get_components")
    id_list = []
    status_list = []
    tag_list = []
    if ids:
        try:
            id_list = ids.split(',')
        except Exception as err:
            return connexion.problem(
                status=400, title="Error parsing the ids provided.",
                detail=str(err))
    if status:
        try:
            status_list = status.split(',')
        except Exception as err:
            return connexion.problem(
                status=400, title="Error parsing the status provided.",
                detail=str(err))
    if tags:
        try:
            tag_list = [tuple(tag.split('=')) for tag in tags.split(',')]
            for tag in tag_list:
                assert(len(tag) == 2)
        except Exception as err:
            return connexion.problem(
                status=400, title="Error parsing the tags provided.",
                detail=str(err))
    response = get_components_data(id_list=id_list, status_list=status_list, enabled=enabled,
                                   config_name=config_name, config_details=config_details,
                                   tag_list=tag_list)
    return response, 200


def get_components_data(id_list=[], status_list=[], enabled=None, config_name="",
                        config_details=False, tag_list=[]):
    """Used by the GET /components API operation=

    Allows filtering using a comma separated list of ids.
    """
    response = []
    if id_list:
        for component_id in id_list:
            data = DB.get(component_id)
            if data:
                response.append(data)
    else:
        # TODO: On large scale systems, this response may be too large
        # and require paging to be implemented
        response = DB.get_all()
    opts = options.Options()
    configs = configurations.Configurations()
    response = [_set_status(r, opts, configs, config_details) for r in response if r]
    if status_list or (enabled is not None) or config_name or tag_list:
        response = [r for r in response if _matches_filter(r, status_list, enabled,
                                                           config_name, tag_list)]
    return response


def _matches_filter(data, status, enabled, config_name, tags):
    data_status = data.get('configurationStatus', '')
    if status and not any([data_status == s for s in status]):
        return False
    if enabled is not None and data.get('enabled', None) != enabled:
        return False
    if config_name and data.get('desiredConfig', '') != config_name:
        return False
    if tags and any([data.get('tags', {}).get(k) != v for k, v in tags]):
        return False
    return True


@dbutils.redis_error_handler
def put_components():
    """Used by the PUT /components API operation"""
    LOGGER.debug("PUT /components invoked put_components")
    try:
        data = connexion.request.get_json()
        components = []
        for component_data in data:
            component_id = component_data['id']
            components.append((component_id, component_data))
    except Exception as err:
        return connexion.problem(
            status=400, title="Error parsing the data provided.",
            detail=str(err))
    response = []
    for component_id, component_data in components:
        component_data = _set_auto_fields(component_data)
        response.append(DB.put(component_id, component_data))
    return response, 200


@dbutils.redis_error_handler
def patch_components():
    """Used by the PATCH /components API operation"""
    LOGGER.debug("PATCH /components invoked patch_components")
    try:
        data = connexion.request.get_json()
        components = []
        for component_data in data:
            component_id = component_data['id']
            if component_id not in DB:
                return connexion.problem(
                    status=404, title="Component could not found.",
                    detail="Component {} could not be found".format(component_id))
            components.append((component_id, component_data))
    except Exception as err:
        return connexion.problem(
            status=400, title="Error parsing the data provided.",
            detail=str(err))
    response = []
    for component_id, component_data in components:
        component_data = _set_auto_fields(component_data)
        response.append(DB.patch(component_id, component_data, _update_handler))
    return response, 200


@dbutils.redis_error_handler
def get_component(component_id, config_details=False):
    """Used by the GET /components/{component_id} API operation"""
    LOGGER.debug("GET /components/id invoked get_component")
    if component_id not in DB:
        return connexion.problem(
            status=404, title="Component could not found.",
            detail="Component {} could not be found".format(component_id))
    component = DB.get(component_id)
    opts = options.Options()
    configs = configurations.Configurations()
    component = _set_status(component, opts, configs, config_details)
    return component, 200


@dbutils.redis_error_handler
def put_component(component_id):
    """Used by the PUT /components/{component_id} API operation"""
    LOGGER.debug("PUT /components/id invoked put_component")
    try:
        data = connexion.request.get_json()
    except Exception as err:
        return connexion.problem(
            status=400, title="Error parsing the data provided.",
            detail=str(err))
    data = _set_auto_fields(data)
    return DB.put(component_id, data), 200


@dbutils.redis_error_handler
def patch_component(component_id):
    """Used by the PATCH /components/{component_id} API operation"""
    LOGGER.debug("PATCH /components/id invoked patch_component")
    if component_id not in DB:
        return connexion.problem(
            status=404, title="Component could not found.",
            detail="Component {} could not be found".format(component_id))
    try:
        data = connexion.request.get_json()
    except Exception as err:
        return connexion.problem(
            status=400, title="Error parsing the data provided.",
            detail=str(err))
    data = _set_auto_fields(data)
    return DB.patch(component_id, data, _update_handler), 200


@dbutils.redis_error_handler
def delete_component(component_id):
    """Used by the DELETE /components/{component_id} API operation"""
    LOGGER.debug("DELETE /components/id invoked delete_component")
    if component_id not in DB:
        return connexion.problem(
            status=404, title="Component could not found.",
            detail="Component {} could not be found".format(component_id))
    return DB.delete(component_id), 204


def _set_auto_fields(data):
    data = _set_last_updated(data)
    if ('desiredState' in data or 'desiredConfig' in data or data.get('state') == [])\
            and 'errorCount' not in data:
        data['errorCount'] = 0
    return data


def _set_last_updated(data):
    if 'state' in data and type(data['state']) == list:
        for layer in data['state']:
            if 'lastUpdated' not in layer:
                layer['lastUpdated'] = datetime.now().strftime(TIME_FORMAT)
    if 'desiredState' in data and type(data['desiredState']) == dict:
        data['desiredState']['lastUpdated'] = datetime.now().strftime(TIME_FORMAT)
    return data


def _set_status(data, options, configs, config_details):
    if 'desiredConfig' in data:
        data['configurationStatus'] = STATUS[_get_status(data, options, configs,
                                                         config_details)]
    else:
        data['configurationStatus'] = STATUS[STATUS_DEPRECATED]
    return data


def _get_status(data, options, configs, config_details):
    """
    Returns the configuration status of a component

    If no desired configuration is set, the component is either unconfigured if there is not state,
        or configured if there is state.  There is no failed state condition, otherwise it's
        possible to go from failed to configured by changing the error count.
    When configuration is set, the component is configured if all layers of the configuration have
        been applied successfully, even if manual sessions have failed.  If there are still layers
        pending, the status depends on the the error count, and the component will be marked failed
        if the error count exceeds the retry count, even if the errors are due to manual sessions.
    """
    maxRetries = False
    retries = int(data.get('retryPolicy', options.default_batcher_retry_policy))
    if retries != -1 and data['errorCount'] >= retries:
        # This component has hit it's retry limit
        maxRetries = True

    currentState = _get_current_state(data)
    desiredState = _get_desired_state(data, configs=configs)

    if config_details:
        data['desiredState'] = []

    if not desiredState:
        if not currentState:
            return STATUS_UNCONFIGURED
        else:
            return STATUS_CONFIGURED

    desiredState = deepcopy(desiredState)

    status = STATUS_CONFIGURED
    for layer in desiredState['layers']:
        layer_status = _get_layer_status(layer, currentState, maxRetries, options)
        layer['status'] = STATUS[layer_status]
        status = min(status, layer_status)
    if (status == STATUS_PENDING) and maxRetries:
        # No desiredState layers have failed, but manual sessions have put this in a failed state
        status = STATUS_FAILED

    if config_details:
        data['desiredState'] = desiredState['layers']
    return status


def _get_layer_status(desiredState, currentStateLayers, maxRetries, options):
    desiredCloneUrl = desiredState.get('cloneUrl', '')
    desiredPlaybook = desiredState.get('playbook', '')
    if not desiredPlaybook:
        desiredPlaybook = options.default_playbook
    desiredCommit = desiredState.get('commit', '')

    if not (desiredCommit and desiredCloneUrl and desiredPlaybook):
        return STATUS_UNCONFIGURED

    for currentState in currentStateLayers:
        currentCommit = currentState.get('commit', '')
        if all([desiredCloneUrl == currentState.get('cloneUrl', ''),
                desiredPlaybook == currentState.get('playbook', ''),
                desiredCommit in currentCommit]):
            if '_failed' in currentCommit:
                if maxRetries:
                    return STATUS_FAILED
                else:
                    return STATUS_PENDING
            if '_incomplete' in currentCommit:
                # Set for successful nodes when any_errors_fatal causes a playbook to exit early.
                return STATUS_PENDING
            return STATUS_CONFIGURED
    return STATUS_PENDING


def _get_current_state(data):
    config = data['state']
    if type(config) == dict:
        config = [config]
    return config


def _get_desired_state(data, configs=None):
    if not configs:
        configs = configurations.Configurations()
    configName = data['desiredConfig']
    config = configs.get_config(configName)
    return config


def _update_handler(data):
    data = _state_append_handler(data)
    data = _tag_cleanup_handler(data)
    return data


def _tag_cleanup_handler(data):
    tags = data.get('tags', {})
    for k, v in tags.items():
        if v == '':
            del tags[k]
    return data


def _state_append_handler(data):
    if 'stateAppend' in data:
        stateAppend = data['stateAppend']
        if type(data['state']) != list:
            data['state'] = []
        if 'lastUpdated' not in stateAppend:
            stateAppend['lastUpdated'] = datetime.now().strftime(TIME_FORMAT)
        newState = []
        # If this configuration was previously applied, update the layer rather than just appending
        for layer in data['state']:
            if not (layer['cloneUrl'] == stateAppend['cloneUrl'] and
                    layer['playbook'] == stateAppend['playbook']):
                newState.append(layer)
        newState.append(stateAppend)
        data['state'] = newState
        del data['stateAppend']
    return data
