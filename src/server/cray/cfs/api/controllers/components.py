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
import connexion
from copy import deepcopy
from datetime import datetime
from functools import partial
import logging

from cray.cfs.api import dbutils
from cray.cfs.api.k8s_utils import get_ara_ui_url
from cray.cfs.api.controllers import options
from cray.cfs.api.controllers import configurations
from cray.cfs.api.models.v2_component_state import V2ComponentState as V2Component

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
def get_components_v2(ids="", status="", enabled=None, config_name="", config_details=False,
                   tags=""):
    """Used by the GET /components API operation for the v2 api"""
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
    components_data, next_page_exists = get_components_data(id_list=id_list, status_list=status_list, enabled=enabled,
                                                            config_name=config_name, config_details=config_details,
                                                            tag_list=tag_list)
    if next_page_exists:
        return connexion.problem(
            status=400, title="The response size is too large",
            detail="The response size exceeds the default_page_size.  Use the v3 API to page through the results.")
    response = [convert_component_to_v2(component) for component in components_data]
    return response, 200


@dbutils.redis_error_handler
@options.defaults(limit="default_page_size")
def get_components_v3(ids="", status="", enabled=None, config_name="", state_details=False, config_details=False,
                      tags="", limit=1, after_id=""):
    called_parameters = locals()
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
    components_data, next_page_exists = get_components_data(id_list=id_list, status_list=status_list, enabled=enabled,
                                                            config_name=config_name, config_details=config_details,
                                                            tag_list=tag_list, limit=limit, after_id=after_id)
    for component in components_data:
        _set_link(component)
        if not state_details:
            del component["state"]
    response = {"components": components_data, "next": None}
    if next_page_exists:
        next_data = called_parameters
        next_data["after_id"] = components_data[-1]["id"]
        response["next"] = next_data
    return response, 200


@options.defaults(limit="default_page_size")
def get_components_data(id_list=[], status_list=[], enabled=None, config_name="",
                        config_details=False, tag_list=[], limit=1, after_id=""):
    """Used by the GET /components API operation=

    Allows filtering using a comma separated list of ids.
    """
    configs = configurations.Configurations()
    component_filter = partial(_component_filter, config_details=config_details, configs=configs,
                               id_list=id_list, status_list=status_list, enabled=enabled,
                               config_name=config_name, tag_list=tag_list)
    component_data_page, next_page_exists = DB.get_all(limit=limit, after_id=after_id, data_filter=component_filter)
    return component_data_page, next_page_exists


def _component_filter(component_data, config_details, configs,
                      id_list, status_list, enabled, config_name, tag_list):
    _set_status(component_data, configs, config_details) # This sets the status both for filtering and for the response data
    if id_list or status_list or (enabled is not None) or config_name or tag_list:
        return _matches_filter(component_data, id_list, status_list, enabled, config_name, tag_list)
    else:
        # No filter is being used so all components are valid
        return True


def _matches_filter(data, id_list, status_list, enabled, config_name, tag_list):
    if id_list and not data.get("id") in id_list:
        return False
    if status_list and not data.get('configuration_status') in status_list:
        return False
    if enabled is not None and data.get('enabled') != enabled:
        return False
    if config_name and data.get('desired_config') != config_name:
        return False
    if tag_list and any([data.get('tags', {}).get(k) != v for k, v in tag_list]):
        return False
    return True


@dbutils.redis_error_handler
def put_components_v2():
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
        component_data = convert_component_to_v3(component_data)
        component_data = _set_auto_fields(component_data)
        response_data = DB.put(component_id, component_data)
        response.append(convert_component_to_v2(response_data))
    return response, 200


@dbutils.redis_error_handler
def put_components_v3():
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
    component_ids = []
    for component_id, component_data in components:
        component_data = _set_auto_fields(component_data)
        DB.put(component_id, component_data)
        component_ids.append(component_id)
    response = {"component_ids": component_ids}
    return response, 200


@dbutils.redis_error_handler
def patch_components_v2():
    """Used by the PATCH /components API operation"""
    LOGGER.debug("PATCH /components invoked patch_components")
    data = connexion.request.get_json()
    if isinstance(data, list):
        return patch_v2_components_list(data)
    elif isinstance(data, dict):
        return patch_v2_components_dict(data)
    else:
        return connexion.problem(
           status=400, title="Error parsing the data provided.",
           detail="Unexpected data type {}".format(str(type(data))))


def patch_v2_components_list(data):
    try:
        components = []
        for component_data in data:
            component_id = component_data['id']
            if component_id not in DB:
                return connexion.problem(
                    status=404, title="Component not found.",
                    detail="Component {} could not be found".format(component_id))
            components.append((component_id, component_data))
    except Exception as err:
        return connexion.problem(
            status=400, title="Error parsing the data provided.",
            detail=str(err))
    response = []
    for component_id, component_data in components:
        component_data = dbutils.convert_data_from_v2(component_data, V2Component)
        component_data = _set_auto_fields(component_data)
        response_data = DB.patch(component_id, component_data, _update_handler)
        response.append(convert_component_to_v2(response_data))
    return response, 200


def patch_v2_components_dict(data):
    filters = data.get("filters", {})
    id_list = []
    status_list = []
    tag_list = []
    if filters.get("ids", None):
        try:
            id_list = filters.get("ids", None).split(',')
        except Exception as err:
            return connexion.problem(
                status=400, title="Error parsing the ids provided.",
                detail=str(err))
    if filters.get("status", None):
        try:
            status_list = filters.get("status", None).split(',')
        except Exception as err:
            return connexion.problem(
                status=400, title="Error parsing the status provided.",
                detail=str(err))
    if filters.get("tags", None):
        try:
            tag_list = [tuple(tag.split('=')) for tag in filters.get("tags", None).split(',')]
            for tag in tag_list:
                assert(len(tag) == 2)
        except Exception as err:
            return connexion.problem(
                status=400, title="Error parsing the tags provided.",
                detail=str(err))

    components = []
    if id_list:
        for component_id in id_list:
            component_data = DB.get(component_id)
            if component_data:
                components.append((component_id, component_data))
    else:
        # On large scale systems, this response may be too large
        # use v3 for smaller responses
        components = DB.get_all()

    response = []
    patch = data.get("patch", {})
    if "id" in patch:
        del patch["id"]
    patch = dbutils.convert_data_from_v2(patch, V2Component)
    patch = _set_auto_fields(patch)
    for component_id, component_data in components:
        if _matches_filter(component_data, status_list, filters.get("enabled", None),
                           filters.get("config_name", None), tag_list):
            response_data = DB.patch(component_id, patch, _update_handler)
            response.append(convert_component_to_v2(response_data))
    return response, 200


@dbutils.redis_error_handler
def patch_components_v3():
    """Used by the PATCH /components API operation"""
    LOGGER.debug("PATCH /components invoked patch_components")
    data = connexion.request.get_json()
    if isinstance(data, list):
        return patch_v3_components_list(data)
    elif isinstance(data, dict):
        return patch_v3_components_dict(data)
    else:
        return connexion.problem(
           status=400, title="Error parsing the data provided.",
           detail="Unexpected data type {}".format(str(type(data))))


def patch_v3_components_list(data):
    try:
        components = []
        for component_data in data:
            component_id = component_data['id']
            if component_id not in DB:
                return connexion.problem(
                    status=404, title="Component not found.",
                    detail="Component {} could not be found".format(component_id))
            components.append((component_id, component_data))
    except Exception as err:
        return connexion.problem(
            status=400, title="Error parsing the data provided.",
            detail=str(err))
    component_ids = []
    for component_id, component_data in components:
        component_data = _set_auto_fields(component_data)
        DB.patch(component_id, component_data, _update_handler)
        component_ids.append(component_id)
    response = {"component_ids": component_ids}
    return response, 200


def patch_v3_components_dict(data):
    filters = data.get("filters", {})
    id_list = []
    status_list = []
    tag_list = []
    if filters.get("ids", None):
        try:
            id_list = filters.get("ids", None).split(',')
        except Exception as err:
            return connexion.problem(
                status=400, title="Error parsing the ids provided.",
                detail=str(err))
    if filters.get("status", None):
        try:
            status_list = filters.get("status", None).split(',')
        except Exception as err:
            return connexion.problem(
                status=400, title="Error parsing the status provided.",
                detail=str(err))
    if filters.get("tags", None):
        try:
            tag_list = [tuple(tag.split('=')) for tag in filters.get("tags", None).split(',')]
            for tag in tag_list:
                assert(len(tag) == 2)
        except Exception as err:
            return connexion.problem(
                status=400, title="Error parsing the tags provided.",
                detail=str(err))

    configs = configurations.Configurations()
    component_filter = partial(_component_filter, config_details=False, configs=configs,
                               id_list=id_list, status_list=status_list, enabled=filters.get("enabled", None),
                               config_name=filters.get("config_name", None), tag_list=tag_list)
    patch = data.get("patch", {})
    if "id" in patch:
        del patch["id"]
    patch = _set_auto_fields(patch)
    component_ids = DB.patch_all(component_filter, patch, _update_handler)
    response = {"component_ids": component_ids}
    return response, 200


@dbutils.redis_error_handler
def get_component_v2(component_id, config_details=False):
    """Used by the GET /components/{component_id} API operation"""
    LOGGER.debug("GET /components/id invoked get_component")
    if component_id not in DB:
        return connexion.problem(
            status=404, title="Component not found.",
            detail="Component {} could not be found".format(component_id))
    component = DB.get(component_id)
    configs = configurations.Configurations()
    component = _set_status(component, configs, config_details)
    component = convert_component_to_v2(component)
    return component, 200


@dbutils.redis_error_handler
def get_component_v3(component_id, state_details=False, config_details=False):
    """Used by the GET /components/{component_id} API operation"""
    LOGGER.debug("GET /components/id invoked get_component")
    if component_id not in DB:
        return connexion.problem(
            status=404, title="Component not found.",
            detail="Component {} could not be found".format(component_id))
    component = DB.get(component_id)
    configs = configurations.Configurations()
    component = _set_status(component, configs, config_details)
    component = _set_link(component)
    if not state_details:
        del component["state"]
    return component, 200


@dbutils.redis_error_handler
def put_component_v2(component_id):
    """Used by the PUT /components/{component_id} API operation"""
    LOGGER.debug("PUT /components/id invoked put_component")
    try:
        data = connexion.request.get_json()
    except Exception as err:
        return connexion.problem(
            status=400, title="Error parsing the data provided.",
            detail=str(err))
    data = convert_component_to_v3(data)
    data = _set_auto_fields(data)
    response_data = DB.put(component_id, data)
    return convert_component_to_v2(response_data), 200


@dbutils.redis_error_handler
def put_component_v3(component_id):
    """Used by the PUT /components/{component_id} API operation"""
    LOGGER.debug("PUT /components/id invoked put_component")
    try:
        data = connexion.request.get_json()
    except Exception as err:
        return connexion.problem(
            status=400, title="Error parsing the data provided.",
            detail=str(err))
    data["id"] = component_id
    data = _set_auto_fields(data)
    return DB.put(component_id, data), 200


@dbutils.redis_error_handler
def patch_component_v2(component_id):
    """Used by the PATCH /components/{component_id} API operation"""
    LOGGER.debug("PATCH /components/id invoked patch_component")
    if component_id not in DB:
        return connexion.problem(
            status=404, title="Component not found.",
            detail="Component {} could not be found".format(component_id))
    try:
        data = connexion.request.get_json()
    except Exception as err:
        return connexion.problem(
            status=400, title="Error parsing the data provided.",
            detail=str(err))
    data = dbutils.convert_data_from_v2(data, V2Component)
    data = _set_auto_fields(data)
    response_data = DB.patch(component_id, data, _update_handler)
    return convert_component_to_v2(response_data), 200


@dbutils.redis_error_handler
def patch_component_v3(component_id):
    """Used by the PATCH /components/{component_id} API operation"""
    LOGGER.debug("PATCH /components/id invoked patch_component")
    if component_id not in DB:
        return connexion.problem(
            status=404, title="Component not found.",
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
def delete_component_v2(component_id):
    """Used by the DELETE /components/{component_id} API operation"""
    LOGGER.debug("DELETE /components/id invoked delete_component")
    if component_id not in DB:
        return connexion.problem(
            status=404, title="Component not found.",
            detail="Component {} could not be found".format(component_id))
    return DB.delete(component_id), 204


@dbutils.redis_error_handler
def delete_component_v3(component_id):
    """Used by the DELETE /components/{component_id} API operation"""
    LOGGER.debug("DELETE /components/id invoked delete_component")
    if component_id not in DB:
        return connexion.problem(
            status=404, title="Component not found.",
            detail="Component {} could not be found".format(component_id))
    return DB.delete(component_id), 204


def _set_auto_fields(data):
    data = _set_last_updated(data)
    if ('desired_state' in data or 'desired_config' in data or data.get('state') == [])\
            and 'error_count' not in data:
        data['error_count'] = 0
    return data


def _set_last_updated(data):
    if 'state' in data and type(data['state']) == list:
        for layer in data['state']:
            if 'last_updated' not in layer:
                layer['last_updated'] = datetime.now().strftime(TIME_FORMAT)
    if 'desired_state' in data and type(data['desired_state']) == dict:
        data['desired_state']['last_updated'] = datetime.now().strftime(TIME_FORMAT)
    return data


def _set_status(data, configs, config_details):
    if 'desired_config' in data:
        data['configuration_status'] = STATUS[_get_status(data, configs, config_details)]
    else:
        data['configuration_status'] = STATUS[STATUS_DEPRECATED]
    return data


def _get_status(data, configs, config_details):
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
    max_retries = False
    retries = data.get('retry_policy')
    if retries is None:
        retries = options.Options().default_batcher_retry_policy
    retries = int(retries)
    if retries != -1 and data['error_count'] >= retries:
        # This component has hit it's retry limit
        max_retries = True

    current_state = _get_current_state(data)
    desired_state = _get_desired_state(data, configs=configs)

    if config_details:
        data['desired_state'] = []

    if not desired_state:
        if not current_state:
            return STATUS_UNCONFIGURED
        else:
            return STATUS_CONFIGURED

    desired_state = deepcopy(desired_state)

    status = STATUS_CONFIGURED
    for layer in desired_state['layers']:
        layer_status = _get_layer_status(layer, current_state, max_retries)
        layer['status'] = STATUS[layer_status]
        status = min(status, layer_status)
    if (status == STATUS_PENDING) and max_retries:
        # No desired_state layers have failed, but manual sessions have put this in a failed state
        status = STATUS_FAILED

    if config_details:
        data['desired_state'] = desired_state['layers']
    return status


def _get_layer_status(desired_state, current_state_layers, max_retries):
    desired_clone_url = desired_state.get('clone_url', '')
    desired_playbook = desired_state.get('playbook', '')
    if not desired_playbook:
        desired_playbook = options.Options().default_playbook
    desired_commit = desired_state.get('commit', '')

    if not (desired_commit and desired_clone_url and desired_playbook):
        return STATUS_UNCONFIGURED

    for current_state in current_state_layers:
        current_status = current_state.get('status', '')
        if all([desired_clone_url == current_state.get('clone_url', ''),
                desired_playbook == current_state.get('playbook', ''),
                desired_commit == current_state.get('commit', '')]):
            if current_status == 'failed':
                if max_retries:
                    return STATUS_FAILED
                else:
                    return STATUS_PENDING
            if current_status == 'incomplete' :
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
    config_name = data['desired_config']
    config = configs.get_config(config_name)
    return config


def _set_link(data):
    if options.Options().include_ara_links:
        data["logs"] = f"{get_ara_ui_url()}/hosts?name={data['id']}"
    return data


def _update_handler(data):
    data = _state_append_handler(data)
    data = _tag_cleanup_handler(data)
    return data


def _tag_cleanup_handler(data):
    tags = data.get('tags', {})
    clean_tags = {}
    for k, v in tags.items():
        if v:
            clean_tags[k] = v
    data['tags'] = clean_tags
    return data


def _state_append_handler(data):
    if 'state_append' in data:
        state_append = data['state_append']
        if type(data['state']) != list:
            data['state'] = []
        if 'last_updated' not in state_append:
            state_append['last_updated'] = datetime.now().strftime(TIME_FORMAT)
        state_append = _convert_component_layer_to_v3(state_append)
        new_state = []
        # If this configuration was previously applied, update the layer rather than just appending
        for layer in data['state']:
            if not (layer['clone_url'] == state_append['clone_url'] and
                    layer['playbook'] == state_append['playbook']):
                new_state.append(layer)
        new_state.append(state_append)
        data['state'] = new_state
        del data['state_append']
    return data


def convert_component_to_v2(data):
    converted_state = [_convert_component_layer_to_v2(layer) for layer in data["state"]]
    data["state"] = converted_state
    data = dbutils.convert_data_to_v2(data, V2Component)
    return data


def convert_component_to_v3(data):
    data = dbutils.convert_data_from_v2(data, V2Component)
    converted_state = [_convert_component_layer_to_v3(layer) for layer in data["state"]]
    data["state"] = converted_state
    return data


def _convert_component_layer_to_v2(layer):
    if "status" in layer and layer["status"] != "applied":
        layer["commit"] = f"{layer['commit']}_{layer['status']}"
        del layer["status"]
    return layer


def _convert_component_layer_to_v3(layer):
    if "_" in layer["commit"]:
        commit, status = layer["commit"].split("_")
        layer["commit"] = commit
        layer["status"] = status
    elif "status" not in layer:
        layer["status"] = "applied"
    return layer
