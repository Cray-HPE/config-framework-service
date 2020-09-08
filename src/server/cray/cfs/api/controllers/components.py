# Cray-provided controllers for the Configuration Framework Service
# Copyright 2020, Cray Inc. All Rights Reserved.

import connexion
from datetime import datetime
import logging

from cray.cfs.api import dbutils
from cray.cfs.api.controllers import options

LOGGER = logging.getLogger('cray.cfs.api.controllers.components')
DB = dbutils.get_wrapper(db='components')
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

STATUS_UNCONFIGURED = 'unconfigured'
STATUS_PENDING = 'pending'
STATUS_FAILED = 'failed'
STATUS_CONFIGURED = 'configured'


@dbutils.redis_error_handler
def get_components(ids="", status=[], enabled=None):
    """Used by the GET /components API operation

    Allows filtering using a comma seperated list of ids.
    """
    LOGGER.debug("GET /components invoked get_components")
    response = []
    if ids:
        try:
            id_list = ids.split(',')
        except Exception as err:
            return connexion.problem(
                status=400, title="Error parsing the data provided.",
                detail=str(err))
        for component_id in id_list:
            data = DB.get(component_id)
            if data:
                response.append(data)
    else:
        # TODO: On large scale systems, this response may be too large
        # and require paging to be implemented
        response = DB.get_all()
    if status or (enabled is not None):
        if status:
            status = status.split(',')

        response = [r for r in response if _matches_filter(r, status, enabled)]
    return response, 200


def _matches_filter(data, status, enabled):
    data_status = data.get('configurationStatus', '')
    if status and not any([data_status == s for s in status]):
        return False
    if enabled is not None and data.get('enabled', None) != enabled:
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
    opts = options.Options()
    for component_id, component_data in components:
        component_data = _set_auto_fields(component_data)
        response.append(DB.patch(component_id, component_data, status_handler(opts)))
    return response, 200


@dbutils.redis_error_handler
def get_component(component_id):
    """Used by the GET /components/{component_id} API operation"""
    LOGGER.debug("GET /components/id invoked get_component")
    if component_id not in DB:
        return connexion.problem(
            status=404, title="Component could not found.",
            detail="Component {} could not be found".format(component_id))
    return DB.get(component_id), 200


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
    return DB.patch(component_id, data, status_handler()), 200


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
    if 'desiredState' in data and 'errorCount' not in data:
        data['errorCount'] = 0
    return data


def _set_last_updated(data):
    if 'state' in data:
        data['state']['lastUpdated'] = datetime.now().strftime(TIME_FORMAT)
    if 'desiredState' in data:
        data['desiredState']['lastUpdated'] = datetime.now().strftime(TIME_FORMAT)
    return data


def status_handler(opts=None):
    if not opts:
        opts = options.Options()

    def _set_status(data):
        data['configurationStatus'] = _get_status(data, opts)
        return data
    return _set_status


def _get_status(data, options):
    retries = int(data.get('retryPolicy', options.default_batcher_retry_policy))
    if retries != -1 and data['errorCount'] >= retries:
        # This component has hit it's retry limit
        return STATUS_FAILED

    # Current State
    currentState = data.get('state', {})
    currentCommit = currentState.get('commit', '')
    if '_skipped' in currentCommit:
        # This desired configuration was run successfully,
        # but this component's state was not updated.
        # However if the desired state has changed, we do want to run CFS.
        currentCommit = currentCommit[:-len('_skipped')]
    currentStateSet = bool(currentCommit)

    # Desired State
    desiredState = data.get('desiredState', {})
    desiredCloneUrl = desiredState.get('cloneUrl', '')
    if not desiredCloneUrl:
        desiredCloneUrl = options.default_clone_url
    desiredPlaybook = desiredState.get('playbook', '')
    if not desiredPlaybook:
        desiredPlaybook = options.default_playbook
    desiredCommit = desiredState.get('commit', '')
    desiredStateSet = True
    if not (desiredCommit and desiredCloneUrl and desiredPlaybook):
        desiredStateSet = False

    if desiredStateSet:
        if all([desiredCloneUrl == currentState.get('cloneUrl', ''),
                desiredPlaybook == currentState.get('playbook', ''),
                desiredCommit == currentCommit]):
            # The component is already in the desired state
            return STATUS_CONFIGURED
        else:
            return STATUS_PENDING

    if currentStateSet:
        # Configuration has run, but desired state has since been unset
        if '_failed' in currentCommit:
            return STATUS_FAILED
        else:
            return STATUS_CONFIGURED

    # If there is no desired or current state.
    return STATUS_UNCONFIGURED
