# Copyright 2019-2020, Cray Inc. All Rights Reserved.
import json
import logging
from uuid import UUID

import connexion
import flask
from kubernetes.client.rest import ApiException

from cray.cfs.api import dbutils
from cray.cfs.api.controllers.options import get_options_data
from cray.cfs.api.models.config_framework_session import ConfigFrameworkSession  # noqa: E501
from cray.cfs.api.models.config_framework_session_create import ConfigFrameworkSessionCreate  # noqa: E501

from cray.cfs.k8s import CFSV1K8SConnector


LOGGER = logging.getLogger('cray.cfs.api.controllers.sessions')

_cfsk8s = None


def _init(namespace='services'):
    """ Initialize the k8s api connector information """
    global _cfsk8s
    _cfsk8s = CFSV1K8SConnector(namespace)


def _k8s_sessionlist2openapi(api_response):
    """
    Convert a list of k8s Custom resource ConfigFrameworkSessions to a list of
    OpenAPI ConfigFrameworkSession objects.

    :rtype: List[ConfigFrameworkSession]
    """
    cfs_objs = []
    for resp in api_response['items']:
        cfs_objs.append(_k8s_session2openapi(resp))
    return cfs_objs


def _k8s_session2openapi(resp):
    """
    Convert a k8s ConfigFrameworkSession custom resource to an OpenAPI
    ConfigFrameworkSession object.

    :rtype: ConfigFrameworkSession
    """
    self_link = {
        'rel': 'self',
        'href': flask.url_for(
            '/apis/cfs.cray_cfs_api_controllers_sessions_get_session',
            session_name=resp['metadata']['name']
        )
    }
    k8s_link = {
        'rel': 'k8s',
        'href': resp['metadata']['selfLink']
    }

    # Stub out sections that are in the full object but may not exist yet
    # because the operator hasn't created them yet.
    if 'status' not in resp['spec']:
        status = {}
    else:
        status = resp['spec']['status']

    cfs = {
        'id': resp['metadata']['uid'],
        'name': resp['metadata']['name'],
        'repo': resp['spec']['repo'],
        'ansible': resp['spec']['ansible'],
        'target': resp['spec']['target'],
        'status': status,
        'links': [self_link, k8s_link]
    }
    return ConfigFrameworkSession.from_dict(cfs)


def _wrangleK8SApiException(err):
    """ Convert a Kubernetes ApiException to RFC7807 """
    return connexion.problem(
        detail=json.loads(err.body)['message'],
        status=err.status,
        title=err.reason
    )


@dbutils.redis_error_handler
def create_session():  # noqa: E501
    """Create a Config Framework Session

    Creates a new ConfigFrameworkSession # noqa: E501

    :rtype: ConfigFrameworkSession
    """
    # Create the session object, do openapi field validation
    LOGGER.debug("Create content: ", connexion.request.get_json())
    try:
        cfsc = ConfigFrameworkSessionCreate.from_dict(connexion.request.get_json())  # noqa: E501
    except ValueError as err:
        return connexion.problem(
            detail=err,
            status=400,
            title="Bad Request"
        )

    # Additional target section data validation
    validation_err = _validate_session_target(cfsc.target)
    if validation_err:
        return validation_err

    # If the following fields aren't set, use their configured default values.
    if not cfsc.ansible_config:
        cfsc.ansible_config = get_options_data()['defaultAnsibleConfig']
    if not cfsc.ansible_playbook:
        cfsc.ansible_playbook = get_options_data()['defaultPlaybook']
    if not cfsc.clone_url:
        cfsc.clone_url = get_options_data()['defaultCloneUrl']

    try:
        _cfsk8s.create(
            cfsc.name, cfsc.clone_url, cfsc.branch, cfsc.commit,
            cfsc.ansible_playbook, cfsc.ansible_limit, cfsc.ansible_config,
            cfsc.ansible_verbosity, cfsc.target.to_dict()
        )
    except ApiException as err:
        return _wrangleK8SApiException(err)

    return get_session(cfsc.name), 201


def delete_session(session_name):  # noqa: E501
    """Delete Config Framework Session

     # noqa: E501

    :param session_name: Config Framework Session name
    :type session_name: str

    :rtype: None
    """
    try:
        _cfsk8s.delete(session_name)
    except ApiException as err:
        return _wrangleK8SApiException(err)

    return None, 204


def get_session(session_name):  # noqa: E501
    """Config Framework Session Details

     # noqa: E501

    :param session_name: Config Framework Session name
    :type session_name: str

    :rtype: ConfigFrameworkSession
    """
    try:
        session = _cfsk8s.get(session_name)
    except ApiException as err:
        return _wrangleK8SApiException(err)

    return _k8s_session2openapi(session)


def get_sessions():  # noqa: E501
    """List Config Framework Sessions

     # noqa: E501

    :rtype: List[ConfigFrameworkSession]
    """
    try:
        session_list = _cfsk8s.list()
    except ApiException as err:
        return _wrangleK8SApiException(err)

    return _k8s_sessionlist2openapi(session_list)


def _validate_session_target(target):
    """Validate the target section

    :param target: Config Framework Session Target specification
    :type target: TargetSpecSection

    :rtype: None or connexion.problem if errors occur
    """
    status = 400
    title = "Bad Request"
    if target.definition in ('repo', 'dynamic'):
        if target.groups:
            return connexion.problem(
                detail="'{}' target definitions must not contain groups specifications.".format(target.definition),  # noqa: E501
                status=status,
                title=title
            )
    elif target.definition in ('spec', 'image'):
        if not target.groups:
            return connexion.problem(
                status=status,
                title=title,
                detail="At least one target group must be specified."
            )
        if any([getattr(grp, 'members', None) is None for grp in target.groups]):
            # Although members is required for a group, swagger is not checking if another
            # data type such as a string or array is passed instead of an object
            return connexion.problem(
                status=status,
                title=title,
                detail="Groups must be an object with the members property."
            )
        if any([grp.members == [] for grp in target.groups]):
            return connexion.problem(
                status=status,
                title=title,
                detail="Group member lists must not be empty."
            )
        if any([member == "" for grp in target.groups for member in grp.members]):  # noqa: E501
            return connexion.problem(
                status=status,
                title=title,
                detail="Group members must not be blank."
            )
        if target.definition == 'image':
            naughty_list = []
            for group in target.groups:
                for member in group.members:
                    try:
                        UUID(member, version=4)
                    except ValueError:
                        naughty_list.append((group.name, member))

            if naughty_list:
                return connexion.problem(
                    status=status,
                    title=title,
                    detail="The following Image target group member(s) are not valid UUIDs: %s." % naughty_list  # noqa: E501
                )
    else:
        # Model validation will handle this case
        pass

    return None
