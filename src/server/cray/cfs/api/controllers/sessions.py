#
# MIT License
#
# (C) Copyright 2019-2025 Hewlett Packard Enterprise Development LP
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
import argparse
import datetime
from functools import partial
import logging
import re
import shlex
from uuid import UUID

import connexion
import dateutil

from cray.cfs.api import dbutils
from cray.cfs.api import kafka_utils
from cray.cfs.api.k8s_utils import get_ara_ui_url
from cray.cfs.api.controllers import options
from cray.cfs.api.models.v2_session import V2Session  # noqa: E501
from cray.cfs.api.models.v2_session_create import V2SessionCreate  # noqa: E501
from cray.cfs.api.models.v3_session_data import V3SessionData as V3Session  # noqa: E501
from cray.cfs.api.models.v3_session_create import V3SessionCreate  # noqa: E501

LOGGER = logging.getLogger('cray.cfs.api.controllers.sessions')
DB = dbutils.get_wrapper(db='sessions')
CONFIG_DB = dbutils.get_wrapper(db='configurations')

_kafka = None


def _init(topic='cfs-session-events'):
    """ Initialize the kafka producer information """
    global _kafka
    _kafka = kafka_utils.ProducerWrapper(topic)


@dbutils.redis_error_handler
def create_session_v2():  # noqa: E501
    """Create a Config Framework Session

    Creates a new V2Session # noqa: E501

    :rtype: V2Session
    """
    # Create the session object, do openapi field validation
    LOGGER.debug("POST /v2/sessions invoked create_session_v2")
    try:
        data = connexion.request.get_json()
        LOGGER.debug("Create content: %s", data)
        v2_session_create = V2SessionCreate.from_dict(connexion.request.get_json())  # noqa: E501
        # This is a workaround for the addition of the configuration name max length in v3
        # The configuration name is restored later
        v2_session_configuration = v2_session_create.configuration_name
        v2_session_create.configuration_name = "temp"
        # end workaround
        session_create = V3SessionCreate.from_dict(v2_session_create.to_dict())
    except Exception as err:
        return connexion.problem(
            detail=err,
            status=400,
            title="Bad Request"
        )

    if session_create.name in DB:
        return connexion.problem(
            detail=f"A session with the name {session_create.name} already exists",
            status=409,
            title="Conflicting session name"
        )

    if v2_session_configuration not in CONFIG_DB:
        return connexion.problem(
            detail=f"No configurations exist named {v2_session_configuration}",
            status=400,
            title="Invalid configuration"
        )

    # Additional target section data validation
    validation_err = _validate_session_target(session_create.target)
    if validation_err:
        return validation_err

    # Additional ansible passthrough data validation
    if session_create.ansible_passthrough:
        validation_err = _validate_ansible_passthrough(session_create.ansible_passthrough)
        if validation_err:
            return validation_err

    # If the following fields aren't set, use their configured default values.
    if not session_create.ansible_config:
        session_create.ansible_config = options.Options().default_ansible_config

    session = _create_session(session_create)
    session_data = session.to_dict()
    # This is a workaround for the addition of the configuration name max length in v3
    session_data['configuration']['name'] = v2_session_configuration
    # end workaround
    session_data['status']['session']['start_time'] = datetime.datetime.now().isoformat(
                                                                                timespec='seconds')
    _kafka.produce(event_type='CREATE', data=session_data)
    response_data = DB.put(session_data['name'], session_data)
    return convert_session_to_v2(response_data), 200


@dbutils.redis_error_handler
def create_session_v3():  # noqa: E501
    """Create a Config Framework Session

    Creates a new V3Session # noqa: E501

    :rtype: V3Session
    """
    # Create the session object, do openapi field validation
    LOGGER.debug("POST /v3/sessions invoked create_session_v3")
    try:
        data = connexion.request.get_json()
        LOGGER.debug("Create content: %s", data)
        session_create = V3SessionCreate.from_dict(connexion.request.get_json())
    except Exception as err:
        return connexion.problem(
            detail=err,
            status=400,
            title="Bad Request"
        )

    if session_create.name in DB:
        return connexion.problem(
            detail=f"A session with the name {session_create.name} already exists",
            status=409,
            title="Conflicting session name"
        )

    if session_create.configuration_name not in CONFIG_DB and not session_create.configuration_name.startswith("debug_"):  # noqa: E501
        return connexion.problem(
            detail=f"No configurations exist named {session_create.configuration_name}",
            status=400,
            title="Invalid configuration"
        )

    # Additional target section data validation
    validation_err = _validate_session_target(session_create.target)
    if validation_err:
        return validation_err

    # Additional ansible passthrough data validation
    if session_create.ansible_passthrough:
        validation_err = _validate_ansible_passthrough(session_create.ansible_passthrough)
        if validation_err:
            return validation_err

    # If the following fields aren't set, use their configured default values.
    if not session_create.ansible_config:
        session_create.ansible_config = options.Options().default_ansible_config

    session = _create_session(session_create)
    data = session.to_dict()
    data['status']['session']['start_time'] = datetime.datetime.now().isoformat(timespec='seconds')
    _kafka.produce(event_type='CREATE', data=data)
    response_data = DB.put(data['name'], data)
    _set_link(response_data)
    return response_data, 201


def _create_session(session_create):
    initial_status = {
        'session': {
            'status': 'pending',
            'succeeded': 'none',
        },
        'artifacts': []
    }
    tags = {}
    if session_create.tags:
        tags = session_create.tags
    body = {
        'name': session_create.name,
        'configuration': {
            "name": session_create.configuration_name,
            "limit": session_create.configuration_limit,
        },
        'ansible': {
            'limit': session_create.ansible_limit,
            'config': session_create.ansible_config,
            'verbosity': session_create.ansible_verbosity,
            'passthrough': session_create.ansible_passthrough
        },
        'status': initial_status,
        'tags': tags,
        'debug_on_failure': session_create.debug_on_failure,
    }
    if session_create.target:
        body['target'] = session_create.target.to_dict()
    else:
        body['target'] = {'definition': 'dynamic'}
    return V3Session.from_dict(body)


@dbutils.redis_error_handler
def delete_session_v2(session_name):  # noqa: E501
    """Delete Config Framework Session

     # noqa: E501

    :param session_name: Config Framework Session name
    :type session_name: str

    :rtype: None
    """
    LOGGER.debug("DELETE /v2/sessions/%s invoked delete_session_v2", session_name)
    session = DB.get_delete(session_name)
    if session is None:
        return connexion.problem(
            status=404, title="Session not found.",
            detail=f"Session {session_name} could not be found")
    _kafka.produce(event_type='DELETE', data=session)
    return None, 204


@dbutils.redis_error_handler
def delete_session_v3(session_name):  # noqa: E501
    """Delete Config Framework Session

     # noqa: E501

    :param session_name: Config Framework Session name
    :type session_name: str

    :rtype: None
    """
    LOGGER.debug("DELETE /v3/sessions/%s invoked delete_session_v3", session_name)
    session = DB.get_delete(session_name)
    if session is None:
        return connexion.problem(
            status=404, title="Session not found.",
            detail=f"Session {session_name} could not be found")
    _kafka.produce(event_type='DELETE', data=session)
    return None, 204


@dbutils.redis_error_handler
def delete_sessions_v2(age=None,  min_age=None, max_age=None,
                       status=None, name_contains=None, succeeded=None, tags=None):
    """Delete Config Framework Sessions

     # noqa: E501

    :param age: An age filter in the form 1d.
    :type age: str
    :param min_age: An age filter in the form 1d.
    :type min_age: str
    :param max_age: An age filter in the form 1d.
    :type max_age: str
    :param status: A session status filter
    :type status: str
    :param name_contains: A filter on session names
    :type name_contains: str
    :param succeeded: A filter on session success
    :type succeeded: bool
    :param tags: A filter on session tags
    :type tags: bool

    :rtype: None
    """
    LOGGER.debug("DELETE /v2/sessions invoked delete_sessions_v2")
    tag_list = []
    if tags:
        try:
            tag_list = [tuple(tag.split('=')) for tag in tags.split(',')]
            for tag in tag_list:
                assert len(tag) == 2
        except Exception as err:
            return connexion.problem(
                status=400, title="Error parsing the tags provided.",
                detail=str(err))
    try:
        sessions_data, _ = _get_filtered_sessions(age=age, min_age=min_age, max_age=max_age,
                                                  status=status, name_contains=name_contains,
                                                  succeeded=succeeded, tag_list=tag_list)
    except ParsingException as err:
        return connexion.problem(
            detail=str(err),
            status=400,
            title='Error parsing age field'
        )
    for session in sessions_data:
        DB.delete(session['name'])
        _kafka.produce(event_type='DELETE', data=session)
    return None, 204


@dbutils.redis_error_handler
def delete_sessions_v3(age=None,  min_age=None, max_age=None,
                       status=None, name_contains=None, succeeded=None, tags=None):
    """Delete Config Framework Sessions

     # noqa: E501

    :param age: An age filter in the form 1d.
    :type age: str
    :param min_age: An age filter in the form 1d.
    :type min_age: str
    :param max_age: An age filter in the form 1d.
    :type max_age: str
    :param status: A session status filter
    :type status: str
    :param name_contains: A filter on session names
    :type name_contains: str
    :param succeeded: A filter on session success
    :type succeeded: bool
    :param tags: A filter on session tags
    :type tags: bool

    :rtype: None
    """
    LOGGER.debug("DELETE /v3/sessions invoked delete_sessions_v3")
    tag_list = []
    if tags:
        try:
            tag_list = [tuple(tag.split('=')) for tag in tags.split(',')]
            for tag in tag_list:
                assert len(tag) == 2
        except Exception as err:
            return connexion.problem(
                status=400, title="Error parsing the tags provided.",
                detail=str(err))
    try:
        session_filter = _get_session_filter(age=age, min_age=min_age, max_age=max_age,
                                             status=status, name_contains=name_contains,
                                             succeeded=succeeded, tag_list=tag_list)
    except ParsingException as err:
        return connexion.problem(
            detail=str(err),
            status=400,
            title='Error parsing age field'
        )
    deletion_handler = partial(_kafka.produce, event_type='DELETE')
    session_ids = DB.delete_all(session_filter, deletion_handler=deletion_handler)
    response = {"session_ids": session_ids}
    return response, 200


@dbutils.redis_error_handler
def get_session_v2(session_name):  # noqa: E501
    """Config Framework Session Details

     # noqa: E501

    :param session_name: Config Framework Session name
    :type session_name: str

    :rtype: V2Session
    """
    LOGGER.debug("GET /v2/sessions/%s invoked get_session_v2", session_name)
    if session_name not in DB:
        return connexion.problem(
            status=404, title="Session not found.",
            detail=f"Session {session_name} could not be found")
    return convert_session_to_v2(DB.get(session_name)), 200


@dbutils.redis_error_handler
def get_session_v3(session_name):  # noqa: E501
    """Config Framework Session Details

     # noqa: E501

    :param session_name: Config Framework Session name
    :type session_name: str

    :rtype: V3Session
    """
    LOGGER.debug("GET /v3/sessions/%s invoked get_session_v3", session_name)
    if session_name not in DB:
        return connexion.problem(
            status=404, title="Session not found.",
            detail=f"Session {session_name} could not be found")
    session_data = DB.get(session_name)
    _set_link(session_data)
    return session_data, 200


@dbutils.redis_error_handler
def get_sessions_v2(age=None, min_age=None, max_age=None, status=None, name_contains=None,
                    succeeded=None, tags=None):  # noqa: E501
    """List Config Framework Sessions

     # noqa: E501

    :rtype: List[V2Session]
    """
    LOGGER.debug("GET /v2/sessions invoked get_sessions_v2")
    tag_list = []
    if tags:
        try:
            tag_list = [tuple(tag.split('=')) for tag in tags.split(',')]
            for tag in tag_list:
                assert len(tag) == 2
        except Exception as err:
            return connexion.problem(
                status=400, title="Error parsing the tags provided.",
                detail=str(err))
    sessions_data, next_page_exists = _get_filtered_sessions(age, min_age, max_age, status,
                                                             name_contains, succeeded, tag_list)
    if next_page_exists:
        return connexion.problem(
            status=400, title="The response size is too large",
            detail="The response size exceeds the default_page_size.  Use the v3 API to page through the results.")  # noqa: E501
    return [convert_session_to_v2(session) for session in sessions_data], 200


@dbutils.redis_error_handler
@options.defaults(limit="default_page_size")
def get_sessions_v3(age=None, min_age=None, max_age=None, status=None, name_contains=None,
                    succeeded=None, tags=None, limit=1, after_id=""):  # noqa: E501
    """List Config Framework Sessions

     # noqa: E501

    :rtype: List[V3Session]
    """
    LOGGER.debug("GET /v3/sessions invoked get_sessions_v3")
    called_parameters = locals()
    tag_list = []
    if tags:
        try:
            tag_list = [tuple(tag.split('=')) for tag in tags.split(',')]
            for tag in tag_list:
                assert len(tag) == 2
        except Exception as err:
            return connexion.problem(
                status=400, title="Error parsing the tags provided.",
                detail=str(err))
    sessions_data, next_page_exists = _get_filtered_sessions(age, min_age, max_age, status,
                                                             name_contains, succeeded, tag_list,
                                                             limit=limit, after_id=after_id)
    for session in sessions_data:
        _set_link(session)
    response = {"sessions": sessions_data, "next": None}
    if next_page_exists:
        next_data = called_parameters
        next_data["after_id"] = sessions_data[-1]["name"]
        response["next"] = next_data
    return response, 200


@dbutils.redis_error_handler
def patch_session_v2(session_name):
    """Update a Config Framework Session

    Updates a new V2Session # noqa: E501

    :rtype: V2Session
    """
    LOGGER.debug("PATCH /v2/sessions/%s invoked patch_session_v2", session_name)
    try:
        data = connexion.request.get_json()
        if any(key != 'status' for key in data):
            raise Exception('Only status can be updated after session creation')
    except Exception as err:
        return connexion.problem(
            status=400, title="Bad Request",
            detail=str(err))

    if session_name not in DB:
        return connexion.problem(
            status=404, title="Session not found.",
            detail=f"Session {session_name} could not be found")
    data = dbutils.convert_data_from_v2(data, V2Session)
    response_data = _patch_session(session_name, data)
    return convert_session_to_v2(response_data), 200


@dbutils.redis_error_handler
def patch_session_v3(session_name):
    """Update a Config Framework Session

    Updates a new V3Session # noqa: E501

    :rtype: V3Session
    """
    LOGGER.debug("PATCH /v3/sessions/%s invoked patch_session_v3", session_name)
    try:
        data = connexion.request.get_json()
        if any(key != 'status' for key in data):
            raise Exception('Only status can be updated after session creation')
    except Exception as err:
        return connexion.problem(
            status=400, title="Bad Request",
            detail=str(err))

    if session_name not in DB:
        return connexion.problem(
            status=404, title="Session not found.",
            detail=f"Session {session_name} could not be found")
    response_data = _patch_session(session_name, data)
    return response_data, 200


# Some status fields should not progress backwards.
# This allows us to have multiple sources of status without worrying about event ordering.
STATUS_ORDERING = {
    'status': ['pending', 'running', 'complete'],
    'succeeded': ['none', 'unknown', 'false', 'true'],
}


def _patch_session(session_name, new_data):
    data = DB.get(session_name)
    status = data['status']
    artifacts = status['artifacts']
    session = status['session']

    # Artifacts
    for artifact in new_data.get('status', {}).get('artifacts', []):
        for existing_artifact in artifacts:
            for key in artifact.keys():
                if existing_artifact.get(key) != artifact.get(key):
                    break  # Not the same artifact, move to next
            else:
                break  # All keys matched, stop looking
        else:
            artifacts.append(artifact)  # No artifacts matched

    # Session Status
    for key, value in new_data.get('status', {}).get('session', {}).items():
        if value:  # Never overwrite with an empty field
            if key in STATUS_ORDERING:
                ordering = STATUS_ORDERING[key]
                current_value = session.get(key)
                current_value_index = -1
                if current_value in ordering:
                    current_value_index = ordering.index(current_value)
                if value in ordering and ordering.index(value) > current_value_index:
                    session[key] = value
            else:
                session[key] = value
    return DB.put(session_name, data)


def _validate_session_target(target):
    """Validate the target section

    :param target: Config Framework Session Target specification
    :type target: TargetSpecSection

    :rtype: None or connexion.problem if errors occur
    """
    status = 400
    title = "Bad Request"
    if not target:
        # Use dynamic inventory by default
        return None
    if target.definition in ('repo', 'dynamic'):
        if target.groups:
            return connexion.problem(
                detail=f"'{target.definition}' target definitions must not contain groups specifications.",  # noqa: E501
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
        if any(getattr(grp, 'members', None) is None for grp in target.groups):
            # Although members is required for a group, swagger is not checking if another
            # data type such as a string or array is passed instead of an object
            return connexion.problem(
                status=status,
                title=title,
                detail="Groups must be an object with the members property."
            )
        if any(grp.members == [] for grp in target.groups):
            return connexion.problem(
                status=status,
                title=title,
                detail="Group member lists must not be empty."
            )
        if any(member == "" for grp in target.groups for member in grp.members):  # noqa: E501
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
                    detail=f"The following Image target group member(s) are not valid UUIDs: {naughty_list}."  # noqa: E501
                )
    else:
        # Model validation will handle this case
        pass

    return None


class ArgumentParserError(Exception):
    pass


class ThrowingArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ArgumentParserError(message)


def _validate_ansible_passthrough(passthrough):
    """Validate the ansible_passthrough

    :param passthrough: Config Framework Session Ansible passthrough
    :type passthrough: string

    :rtype: None or connexion.problem if errors occur
    """

    parser = ThrowingArgumentParser()
    parser.add_argument('-e', '--extra-vars', type=str)
    parser.add_argument('-f', '--forks', type=int)
    parser.add_argument('--skip-tags', type=str)
    parser.add_argument('--start-at-task', type=str)
    parser.add_argument('-t', '--tags', type=str)
    passthrough_arguments = shlex.split(passthrough, posix=False)
    try:
        parser.parse_args(passthrough_arguments)
    except Exception as e:
        return connexion.problem(
            detail=f"Error validating ansible-passthrough: {e}",
            status=400,
            title='Bad Request'
        )
    return None


@options.defaults(limit="default_page_size")
def _get_filtered_sessions(age, min_age, max_age, status, name_contains, succeeded, tag_list,
                           limit=1, after_id=""):
    filters = []
    filters.append(_get_session_filter(age, min_age, max_age, status, name_contains, succeeded,
                                       tag_list))
    session_data_page, next_page_exists = DB.get_all(limit=limit, after_id=after_id,
                                                     data_filters=filters)
    return session_data_page, next_page_exists


def _get_session_filter(age, min_age, max_age, status, name_contains, succeeded, tag_list):
    min_start = None
    max_start = None
    if age:
        try:
            max_start = _age_to_timestamp(age)
        except Exception as e:
            LOGGER.warning('Unable to parse age: %s', age)
            raise ParsingException(e) from e
    if min_age:
        try:
            max_start = _age_to_timestamp(min_age)
        except Exception as e:
            LOGGER.warning('Unable to parse min_age: %s', min_age)
            raise ParsingException(e) from e
    if max_age:
        try:
            min_start = _age_to_timestamp(max_age)
        except Exception as e:
            LOGGER.warning('Unable to parse max_age: %s', max_age)
            raise ParsingException(e) from e
    session_filter = partial(_session_filter, min_start=min_start, max_start=max_start,
                             status=status, name_contains=name_contains,
                             succeeded=succeeded, tag_list=tag_list)
    return session_filter


def _session_filter(session_data, min_start, max_start, status, name_contains, succeeded,
                    tag_list):
    if any([min_start, max_start, status, name_contains, succeeded, tag_list]):
        return _matches_filter(session_data, min_start, max_start, status, name_contains,
                               succeeded, tag_list)
    # No filter is being used so all components are valid
    return True


def _matches_filter(data, min_start, max_start, status, name_contains, succeeded, tags):
    session_name = data['name']
    if name_contains and name_contains not in session_name:
        return False
    session_status = data.get('status', {}).get('session', {})
    if status and status != session_status.get('status'):
        return False
    if succeeded and succeeded != session_status.get('succeeded'):
        return False
    start_time = session_status['start_time']
    session_start = None
    if start_time:
        session_start = dateutil.parser.parse(start_time).replace(tzinfo=None)
    if min_start and (not session_start or session_start < min_start):
        return False
    if max_start and (not session_start or session_start > max_start):
        return False
    if tags and any(data.get('tags', {}).get(k) != v for k, v in tags):
        return False
    return True


def _age_to_timestamp(age):
    delta = {}
    for interval in ['weeks', 'days', 'hours', 'minutes']:
        result = re.search(rf'(\d+)\w*{interval[0]}', age, re.IGNORECASE)
        if result:
            delta[interval] = int(result.groups()[0])
    delta = datetime.timedelta(**delta)
    return datetime.datetime.now() - delta


def _set_link(data):
    if options.Options().include_ara_links:
        data["logs"] = f"{get_ara_ui_url()}/?label={data['name']}"
    return data


def convert_session_to_v2(data):
    data = dbutils.convert_data_to_v2(data, V2Session)
    return data


def convert_session_to_v3(data):
    data = dbutils.convert_data_from_v2(data, V2Session)
    return data


class ParsingException(Exception):
    pass
