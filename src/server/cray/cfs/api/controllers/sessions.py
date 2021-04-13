# Copyright 2019-2021 Hewlett Packard Enterprise Development LP

import datetime
import dateutil
import logging
import re
from uuid import UUID

import connexion

from cray.cfs.api import dbutils
from cray.cfs.api import kafka_utils
from cray.cfs.api.controllers.options import get_options_data
from cray.cfs.api.models.v1_session import V1Session  # noqa: E501
from cray.cfs.api.models.v2_session import V2Session  # noqa: E501
from cray.cfs.api.models.v1_session_create import V1SessionCreate  # noqa: E501
from cray.cfs.api.models.v2_session_create import V2SessionCreate  # noqa: E501

LOGGER = logging.getLogger('cray.cfs.api.controllers.sessions')
DB = dbutils.get_wrapper(db='sessions')
CONFIG_DB = dbutils.get_wrapper(db='configurations')

_kafka = None


def _init(topic='cfs-session-events'):
    """ Initialize the kafka producer information """
    global _kafka
    _kafka = kafka_utils.ProducerWrapper(topic)




@dbutils.redis_error_handler
def create_session():  # noqa: E501
    """Create a Config Framework Session

    Creates a new V1Session # noqa: E501

    :rtype: V1Session
    """
    # Create the session object, do openapi field validation
    LOGGER.debug("Create content: ", connexion.request.get_json())
    try:
        session_create = V1SessionCreate.from_dict(connexion.request.get_json())  # noqa: E501
    except ValueError as err:
        return connexion.problem(
            detail=err,
            status=400,
            title="Bad Request"
        )

    if session_create.name in DB:
        return connexion.problem(
            detail="A session with the name {} already exists".format(session_create.name),
            status=409,
            title="Conflicting session name"
        )

    # Additional target section data validation
    validation_err = _validate_session_target(session_create.target)
    if validation_err:
        return validation_err

    # If the following fields aren't set, use their configured default values.
    if not session_create.ansible_config:
        session_create.ansible_config = get_options_data()['defaultAnsibleConfig']
    if not session_create.ansible_playbook:
        session_create.ansible_playbook = get_options_data()['defaultPlaybook']
    if not session_create.clone_url:
        session_create.clone_url = get_options_data()['defaultCloneUrl']

    session = _create_session(session_create)
    session = session.to_dict()
    data = dbutils.snake_to_camel_json(session)
    _kafka.produce('CREATE', data=data)
    response = DB.put(data['name'], data)

    return response, 200


def _create_session(session_create):
    initial_status = {
        'session': {
            'status': 'pending',
            'succeeded': 'none',
        },
        'artifacts': []
    }
    body = {
        'name': session_create.name,
        'repo': {
            "cloneUrl": session_create.clone_url,
            "branch": session_create.branch,
            "commit": session_create.commit,
        },
        'ansible': {
            'playbook': session_create.ansible_playbook,
            'limit': session_create.ansible_limit,
            'config': session_create.ansible_config,
            'verbosity': session_create.ansible_verbosity,
        },
        'target': session_create.target.to_dict(),
        'status': initial_status,
    }
    return V1Session.from_dict(body)


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
        LOGGER.debug("Create content: ", data)
        session_create = V2SessionCreate.from_dict(connexion.request.get_json())  # noqa: E501
    except Exception as err:
        return connexion.problem(
            detail=err,
            status=400,
            title="Bad Request"
        )

    if session_create.name in DB:
        return connexion.problem(
            detail="A session with the name {} already exists".format(session_create.name),
            status=409,
            title="Conflicting session name"
        )

    if session_create.configuration_name not in CONFIG_DB:
        return connexion.problem(
            detail="No configurations exist named {}".format(session_create.configuration_name),
            status=400,
            title="Invalid configuration"
        )

    # Additional target section data validation
    validation_err = _validate_session_target(session_create.target)
    if validation_err:
        return validation_err

    # If the following fields aren't set, use their configured default values.
    if not session_create.ansible_config:
        session_create.ansible_config = get_options_data()['defaultAnsibleConfig']

    session = _create_session_v2(session_create)
    session = session.to_dict()
    data = dbutils.snake_to_camel_json(session)
    data['tags'] = session['tags']  # Don't alter these, they are user defined
    _kafka.produce('CREATE', data=data)
    response = DB.put(data['name'], data)

    return response, 200


def _create_session_v2(session_create):
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
        },
        'target': session_create.target.to_dict(),
        'status': initial_status,
        'tags': tags,
    }
    return V2Session.from_dict(body)


@dbutils.redis_error_handler
def delete_session(session_name):  # noqa: E501
    """Delete Config Framework Session

     # noqa: E501

    :param session_name: Config Framework Session name
    :type session_name: str

    :rtype: None
    """
    LOGGER.debug("DELETE /sessions/id invoked delete_session")
    return _delete_session(session_name)


@dbutils.redis_error_handler
def delete_session_v2(session_name):  # noqa: E501
    """Delete Config Framework Session

     # noqa: E501

    :param session_name: Config Framework Session name
    :type session_name: str

    :rtype: None
    """
    LOGGER.debug("DELETE /v2/sessions/id invoked delete_session_v2")
    return _delete_session(session_name)


def _delete_session(session_name):  # noqa: E501
    if session_name not in DB:
        return connexion.problem(
            status=404, title="Session could not found.",
            detail="Session {} could not be found".format(session_name))
    session = DB.get(session_name)
    DB.delete(session_name)
    _kafka.produce('DELETE', data=session)
    return None, 204


@dbutils.redis_error_handler
def delete_sessions(age=None, min_age=None, max_age=None,
                    status=None, name_contains=None, succeeded=None):  # noqa: E501
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

    :rtype: None
    """
    LOGGER.debug("DELETE /sessions invoked delete_sessions")
    _delete_sessions(age, min_age, max_age, status, name_contains, succeeded)


@dbutils.redis_error_handler
def delete_sessions_v2(age=None,  min_age=None, max_age=None,
                       status=None, name_contains=None, succeeded=None, tags=None):  # noqa: E501
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
    _delete_sessions(age, min_age, max_age, status, name_contains, succeeded, tags)


def _delete_sessions(age=None, min_age=None, max_age=None, status=None, name_contains=None,
                     succeeded=None, tags=None):  # noqa: E501
    tag_list = []
    if tags:
        try:
            tag_list = [tuple(tag.split('=')) for tag in tags.split(',')]
            for tag in tag_list:
                assert(len(tag) == 2)
        except Exception as err:
            return connexion.problem(
                status=400, title="Error parsing the tags provided.",
                detail=str(err))
    try:
        sessions = _get_filtered_sessions(age=age, min_age=min_age, max_age=max_age,
                                          status=status, name_contains=name_contains,
                                          succeeded=succeeded, tag_list=tag_list)
        for session in sessions:
            session_name = session['name']
            DB.delete(session_name)
            _kafka.produce('DELETE', data=session)
    except ParsingException as err:
        return connexion.problem(
            detail=str(err),
            status=400,
            title='Error parsing age field'
        )
    return None, 204


@dbutils.redis_error_handler
def get_session(session_name):  # noqa: E501
    """Config Framework Session Details

     # noqa: E501

    :param session_name: Config Framework Session name
    :type session_name: str

    :rtype: V1Session
    """
    LOGGER.debug("GET /sessions/id invoked get_session")
    return _get_session(session_name)


@dbutils.redis_error_handler
def get_session_v2(session_name):  # noqa: E501
    """Config Framework Session Details

     # noqa: E501

    :param session_name: Config Framework Session name
    :type session_name: str

    :rtype: V2Session
    """
    LOGGER.debug("GET /v2/sessions/id invoked get_session_v2")
    return _get_session(session_name)


def _get_session(session_name):  # noqa: E501
    if session_name not in DB:
        return connexion.problem(
            status=404, title="Session could not found.",
            detail="Session {} could not be found".format(session_name))
    return DB.get(session_name), 200


@dbutils.redis_error_handler
def get_sessions(age=None, min_age=None, max_age=None,
                 status=None, name_contains=None, succeeded=None):  # noqa: E501
    """List Config Framework Sessions

     # noqa: E501

    :rtype: List[V1Session]
    """
    LOGGER.debug("GET /sessions invoked get_sessions")
    return _get_sessions(age, min_age, max_age, status, name_contains, succeeded)


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
                assert(len(tag) == 2)
        except Exception as err:
            return connexion.problem(
                status=400, title="Error parsing the tags provided.",
                detail=str(err))
    return _get_sessions(age, min_age, max_age, status, name_contains, succeeded, tag_list)


def _get_sessions(age=None, min_age=None, max_age=None, status=None, name_contains=None,
                  succeeded=None, tag_list=None):  # noqa: E501
    return _get_filtered_sessions(age, min_age, max_age, status,
                                  name_contains, succeeded, tag_list), 200


@dbutils.redis_error_handler
def patch_session_v2(session_name):
    """Update a Config Framework Session

    Updates a new V2Session # noqa: E501

    :rtype: V2Session
    """
    LOGGER.debug("PATCH /v2/sessions/id invoked patch_session_v2")
    try:
        data = connexion.request.get_json()
        for key in data.keys():
            if key != 'status':
                raise Exception('Only status can be updated after session creation')
    except Exception as err:
        return connexion.problem(
            status=400, title="Bad Request",
            detail=str(err))

    if session_name not in DB:
        return connexion.problem(
            status=404, title="Session could not found.",
            detail="Session {} could not be found".format(session_name))
    response = _patch_session(session_name, data)
    return response, 200


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


def _get_filtered_sessions(age, min_age, max_age, status, name_contains, succeeded, tag_list):
    response = DB.get_all()
    min_start = None
    max_start = None
    if age:
        try:
            max_start = _age_to_timestamp(age)
        except Exception as e:
            LOGGER.warning('Unable to parse age: {}'.format(age))
            raise ParsingException(e) from e
    if min_age:
        try:
            max_start = _age_to_timestamp(min_age)
        except Exception as e:
            LOGGER.warning('Unable to parse age: {}'.format(age))
            raise ParsingException(e) from e
    if max_age:
        try:
            min_start = _age_to_timestamp(max_age)
        except Exception as e:
            LOGGER.warning('Unable to parse age: {}'.format(age))
            raise ParsingException(e) from e
    if any([min_start, max_start, status, name_contains, succeeded, tag_list]):
        response = [r for r in response if _matches_filter(r, min_start, max_start, status,
                                                           name_contains, succeeded, tag_list)]
    return response


def _matches_filter(data, min_start, max_start, status, name_contains, succeeded, tags):
    session_name = data['name']
    if name_contains and name_contains not in session_name:
        return False
    session_status = data.get('status', {}).get('session', {})
    if status and status != session_status.get('status'):
        return False
    if succeeded and succeeded != session_status.get('succeeded'):
        return False
    start_time = session_status['startTime']
    session_start = None
    if start_time:
        session_start = dateutil.parser.parse(start_time).replace(tzinfo=None)
    if min_start and (not session_start or session_start < min_start):
        return False
    if max_start and (not session_start or session_start > max_start):
        return False
    if tags and any([data.get('tags', {}).get(k) != v for k, v in tags]):
        return False
    return True


def _age_to_timestamp(age):
    delta = {}
    for interval in ['weeks', 'days', 'hours', 'minutes']:
        result = re.search('(\d+)\w*{}'.format(interval[0]), age, re.IGNORECASE)
        if result:
            delta[interval] = int(result.groups()[0])
    delta = datetime.timedelta(**delta)
    return datetime.datetime.now() - delta


class ParsingException(Exception):
    pass
