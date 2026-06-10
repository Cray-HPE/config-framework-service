#
# MIT License
#
# (C) Copyright 2019-2026 Hewlett Packard Enterprise Development LP
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
import logging
import shlex
from uuid import UUID

import connexion
from connexion.lifecycle import ConnexionResponse as CxResponse
from csm_utils.logging import exc_type_msg

from cray.cfs.api.models.v3_session_data import V3SessionData as V3Session  # noqa: E501

from .db import SESSIONS_DB as DB
from .kafka import kafka_create_event
from .types import V3SessionData

LOGGER = logging.getLogger('cray.cfs.api.controllers.sessions.create_helpers')


class ArgumentParserError(Exception):
    pass


class ThrowingArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ArgumentParserError(message)


def validate_session_target(target):
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
                status=status,
                title=title,
                detail=f"'{target.definition}' target definitions must not "
                       "contain groups specifications."  # noqa: E501
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
                    detail="The following Image target group members are not "
                           f"valid UUIDs: {naughty_list}."  # noqa: E501
                )
    else:
        # Model validation will handle this case
        pass

    return None


def validate_ansible_passthrough(passthrough):
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
    except Exception as err:
        return connexion.problem(
            detail=f"Error validating ansible-passthrough: {exc_type_msg(err)}",
            status=400,
            title='Bad Request'
        )
    return None


def create_session(session_create):
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


def finish_session_create(data: V3SessionData) -> CxResponse | V3SessionData:
    """
    Common function shared between v2 and v3 which does the following:

    1. Set the session start time
    2. Send the CREATE event to the Kafka bus
    3. Write the session to the database
    """
    data['status']['session']['start_time'] = datetime.datetime.now().isoformat(timespec='seconds')
    kafka_create_event(data)
    session_name = data['name']
    LOGGER.debug("finish_session_create: Writing new session '%s' to database", session_name)
    response_data = DB.put(session_name, data)
    LOGGER.debug("finish_session_create: DB put complete for '%s'", session_name)
    return response_data
