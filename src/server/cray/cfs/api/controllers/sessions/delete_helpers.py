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

import logging
from typing import Optional

import connexion
from csm_utils.logging import exc_type_msg

from cray.cfs.api import dbutils

from .db import SESSIONS_DB as DB
from .filters import (
    get_session_filter,
    ParsingException,
)
from .kafka import kafka_delete_event
from .types import (
    DeleteSessionResponse,
    V3DeleteSessionsResponse,
)

LOGGER = logging.getLogger('cray.cfs.api.controllers.sessions.delete_helpers')


def delete_session(session_name: str) -> DeleteSessionResponse:
    """
    Deletes the session from the database.
    If it does not exist, return a 404 error.
    Otherwise, add a delete event for this session to the Kafka bus, and return None, 204
    """
    LOGGER.debug("delete_session: Deleting '%s' in database", session_name)
    try:
        session = DB.get_delete(session_name)
    except dbutils.DBNoEntryError as err:
        LOGGER.debug(err)
        return connexion.problem(
            status=404, title="Session not found.",
            detail=f"Session {session_name} could not be found")
    LOGGER.debug("delete_session: Deleted '%s' in database", session_name)
    kafka_delete_event(session)
    return None, 204


def delete_sessions(age: Optional[str],
                    min_age: Optional[str],
                    max_age: Optional[str],
                    status: Optional[str],
                    name_contains: Optional[str],
                    succeeded: Optional[str],
                    tags: Optional[str]) -> V3DeleteSessionsResponse:
    """Delete Config Framework Sessions

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
    :type succeeded: str
    :param tags: A filter on session tags
    :type tags: str

    :rtype: dict { "session_ids": [ "list", "of", "session", "ids" ] } (if successful)
    Otherwise returns a connexion.problem object
    """
    tag_list = []
    if tags:
        try:
            tag_list = [tuple(tag.split('=')) for tag in tags.split(',')]
            for tag in tag_list:
                assert len(tag) == 2
        except Exception as err:
            return connexion.problem(
                status=400, title="Error parsing the tags provided.",
                detail=exc_type_msg(err))
    try:
        session_filter = get_session_filter(age=age, min_age=min_age, max_age=max_age,
                                            status=status, name_contains=name_contains,
                                            succeeded=succeeded, tag_list=tag_list)
    except ParsingException as err:
        return connexion.problem(
            detail=exc_type_msg(err),
            status=400,
            title='Error parsing age field'
        )

    session_ids = DB.delete_all(session_filter, deletion_handler=kafka_delete_event)
    response = {"session_ids": session_ids}
    return response, 200
