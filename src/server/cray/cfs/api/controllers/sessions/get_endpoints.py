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

import connexion
from csm_utils.logging import exc_type_msg

from cray.cfs.api import dbutils
from cray.cfs.api.controllers import options
from cray.cfs.api.server_entrypoint import server_entrypoint

from .db import SESSIONS_DB as DB
from .get_helpers import get_filtered_sessions
from .types import (
    V2GetSessionResponse,
    V3GetSessionResponse,
)
from .utils import convert_session_to_v2, set_link

LOGGER = logging.getLogger('cray.cfs.api.controllers.sessions.get_endpoints')


@dbutils.redis_error_handler
@server_entrypoint
def get_session_v2(session_name: str) -> V2GetSessionResponse:  # noqa: E501
    """Config Framework Session Details

    :param session_name: Config Framework Session name
    :type session_name: str

    :rtype: V2Session
    """
    LOGGER.debug("GET /v2/sessions/%s invoked get_session_v2", session_name)
    try:
        v3_session_data = DB.get(session_name)
    except dbutils.DBNoEntryError as err:
        LOGGER.debug(err)
        return connexion.problem(
            status=404, title="Session not found.",
            detail=f"Session {session_name} could not be found")
    return convert_session_to_v2(v3_session_data), 200


@dbutils.redis_error_handler
@server_entrypoint
def get_session_v3(session_name: str) -> V3GetSessionResponse:  # noqa: E501
    """Config Framework Session Details

    :param session_name: Config Framework Session name
    :type session_name: str

    :rtype: V3Session
    """
    LOGGER.debug("GET /v3/sessions/%s invoked get_session_v3", session_name)
    try:
        v3_session_data = DB.get(session_name)
    except dbutils.DBNoEntryError as err:
        LOGGER.debug(err)
        return connexion.problem(
            status=404, title="Session not found.",
            detail=f"Session {session_name} could not be found")
    set_link(v3_session_data)
    return v3_session_data, 200


@dbutils.redis_error_handler
@server_entrypoint
def get_sessions_v2(age=None, min_age=None, max_age=None, status=None, name_contains=None,
                    succeeded=None, tags=None):  # noqa: E501
    """List Config Framework Sessions

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
                detail=exc_type_msg(err))
    sessions_data, next_page_exists = get_filtered_sessions(age, min_age, max_age, status,
                                                            name_contains, succeeded, tag_list)
    if next_page_exists:
        return connexion.problem(
            status=400, title="The response size is too large",
            detail="The response size exceeds the default_page_size. "
                   "Use the v3 API to page through the results.")  # noqa: E501
    return [convert_session_to_v2(session) for session in sessions_data], 200


@dbutils.redis_error_handler
@server_entrypoint
@options.defaults(limit="default_page_size")
def get_sessions_v3(age=None, min_age=None, max_age=None, status=None, name_contains=None,
                    succeeded=None, tags=None, limit=1, after_id=""):  # noqa: E501
    """List Config Framework Sessions

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
                detail=exc_type_msg(err))
    sessions_data, next_page_exists = get_filtered_sessions(age, min_age, max_age, status,
                                                            name_contains, succeeded, tag_list,
                                                            limit=limit, after_id=after_id)
    for session in sessions_data:
        set_link(session)
    response = {"sessions": sessions_data, "next": None}
    if next_page_exists:
        next_data = called_parameters
        next_data["after_id"] = sessions_data[-1]["name"]
        response["next"] = next_data
    return response, 200
