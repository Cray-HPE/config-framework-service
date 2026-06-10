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
from connexion.lifecycle import ConnexionResponse as CxResponse
from csm_utils.logging import exc_type_msg

from cray.cfs.api import dbutils
from cray.cfs.api.server_entrypoint import server_entrypoint

from .delete_helpers import delete_session, delete_sessions
from .types import (
    DeleteSessionResponse,
    V2DeleteSessionsResponse,
    V3DeleteSessionsResponse,
)

LOGGER = logging.getLogger('cray.cfs.api.controllers.sessions.delete_endpoints')


@dbutils.redis_error_handler
@server_entrypoint
def delete_session_v2(session_name: str) -> DeleteSessionResponse:  # noqa: E501
    """Delete Config Framework Session

    :param session_name: Config Framework Session name
    :type session_name: str

    :rtype: None
    """
    LOGGER.debug("DELETE /v2/sessions/%s invoked delete_session_v2", session_name)
    return delete_session(session_name)


@dbutils.redis_error_handler
@server_entrypoint
def delete_session_v3(session_name: str) -> DeleteSessionResponse:  # noqa: E501
    """Delete Config Framework Session

    :param session_name: Config Framework Session name
    :type session_name: str

    :rtype: None
    """
    LOGGER.debug("DELETE /v3/sessions/%s invoked delete_session_v3", session_name)
    return delete_session(session_name)


@dbutils.redis_error_handler
@server_entrypoint
def delete_sessions_v2(age: Optional[str] = None,
                       min_age: Optional[str] = None,
                       max_age: Optional[str] = None,
                       status: Optional[str] = None,
                       name_contains: Optional[str] = None,
                       succeeded: Optional[str] = None,
                       tags: Optional[str] = None) -> V2DeleteSessionsResponse:
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

    :rtype: None
    """
    LOGGER.debug("DELETE /v2/sessions invoked delete_sessions_v2")
    # This endpoint is the same as the v3 version except for what it returns when successful:
    # the v3 endpoint returns 204 status and a dict containing the deleted IDs
    # the v2 endpoint returns 200 status and None
    #
    # Because of this, both endpoints use a common function to do the actual work.
    response, status_code = delete_sessions(age=age, min_age=min_age, max_age=max_age,
                                            status=status, name_contains=name_contains,
                                            succeeded=succeeded, tags=tags)
    if status_code == 200:
        # This means it was successful. The v2 endpoint returns None, 204 in this case.
        return None, 204

    # This means there was an error, in which case the v2 and v3 endpoints are the same in
    # in terms of the response.
    return response, status_code


@dbutils.redis_error_handler
@server_entrypoint
def delete_sessions_v3(age: Optional[str] = None,
                       min_age: Optional[str] = None,
                       max_age: Optional[str] = None,
                       status: Optional[str] = None,
                       name_contains: Optional[str] = None,
                       succeeded: Optional[str] = None,
                       tags: Optional[str] = None) -> V3DeleteSessionsResponse:
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
    LOGGER.debug("DELETE /v3/sessions invoked delete_sessions_v3")
    return delete_sessions(age=age, min_age=min_age, max_age=max_age,
                           status=status, name_contains=name_contains,
                           succeeded=succeeded, tags=tags)
