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

from functools import partial
import logging

import connexion
from csm_utils.logging import exc_type_msg

from cray.cfs.api import dbutils
from cray.cfs.api.models.v2_session import V2Session  # noqa: E501
from cray.cfs.api.server_entrypoint import server_entrypoint

from .db import SESSIONS_DB as DB
from .update_helpers import patch_session, JobFieldAlreadySet
from .types import (
    V2PatchSessionResponse,
    V3PatchSessionResponse,
)
from .utils import (
    convert_session_to_v2,
    convert_session_to_v3,
)

LOGGER = logging.getLogger('cray.cfs.api.controllers.sessions.update_endpoints')


@dbutils.redis_error_handler
@server_entrypoint
def patch_session_v2(session_name: str) -> V2PatchSessionResponse:
    """Update a Config Framework Session

    Updates a V2Session # noqa: E501
    :rtype: V2Session
    """
    LOGGER.debug("PATCH /v2/sessions/%s invoked patch_session_v2", session_name)
    try:
        v2_patch_data = connexion.request.get_json()
        if any(key != 'status' for key in v2_patch_data):
            raise Exception('Only status can be updated after session creation')
    except Exception as err:
        return connexion.problem(
            status=400, title="Bad Request",
            detail=exc_type_msg(err))
    LOGGER.debug("patch_session_v2(%s): v2_patch_data=%s", session_name, v2_patch_data)
    v3_patch_data = convert_session_to_v3(v2_patch_data)
    # CASMCMS-9627: To minimize changes, only update the V3 API.
    # This is fine because that is what cfs-operator uses. This also allows
    # a way for an admin to bypass the restrictions, if for some reason it is ever
    # needed.
    patch_handler = partial(patch_session, job_update_restrictions=False)
    try:
        v3_session_data = DB.patch(session_name, v3_patch_data, patch_handler=patch_handler)
    except dbutils.DBNoEntryError as err:
        LOGGER.debug(err)
        return connexion.problem(
            status=404, title="Session not found.",
            detail=f"Session {session_name} could not be found")
    resp_body = convert_session_to_v2(v3_session_data)
    LOGGER.debug("patch_session_v2(%s): Returning 200 with body: %s", session_name, resp_body)
    return resp_body, 200


@dbutils.redis_error_handler
@server_entrypoint
def patch_session_v3(session_name: str) -> V3PatchSessionResponse:
    """Update a Config Framework Session

    Updates a V3Session # noqa: E501

    If the job field is being patched, return a 409 if the job field has already been set

    :rtype: V3Session
    """
    LOGGER.debug("PATCH /v3/sessions/%s invoked patch_session_v3", session_name)
    try:
        v3_patch_data = connexion.request.get_json()
        if any(key != 'status' for key in v3_patch_data):
            raise Exception('Only status can be updated after session creation')
    except Exception as err:
        return connexion.problem(
            status=400, title="Bad Request",
            detail=exc_type_msg(err))
    LOGGER.debug("patch_session_v3(%s): v3_patch_data=%s", session_name, v3_patch_data)
    patch_handler = partial(patch_session, job_update_restrictions=True)
    try:
        v3_session_data = DB.patch(session_name, v3_patch_data, patch_handler=patch_handler)
    except dbutils.DBNoEntryError as err:
        LOGGER.debug(err)
        return connexion.problem(
            status=404, title="Session not found.",
            detail=f"Session {session_name} could not be found")
    except JobFieldAlreadySet as err:
        LOGGER.debug(err)
        return connexion.problem(
            status=409, title="Session patch conflict.",
            detail=f"Session {session_name} could not be patched: {err}")
    LOGGER.debug("patch_session_v3(%s): Returning 200 with body: %s", session_name, v3_session_data)
    return v3_session_data, 200
