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
from connexion.lifecycle import ConnexionResponse as CxResponse
from csm_utils.logging import exc_type_msg

from cray.cfs.api import dbutils
from cray.cfs.api.controllers import options
from cray.cfs.api.models.v2_session_create import V2SessionCreate  # noqa: E501
from cray.cfs.api.models.v3_session_create import V3SessionCreate  # noqa: E501
from cray.cfs.api.server_entrypoint import server_entrypoint

from .create_helpers import (
    create_session,
    finish_session_create,
    validate_ansible_passthrough,
    validate_session_target,
)
from .db import SESSIONS_DB as DB
from .db import CONFIG_DB
from .utils import convert_session_to_v2, set_link

LOGGER = logging.getLogger('cray.cfs.api.controllers.sessions.create_endpoints')


@dbutils.redis_error_handler
@server_entrypoint
def create_session_v2():  # noqa: E501
    """Create a Config Framework Session

    Creates a new V2Session

    :rtype: V2Session
    """
    # Create the session object, do openapi field validation
    LOGGER.debug("POST /v2/sessions invoked create_session_v2")
    try:
        data = connexion.request.get_json()
        LOGGER.debug("Create session: %s", data)
        v2_session_create = V2SessionCreate.from_dict(connexion.request.get_json())  # noqa: E501
        # This is a workaround for the addition of the configuration name max length in v3
        # The configuration name is restored later
        v2_session_configuration = v2_session_create.configuration_name
        v2_session_create.configuration_name = "temp"
        # end workaround
        session_create = V3SessionCreate.from_dict(v2_session_create.to_dict())
    except Exception as err:
        return connexion.problem(
            detail=exc_type_msg(err),
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
    validation_err = validate_session_target(session_create.target)
    if validation_err:
        return validation_err

    # Additional ansible passthrough data validation
    if session_create.ansible_passthrough:
        validation_err = validate_ansible_passthrough(session_create.ansible_passthrough)
        if validation_err:
            return validation_err

    # If the following fields aren't set, use their configured default values.
    if not session_create.ansible_config:
        session_create.ansible_config = options.Options().default_ansible_config

    session = create_session(session_create)
    session_data = session.to_dict()
    # This is a workaround for the addition of the configuration name max length in v3
    session_data['configuration']['name'] = v2_session_configuration
    # end workaround

    response_data = finish_session_create(session_data)
    if isinstance(response_data, CxResponse):
        return response_data

    resp_body = convert_session_to_v2(response_data)
    LOGGER.debug("create_session_v2: Sending 200 response with body: %s", resp_body)
    return resp_body, 200


@dbutils.redis_error_handler
@server_entrypoint
def create_session_v3():  # noqa: E501
    """Create a Config Framework Session

    Creates a new V3Session

    :rtype: V3Session
    """
    # Create the session object, do openapi field validation
    LOGGER.debug("POST /v3/sessions invoked create_session_v3")
    try:
        data = connexion.request.get_json()
        LOGGER.debug("Create session: %s", data)
        session_create = V3SessionCreate.from_dict(connexion.request.get_json())  # noqa: E501
    except Exception as err:
        return connexion.problem(
            detail=exc_type_msg(err),
            status=400,
            title="Bad Request"
        )

    if session_create.name in DB:
        return connexion.problem(
            detail=f"A session with the name {session_create.name} already exists",
            status=409,
            title="Conflicting session name"
        )

    if (
        session_create.configuration_name not in CONFIG_DB and
        not session_create.configuration_name.startswith("debug_")  # noqa: E501
    ):
        return connexion.problem(
            detail=f"No configurations exist named {session_create.configuration_name}",
            status=400,
            title="Invalid configuration"
        )

    # Additional target section data validation
    validation_err = validate_session_target(session_create.target)
    if validation_err:
        return validation_err

    # Additional ansible passthrough data validation
    if session_create.ansible_passthrough:
        validation_err = validate_ansible_passthrough(session_create.ansible_passthrough)
        if validation_err:
            return validation_err

    # If the following fields aren't set, use their configured default values.
    if not session_create.ansible_config:
        session_create.ansible_config = options.Options().default_ansible_config

    session = create_session(session_create)
    data = session.to_dict()

    response_data = finish_session_create(data)
    if isinstance(response_data, CxResponse):
        return response_data

    set_link(response_data)
    LOGGER.debug("create_session_v3: Sending 201 response with body: %s", response_data)
    return response_data, 201
