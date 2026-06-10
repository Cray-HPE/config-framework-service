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

from cray.cfs.api.controllers.options import OPTIONS_LOGLVL_UPDATE_PRI
from cray.cfs.api.server_entrypoint import server_entrypoint, RunPriority

from .create_endpoints import create_session_v2, create_session_v3
from .db import SESSIONS_DB
from .delete_endpoints import (
    delete_session_v2,
    delete_session_v3,
    delete_sessions_v2,
    delete_sessions_v3,
)
from .get_endpoints import (
    get_session_v2,
    get_session_v3,
    get_sessions_v2,
    get_sessions_v3,
)
from .kafka import start_kafka_supervisor, KAFKA_TOPIC
from .tardy_scanner import start_tardy_scan_supervisor
from .update_endpoints import patch_session_v2, patch_session_v3
from .utils import convert_session_to_v2, convert_session_to_v3

# The supervisor threads should run AFTER the log level has been updated
# (but we don't care when after)
_START_SUPERVISOR_PRI: RunPriority = OPTIONS_LOGLVL_UPDATE_PRI + 10

server_entrypoint.add(start_kafka_supervisor, _START_SUPERVISOR_PRI)
server_entrypoint.add(start_tardy_scan_supervisor, _START_SUPERVISOR_PRI)

__all__ = [
    'convert_session_to_v2',
    'convert_session_to_v3',
    'create_session_v2',
    'create_session_v3',
    'delete_session_v2',
    'delete_session_v3',
    'delete_sessions_v2',
    'delete_sessions_v3',
    'get_session_v2',
    'get_session_v3',
    'get_sessions_v2',
    'get_sessions_v3',
    'patch_session_v2',
    'patch_session_v3',
    'KAFKA_TOPIC',
    'SESSIONS_DB',
]
