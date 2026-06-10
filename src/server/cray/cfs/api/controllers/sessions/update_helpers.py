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

from cray.cfs.api import dbutils

from .types import (
    V3SessionData,
    V3SessionPatchData,
)


LOGGER = logging.getLogger('cray.cfs.api.controllers.sessions.update_helpers')


class JobFieldAlreadySet(Exception):
    """
    CASMCMS-9627: Raised when attempting to patch a status.session.job field when it is already set
    """
    def __init__(self, patch_job: str, actual_job: str) -> None:
        super().__init__(
            "status.session.job field cannot be updated after it has been set; "
            f"current value: {actual_job}, patch value: {patch_job}"
        )


# Some status fields should not progress backwards.
# This allows us to have multiple sources of status without worrying about event ordering.
STATUS_ORDERING = {
    'status': ['pending', 'running', 'complete'],
    'succeeded': ['none', 'unknown', 'false', 'true'],
}


def patch_session(session_data: V3SessionData,
                   patch_data: V3SessionPatchData,
                   job_update_restrictions: bool) -> V3SessionData:
    """
    Applies the patch_data to the specified session_data, and returns the updated session data.
    If job_update_restrictions is true, call _enforce_job_update_restrictions.
    """
    LOGGER.debug(
        "patch_session: session_data=%s, patch_data=%s, job_update_restrictions=%s",
        session_data, patch_data, job_update_restrictions,
    )
    status = session_data['status']
    artifacts = status['artifacts']
    session = status['session']

    patch_session = patch_data.get('status', {}).get('session', {})

    if job_update_restrictions:
        _enforce_job_update_restrictions(session, patch_session)

    # Artifacts
    for artifact in patch_data.get('status', {}).get('artifacts', []):
        for existing_artifact in artifacts:
            for key in artifact.keys():
                if existing_artifact.get(key) != artifact.get(key):
                    break  # Not the same artifact, move to next
            else:
                break  # All keys matched, stop looking
        else:
            artifacts.append(artifact)  # No artifacts matched

    # Session Status
    for key, value in patch_session.items():
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

    LOGGER.debug("patch_session: Patched session data=%s", session_data)
    return session_data


def _enforce_job_update_restrictions(session_status_session_data: dbutils.JsonDict,
                                     patch_status_session_data: dbutils.JsonDict) -> None:
    """
    Raise JobFieldAlreadySet exception if the patch is setting the job field after
    it already has been set.

    session_status_session_data = .status.session for the existing CFS session
    patch_status_session_data = .status.session for the patch
    """
    try:
        session_job = session_status_session_data["job"]
        patch_job = patch_status_session_data["job"]
    except KeyError:
        # No problem if the field is not present in the existing CFS entry,
        # or not in the patch data
        return

    # The field is present in both the CFS DB and the patch data. Raise an exception if
    # the CFS DB field has already been set and the patch is trying to set it.
    if session_job and patch_job:
        raise JobFieldAlreadySet(patch_job, session_job)
