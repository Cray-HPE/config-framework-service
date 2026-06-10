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

import datetime
from functools import partial
import logging
import re
from typing import Optional

import dateutil

from .types import (
    TagList,
    V3SessionData,
    V3SessionFilter,
)


LOGGER = logging.getLogger('cray.cfs.api.controllers.sessions.filters')


class ParsingException(Exception):
    pass


def get_session_filter(
    age: Optional[str],
    min_age: Optional[str],
    max_age: Optional[str],
    status: Optional[str],
    name_contains: Optional[str],
    succeeded: Optional[str],
    tag_list: Optional[TagList],
) -> V3SessionFilter:
    if not any([age, min_age, max_age, status, name_contains, succeeded, tag_list]):
        # No filter is being used so all components are valid
        return lambda _: True
    min_start = None
    max_start = None
    if age:
        try:
            max_start = _age_to_timestamp(age)
        except Exception as err:
            LOGGER.warning('Unable to parse age: %s', age)
            raise ParsingException(err) from err
    if min_age:
        try:
            max_start = _age_to_timestamp(min_age)
        except Exception as err:
            LOGGER.warning('Unable to parse min_age: %s', min_age)
            raise ParsingException(err) from err
    if max_age:
        try:
            min_start = _age_to_timestamp(max_age)
        except Exception as err:
            LOGGER.warning('Unable to parse max_age: %s', max_age)
            raise ParsingException(err) from err

    return partial(
        matches_filter,
        min_start=min_start,
        max_start=max_start,
        status=status,
        name_contains=name_contains,
        succeeded=succeeded,
        tag_list=tag_list,
    )


def matches_filter(
    data: V3SessionData,
    min_start: Optional[datetime.datetime],
    max_start: Optional[datetime.datetime],
    status: Optional[str],
    name_contains: Optional[str],
    succeeded: Optional[str],
    tag_list: Optional[TagList],
    job_set: Optional[bool] = None,
) -> bool:
    # First check the non-status filters
    session_name = data['name']
    if name_contains and name_contains not in session_name:
        return False
    if tag_list and any(data.get('tags', {}).get(k) != v for k, v in tag_list):
        return False
    # Now get the status and check those filters
    session_status = data.get('status', {}).get('session', {})
    if status and status != session_status.get('status'):
        return False
    if succeeded and succeeded != session_status.get('succeeded'):
        return False
    if job_set is not None and bool(session_status.get('job')) != job_set:
        return False
    # Only do the time calculations if needed
    if min_start or max_start:
        start_time = session_status['start_time']
        session_start = None
        if start_time:
            session_start = dateutil.parser.parse(start_time).replace(tzinfo=None)
        if not session_start:
            return False
        if min_start and session_start < min_start:
            return False
        if max_start and session_start > max_start:
            return False
    return True


def _age_to_timestamp(age: str) -> datetime.datetime:
    delta = {}
    for interval in ['weeks', 'days', 'hours', 'minutes']:
        result = re.search(rf'(\d+)\w*{interval[0]}', age, re.IGNORECASE)
        if result:
            delta[interval] = int(result.groups()[0])
    delta = datetime.timedelta(**delta)
    return datetime.datetime.now() - delta
