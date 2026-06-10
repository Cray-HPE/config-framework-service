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

from typing import Optional

from cray.cfs.api.controllers import options

from .db import SESSIONS_DB as DB
from .filters import get_session_filter
from .types import TagList

@options.defaults(limit="default_page_size")
def get_filtered_sessions(
    age: Optional[str],
    min_age: Optional[str],
    max_age: Optional[str],
    status: Optional[str],
    name_contains: Optional[str],
    succeeded: Optional[str],
    tag_list: Optional[TagList],
    limit: int = 1,
    after_id: Optional[str] = ""
):
    filters = [
        get_session_filter(
            age=age,
            min_age=min_age,
            max_age=max_age,
            status=status,
            name_contains=name_contains,
            succeeded=succeeded,
            tag_list=tag_list,
        ),
    ]
    session_data_page, next_page_exists = DB.get_all(limit=limit,
                                                     after_id=after_id,
                                                     data_filters=filters)
    return session_data_page, next_page_exists
