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

from cray.cfs.api import dbutils
from cray.cfs.api.controllers import options
from cray.cfs.api.k8s_utils import get_ara_ui_url
from cray.cfs.api.models.v2_session import V2Session  # noqa: E501
from cray.cfs.api.models.v3_session_data import V3SessionData as V3Session  # noqa: E501

from .types import (
    V2SessionData,
    V2SessionPatchData,
    V3SessionData,
    V3SessionPatchData,
)

def set_link(data):
    if options.Options().include_ara_links:
        data["logs"] = f"{get_ara_ui_url()}/?label={data['name']}"
    return data


def convert_session_to_v2(data: V3SessionData | V3SessionPatchData) -> V2SessionData | V2SessionPatchData:
    data = dbutils.convert_data_to_v2(data, V2Session)
    return data


def convert_session_to_v3(data: V2SessionData | V2SessionPatchData) -> V3SessionData | V3SessionPatchData:
    data = dbutils.convert_data_from_v2(data, V2Session)
    return data
