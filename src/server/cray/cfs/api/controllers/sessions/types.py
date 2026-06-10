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

"""
Rudimentary type annotations
"""

from collections.abc import Callable
from typing import (
    final,
    Literal,
    NewType,
    NoReturn,
    Optional,
    TypedDict,
)

from connexion.lifecycle import ConnexionResponse as CxResponse

from cray.cfs.api.dbutils import JsonDict


V2SessionData = NewType("V2SessionData", JsonDict)
V3SessionData = NewType("V3SessionData", JsonDict)

V2SessionPatchData = NewType("V2SessionPatchData", JsonDict)
V3SessionPatchData = NewType("V3SessionPatchData", JsonDict)

# Marked as final because we do not intend to subclass this. It doesn't really
# matter at this point, but if type checking is ever properly added, this helps
# the type checker.
@final
class SessionIdListDict(TypedDict):
    """
    Used for type hinting the v3 session endpoints which return ID list dicts
    """
    session_ids: list[str]


# The response format for the delete session endpoint is the same for v2 and v3
type DeleteSessionResponse = tuple[None, Literal[204]] | CxResponse

type V2GetSessionResponse = tuple[V2SessionData, Literal[200]] | CxResponse
type V3GetSessionResponse = tuple[V3SessionData, Literal[200]] | CxResponse

type V2DeleteSessionsResponse = tuple[None, Literal[204]] | CxResponse
type V3DeleteSessionsResponse = tuple[SessionIdListDict, Literal[200]] | CxResponse

# Although it does not conform to convention, the successful patch requests return 200 status
type V2PatchSessionResponse = tuple[V2SessionData, Literal[200]] | CxResponse
type V3PatchSessionResponse = tuple[V3SessionData, Literal[200]] | CxResponse

type TagList = list[tuple[str, str]]
type V3SessionFilter = Callable[[V3SessionData], bool]
