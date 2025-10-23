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
Definitions for type hinting
"""

from collections.abc import Callable
# Have to use List and Dict for recursive TypeAlias definition
from typing import Dict, List, Literal, Union

from typing_extensions import TypeAlias


# Because the following definition is recursive, we have to:
# * Use List/Dict instead of list/dict
# * Quote JsonData on the right side
JsonData: TypeAlias = Union[bool, str, None, int, float, List["JsonData"], Dict[str, "JsonData"]]
JsonDict: TypeAlias = dict[str, JsonData]
JsonList: TypeAlias = list[JsonData]
# All CFS database entries are dicts that are stored in JSON.
DbEntry: TypeAlias = JsonDict
DbKey: TypeAlias = Union[str, bytes]
DatabaseNames = Literal["options", "sessions", "components", "configurations", "sources"]
DbIdentifier: TypeAlias = Union[DatabaseNames, int]
DataFilter: TypeAlias = Callable[[DbEntry], bool]

PatchHandler: TypeAlias = Callable[[DbEntry, JsonDict], DbEntry]
UpdateHandler: TypeAlias = Callable[[DbEntry], DbEntry]
DeletionHandler: TypeAlias = Callable[[DbEntry], None]
