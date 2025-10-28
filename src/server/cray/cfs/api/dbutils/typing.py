#
# MIT License
#
# (C) Copyright 2019-2025 Hewlett Packard Enterprise Development LP
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
from typing import Literal

type JsonData = bool | str | None | int | float | list[JsonData] | dict[str, JsonData]
type JsonDict = dict[str, JsonData]
type JsonList = list[JsonData]
# All CFS database entries are dicts that are stored in JSON.
type DbEntry = JsonDict
type DbKey = str | bytes
DatabaseNames = Literal["options", "sessions", "components", "configurations", "sources"]
type DbIdentifier = DatabaseNames | int
type DataFilter = Callable[[DbEntry], bool]

type PatchHandler = Callable[[DbEntry, JsonDict], DbEntry]
type UpdateHandler = Callable[[DbEntry], DbEntry]
type DeletionHandler = Callable[[DbEntry], None]
type EntryChecker = Callable[[DbEntry], bool]
