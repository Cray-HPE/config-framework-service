#
# MIT License
#
# (C) Copyright 2025-2026 Hewlett Packard Enterprise Development LP
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
Decorator that wraps all entry point functions for the CFS server
(i.e. endpoint controllers)
"""

import functools
import logging
# typing.Callable is deprecated since Python 3.9 (in favor of collections.abc.Callable).
# However, in Python 3.9, collections.abc.Callable will not work if it is subscripted with
# typing_extensions.ParamSpec.
# Therefore we still use typing.Callable, until we are definitely at a Python version where
# collections.abc.Callable will work with ParamSpec
from typing import Any, Callable, Optional, TypeVar, Union

from typing_extensions import ParamSpec, TypeAlias

LOGGER = logging.getLogger(__name__)

# Acceptable run functions must be runnable with no arguments. We don't
# care about the return type (it is discarded)
RunFunc: TypeAlias = Callable[[], Any]

RunPriority: TypeAlias = Union[float, int]

# An entry in a run list has a float priority and the function to be called
_RunListEntry: TypeAlias = tuple[RunPriority, RunFunc]

# For run functions that do not care when they run, this is the assigned priority
DEFAULT_PRIORITY: RunPriority = 100

P = ParamSpec("P")
R = TypeVar("R")

class EntryRegistry:
    def __init__(self, default_priority: RunPriority = DEFAULT_PRIORITY) -> None:
        self._run_list: list[_RunListEntry] = []
        assert isinstance(default_priority, (float, int))
        self._default_priority = default_priority

    @property
    def default_priority(self) -> RunPriority:
        return self._default_priority

    def add(self, func: RunFunc, priority: Optional[RunPriority] = None) -> None:
        """
        Add a function to the run_list, with the given priority.
        This function sorts the list after each function is added, so that
        it doesn't need to be sorted later when the list is being used.
        Sorting on every add is fine because we only expect a small number
        of functions to be added. If that ever changes, the sorting strategy
        may require revision.

        No locking is done. The assumption is that this function should be
        called before any entrypoints could possibly have run. Calling this
        function after that point is not intended.
        """
        if priority is None:
            priority = self.default_priority
        else:
            assert isinstance(priority, (float, int))
        LOGGER.debug("Adding %s() to run_list with priority %f",
                     getattr(func, "__name__", repr(func)),
                     priority)
        new_entry = (priority, func)
        # Should not happen, but make sure we don't double add anything
        if new_entry in self._run_list:
            return
        self._run_list.append(new_entry)
        self._run_list.sort(key=lambda entry: entry[0])
        LOGGER.debug("run_list now has %d entries", len(self._run_list))

    def __call__(self, func: Callable[P, R]) -> Callable[P, R]:
        """
        This is a decorator to put around all API controller functions
        (so that it runs on all entrypoints into the server, other than initial startup).

        First it calls all of the entry run list functions, in order
        (sorted by their priority values).
        Then it calls the function and returns whatever it returns.
        It does not change the signature of the function.
        It also does no try/except-ing, so excepts raised by the run list
        functions will derail this process.
        """
        @functools.wraps(func)
        def _wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            for _, runfunc in self._run_list:
                runfunc()
            return func(*args, **kwargs)
        return _wrapper

server_entrypoint = EntryRegistry()
