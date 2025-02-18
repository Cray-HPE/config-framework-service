#
# MIT License
#
# (C) Copyright 2025 Hewlett Packard Enterprise Development LP
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

import traceback
import requests
import requests_retry_session as rrs
from functools import partial
from typing import Iterator, Optional


PROTOCOL = 'http'
DEFAULT_RETRY_ADAPTER_ARGS = rrs.RequestsRetryAdapterArgs(
    retries=10,
    backoff_factor=0.5,
    status_forcelist=(500, 502, 503, 504),
    connect_timeout=3,
    read_timeout=10)

retry_session_manager = partial(rrs.retry_session_manager,
                                protocol=PROTOCOL,
                                **DEFAULT_RETRY_ADAPTER_ARGS)


def retry_session(
    session: Optional[requests.Session] = None,
    protocol: Optional[str] = None,
    adapter_kwargs: Optional[rrs.RequestsRetryAdapterArgs] = None
) -> Iterator[requests.Session]:
    if session is not None:
        return nullcontext(session)
    kwargs = adapter_kwargs or {}
    if protocol is not None:
        return retry_session_manager(protocol=protocol, **kwargs)  # pylint: disable=redundant-keyword-arg
    return retry_session_manager(**kwargs)


def retry_session_get(*get_args,
                      session: Optional[requests.Session] = None,
                      protocol: Optional[str] = None,
                      adapter_kwargs: Optional[
                          rrs.RequestsRetryAdapterArgs] = None,
                      **get_kwargs) -> Iterator[requests.Response]:
    with retry_session(session=session,
                       protocol=protocol,
                       adapter_kwargs=adapter_kwargs) as _session:
        return _session.get(*get_args, **get_kwargs)


def exc_type_msg(exc: Exception) -> str:
    """
    Given an exception, returns a string of its type and its text
    (e.g. TypeError: 'int' object is not subscriptable)
    """
    return ''.join(traceback.format_exception_only(type(exc), exc))