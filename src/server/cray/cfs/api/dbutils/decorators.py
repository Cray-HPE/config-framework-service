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
Database-related decorator definitions
"""

from collections.abc import Callable
import functools
import logging
from typing import Concatenate, Protocol

import connexion
from connexion.lifecycle import ConnexionResponse as CxResponse
import redis

from .exceptions import DBTooBusyError


LOGGER = logging.getLogger(__name__)

def redis_error_handler[**P, R](func: Callable[P, R]) -> Callable[P, R|CxResponse]:
    """Decorator for returning better errors if Redis is unreachable or busy"""

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R | CxResponse:
        # Our get/patch functions don't take body, but the **kwargs
        # in the arguments to this wrapper cause it to get passed.
        kwargs.pop('body', None)
        try:
            return func(*args, **kwargs)
        except redis.exceptions.ConnectionError as err:
            LOGGER.error('Unable to connect to the Redis database: %s', err)
            return connexion.problem(
                status=503,
                title='Unable to connect to the Redis database',
                detail=str(err))
        except DBTooBusyError as err:
            LOGGER.error('Database busy: %s', err)
            return connexion.problem(
                status=503,
                title='Database busy',
                detail=str(err))

    return wrapper


class HasTooBusyExceptionMethod(Protocol):
    """
    Protocol for classes that have a too_busy_exception method
    (e.g. DBWrapper)
    """
    def too_busy_exception(self) -> DBTooBusyError:
        pass


def convert_db_watch_errors[**P, C: HasTooBusyExceptionMethod, R](
    method: Callable[Concatenate[C, P], R]
) -> Callable[Concatenate[C, P], R]:
    """
    Decorator to put around DBWrapper methods that catches redis WatchError
    and raises DBTooBusyError from it
    """

    @functools.wraps(method)
    def wrapper(self: C, /, *args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return method(self, *args, **kwargs)
        except redis.exceptions.WatchError as err:
            LOGGER.debug(err)
            # Because C is bounded by the HasTooBusyExceptionMethod protocol, we know that
            # self.too_busy_exception() must exist and return a DBTooBusyError
            raise self.too_busy_exception() from err

    return wrapper


class HasRedisClient(Protocol):
    """
    Protocol for classes that have a Redis client instance variable named 'client'
    (e.g. DBWrapper)
    """
    client: redis.client.Redis

def redis_pipeline[**P, C: HasRedisClient, R](
    method: Callable[Concatenate[C, redis.client.Pipeline, P], R]
) -> Callable[Concatenate[C, P], R]:
    """
    Decorator to put around DBWrapper methods that provides a Redis pipeline as the
    first argument to the function (inside of a context manager).
    """

    @functools.wraps(method)
    def wrapper(self: C, /, *args: P.args, **kwargs: P.kwargs) -> R:
        # Because C is bounded by the HasRedisClient protocol, we know that
        # self.client is a redis.client.Redis object
        with self.client.pipeline() as pipe:
            return method(self, pipe, *args, **kwargs)

    return wrapper
