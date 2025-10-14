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

"""
Utilities for getting information from environment variables
"""

import logging
import os
from typing import Optional

LOGGER = logging.getLogger(__name__)

def get_pos_int_env_var(varname: str) -> Optional[int]:
    """
    If the specified environment variable is set to a positive base-10 integer string
    value, return it (as an integer). Otherwise, log a relevant warning message and return None.
    """
    env_value = os.environ.get(varname)
    if env_value is None:
        LOGGER.debug("%s environment variable not set", varname)
        return None
    try:
        env_value_int = int(env_value)
    except ValueError as err:
        LOGGER.debug("ValueError parsing %s environment variable: %s", varname, err)
        LOGGER.warning("%s environment variable not a base 10 integer: %s", varname, env_value)
        return None
    if env_value_int > 0:
        LOGGER.debug("%s environment variable set to %d", varname, env_value_int)
        return env_value_int
    LOGGER.warning(
        "%s environment variable not a positive base 10 integer: %d", varname, env_value_int
    )
    return None

def get_pos_int_env_var_or_default(env_var_name: str, default_value: int) -> int:
    """
    If the specified environment variable is set to a positive base-10 integer string
    value, return it (as an integer). Otherwise, log a relevant warning message and return the
    default value.
    """
    value_from_env = get_pos_int_env_var(env_var_name)
    if value_from_env is not None:
        return value_from_env
    LOGGER.debug("Using default value (%d) for %s", default_value, env_var_name)
    return default_value
