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
dbutils: constants and global settings
"""

import logging

from kubernetes import config, client
from kubernetes.config.config_exception import ConfigException

from .env_vars import get_pos_int_env_var_or_default
from .typing import DatabaseNames

LOGGER = logging.getLogger(__name__)
DATABASES: list[DatabaseNames] = ["options",
                                  "sessions",
                                  "components",
                                  "configurations",
                                  "sources"]  # Index is the db id.

try:
    config.load_incluster_config()
except ConfigException:  # pragma: no cover
    config.load_kube_config()  # Development

_api_client = client.ApiClient()
k8ssvcs = client.CoreV1Api(_api_client)
svc_obj = k8ssvcs.read_namespaced_service("cray-cfs-api-db", "services")
DB_HOST = svc_obj.spec.cluster_ip
DB_PORT = 6379

# A DB method will wait a maximum of DB_BUSY_SECONDS seconds to acquire the DB lock,
# before giving up and raising a DBTooBusyError exception.
# In a watch/execute pipeline, a DB method will not start a new retry iteration if
# more than DB_BUSY_SECONDS have elapsed since the pipeline started.
DEFAULT_DB_BUSY_SECONDS = 60
DB_BUSY_SECONDS = get_pos_int_env_var_or_default("DB_BUSY_SECONDS",
                                                 DEFAULT_DB_BUSY_SECONDS)
LOGGER.debug("DB_BUSY_SECONDS = %d", DB_BUSY_SECONDS)

# The DB lock will automatically expire after DB_LOCK_EXPIRE_SECONDS seconds. This is
# to avoid cases where the lock holder dies and fails to release the lock. This should
# be set higher than any actual DB operation is likely to take, to avoid releasing the lock
# when work is still actually being done.
#
# The DB lock is exclusively put around places where entries are being added or deleted
# from the database (or when other methods need to check if a DB entry exists and want
# to avoid race conditions where the entry is deleted/created right after they check).
DEFAULT_DB_LOCK_EXPIRE_SECONDS = 31
DB_LOCK_EXPIRE_SECONDS = get_pos_int_env_var_or_default("DB_LOCK_EXPIRE_SECONDS",
                                                        DEFAULT_DB_LOCK_EXPIRE_SECONDS)
LOGGER.debug("DB_LOCK_EXPIRE_SECONDS = %d", DB_LOCK_EXPIRE_SECONDS)
