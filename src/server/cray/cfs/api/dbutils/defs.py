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
from typing import Final

from kubernetes import config, client
from kubernetes.config.config_exception import ConfigException

from .env_vars import get_pos_int_env_var_or_default
from .typing import DatabaseNames

LOGGER = logging.getLogger(__name__)
DATABASES: Final[tuple[DatabaseNames]] = (
    "options",
    "sessions",
    "components",
    "configurations",
    "sources")  # Index is the db id.
CFS_DB_SERVICE_NAME: Final[str] = "cray-cfs-api-db"
CFS_DB_SERVICE_NS: Final[str] = "services"

try:
    config.load_incluster_config()
except ConfigException:  # pragma: no cover
    config.load_kube_config()  # Development

_api_client = client.ApiClient()
k8ssvcs = client.CoreV1Api(_api_client)
svc_obj = k8ssvcs.read_namespaced_service(CFS_DB_SERVICE_NAME, CFS_DB_SERVICE_NS)
DB_HOST = svc_obj.spec.cluster_ip
DB_PORT: Final[int] = 6379

# In a watch/execute pipeline, a DB method will not start a new retry iteration if
# more than DB_BUSY_SECONDS have elapsed since the DB operation started.
DEFAULT_DB_BUSY_SECONDS: Final[int] = 60
DB_BUSY_SECONDS = get_pos_int_env_var_or_default("DB_BUSY_SECONDS",
                                                 DEFAULT_DB_BUSY_SECONDS)
LOGGER.debug("DB_BUSY_SECONDS = %d", DB_BUSY_SECONDS)
