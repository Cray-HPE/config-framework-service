#
# MIT License
#
# (C) Copyright 2020-2026 Hewlett Packard Enterprise Development LP
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
from cachetools import TTLCache, cached
from kubernetes import config, client
from kubernetes.config.config_exception import ConfigException

from cray.cfs.api.env_utils import (
    get_env_var_or_default,
    get_pos_float_env_var_or_default,
    get_pos_int_env_var_or_default,
)

try:
    config.load_incluster_config()
except ConfigException:  # pragma: no cover
    config.load_kube_config()  # Development

DEFAULT_SVC_NAME = "cray-shared-kafka-kafka-bootstrap"
DEFAULT_SVC_NS = "services"
DEFAULT_PORT = 9092

# Amount of time (in seconds) to cache the cluster IP from the Kafka service
DEFAULT_CACHE_TTL = 90

# Allow defaults to be overridden
CACHE_TTL = get_pos_float_env_var_or_default('K8S_KAFKA_SVC_CACHE_TTL', DEFAULT_CACHE_TTL)
SVC_NAME = get_env_var_or_default('K8S_KAFKA_SVC_NAME', DEFAULT_SVC_NAME)
SVC_NS = get_env_var_or_default('K8S_KAFKA_SVC_NS', DEFAULT_SVC_NS)
PORT = get_pos_int_env_var_or_default('KAFKA_PORT', DEFAULT_PORT)

cache = TTLCache(maxsize=1, ttl=CACHE_TTL)
_api_client = client.ApiClient()
k8ssvcs = client.CoreV1Api(_api_client)

@cached(cache)
def kafka_bootstrap_host() -> str:
    svc_obj = k8ssvcs.read_namespaced_service(SVC_NAME, SVC_NS)
    host = svc_obj.spec.cluster_ip
    return f"{host}:{PORT}"
