from kubernetes import client, config
from kubernetes.config.config_exception import ConfigException

try:
    config.load_incluster_config()
except ConfigException:  # pragma: no cover
    config.load_kube_config()  # Development

_api_client = client.ApiClient()
k8scustom = client.CustomObjectsApi(_api_client)

ARA_UI_URL = ""
