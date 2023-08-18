#
# MIT License
#
# (C) Copyright 2023 Hewlett Packard Enterprise Development LP
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

from kubernetes import client, config
from kubernetes.config.config_exception import ConfigException

try:
    config.load_incluster_config()
except ConfigException:  # pragma: no cover
    config.load_kube_config()  # Development

_api_client = client.ApiClient()
k8scustom = client.CustomObjectsApi(_api_client)

ARA_UI_URL = ""

def get_ara_ui_url():
    global ARA_UI_URL
    if not ARA_UI_URL:
        try:
            data = k8scustom.get_namespaced_custom_object("networking.istio.io", "v1beta1", "services", "virtualservices", "cfs-ara-external")
            ARA_UI_URL = data["spec"]["hosts"][0]
        except:
            pass
    return ARA_UI_URL
