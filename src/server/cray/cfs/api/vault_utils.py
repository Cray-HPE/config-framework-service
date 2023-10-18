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
import os
import typing

import hvac


def get_client() -> hvac.Client:
    client = hvac.Client(url=os.environ['VAULT_ADDR'])
    with open('/var/run/secrets/kubernetes.io/serviceaccount/token', 'r') as file:
        jwt = file.read()
    with open('/var/run/secrets/kubernetes.io/serviceaccount/namespace', 'r') as file:
        role = file.read()
    hvac.api.auth_methods.Kubernetes(client.adapter).login(
        jwt=jwt,
        role=role
    )
    return client


def put_secret(secret_path: str, secret_data: typing.Dict[str, str]) -> None:
    client = get_client()
    client.secrets.kv.v2.create_or_update_secret(path=secret_path, secret=secret_data)


def get_secret(secret_path: str) -> typing.Dict[str, str]:
    client = get_client()
    secret = client.secrets.kv.read_secret_version(secret_path)
    return secret["data"]["data"]


def delete_secret(secret_path: str) -> None:
    client = get_client()
    client.secrets.kv.delete_metadata_and_all_versions(secret_path)
