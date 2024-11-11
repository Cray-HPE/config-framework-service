#
# MIT License
#
# (C) Copyright 2019-2024 Hewlett Packard Enterprise Development LP
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
# Generate API
FROM openapitools/openapi-generator-cli:v7.8.0 as codegen
WORKDIR /app
COPY api/openapi.yaml api/openapi.yaml
COPY config/autogen-server.json config/autogen-server.json
COPY src/server/.openapi-generator-ignore lib/server/.openapi-generator-ignore
RUN /usr/local/bin/docker-entrypoint.sh generate \
    -i api/openapi.yaml \
    -g python-flask \
    -o lib/server \
    -c config/autogen-server.json

# Base image
FROM artifactory.algol60.net/csm-docker/stable/docker.io/library/alpine:3.15 as base
WORKDIR /app
COPY --from=codegen /app .
COPY constraints.txt requirements.txt ./
# The openapi-generator creates a requirements file that specifies exactly Flask==2.1.1
# However, using Flask 2.2.5 is also compatible, and resolves a CVE.
# Accordingly, we relax their requirements file.
RUN cat lib/server/requirements.txt && \
    sed -i 's/Flask == 2\(.*\)$/Flask >= 2\1\nFlask < 3/' lib/server/requirements.txt && \
    cat lib/server/requirements.txt && \
    apk add --upgrade --no-cache apk-tools &&  \
	apk update && \
	apk add --no-cache gcc python3-dev py3-pip musl-dev libffi-dev openssl-dev git yq && \
	apk -U upgrade --no-cache && \
    pip3 list --format freeze && \
    pip3 install --no-cache-dir -U pip && \
    pip3 list --format freeze && \
    pip3 install --no-cache-dir -r requirements.txt && \
    pip3 list --format freeze && \
    yq -o=json eval /app/api/openapi.yaml > /app/api/openapi.json
COPY src/server/cray/cfs/api/controllers lib/server/cray/cfs/api/controllers
COPY src/server/cray/cfs/api/__main__.py \
     src/server/cray/cfs/api/__init__.py \
     src/server/cray/cfs/api/dbutils.py \
     src/server/cray/cfs/api/kafka_utils.py \
     src/server/cray/cfs/api/k8s_utils.py \
     src/server/cray/cfs/api/vault_utils.py \
     src/server/cray/cfs/api/migrations.py \
     lib/server/cray/cfs/api/

# Application Image
FROM base as application
ENV PYTHONPATH "/app/lib/server"
WORKDIR /app/
EXPOSE 9000
RUN apk add --no-cache uwsgi-python3
COPY config/uwsgi.ini ./
USER nobody:nobody
ENTRYPOINT ["uwsgi", "--ini", "/app/uwsgi.ini"]
