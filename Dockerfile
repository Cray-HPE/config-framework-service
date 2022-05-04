#
# MIT License
#
# (C) Copyright 2019-2022 Hewlett Packard Enterprise Development LP
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
FROM openapitools/openapi-generator-cli:v4.1.2 as codegen
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
RUN apk add --upgrade --no-cache apk-tools &&  \
	apk update && \
	apk add --no-cache gcc python3-dev py3-pip musl-dev libffi-dev openssl-dev git && \
	apk -U upgrade --no-cache && \
    pip3 install --no-cache-dir -U pip && \
    pip3 install --no-cache-dir -U 'setuptools<46.0.0' && \
    pip3 install --no-cache-dir -r requirements.txt
COPY src/server/cray/cfs/api/controllers lib/server/cray/cfs/api/controllers
COPY src/server/cray/cfs/api/__main__.py \
     src/server/cray/cfs/api/__init__.py \
     src/server/cray/cfs/api/dbutils.py \
     src/server/cray/cfs/api/kafka_utils.py \
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
