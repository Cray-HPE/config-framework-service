# Dockerfile for Cray Configuration Framework Service
# Copyright 2019, Cray Inc. All rights reserved.

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
FROM dtr.dev.cray.com/baseos/alpine:3.11.5 as base
WORKDIR /app
COPY --from=codegen /app .
COPY constraints.txt requirements.txt ./
RUN apk add --no-cache gcc python3-dev musl-dev libffi-dev openssl-dev && \
    PIP_INDEX_URL=http://dst.us.cray.com/dstpiprepo/simple \
    PIP_TRUSTED_HOST=dst.us.cray.com \
    pip3 install --no-cache-dir -U pip && \
    pip3 install --no-cache-dir -r requirements.txt
COPY src/server/cray/cfs/api/controllers lib/server/cray/cfs/api/controllers
COPY src/server/cray/cfs/api/__main__.py \
     src/server/cray/cfs/api/__init__.py \
     src/server/cray/cfs/api/dbutils.py \
     lib/server/cray/cfs/api/

# Application Image
FROM base as application
ENV PYTHONPATH "/app/lib/server"
WORKDIR /app/
EXPOSE 80
RUN apk add --no-cache uwsgi-python3
COPY config/uwsgi.ini ./
ENTRYPOINT ["uwsgi", "--ini", "/app/uwsgi.ini"]
