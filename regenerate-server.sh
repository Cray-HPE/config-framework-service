#!/bin/bash
# Copyright 2019-2021 Hewlett Packard Enterprise LP
VERSION=`cat .version`
sed -i .run s/@VERSION@/${VERSION}/g api/openapi.yaml
docker run --rm -v ${PWD}:/local openapitools/openapi-generator-cli:v4.1.1 \
  generate \
    -i local/api/openapi.yaml.run \
    -g python-flask \
    -o local/src/server \
    -c local/config/autogen-server.json

echo "Code has been generated within src/server for development purposes ONLY"
echo "This project is setup to automatically generate server side code as a"
echo "function of docker image build. Adjust .gitignore before checking in"
echo "anything you did not author!"
