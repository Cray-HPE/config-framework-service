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
# If you wish to perform a local build, you will need to clone or copy the contents of the
# cms-meta-tools repo to ./cms_meta_tools

DOCKER_NAME ?= cray-cfs
CHART_NAME ?= cray-cfs-api
CHART_PATH ?= kubernetes

DOCKER_VERSION ?= $(shell head -1 .docker_version)
CHART_VERSION ?= $(shell head -1 .chart_version)

HELM_UNITTEST_IMAGE ?= quintush/helm-unittest:3.3.0-0.2.5

ifneq ($(wildcard ${HOME}/.netrc),)
	DOCKER_ARGS ?= --secret id=netrc,src=${HOME}/.netrc
endif

all : runbuildprep lint image chart
chart: chart_setup chart_package chart_test

runbuildprep:
		./cms_meta_tools/scripts/runBuildPrep.sh

lint:
		./cms_meta_tools/scripts/runLint.sh

image:
		docker build --pull ${DOCKER_ARGS} --tag '${DOCKER_NAME}:${DOCKER_VERSION}' .

chart_setup:
		mkdir -p ${CHART_PATH}/.packaged
		printf "\nglobal:\n  appVersion: ${DOCKER_VERSION}" >> ${CHART_PATH}/${CHART_NAME}/values.yaml

chart_package:
		helm dep up ${CHART_PATH}/${CHART_NAME}
		helm package ${CHART_PATH}/${CHART_NAME} -d ${CHART_PATH}/.packaged --app-version ${DOCKER_VERSION} --version ${CHART_VERSION}

chart_test:
		helm lint "${CHART_PATH}/${CHART_NAME}"
		docker run --rm -v ${PWD}/${CHART_PATH}:/apps ${HELM_UNITTEST_IMAGE} -3 ${CHART_NAME}
