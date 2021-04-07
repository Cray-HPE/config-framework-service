#!/usr/bin/env sh

#
# Copyright 2020-2021 Hewlett Packard Enterprise Development LP
#

VERSION=`cat .version`
sed -i s/@VERSION@/${VERSION}/g kubernetes/cray-cfs-api/Chart.yaml
sed -i s/@VERSION@/${VERSION}/g api/openapi.yaml
