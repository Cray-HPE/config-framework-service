# Cray-provided controllers for the Configuration Framework Service
# Copyright 2020, Cray Inc. All Rights Reserved.

import logging

from cray.cfs.api import dbutils
from cray.cfs.api.models.healthz import Healthz
from cray.cfs.k8s import CFSV1K8SConnector

LOGGER = logging.getLogger('cray.cfs.api.controllers.healthz')
DB = dbutils.get_wrapper(db='options')
K8S = CFSV1K8SConnector("services")


def get_healthz():
    status_code = 200

    db_status = _get_db_status()
    crd_status = _get_crd_status()

    for status in [db_status, crd_status]:
        if status != 'ok':
            status_code = 503
            break

    return Healthz(
        db_status=db_status,
        crd_status=crd_status,
    ), status_code


def _get_db_status():
    available = False
    try:
        if DB.info():
            available = True
    except Exception as e:
        LOGGER.error(e)

    if available:
        return 'ok'
    return 'not_available'


def _get_crd_status():
    available = False
    try:
        if K8S.list(limit=1):
            available = True
    except Exception as e:
        LOGGER.error(e)

    if available:
        return 'ok'
    return 'not_available'
