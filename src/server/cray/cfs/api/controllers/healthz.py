# Cray-provided controllers for the Configuration Framework Service
# Copyright 2020, Cray Inc. All Rights Reserved.

import logging

from cray.cfs.api import dbutils
from cray.cfs.api import kafka_utils
from cray.cfs.api.models.healthz import Healthz

LOGGER = logging.getLogger('cray.cfs.api.controllers.healthz')
DB = dbutils.get_wrapper(db='options')
KAFKA = None

def get_healthz():
    status_code = 200

    db_status = _get_db_status()
    kafka_status = _get_kafka_status()

    for status in [db_status, kafka_status]:
        if status != 'ok':
            status_code = 503
            break

    return Healthz(
        db_status=db_status,
        kafka_status=kafka_status
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


def _get_kafka_status():
    global KAFKA
    available = False
    try:
        if not KAFKA or not KAFKA.producer:
            KAFKA = kafka_utils.ProducerWrapper(ensure_init=False)
            available = True
        elif KAFKA.producer.metrics():
            available = True
    except Exception as e:
        LOGGER.error(e)

    if available:
        return 'ok'
    return 'not_available'
