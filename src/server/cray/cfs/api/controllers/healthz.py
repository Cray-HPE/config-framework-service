#
# MIT License
#
# (C) Copyright 2020-2022, 2025 Hewlett Packard Enterprise Development LP
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
# Cray-provided controllers for the Configuration Framework Service

import logging
from typing import Literal

from cray.cfs.api import dbutils, kafka_utils
from cray.cfs.api.models.healthz import Healthz

LOGGER = logging.getLogger('cray.cfs.api.controllers.healthz')
DB = dbutils.get_wrapper(db='options')
KAFKA = None

def get_healthz() -> tuple[Healthz, Literal[200, 503]]:
    """Used by the GET /healthz API operation"""
    LOGGER.debug("GET /healthz invoked get_healthz")
    status_code = 200

    db_status = _get_db_status()
    kafka_status = _get_kafka_status()

    if db_status != 'ok' or kafka_status != 'ok':
        status_code = 503

    return Healthz(
        db_status=db_status,
        kafka_status=kafka_status
    ), status_code


def _get_db_status() -> str:
    try:
        if DB.info():
            return 'ok'
    except Exception as err:
        LOGGER.error(err)
    return 'not_available'


def _get_kafka_status() -> str:
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
