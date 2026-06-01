#
# MIT License
#
# (C) Copyright 2020-2026 Hewlett Packard Enterprise Development LP
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
import datetime
import logging
import threading
import time
from typing import NoReturn, Optional

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaTimeoutError
import ujson as json

from cray.cfs.api.dbutils import JsonData, JsonDict

from .defs import KAFKA_FLUSH_TIMEOUT
from .k8s import get_kafka_bootstrap_server

LOGGER = logging.getLogger(__name__)


def send_event(topic, event_type, data, first_attempt=True, kafka=None):
    if kafka is None:
        kafka = get_kafka_bootstrap_server()
    producer = KafkaProducer(
        bootstrap_servers=kafka,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_block_ms=5000,
    )
    event_data = {
        'type': event_type,
        'data': data,
        'sent': datetime.datetime.now().isoformat(timespec='seconds'),
    }
    try:
        producer.send(topic, value=event_data)
        producer.flush(timeout=5)
    except KafkaTimeoutError:
        if first_attempt:
            return send_event(topic, event_type, data, first_attempt=False, kafka=kafka)
        raise
