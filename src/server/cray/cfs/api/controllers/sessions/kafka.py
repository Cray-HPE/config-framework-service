#
# MIT License
#
# (C) Copyright 2019-2026 Hewlett Packard Enterprise Development LP
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

from functools import partial
import logging
from typing import Literal

from cray.cfs.api import kafka_utils


from .types import V3SessionData

LOGGER = logging.getLogger('cray.cfs.api.controllers.sessions.kafka')

KAFKA_TOPIC = 'cfs-session-events'

start_kafka_supervisor = partial(kafka_utils.start_supervisor, topic=KAFKA_TOPIC)

def _add_kafka_event(
    data: V3SessionData,
    event_type: Literal['CREATE', 'DELETE']
) -> None:
    LOGGER.debug("_add_kafka_event: Queueing Kafka %s event for '%s'",
                 event_type, data)
    kafka_utils.add_event(event_type=event_type, data=data)
    LOGGER.debug("_add_kafka_event: Done")

kafka_create_event = partial(_add_kafka_event, event_type='CREATE')
kafka_delete_event = partial(_add_kafka_event, event_type='DELETE')
