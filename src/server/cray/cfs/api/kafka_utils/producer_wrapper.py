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
import logging
import threading
from typing import NoReturn, Optional

from confluent_kafka import Producer
import ujson as json

from cray.cfs.api.dbutils import JsonData

from .defs import KAFKA_FLUSH_TIMEOUT
from .k8s import get_kafka_bootstrap_server

LOGGER = logging.getLogger(__name__)


class ProducerWrapper:
    """
    A wrapper around a Kafka connection.
    In this case, it is only concerned with sending events, not receiving them.
    """

    def __init__(self, topic: Optional[str]=None) -> None:
        self.topic = topic
        kafka = get_kafka_bootstrap_server()
        LOGGER.debug("Initializing Kafka Producer, bootstrap.servers %s", kafka)
        self.producer = Producer({
            "bootstrap.servers": kafka,
            "enable.idempotence": True,
        })
        LOGGER.info(
            "Kafka Producer initialized with topic=%s bootstrap.servers=%s",
            topic,
            kafka
        )
        self._poll_thread = threading.Thread(
            target=self._poll_loop,
            daemon=True
        )
        self._poll_thread.start()
        LOGGER.debug("Background polling thread started")

    def _poll_loop(self) -> NoReturn:
        """
        Loop forever calling producer.poll(0.1)
        """
        while True:
            self.producer.poll(0.1)

    @staticmethod
    def _value(data: JsonData, event_type: str) -> str:
        """
        Create the event dictionary.
        Return a utf-8 JSON string representation of the event
        """
        event = {
            'type': event_type,
            'data': data
        }
        return json.dumps(event).encode('utf-8')

    def produce(
        self,
        data: JsonData,
        event_type: str,
        topic: Optional[str]=None
    ) -> None:
        LOGGER.debug("produce: event_type=%s, data=%s, topic=%s", event_type, data, topic)
        if not topic:
            topic = self.topic
        value = self._value(data, event_type)
        LOGGER.debug("produce: Calling producer.send(%s, value=%s)", topic, value)
        self.producer.produce(topic, value=value)
        LOGGER.debug("produce: Done")

    def flush(self, timeout: int=KAFKA_FLUSH_TIMEOUT) -> None:
        LOGGER.debug("produce: Calling producer.flush(timeout=%d)", timeout)
        self.producer.flush(timeout=timeout)
        LOGGER.debug("produce: producer.flush(timeout=%d) completed", timeout)
