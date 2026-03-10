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
import ujson as json
import logging
import os
import time

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

from kubernetes import config, client
from kubernetes.config.config_exception import ConfigException

LOGGER = logging.getLogger(__name__)

try:
    config.load_incluster_config()
except ConfigException:  # pragma: no cover
    config.load_kube_config()  # Development

_api_client = client.ApiClient()
k8ssvcs = client.CoreV1Api(_api_client)
KAFKA_PORT = '9092'
KAFKA_TIMEOUT = os.getenv('KAFKA_PRODUCER_TIMEOUT', default=1)


class ProducerWrapper:
    """A wrapper around a Kafka connection"""

    def __init__(self, topic=None, ensure_init=True):
        self.topic = topic
        self.producer = None
        self._init_producer(retry=ensure_init)

    def _init_producer(self, retry=True):
        if self.producer:
            LOGGER.debug("_init_producer: calling self.producer.close()")
            try:
                self.producer.close(timeout=KAFKA_TIMEOUT)
            except KafkaTimeoutError as e:
                LOGGER.warning('Unable to close previous Kafka producer: {}'.format(e))
            LOGGER.debug("_init_producer: self.producer.close() completed")
            self.producer = None
        while not self.producer:
            svc_obj = k8ssvcs.read_namespaced_service("cray-shared-kafka-kafka-bootstrap",
                                                      "services")
            host = svc_obj.spec.cluster_ip
            kafka = host+':'+KAFKA_PORT
            LOGGER.debug("_init_producer: Initializing KafkaProducer, bootstrap_server %s", kafka)
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=[kafka],
                    value_serializer=lambda m: json.dumps(m).encode('utf-8'),
                    retries=5)
            except Exception as e:
                LOGGER.error('Error initializing Kafka producer: {}'.format(e))
                if not retry:
                    return
                time.sleep(5)
            LOGGER.debug("_init_producer: KafkaProducer successfully initialized")

    def produce(self, data, event_type, topic=None):
        LOGGER.debug("produce: event_type=%s, data=%s", event_type, data)
        if not topic:
            topic = self.topic
        event = {
            'type': event_type,
            'data': data
        }
        try:
            self._produce(topic, event)
            return
        except KafkaTimeoutError:
            # The networking may have changed, causing writing to hang.
            LOGGER.warning('There was a timeout while writing to Kafka. Restarting the kafka producer and retrying...')
        self._init_producer()
        self._produce(topic, event)

    def _produce(self, topic, data):
        LOGGER.debug("_produce: Calling self.producer.send(), topic=%s", topic)
        self.producer.send(topic, data)
        LOGGER.debug("_produce: Calling self.producer.flush(timeout=%s)", KAFKA_TIMEOUT)
        self.producer.flush(timeout=KAFKA_TIMEOUT)
        LOGGER.debug("_produce: Done")

    def flush(self):
        LOGGER.debug("flush: Calling self.producer.flush()")
        self.producer.flush()
        LOGGER.debug("flush: Done")
