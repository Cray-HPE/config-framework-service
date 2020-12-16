# Copyright 2020 Hewlett Packard Enterprise Development LP

import json
import logging
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
KAFKA_TIMEOUT = 0.5


class ProducerWrapper:
    """A wrapper around a Kafka connection"""

    def __init__(self, topic=None, ensure_init=True):
        self.topic = topic
        self.producer = None
        self._init_producer(retry=ensure_init)

    def _init_producer(self, retry=True):
        if self.producer:
            try:
                self.producer.close(timeout=KAFKA_TIMEOUT)
            except KafkaTimeoutError as e:
                LOGGER.warning('Unable to close previous Kafka producer: {}'.format(e))
            self.producer = None
        while not self.producer:
            svc_obj = k8ssvcs.read_namespaced_service("cray-shared-kafka-kafka-bootstrap",
                                                      "services")
            host = svc_obj.spec.cluster_ip
            kafka = host+':'+KAFKA_PORT
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


    def produce(self, event_type, topic=None, data={}):
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
        self.producer.send(topic, data)
        self.producer.flush(timeout=KAFKA_TIMEOUT)

    def flush(self):
        self.producer.flush()
