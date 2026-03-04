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
import os
import threading
import time

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from kubernetes import config, client
from kubernetes.config.config_exception import ConfigException
from readerwriterlock import rwlock
import ujson as json

from cray.cfs.api.env_utils import get_pos_float_env_var_or_default

LOGGER = logging.getLogger(__name__)

try:
    config.load_incluster_config()
except ConfigException:  # pragma: no cover
    config.load_kube_config()  # Development

DEFAULT_KAFKA_TIMEOUT=1
DEFAULT_LOCK_TIMEOUT=5

_api_client = client.ApiClient()
k8ssvcs = client.CoreV1Api(_api_client)
KAFKA_PORT = '9092'
KAFKA_TIMEOUT = get_pos_float_env_var_or_default('KAFKA_PRODUCER_TIMEOUT',
                                                 DEFAULT_KAFKA_TIMEOUT)

# Allow a different close timeout value to be specified, defaulting to the general Kafka timeout
KAFKA_CLOSE_TIMEOUT = get_pos_float_env_var_or_default('KAFKA_PRODUCER_CLOSE_TIMEOUT',
                                                       KAFKA_TIMEOUT)

# Allow a different flush timeout value to be specified, defaulting to the general Kafka timeout
KAFKA_FLUSH_TIMEOUT = get_pos_float_env_var_or_default('KAFKA_PRODUCER_FLUSH_TIMEOUT',
                                                       KAFKA_TIMEOUT)

LOCK_TIMEOUT = get_pos_float_env_var_or_default('KAFKA_PRODUCER_LOCK_TIMEOUT',
                                                DEFAULT_LOCK_TIMEOUT)

# Allow a different send lock timeout value to be specified, defaulting to the general lock timeout
SEND_LOCK_TIMEOUT = get_pos_float_env_var_or_default('KAFKA_PRODUCER_SEND_LOCK_TIMEOUT',
                                                     LOCK_TIMEOUT)

# Allow a different init lock timeout value to be specified, defaulting to the general lock timeout
INIT_LOCK_TIMEOUT = get_pos_float_env_var_or_default('KAFKA_PRODUCER_INIT_LOCK_TIMEOUT',
                                                      LOCK_TIMEOUT)

class LockTimeoutError(Exception):
    """
    Raised when a timeout occurs trying to obtain the
    ProducerWrapper lock
    """
    def __init__(self, ltype: str, timeout: float) -> None:
        super().__init__(f"Unable to take Kafka producer {ltype} lock within {timeout} seconds")

class InitLockTimeoutError(LockTimeoutError):
    def __init__(self) -> None:
        super().__init__("init", INIT_LOCK_TIMEOUT)

class SendLockTimeoutError(LockTimeoutError):
    def __init__(self) -> None:
        super().__init__("send", SEND_LOCK_TIMEOUT)

class ProducerWrapper:
    """A wrapper around a Kafka connection"""

    def __init__(self, topic=None, ensure_init=True):
        self._lock = rwlock.RWLockWrite()
        self.topic = topic
        self.producer: KafkaProducer|None = None
        self.close_attempted = False
        self._init_producer(retry=ensure_init)
        if ensure_init:
            assert isinstance(self.producer, KafkaProducer)

    def _init_lock(self):
        """
        The init lock is the write-type lock in this write-preferred read-write lock.
        This is what must be taken before changing the self.producer object.
        When this lock is held, sends and flushes may also be used.
        """
        return self._lock.gen_wlock()

    def _send_lock(self):
        """
        The send lock is the read-type lock in this write-preferred read-write lock.
        Taking this lock allows doing sends or flushes to the self.producer object.
        Note that the init lock also allows sends or flushes.
        """
        return self._lock.gen_rlock()

    def _reinit_producer(self, old_producer: KafkaProducer) -> None:
        """
        If self.producer != old_producer, then return.
        Otherwise, take the init lock.
        If self.producer still equals old_producer, then call _init_producer
        to reinitialize it.
        """
        # It is possible that another thread has already taken care of this for us
        if self.producer is not old_producer:
            return

        # Get the init lock
        init_lock = self._init_lock()
        if not init_lock.acquire(blocking=True, timeout=INIT_LOCK_TIMEOUT):
            raise InitLockTimeoutError()

        # Enclose this in a try block to ensure that we release the lock
        try:
            LOGGER.debug("produce: Init lock acquired")
            # Now that we have the lock, check again to see if this is the
            # same producer (it is possible a different thread updated it
            # while we were getting the lock)
            if self.producer is not old_producer:
                return

            # It is still the same, so initialize it
            self._init_producer()
        finally:
            LOGGER.debug("produce: Releasing init lock")
            init_lock.release()
            LOGGER.debug("produce: Init lock released")

    def _init_producer(self, retry: bool=True) -> None:
        """
        This method should only be called when the init lock is held, or from __init__
        """
        # If we have a current producer set, and we have not previously attempted
        # to close it, then we should do so first, before re-initializing
        if self.producer is not None and not self.close_attempted:
            # Set this so we don't get stuck in a possible loop attempting to close this producer
            self.close_attempted = True
            LOGGER.debug("_init_producer: calling self.producer.close(timeout=%f)",
                         KAFKA_CLOSE_TIMEOUT)
            try:
                self.producer.close(timeout=KAFKA_CLOSE_TIMEOUT)
            except KafkaTimeoutError as e:
                LOGGER.warning('Unable to close current Kafka producer: %s', e)
            LOGGER.debug("_init_producer: self.producer.close() completed")
        while True:
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
            # Set close_attempted to False for this new producer
            self.close_attempted = False
            LOGGER.debug("_init_producer: KafkaProducer successfully initialized")
            return

    def produce(self, data, event_type, topic=None):
        LOGGER.debug("produce: event_type=%s, data=%s, topic=%s", event_type, data, topic)
        if not topic:
            topic = self.topic
        event = {
            'type': event_type,
            'data': data
        }
        self._produce(topic=topic, event=event, retry=True)

    def _produce(self, topic, event, retry: bool) -> None:
        LOGGER.debug("_produce: topic=%s, event=%s, retry=%s", topic, event, retry)
        send_lock = self._send_lock()
        LOGGER.debug("_produce: Trying to acquire send lock")
        if not send_lock.acquire(blocking=True, timeout=SEND_LOCK_TIMEOUT):
            raise SendLockTimeoutError()

        # Enclose this in a try block to ensure that we release the lock
        try:
            LOGGER.debug("_produce: Send lock acquired")
            # Remember our current producer
            my_producer = self.producer
            LOGGER.debug("_produce: Calling my_producer.send(), topic=%s", topic)
            my_producer.send(topic, event)
            LOGGER.debug("_produce: Calling my_producer.flush(timeout=%f)", KAFKA_FLUSH_TIMEOUT)
            my_producer.flush(timeout=KAFKA_FLUSH_TIMEOUT)
            LOGGER.debug("_produce: Done")
            return
        except KafkaTimeoutError:
            # The networking may have changed, causing writing to hang.
            if not retry:
                raise
            LOGGER.warning('There was a timeout while writing to Kafka. Restarting the kafka producer and retrying...')
        finally:
            LOGGER.debug("_produce: Releasing send lock")
            send_lock.release()
            LOGGER.debug("_produce: Send lock released")

        # We want to use a new producer for our retry attempt.
        self._reinit_producer(my_producer)

        # Retry, but if this next attempt fails, do not retry again
        self._produce(topic=topic, event=event, retry=False)

    def flush(self):
        LOGGER.debug("flush: Getting send lock")
        with self._send_lock():
            LOGGER.debug("flush: Send lock taken, calling self.producer.flush()")
            self.producer.flush()
            LOGGER.debug("flush: Done, releasing send lock")
        LOGGER.debug("flush: Send lock released")
