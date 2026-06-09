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
from functools import lru_cache
import logging
import time
from typing import NamedTuple
import uuid

from csm_utils.logging import exc_type_msg
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
import ujson as json

from cray.cfs.api.env_utils import (
    get_pos_float_env_var_or_default,
)

from .k8s import kafka_bootstrap_host


LOGGER = logging.getLogger(__name__)


DEFAULT_KAFKA_FLUSH_TIMEOUT = 3
# Close timeout should be > flush timeout, because
# a close implicitly include a flush
DEFAULT_KAFKA_CLOSE_TIMEOUT = 6
DEFAULT_INIT_TIMEOUT = 30

# Allow a different close timeout value to be specified
KAFKA_CLOSE_TIMEOUT = get_pos_float_env_var_or_default('KAFKA_PRODUCER_CLOSE_TIMEOUT',
                                                       DEFAULT_KAFKA_CLOSE_TIMEOUT)

# Allow a different flush timeout value to be specified
KAFKA_FLUSH_TIMEOUT = get_pos_float_env_var_or_default('KAFKA_PRODUCER_FLUSH_TIMEOUT',
                                                       DEFAULT_KAFKA_FLUSH_TIMEOUT)

INIT_TIMEOUT = get_pos_float_env_var_or_default('KAFKA_PRODUCER_INIT_TIMEOUT',
                                                DEFAULT_INIT_TIMEOUT)

DEFAULT_PRODUCER_INIT_KWARGS = {
    'value_serializer' : lambda m: json.dumps(m).encode('utf-8'),
    'acks': 'all',
    'retries': 10,
    'retry_backoff_ms': 100,
    'linger_ms': 10,
    'request_timeout_ms': 5000,
    'max_block_ms': 10000,
}

class KafkaEvent(NamedTuple):
    data: dict
    event_type: str

    @property
    def send_value(self) -> dict:
        # CFS operator wants the event type in a field named 'type'
        return { 'data': self.data, 'type': self.event_type }

@lru_cache(maxsize=5)
def producer_init_kwargs(**kwargs):
    init_kwargs = DEFAULT_PRODUCER_INIT_KWARGS.copy()
    init_kwargs.update(kwargs)
    return init_kwargs

class ProducerWrapper:
    """
    A wrapper around a Kafka connection.
    In this case, it is only concerned with sending events, not receiving them.
    """

    def __init__(self, topic: str) -> None:
        self.topic = topic
        self.producer = self._init_producer()

    def send_flush_events(self, *events: KafkaEvent, second_attempt: bool=False) -> None:
        # The events list should never be empty, but just in case
        if not events:
            LOGGER.warning("send_flush_list: Received empty events list")
            return

        if second_attempt:
            LOGGER.info('Reinitializing and retrying...')

            # We want to use a new producer for our retry attempt.
            self._reinit_producer()

        try:
            for e in events:
                self._send(e)
        except KafkaTimeoutError as err:
            # The networking may have changed, causing writing to hang.
            LOGGER.warning('Timeout sending to KafkaProducer: %s', exc_type_msg(err))
            if second_attempt:
                raise

            # Retry, but if this next attempt fails, do not retry again
            self.send_flush_events(*events, second_attempt=True)
            return

        try:
            self._flush()
        except KafkaTimeoutError as err:
            # The networking may have changed, causing writing to hang.
            LOGGER.warning('Timeout flushing KafkaProducer: %s', exc_type_msg(err))

            if second_attempt:
                raise

            # Retry, but if this next attempt fails, do not retry again
            self.send_flush_events(*events, second_attempt=True)

    def healthy(self, retry: bool=True) -> bool:
        """
        Verifies connectivity and ability to retrieve metadata
        """
        if not self.bootstrap_connected:
            if retry:
                # We want to use a new producer for our retry attempt.
                self._reinit_producer()
                # Do not retry if it still fails
                return self.healthy(retry=False)
            return False

        if self.partitions_available:
            return True

        if retry:
            # We want to use a new producer for our retry attempt.
            self._reinit_producer()
            # Do not retry if it still fails
            return self.healthy(retry=False)

        return False

    @property
    def bootstrap_connected(self) -> bool:
        # No need for try/except for KafkaTimeoutError because
        # bootstap_connected is a local check
        if self.producer.bootstrap_connected():
            LOGGER.debug('healthy: bootstrap_connected is True')
            return True
        LOGGER.warning('healthy: bootstrap_connected is False')
        return False

    @property
    def partitions_available(self) -> bool:
        try:
            partitions = self.producer.partitions_for(self.topic)
        except KafkaTimeoutError as err:
            LOGGER.warning('Timeout in KafkaProducer partitions_for(%s): %s',
                           self.topic, exc_type_msg(err))
            return False
        if partitions is None:
            LOGGER.warning('Null value returned by KafkaProducer partitions_for(%s)',
                           self.topic)
            return False
        LOGGER.debug('KafkaProducer partitions_for(%s) returned %s',
                     self.topic, partitions)
        return True

    def _reinit_producer(self) -> None:
        try:
            self._close()
        except KafkaTimeoutError as err:
            LOGGER.warning('Unable to close current Kafka producer: %s', exc_type_msg(err))

        self.producer = self._init_producer()

    def _init_producer(self) -> KafkaProducer:
        """
        This method should only be called when the init lock is held, or from __init__
        """
        timeout_time = time.monotonic() + INIT_TIMEOUT
        while True:
            kafka = kafka_bootstrap_host()
            init_kwargs = producer_init_kwargs(bootstrap_servers=kafka)
            LOGGER.debug('_init_producer: Initializing KafkaProducer with kwargs %s', init_kwargs)
            try:
                producer = KafkaProducer(**init_kwargs)
                LOGGER.debug('_init_producer: KafkaProducer successfully initialized')
                return producer
            except Exception as err:
                LOGGER.error('Error initializing Kafka producer: %s', exc_type_msg(err))
                time_left = timeout_time - time.monotonic()
                if time_left <= 0:
                    raise
                # Retry after 5 seconds, or until our retry time has expired, whichever is shorter
                time.sleep(min(1, time_left))

    def _close(self) -> None:
        LOGGER.debug('_close: calling producer.close(timeout=%f)',
                     KAFKA_CLOSE_TIMEOUT)
        self.producer.close(timeout=KAFKA_CLOSE_TIMEOUT)
        LOGGER.debug('_close: Done')

    def _flush(self) -> None:
        LOGGER.debug('_flush: Calling producer.flush(timeout=%f)',
                     KAFKA_FLUSH_TIMEOUT)
        self.producer.flush(timeout=KAFKA_FLUSH_TIMEOUT)
        LOGGER.debug('_flush: Done')

    def _send(self, event: KafkaEvent) -> None:
        send_kwargs = { 'topic': self.topic, 'value': event.send_value }
        # Add a UUID for internal tracking
        send_kwargs['value']['uuid'] = str(uuid.uuid4())
        LOGGER.debug('_send: Calling producer.send() with kwargs %s', send_kwargs)
        future = self.producer.send(**send_kwargs)
        LOGGER.debug('_send: Done, adding callbacks')
        future.add_callback(
            lambda metadata: LOGGER.debug(
                "Kafka send (%s) succeeded: topic=%s partition=%s offset=%s",
                send_kwargs,
                metadata.topic,
                metadata.partition,
                metadata.offset,
            )
        )
        LOGGER.debug('_send: Callback added')
        future.add_errback(lambda exc: LOGGER.exception("Kafka send (%s) failed: %s", send_kwargs, exc))
        LOGGER.debug('_send: Errback added')
