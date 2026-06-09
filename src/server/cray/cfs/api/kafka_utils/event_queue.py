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
import queue
import threading
import time
from typing import Generator, NoReturn

from cray.cfs.api.controllers.options import update_server_log_level
from cray.cfs.api.env_utils import (
    get_pos_float_env_var_or_default,
    get_pos_int_env_var_or_default,
)

from .producer_wrapper import KafkaEvent, ProducerWrapper

LOGGER = logging.getLogger(__name__)


DEFAULT_MAX_BATCH_SIZE = 100
# Units: seconds
DEFAULT_MAX_BATCH_WINDOW = 2

# Allow defaults to be overridden
MAX_BATCH_SIZE = get_pos_int_env_var_or_default('DEFAULT_MAX_KAFKA_BATCH_SIZE',
                                                DEFAULT_MAX_BATCH_SIZE)
MAX_BATCH_WINDOW = get_pos_float_env_var_or_default('DEFAULT_MAX_KAFKA_BATCH_WINDOW',
                                                    DEFAULT_MAX_BATCH_WINDOW)

_supervisor_thread = None
_supervisor_thread_lock = threading.Lock()
_kafka_queue = queue.Queue(maxsize=500)

def add_event(data: dict, event_type: str) -> None:
    LOGGER.debug("add_event: event_type=%s, data=%s", event_type, data)
    # block=False so that we immediately raise an exception if the queue is full,
    # because if that happens, something is very wrong
    _kafka_queue.put(
        KafkaEvent(data=data, event_type=event_type),
        block=False,
    )

def _event_batch_window() -> Generator[KafkaEvent, None, None]:
    # Wait until there is an event, then yield it
    LOGGER.debug("_event_batch_window: Calling queue.get()")
    event = _kafka_queue.get()
    batch_size = 1
    LOGGER.debug("_event_batch_window: event=%s", event)
    yield event

    # Now yield any additional events, until we hit our batch size
    # limit or window time limit
    window_end = time.monotonic() + MAX_BATCH_WINDOW
    while batch_size < MAX_BATCH_SIZE:
        timeout = window_end - time.monotonic()
        if timeout <= 0:
            # Max window time has been reached -- stop generating
            return

        try:
            event = _kafka_queue.get(timeout=timeout)
        except queue.Empty:
            # Timeout has been reached -- stop generating
            return

        batch_size += 1
        LOGGER.debug("_event_batch_window: event=%s", event)
        yield event

def _handle_events(topic: str) -> NoReturn:
    LOGGER.debug("Kafka event_handler thread started")
    producer = ProducerWrapper(topic)
    while True:
        producer.send_flush_events(*_event_batch_window())
        update_server_log_level()

def _start_event_handler(topic: str) -> threading.Thread:
    t = threading.Thread(target=_handle_events, kwargs={'topic': topic}, daemon=True)
    t.start()
    return t

def _supervisor(topic: str) -> NoReturn:
    LOGGER.debug("Kafka event_handler supervisor thread started (topic=%s)", topic)
    t = _start_event_handler(topic)

    while True:
        if not t.is_alive():
            update_server_log_level()
            LOGGER.warning("Kafka event_handler thread died, restarting... (topic=%s)",
                           topic)
            t = _start_event_handler(topic)
        time.sleep(1)

def start_supervisor(topic: str) -> None:
    global _supervisor_thread
    # Start queue supervisor if not running
    if _supervisor_thread is not None:
        # Already running
        return
    with _supervisor_thread_lock:
        if _supervisor_thread is not None:
            # Already running
            return
        t = threading.Thread(target=_supervisor, kwargs={'topic': topic}, daemon=True)
        t.start()
        _supervisor_thread = t
