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

import datetime
from functools import partial
import logging
import os
import random
import socket
import threading
import time
from typing import (
    final,
    Final,
    Literal,
    NoReturn,
    TypedDict,
)

from csm_utils.logging import exc_type_msg

from cray.cfs.api.controllers import options

from .db import LOCK_DB, SESSIONS_DB
from .filters import matches_filter
from .kafka import kafka_create_event
from .types import V3SessionData


LOGGER = logging.getLogger('cray.cfs.api.controllers.sessions.tardy_scanner')

HOSTNAME = socket.gethostname()
TARDY_SCAN_LOCK_DB_KEY: Final[str] = "tardy_pending_session_scan"
TARDY_SCAN_INTERVAL_SECONDS = 60
TARDY_SCAN_INTERVAL = datetime.timedelta(seconds=TARDY_SCAN_INTERVAL_SECONDS)
_tardy_scan_supervisor_thread = None
_tardy_scan_supervisor_thread_lock = threading.Lock()

# Marked as final because we do not intend to subclass this. It doesn't really
# matter at this point, but if type checking is ever properly added, this helps
# the type checker.
@final
class TardyLockDbEntry(TypedDict):
    hostname: str
    pid: int
    tid: int
    start_time: str


def _tardy_scanner() -> NoReturn:
    """
    Every 15 seconds, check to see when the last scan was run.
    """
    while True:
        now = datetime.datetime.now()
        lock_entry = TardyLockDbEntry(
                        hostname=HOSTNAME,
                        pid=os.getpid(),
                        tid=threading.get_ident(),
                        start_time=now.isoformat(),
                     )
        patched_entry = LOCK_DB.patch(
                            key=TARDY_SCAN_LOCK_DB_KEY,
                            patch_data=lock_entry,
                            patch_handler=_lock_db_patch_handler,
                            default_entry=lock_entry,
                        )
        if patched_entry == lock_entry:
            # This means we should scan
            _do_tardy_session_scan()
            # Given that we have scanned, we can sleep for the tardy scan interval
            # Add small "jitter" to the sleeps, to avoid the different processes synching up
            time.sleep(TARDY_SCAN_INTERVAL_SECONDS + random.random())
        else:
            # Since we did not scan this time, only sleep for 15 seconds
            # Add small "jitter" to the sleeps, to avoid the different processes synching up
            time.sleep(15 + random.random())
        options.update_server_log_level()


def _lock_db_patch_handler(
    db_entry: TardyLockDbEntry,
    new_entry: TardyLockDbEntry,
) -> TardyLockDbEntry:
    """
    If new_entry['start_time'] - db_entry['start_time'] >= TARDY_SCAN_INTERVAL,
    return new_entry. Otherwise, return db_entry
    """
    try:
        new_entry_start_time = datetime.datetime.fromisoformat(new_entry['start_time'])
    except Exception as err:
        LOGGER.warning("Error parsing new tardy scan lock DB entry (%s): %s",
                       new_entry,
                       exc_type_msg(err))
        # In this case, keep the current entry unchanged
        return db_entry

    try:
        db_entry_start_time = datetime.datetime.fromisoformat(db_entry['start_time'])
    except Exception as err:
        LOGGER.warning("Error parsing tardy scan lock DB entry (%s): %s",
                       db_entry,
                       exc_type_msg(err))
        # In this case, replace with our entry
        return new_entry
    try:
        if new_entry_start_time - db_entry_start_time >= TARDY_SCAN_INTERVAL:
            return new_entry
    except Exception as err:
        LOGGER.warning("Error comparing tardy scan lock DB entry (%s) to new entry (%s): %s",
                       db_entry,
                       new_entry,
                       exc_type_msg(err))
        # In this case, replace with our entry
        return new_entry

    # Getting here means the last scan was within the TARDY_SCAN_INTERVAL, so no need
    # for us to do one -- thus, leave the entry unchanged
    return db_entry


def _tardy_filter(
    data: V3SessionData,
    min_start: datetime.datetime,
    max_start: datetime.datetime,
) -> Literal[False]:
    """
    For any eligible tardy pending sessions, re-send the Kafka create for them.
    Our filter always returns False because we aren't actually trying to return
    a list of sessions.
    """        
    if matches_filter(
        data=data,
        min_start=min_start,
        max_start=max_start,
        status='pending',
        name_contains=None,
        succeeded=None,
        tag_list=None,
        job_set=False,
    ):
        # If we get here, it means the session is pending, has no job set, and
        # started within our tardy window. So, re-send its Kafka create event
        LOGGER.info("Re-sending Kafka CREATE event for tardy session %s", data)
        kafka_create_event(data)
    # Regardless, return False
    return False


def _do_tardy_session_scan() -> None:
    opts = options.Options()
    min_age_seconds = 45
    # Regardless of what the batcher pending timeout is set to,
    # ignore sessions over 10 minutes old.
    max_age_seconds = min(opts.batcher_pending_timeout, 600) - 45
    if min_age_seconds >= max_age_seconds:
        # No session age window, so nothing to do
        return
    now = datetime.datetime.now()
    min_start = now - datetime.timedelta(seconds=max_age_seconds)
    max_start = now - datetime.timedelta(seconds=min_age_seconds)
    _filter = partial(_tardy_filter, min_start=min_start, max_start=max_start)
    # Scan all sessions using this filter
    SESSIONS_DB.get_all(limit=0, data_filters=[_filter])


def _start_tardy_scanner() -> threading.Thread:
    t = threading.Thread(target=_tardy_scanner, daemon=True)
    t.start()
    return t


def _tardy_scan_supervisor() -> NoReturn:
    LOGGER.debug("Tardy session scanner supervisor thread started")
    t = _start_tardy_scanner()

    while True:
        if not t.is_alive():
            options.update_server_log_level()
            LOGGER.warning("Tardy session scanner thread died, restarting...")
            t = _start_tardy_scanner()
        time.sleep(1)


def start_tardy_scan_supervisor() -> None:
    global _tardy_scan_supervisor_thread
    # Start queue supervisor if not running
    if _tardy_scan_supervisor_thread is not None:
        # Already running
        return
    with _tardy_scan_supervisor_thread_lock:
        if _tardy_scan_supervisor_thread is not None:
            # Already running
            return
        t = threading.Thread(target=_tardy_scan_supervisor, daemon=True)
        t.start()
        _tardy_scan_supervisor_thread = t
