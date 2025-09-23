#
# MIT License
#
# (C) Copyright 2023, 2025 Hewlett Packard Enterprise Development LP
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

from cray.cfs.api.controllers.components import convert_component_to_v2, convert_component_to_v3
from cray.cfs.api.controllers.components import DB as COMPONENTS_DB
from cray.cfs.api.controllers.configurations import (
                                                        convert_configuration_to_v2,
                                                        convert_configuration_to_v3
                                                    )
from cray.cfs.api.controllers.configurations import DB as CONFIGURATIONS_DB
from cray.cfs.api.controllers.sessions import convert_session_to_v2, convert_session_to_v3
from cray.cfs.api.controllers.sessions import DB as SESSIONS_DB
from cray.cfs.api.controllers.options import convert_options_to_v2
from cray.cfs.api.controllers.options import DB as OPTIONS_DB
from cray.cfs.api.controllers.options import cleanup_old_options


log_level = os.environ.get('STARTING_LOG_LEVEL', 'INFO')
LOG_FORMAT = "%(asctime)-15s - %(levelname)-7s - %(name)s - %(message)s"
logging.basicConfig(level=log_level, format=LOG_FORMAT)
LOGGER = logging.getLogger('cfs.api.migrations')


def migrate_database(db, convert, check_function=lambda data : True):
    for key in db.get_keys():
        old_data = db.get(key)
        if not check_function(old_data):
            continue
        new_data = convert(old_data)
        db.put(key, new_data)
        LOGGER.info("Migrating %s to the new data structure", key)


def migrate_v2_to_v3():
    LOGGER.info("Migrating Components")
    migrate_database(COMPONENTS_DB, convert_component_to_v3, lambda data : "errorCount" in data)
    LOGGER.info("Migrating Configurations")
    migrate_database(CONFIGURATIONS_DB, convert_configuration_to_v3,
                     lambda data : "lastUpdated" in data)
    LOGGER.info("Migrating Sessions")
    migrate_database(SESSIONS_DB, convert_session_to_v3,
                     lambda data : "startTime" in data.get("status", {}).get("session", {}))
    LOGGER.info("Migration Complete")
    # Migrating the options is taken care of by the options cleanup


def perform_migrations():
    LOGGER.info("Starting data migrations")
    LOGGER.info("Running options cleanup")
    cleanup_old_options()
    LOGGER.info("Migrating v2 records to v3")
    migrate_v2_to_v3()


def rollback_to_v2():
    # This isn't used automatically and is for testing and emergency purposes
    migrate_database(COMPONENTS_DB, convert_component_to_v2)
    migrate_database(CONFIGURATIONS_DB, convert_configuration_to_v2)
    migrate_database(SESSIONS_DB, convert_session_to_v2)
    migrate_database(OPTIONS_DB, convert_options_to_v2)


if __name__ == "__main__":
    perform_migrations()
