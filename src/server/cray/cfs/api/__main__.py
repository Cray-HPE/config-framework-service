#!/usr/bin/env python3
#
# MIT License
#
# (C) Copyright 2019-2022, 2025 Hewlett Packard Enterprise Development LP
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
# Config Framework Service API Main

import os
import logging
import connexion

from cray.cfs.api import encoder
from cray.cfs.api.controllers import options
from cray.cfs.api.controllers import sessions

log_level = os.environ.get('STARTING_LOG_LEVEL', 'WARN')
LOG_FORMAT = "%(asctime)-15s - %(process)d - %(thread)d - %(levelname)-7s - %(name)s - %(message)s"
logging.basicConfig(level=log_level, format=LOG_FORMAT)
LOGGER = logging.getLogger(__name__)


def create_app():
    sessions._init()
    options._init()

    LOGGER.info("Starting Configuration Framework Service API server")
    app = connexion.App(__name__, specification_dir='./openapi/')
    app.app.json_encoder = encoder.JSONEncoder
    app.add_api('openapi.yaml', arguments={'title': 'Configuration Framework Service'},
                pythonic_params=True, base_path='/')
    return app


app = create_app()

if __name__ == '__main__':
    app.run()
