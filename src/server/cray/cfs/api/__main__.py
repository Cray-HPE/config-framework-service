#!/usr/bin/env python3
# Config Framework Service API Main
# Copyright 2019, Cray Inc. All Rights Reserved.
import os
import logging
import connexion

from cray.cfs.api import encoder
from cray.cfs.api.controllers import sessions

log_level = os.environ.get('LOG_LEVEL', 'WARN')
LOG_FORMAT = "%(asctime)-15s - %(levelname)-7s - %(name)s - %(message)s"
logging.basicConfig(level=log_level, format=LOG_FORMAT)
LOGGER = logging.getLogger(__name__)


def create_app():
    logging.basicConfig(level=logging.INFO)
    namespace = os.environ.get('NAMESPACE', 'services')
    sessions._init(namespace=namespace)

    LOGGER.info("Starting Configuration Framework Service API server")
    app = connexion.App(__name__, specification_dir='./openapi/')
    app.app.json_encoder = encoder.JSONEncoder
    app.add_api('openapi.yaml', arguments={'title': 'Configuration Framework Service'}, pythonic_params=True)
    return app

app = create_app()

if __name__ == '__main__':
    app.run()
