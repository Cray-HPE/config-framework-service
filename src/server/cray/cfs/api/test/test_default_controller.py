# coding: utf-8
# Copyright 2019, Cray Inc.  All Rights Reserved.

from __future__ import absolute_import

from flask import json
from six import BytesIO

from cray.cfs.api.models.config_framework_session import ConfigFrameworkSession  # noqa: E501
from cray.cfs.api.models.config_framework_session_create import ConfigFrameworkSessionCreate  # noqa: E501
from cray.cfs.api.models.problem_details import ProblemDetails  # noqa: E501
from cray.cfs.api.test import BaseTestCase


class TestDefaultController(BaseTestCase):
    """DefaultController integration test stubs"""

    def test_create_session(self):
        """Test case for create_session

        Create a Config Framework Session
        """
        config_framework_session_create = ConfigFrameworkSessionCreate()
        response = self.client.open(
            '/sessions',
            method='POST',
            data=json.dumps(config_framework_session_create),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_session(self):
        """Test case for delete_session

        Delete Config Framework Session
        """
        response = self.client.open(
            '/sessions/{session_name}'.format(session_name='session_name_example'),
            method='DELETE')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_session(self):
        """Test case for get_session

        Config Framework Session Details
        """
        response = self.client.open(
            '/sessions/{session_name}'.format(session_name='session_name_example'),
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_sessions(self):
        """Test case for get_sessions

        List Config Framework Sessions
        """
        response = self.client.open(
            '/sessions',
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
