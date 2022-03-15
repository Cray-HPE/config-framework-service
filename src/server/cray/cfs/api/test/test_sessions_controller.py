#
# MIT License
#
# (C) Copyright 2019, 2021-2022 Hewlett Packard Enterprise Development LP
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
# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from cray.cfs.api.models.config_framework_session import ConfigFrameworkSession  # noqa: E501
from cray.cfs.api.models.config_framework_session_create import ConfigFrameworkSessionCreate  # noqa: E501
from cray.cfs.api.models.problem_details import ProblemDetails  # noqa: E501
from cray.cfs.api.test import BaseTestCase


class TestSessionsController(BaseTestCase):
    """SessionsController integration test stubs"""

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
