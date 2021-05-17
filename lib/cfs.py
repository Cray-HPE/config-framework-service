#!/usr/bin/env python
# Copyright 2019, 2021 Hewlett Packard Enterprise Development LP
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
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# (MIT License)

"""
The purpose of this ansible module is to invoke CFS' image customization on
an image that has been established and uploaded into IMS.

This module is designed to primarily work with Python3; Python2 functionality is
not tested nor directly supported.
"""

from ansible.module_utils.basic import AnsibleModule
from logging.handlers import BufferingHandler
from collections import defaultdict
from base64 import decodestring

import logging
import os
import oauthlib.oauth2
import requests
import requests_oauthlib
import uuid
import time
import subprocess

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

PROTOCOL = 'https'
ORGANIZATION = 'cray'
API_GW_DNSNAME = 'api-gw-service-nmn.local'
IMS_URL_DEFAULT = "{}://{}/apis/ims".format(PROTOCOL, API_GW_DNSNAME)
ARS_URL_DEFAULT = "{}://{}/apis/ars".format(PROTOCOL, API_GW_DNSNAME)
TOKEN_URL_DEFAULT = "{}://{}/keycloak/realms/shasta/protocol/openid-connect/token".format(PROTOCOL, API_GW_DNSNAME)
CFS_ENDPOINT = '%s://%s/apis/cfs/sessions' % (PROTOCOL, API_GW_DNSNAME)
CFS_REPO_DEFAULT = "%s://%s/vcs/cray/config-management.git" % (PROTOCOL, API_GW_DNSNAME)
CFS_REPO_BRANCH_DEFAULT = "master"
CFS_TARGET_DEF_DEFAULT = 'image'
# The default amount of time, in seconds, to wait for CFS to complete
CFS_TIMEOUT_DEFAULT = 1200
CFS_CHECK_INTERVAL = 2
VERIFY = False
OAUTH_CLIENT_ID_DEFAULT = "admin-client"
CERT_PATH_DEFAULT = "/var/opt/cray/certificate_authority/certificate_authority.crt"
LOG_DIR = '/var/opt/%s/log' %(ORGANIZATION)
LOG_FILE = os.path.join(LOG_DIR, 'cfs.log')
LOGGER = logging.getLogger(__file__)

ANSIBLE_METADATA = {
    'metadata_version': '2.8',
    'status': ['preview', 'stableinterface'],
    'supported_by': 'community'
}

DOCUMENTATION = '''
---
module: cfs

short_description: This module invokes CFS

version_added: "2.8"

description:
    - Applies configuration information to one or more images from IMS
    - Designed to be used during initial install
    - Designed to be invoked from outside of the management plane cluster
    - Removes CFS session on successful completion

options:
    repo:
        required: False
        type: string
        default: {CFS_REPO_DEFAULT}
    branch:
        required: False
        type: string
        default: {CFS_REPO_BRANCH_DEFAULT}
    target_def:
        required: False
        type: string
        default: {CFS_TARGET_DEF_DEFAULT}
    cleanup:
        required: False
        default: True
    timeout:
        description: The amount of time, in seconds, to wait for the operation to complete.
        required: False
        default: {CFS_TIMEOUT_DEFAULT}
        type: int
    check_interval:
        required: False
        type: int
        default: {CFS_CHECK_INTERVAL}
    groups:
        required: True
        <group name>:
            type; List
            description: A list of IMS UUIDs
            requirements: A non-zero list of elements
    token-url:
        required: False
        type: String
        default: {TOKEN_URL_DEFAULT}
    oath-client-id:
        required: False
        type: String
        default: {OAUTH_CLIENT_ID_DEFAULT}
    oath-client-secret
        required: False
        type: String
        default': ''
    certificate
        required: False
        type: String
        default: {CERT_PATH_DEFAULT}

'''.format(CFS_REPO_DEFAULT=CFS_REPO_DEFAULT,
           CFS_REPO_BRANCH_DEFAULT=CFS_REPO_BRANCH_DEFAULT,
           CFS_TARGET_DEF_DEFAULT=CFS_TARGET_DEF_DEFAULT,
           CFS_TIMEOUT_DEFAULT=CFS_TIMEOUT_DEFAULT,
           CFS_CHECK_INTERVAL=CFS_CHECK_INTERVAL,
           TOKEN_URL_DEFAULT=TOKEN_URL_DEFAULT,
           OAUTH_CLIENT_ID_DEFAULT=OAUTH_CLIENT_ID_DEFAULT,
           CERT_PATH_DEFAULT=CERT_PATH_DEFAULT)


EXAMPLES = '''
# Apply Image Customization to an existing IMS image
- name: Perform Image Customization via cfs_image_customization (Single Image, Single Groups)
  cfs:
    branch: cray/cme-premium-7.9.1
    groups:
        compute:
            - 9bd1c8f0-066c-4316-b660-b13851f18554
  register: result

- name: Perform Image Customization via cfs_image_customization (Single Image, Multiple Groups)
  cfs:
    branch: cray/cme-premium-7.9.1
    groups:
        compute:
            - 9bd1c8f0-066c-4316-b660-b13851f18554
        uan:
            - 9bd1c8f0-066c-4316-b660-b13851f18554
  register: result

- name: Perform Image Customization via cfs_image_customization (Multiple Image, Multiple Groups)
  cfs:
    branch: cray/cme-premium-7.9.1
    groups:
        compute:
            - 9bd1c8f0-066c-4316-b660-b13851f18554
            - eeeeeeee-066c-4316-b660-b13851f18554
        uan:
            - 9bd1c8f0-066c-4316-b660-b13851f18554
            - eeeeeeee-066c-4316-b660-b13851f18554
  register: result

'''

RETURN = '''
artifacts:
  b9aca16f-af8a-4d06-8de6-a458fa8997c1: b889f8b2-fece-4d54-a725-a86858fd8c94
changed: true
failed: false
msg: Full log of transaction available on target system within '/var/opt/cray/log/cfs.log'
params:
  branch: master
  check_interval: 2
  cleanup: false
  groups:
    computes:
    - b9aca16f-af8a-4d06-8de6-a458fa8997c1
  repo: https://api-gw-service-nmn.local/vcs/cray/config-management.git
  target_def: image
  timeout: 1200
stdout: |-
  Module Instantiated.
  Creating new CFS session
  Requesting configuration of {'name': 'computes', 'members': ['b9aca16f-af8a-4d06-8de6-a458fa8997c1']}
  Call for creation of CFS session with body: {'cloneUrl': 'https://api-gw-service-nmn.local/vcs/cray/config-management.git', 'name': '2a2d8b9c-9910-11e9-bb4c-a4bf0138e991', 'branch': 'master', 'target': {'definition': 'image', 'groups': [{'name': 'computes', 'members': ['b9aca16f-af8a-4d06-8de6-a458fa8997c1']}]}} to https://api-gw-service-nmn.local/apis/cfs/sessions
  CFS Session Submitted without issue.
  Waiting for completion of CFS Session '2a2d8b9c-9910-11e9-bb4c-a4bf0138e991'
  Session completed after 781 seconds
stdout_lines:
- Module Instantiated.
- Creating new CFS session
- 'Requesting configuration of {''name'': ''computes'', ''members'': [''b9aca16f-af8a-4d06-8de6-a458fa8997c1'']}'
- 'Call for creation of CFS session with body: {''cloneUrl'': ''https://api-gw-service-nmn.local/vcs/cray/config-management.git'', ''name'': ''2a2d8b9c-9910-11e9-bb4c-a4bf0138e991'', ''branch'': ''master'', ''target'': {''definition'': ''image'', ''groups'': [{''name'': ''computes'', ''members'': [''b9aca16f-af8a-4d06-8de6-a458fa8997c1'']}]}} to https://api-gw-service-nmn.local/apis/cfs/sessions'
- CFS Session Submitted without issue.
- Waiting for completion of CFS Session '2a2d8b9c-9910-11e9-bb4c-a4bf0138e991'
- Session completed after 781 seconds
'''


class TimeoutException(ValueError):
    """
    Raised when an action took longer than we were comfortable waiting for it
    to complete.
    """
    pass

class FlushlessBufferingHandler(BufferingHandler):
    """
    This is a BufferingHandler that never flushes to disk.
    """
    def shouldFlush(self, record):
        return False

    @property
    def as_stream(self):
        """
        Returns the contents of its buffer as if replayed as a stream buffer.
        """
        return '\n'.join([record.getMessage() for record in self.buffer])


class CFSSessionClient(object):
    """
    Represents a call to create a CFS Session and allows subsequent interaction
    and pull-forward of results from the session.
    """
    def __init__(self, repo, branch, target_def, groups,
                 oauth_client_id, oauth_client_secret, cert, token_url,
                 timeout=CFS_TIMEOUT_DEFAULT, interval=CFS_CHECK_INTERVAL):
        LOGGER.debug("Creating new CFS session")
        # Create a unique ID to use throughout the health of the session
        self.name = str(uuid.uuid1())
        self.repo = repo
        self.branch = branch
        self.target_def = target_def
        self.groups = groups
        self.timeout = timeout
        self.interval = interval
        self.req_session = self._create_session(
            oauth_client_id, oauth_client_secret, cert, token_url, timeout)

        # Create a new session from what we've been given
        body = {'name': self.name,
                'cloneUrl': self.repo,
                'branch': self.branch,
                'target': {'definition': self.target_def,
                            'groups': []}}

        # Following the spec, groups is a list, containing a list of members, e.g.
        # ['foo', ['bar', 'baz']]. We could pass this in blindly to CFS, but
        # we want to iterate over it so that we can log the contents of our call
        # for visibility
        for group, members in self.groups.iteritems():
            group_obj = {'name': group,
                         'members': members}
            body['target']['groups'].append(group_obj)
            LOGGER.debug("Requesting configuration of %s", group_obj)

        # Capture what we requested
        LOGGER.info("Call for creation of CFS session with body: %s to %s", body, CFS_ENDPOINT)

        # Complete the call to create a Session
        response = self.req_session.post(CFS_ENDPOINT, json=body)
        response.raise_for_status()
        LOGGER.debug('CFS Session Submitted without issue.')

    @property
    def endpoint(self):
        return os.path.join(CFS_ENDPOINT, self.name)

    def __repr__(self):
        return "CFS Session '%s'" % (self.name)

    def delete(self):
        self.req_session.delete(self.endpoint, verify=VERIFY)

    @property
    def status(self):
        resp = self.req_session.get(self.endpoint)
        resp.raise_for_status()
        return resp.json()['status']

    @property
    def complete(self):
        """
        Returns True once all running processes have completed, otherwise
        returns False. Does not give an indication over the success or failure
        of a particular session.
        """
        try:
            return self.status['session']['status'] == 'complete'
        except KeyError:
            # Protects against an early edge case where the complete field isn't
            # yet created
            LOGGER.error("Complete field not ready yet")
            return False

    @property
    def succeeded(self):
        """
        Returns true when all expected operations have finished, otherwise returns
        false.
        """
        return self.status['session']['succeeded'] == 'true'

    @property
    def artifacts(self):
        """
        Returns the completed artifacts, as submitted, e.g.
            "artifacts": [
                          {
                            "image_id": "8eaa3b33-390f-42f2-a1af-7825367f15ee",
                            "result_id": "5913f6c0-3bb3-47a8-8856-073b2ca8c38c",
                            "type": "ims_customized_image"
                          }
                        ]
    """
        return self.status['artifacts']

    @property
    def job(self):
        return self.status['session']['job']

    def _create_session(self, oauth_client_id, oauth_client_secret, ssl_cert, token_url, timeout):
        oauth_client = oauthlib.oauth2.BackendApplicationClient(
            client_id=oauth_client_id)

        session = requests_oauthlib.OAuth2Session(
            client=oauth_client, auto_refresh_url=token_url,
            auto_refresh_kwargs={
                'client_id': oauth_client_id,
                'client_secret': oauth_client_secret,
            },
            token_updater=lambda t: None)

        session.verify = ssl_cert
        session.timeout = timeout

        session.fetch_token(
            token_url=token_url, client_id=oauth_client_id,
            client_secret=oauth_client_secret, timeout=500)

        # Creates a URL retry object and HTTP adapter to use with our session;
        # this allows us to interact with ARS in a more resilient manner
        retries = Retry(total=10, backoff_factor=2, status_forcelist=[502, 503, 504])
        session.mount(self.endpoint, HTTPAdapter(max_retries=retries))

        return session

    def wait_for_complete(self):
        """
        Waits until self.completed is true, up until self.timeout seconds has
        elapsed, checking once every self.interval seconds.
        """
        start_time = time.time()
        latest_time = start_time + self.timeout
        LOGGER.info("Waiting for completion of %s", self)
        while time.time() <= latest_time:
            if self.complete:
                LOGGER.info("Session completed after %i seconds" % (time.time() - start_time))
                return
            time.sleep(self.interval)
        raise TimeoutException("Waited > %i seconds for %s to finish. " % (self.timeout, self))



class CFSModule(AnsibleModule):
    """
    This is the heart of the module responsible for interaction with CFS
    """
    IMPLEMENTED_TARGETS = frozenset(['image'])
    def __init__(self, fbh, *args, **kwargs):
        super(CFSModule, self).__init__(*args, **kwargs)
        self.fbh = fbh
        self.response = {'changed': False,
                         'failed': True,
                         'msg': "Full log of transaction available on target system within '%s'" %(LOG_FILE),
                         'artifacts': {},
                         'params': self.params,
                         'stdout': fbh.as_stream}
        if self.params['target_def'] not in self.IMPLEMENTED_TARGETS:
            LOGGER.critical("Target type '%s' not implemented.", self.params['target_def'])
            self.exit_json(**self.response)

        self.populate_oath_client_secret()

        self.cfs_session = None
        self._src_images = None
        LOGGER.debug("Module Instantiated.")

    @property
    def src_images(self):
        """
        Creates an image to groups mapping from given parameters. This is used
        to translate requested image types from their original image ID to their
        resultant image ID that comes back from IMS.
        """
        if self._src_images:
            return self._src_images
        self._src_images = defaultdict(set)
        for group_name, image_list in self.params['groups'].iteritems():
            for image_name in image_list:
                self._src_images[image_name].add(group_name)
        return self._src_images

    def attach_artifacts(self):
        """
        Maps called IMS target "groups" to their corresponding defined images,
        using the source_image ID field. This allows end clients (ansible tasks)
        to reference artifact results via:
        result.artifacts.<source image id> == "IMS image result"

        As a result, we would expect to find the IMS configured image ID at:
        results.artifacts['8eaa3b33-390f-42f2-a1af-7825367f15ee]
        """
        for artifact in self.cfs_session.artifacts:
            source_image_id = artifact['image_id']
            resultant_image_id = artifact['result_id']
            self.response['artifacts'][source_image_id] = resultant_image_id

    def health_check_ars(self):
        LOGGER.info("Waiting for ARS to be healthy...")
        endpoint = '%s/artifacts' % (ARS_URL_DEFAULT)
        while True:
            response = requests.get(endpoint)
            if response.ok:
                LOGGER.info("ARS nominal response on '%s'" % (endpoint))
                return
            else:
                time.sleep(1)

    def health_check_ims(self):
        LOGGER.info("Waiting for IMS to be healthy...")
        endpoint = '%s/image-artifacts' % (IMS_URL_DEFAULT)
        while True:
            response = requests.get(endpoint)
            if response.ok:
                LOGGER.info("IMS nominal response on '%s'" % (endpoint))
                return
            else:
                time.sleep(1)

    def health_check_cfs(self):
        LOGGER.info("Waiting for CFS to be healthy...")
        while True:
            response = self.cfs_session.get(CFS_ENDPOINT)
            if response.ok:
                LOGGER.info("CFS nominal response on '%s'" % (CFS_ENDPOINT))
                return
            else:
                time.sleep(1)

    def api_health_checks(self):
        """
        Blocks and waits for required API endpoints to respond with a known good
        response; this ensures proper ordering of actions during install, which
        can come online asynchronously.
        """
        self.health_check_ars()
        self.health_check_ims()
        self.health_check_cfs()

    def __call__(self):
        try:
            self.response['changed'] = True
            self.cfs_session = CFSSessionClient(self.params['repo'],
                                                self.params['branch'],
                                                self.params['target_def'],
                                                self.params['groups'],
                                                self.params['oath-client-id'],
                                                self.params['oath-client-secret'],
                                                self.params['certificate'],
                                                self.params['token-url'],
                                                self.params['timeout'],
                                                self.params['check_interval'])
            self.cfs_session.wait_for_complete()
            self.response['failed'] = not self.cfs_session.succeeded
            # Attach artifacts from CFS
            self.attach_artifacts()
            self.response['stdout'] = fbh.as_stream
        except TimeoutException as te:
            LOGGER.critical("Operation did not complete within configured timeout: {}".format(te))
            self.response['failed'] = True
            self.response['stderr'] = fbh.as_stream
        except Exception as e:
            LOGGER.critical("An unexpected exception occurred: {}".format(e), exc_info=True)
            self.response['failed'] = True
            self.response['stderr'] = fbh.as_stream
        finally:
            try:
                if self.params['cleanup']:
                    self.cfs_session.delete()
            except Exception as e:
                LOGGER.warn("Unable clean up %s: %s", self.cfs_session, e)
        self.exit_json(**self.response)

    def populate_oath_client_secret(self):
        """
        Talk with kubernetes and obtain the client secret; this only works if the
        remote execution target allows such interactions; otherwise specify the
        oath-client-secret value in the call to this module.
        """
        if self.params['oath-client-secret']:
            return
        stdout = subprocess.check_output(['kubectl', 'get', 'secrets', 'admin-client-auth', "-ojsonpath='{.data.client-secret}"])
        self.params['oath-client-secret'] = decodestring(stdout.strip())


def main(fbh):
    fields = {'repo': {'required': False, "type": "str", 'default': CFS_REPO_DEFAULT},
              'branch': {'required': False, "type": "str", 'default': CFS_REPO_BRANCH_DEFAULT},
              'cleanup': {'required': False, "type": "bool", 'default': True},
              'groups': {'required': True, "type": "dict"},
              'target_def': {'required': False, "type": "str", 'default': CFS_TARGET_DEF_DEFAULT},
              'timeout': {'required': False, 'type': 'int', 'default': CFS_TIMEOUT_DEFAULT},
              'check_interval': {'required': False, 'type': 'int', 'default': CFS_CHECK_INTERVAL},

              # Authentication Information
              'token-url': {'required': False, "type": 'str', 'default': TOKEN_URL_DEFAULT},
              'oath-client-id': {'required': False, "type": "str", 'default': OAUTH_CLIENT_ID_DEFAULT},
              'oath-client-secret': {'required': False, "type": 'str', 'default': ''},
              'certificate': {'required': False, "type": "str", "default": CERT_PATH_DEFAULT}}
    module = CFSModule(fbh, argument_spec=fields, supports_check_mode=False)
    try:
        module()
    except Exception as e:
        module.response['stderr'] = str(e)
        module.fail_json(**module.response)


if __name__ == '__main__':
    LOGGER.setLevel(logging.DEBUG)
    # Create the logging directory if need be.
    try:
        os.makedirs(LOG_DIR)
    except OSError as ose:
        if ose.errno != 17:
            raise
    _fh = logging.FileHandler(LOG_FILE)
    _fh.setLevel(logging.DEBUG)
    LOGGER.addHandler(_fh)
    fbh = FlushlessBufferingHandler(4096)
    fbh.setLevel(logging.DEBUG)
    LOGGER.addHandler(fbh)
    main(fbh)

