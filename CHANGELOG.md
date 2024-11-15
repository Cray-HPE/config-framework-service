# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Changed
- CASMCMS-9211: Improve performance of configuration delete operation.

## [1.23.4] - 11/15/2024
### Fixed
- CASMCMS-9208: Decode source name before restoring source data

## [1.23.3] - 11/15/2024
### Fixed
- CASMCMS-9207: Update API spec to reflect actual status code for successful POST to v3/sources/{source_id}

## [1.23.2] - 11/14/2024
### Fixed
- CASMCMS-9206: Update API spec to reflect that in v3, a layer requires exactly one of `clone_url` and `source`.

## [1.23.1] - 11/14/2024
### Added
- Added note to API spec indicating minimum CFS version to use new POST v3/sources/{source_id}

## [1.23.0] - 11/14/2024
### Added
- CASMCMS-9202: Add POST option to v3/sources/{source_id} endpoint to allow restoring a previous
  source by specifying its Vault secret name rather than a username/password.

### Fixed
- CASMCMS-9200: Make Options class thread-safe and prevent redundant initialization

## [1.22.0] - 11/13/2024
### Changed
- Do not make database call to look for configuration with null name
- CASMCMS-9197: Bypass needless code when listing configurations and sources

### Fixed
- CASMCMS-9196: Use the default value for `authentication_method` when creating a source, if the request does not specify it.
- CASMCMS-9198: Enforce same value restrictions on CFS options between v2 and v3 (for options that exist in both).

## [1.21.0] - 11/06/2024
### Fixed
- CASMCMS-9189: Two corrections to the CFS API spec
  - The spec indicated that PUT requests to the `/components` endpoints could specify either a dictionary or a list, just as with PATCH requests to those endpoints.
    However, the server code for the PUT endpoints only handled the list case. The API spec has been updated to reflect this reality.
  - The spec allowed for components whose ID fields were set to 0-length strings, which should never be the case. There are some cases where the schema should permit
    the field to be omitted entirely, but it should never be set to a 0-length string.

## [1.20.2] - 09/09/2024
### Changes
- Log installed Python packages in Dockerfile for purposes of build logging

### Dependencies
- CASMCMS-9138: Bump Python dependency versions to resolve CVEs
- Move to `openapi-generator-cli` v7.8.0
- Pin major/minor of Python dependencies but take latest patch version

## [1.20.1] - 09/05/2024
### Dependencies
- CSM 1.6 moved to Kubernetes 1.24, so use client v24.x to ensure compatability
- CASMCMS-9135: Bump minimum `cray-services` base chart version from 10.0.5 to 11.0.0

## [1.20.0] - 07/24/2024
### Changed
- Update API spec to reflect the actual requirements and format for the age/TTL fields.

## [1.19.7] - 06/28/2024
### Fixed
- Add missing pod `securityContext`

### Dependencies
- CASMCMS-9005: Bump minimum `cray-services` base chart version from 7.0.0 to 10.0.5

Bumped dependency versions to resolve CVEs

| Package                  | From       | To        |
|--------------------------|------------|-----------|
| `certifi`                | 2018.11.29 | 2023.7.22 |
| `urllib3`                | 1.25.11    | 1.26.19   |
| `requests`               | 2.22.0     | 2.31.0    |
| `idna`                   | 2.8        | 3.7       |
| `setuptools`             | unpinned   | 65.5.1    |

## [1.19.6] - 04/17/2024
### Fixed
- Fix broken `_matches_filter` call in `patch_v2_components_dict`.

## [1.19.5] - 04/17/2024
### Fixed
- Added missing `special_parameters` to `V3ConfigurationLayer` schema in API spec.

## [1.19.4] - 04/09/2024
### Dependencies
- Bump `connexion` from `2.6.0` to `2.14.2` to pick up bug fixes to prevent false schema errors being logged.
- Bump `Werkzeug` from `0.15.6` to `1.0.1` to meet `connexion` requirements.

## [1.19.3] - 04/05/2024
### Fixed
- Fix bug in `patch_v2_components_dict` to properly handle response from `DB.get_all()`

## [1.19.2] - 04/04/2024
### Fixed
- Corrected errors in the API spec to make it properly follow OAS 3.0.2 and to
  accurately reflect actual CFS behavior.

## [1.19.1] - 02/26/2024
### Fixed
- Fixed ARA link returned with session data

## [1.19.0] - 02/22/2024
### Dependencies
- Bump `kubernetes` from 11.0.0 to 22.6.0 to match CSM 1.6 Kubernetes version

## [1.18.0] - 01/12/2024
### Fixed
- Changed API behavior to match spec
  - v3 session create return status code 201 on success
  - v3 multi-session delete return status code 200 on success
- Changed spec to match API behavior
  - v2 configuration patch returns 404 if configuration not found
  - v3 configuration patch returns 404 if configuration not found
  - v2 session create return status code 200 on success
  - v2 sessions list returns 400 in case of some errors
  - v3 sessions list returns 400 in case of some errors
  - v3 source patch returns 404 if source not found
- Changed both spec and API behavior
  - v3 source creation now returns 201 on success, per convention
    for indicating successful creation of a new resource
- Corrected minor mistake in a code comment
- Fix bug in patch_all method in dbutils (use DB client, not Kubernetes Python module)
- Convert bytes to strings in patch_all and delete_all methods in dbutils, so
  they can be JSON serialized

## [1.17.2] - 12/08/2023
## Fixed
- Fixed branch updates with the v2 api
- Fixed server error when using on clone_url in v3

## [1.17.1] - 12/06/2023
## Fixed
- Fixed branch conversion for additional inventory in the configuration

## [1.17.0] - 10/18/2023
### Added
- Added sources to support cloning from external repos
- Added a drop_branches option when updating configurations

## [1.16.1] - 10/11/2023
## Fixed
- Fixed v2 session creation using the wrong configuration name.

## [1.16.0] - 9/29/2023
## Fixed
- Fixed v2 session creation with configuration names exceeding the v3 limit.

## [1.15.0] - 9/23/2023
### Added
- Added an ims_job field for session status

### Fixed
- Fixed component status handling for failed and incomplete layers

## [1.14.2] - 8/30/2023
### Fixed
- Fixed component id list filtering when used with paging
- Fixed the options migrations when cfs-api is upgraded to v3 far ahead of other CFS services
- Fixed the component definition to allow new components with no desired configuration

## [1.14.1] - 8/24/2023
### Fixed
- Updated the jsonschema dependecny to address a bug in openapi-schema-validator

## [1.14.0] - 8/18/2023
### Added
- V3 api with support for paging
- Additional debugging options including debug_on_failure
- ARA links in the component and session records

### Changed
- Disabled concurrent Jenkins builds on same branch/commit
- Added build timeout to avoid hung builds

### Dependencies
Bumped dependency versions
| Package                  | From    | To       |
|--------------------------|---------|----------|
| `adal`                   | 1.2.0   | 1.2.7    |
| `cffi`                   | 1.12.2  | 1.12.3   |
| `google-auth`            | 1.6.1   | 1.6.3    |
| `isort`                  | 4.3.16  | 4.3.21   |
| `Jinja2`                 | 2.10.1  | 2.10.3   |
| `openapi-spec-validator` | 0.2.4   | 0.2.10   |
| `pyasn1`                 | 0.4.4   | 0.4.8    |
| `pyasn1-modules`         | 0.2.2   | 0.2.8    |
| `PyJWT`                  | 1.7.0   | 1.7.1    |
| `python-dateutil`        | 2.6.0   | 2.6.1    |
| `rsa`                    | 4.7     | 4.7.2    |
| `typed-ast`              | 1.3.1   | 1.3.5    |
| `urllib3`                | 1.25.9  | 1.25.11  |
| `Werkzeug`               | 0.15.5  | 0.15.6   |
| `wrapt`                  | 1.11.1  | 1.11.2   |

## [1.13.2] - 7/21/2023
### Dependencies
- Bump `cryptography` from 2.6.1 to 41.0.2 to fix [Improper Certificate Validation CVE](https://security.snyk.io/vuln/SNYK-PYTHON-CRYPTOGRAPHY-5777683)

## [1.13.1] - 7/18/2023
### Dependencies
- Bumped `PyYAML` from 5.4.1 to 6.0.1 to avoid build issue caused by https://github.com/yaml/pyyaml/issues/601

## [1.13.0] - 6/27/2023
### Added
- Added a new configuration parameter for enabling DKMS in IMS

## [1.12.2] - 4/10/2023
### Changed
- Quadrupled the size the uwsgi buffer for the API.

## [1.12.1] - 1/19/2023
### Changed
- Language linting of description text fields in openapi spec file

## [1.12.0] - 1/12/2023
### Changed
- Restricted parameters for configurations and status filtering
- Enabled building of unstable artifacts
- Updated header of update_versions.conf to reflect new tool options

### Added
- Added new parameter for naming image customization results
- Added additional control options for batcher
- Added ability to bulk update components
- Added option to control CFS log levels
- Added description field for configurations
- Added version endpoints for the API

## [1.11.3] - 2022-12-20
### Added
- Add Artifactory authentication to Jenkinsfile

## [1.11.2] - 2022-08-19
### Fixed
- Spelling corrections.
- Updated Chart with correct image and chart version strings during builds.
- Modified version string placeholder tag in openapi.yaml to avoid unintentional string replacement during builds.

## [1.11.1] - 8/18/22
### Fixed
- Escalated pod priority so that configuration has a better chance of running when a node is cordoned

## [1.11.0] - 2022-07-27
### Added
- Conversion of repository to gitflow
