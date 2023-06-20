# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

[Unreleased]
### Added
- V3 api with support for paging

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
