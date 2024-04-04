# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.12.5] - 4/4/2024
### Fixed
- Corrected errors in the API spec to make it properly follow OAS 3.0.2 and to
  accurately reflect actual CFS behavior.

## [1.12.4] - 7/21/2023
### Dependencies
- Bump `cryptography` from 2.6.1 to 41.0.2 to fix [Improper Certificate Validation CVE](https://security.snyk.io/vuln/SNYK-PYTHON-CRYPTOGRAPHY-5777683)

## [1.12.3] - 7/18/2023
### Dependencies
- Bump `PyYAML` from 5.4.1 to 6.0.1 to avoid build issue caused by https://github.com/yaml/pyyaml/issues/601

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
