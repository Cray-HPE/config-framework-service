# Cray Configuration Framework Service

## Generating the server

The OpenAPI specification automatically generates server code as a function of
building the docker image, however, it may be desireable to generate the server code
while writing and testing code locally, outside of the docker image itself. This
is helpful when the openapi code in question generates stubbed content, to be later
filled in by the application developer.

_NOTE_: Generated code that does not have Cray authored additions should not be
checked in for this repository. The .gitignore file has patterns that match
generated code to help prevent this kind of check-in.

To manually update the server code into your local checkout, run the following command:

```
$ cd $REPO
$ ./regenerate-server.sh
```

## Testing

See cms-tools repo for details on running CT tests for this service.

## Versioning
Use [SemVer](http://semver.org/). The version is located in the [.version](.version) file. Other files either
read the version string from this file or have this version string written to them at build time
based on the information in the [update_versions.conf](update_versions.conf) file.

## Copyright and License
This project is copyrighted by Hewlett Packard Enterprise Development LP and is under the MIT
license. See the [LICENSE](LICENSE) file for details.

When making any modifications to a file that has a Cray/HPE copyright header, that header
must be updated to include the current year.

When creating any new files in this repo, if they contain source code, they must have
the HPE copyright and license text in their header, unless the file is covered under
someone else's copyright/license (in which case that should be in the header). For this
purpose, source code files include Dockerfiles, Ansible files, RPM spec files, and shell
scripts. It does **not** include Jenkinsfiles, OpenAPI/Swagger specs, or READMEs.

When in doubt, provided the file is not covered under someone else's copyright or license, then
it does not hurt to add ours to the header.

## Contributing

When making contributions, please update the `.version` file with the
appropriate SemVer version for the changes.
