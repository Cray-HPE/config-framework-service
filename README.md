# Cray Configuration Framework Service

# License
Copyright 2019-2021 Hewlett Packard Enterprise Development LP

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
