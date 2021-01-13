# Cray Configuration Framework Service

# License
Copyright 2019, Cray Inc. All rights reserved.


## Testing

### CT Tests
CFS CT tests can be found in /ct-tests

On a physical system, CMS tests can be found in /opt/cray/tests/crayctl-stage{NUMBER}/cms.
Please see https://connect.us.cray.com/confluence/display/DST/Stage+Tests+Guidelines for more details.

example: run CT test for CFS at crayctl stage 4
```
# /opt/cray/tests/crayctl-stage4/cms/cfs_stage4_ct_tests.sh or
# cmsdev test cfs --ct
```

Tests return 0 for success, 1 otherwise

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
