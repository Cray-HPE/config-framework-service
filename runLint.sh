#!/bin/bash

#
# Copyright 2019, Cray Inc.  All Rights Reserved.
#

# Very simple scanner for files missing copyrights

# Extensions to check
CODE_EXTENSIONS="py sh"

# Additional files to check, uses exact match
EXTRA_FILES="Dockerfile api/openapi.yaml"

WHITELIST_FILES="src/server/cray/cfs/api/models/config_framework_session.py
src/server/cray/cfs/api/models/config_framework_session_ansible.py
src/server/cray/cfs/api/models/config_framework_session_create.py
src/server/cray/cfs/api/models/config_framework_session_repo.py
src/server/cray/cfs/api/models/config_framework_session_status.py
src/server/cray/cfs/api/models/config_framework_session_status_artifacts.py
src/server/cray/cfs/api/models/config_framework_session_status_session.py
src/server/cray/cfs/api/models/config_framework_session_status_targets.py
src/server/cray/cfs/api/models/link.py
src/server/cray/cfs/api/models/problem_details.py
src/server/cray/cfs/api/models/target_spec_section.py
src/server/cray/cfs/api/models/target_spec_section_groups.py
src/server/cray/cfs/api/controllers/sessions_controller.py
src/server/README.md
src/server/test-requirements.txt
src/server/requirements.txt
src/server/cray/cfs/api/util.py
src/server/cray/cfs/api/typing_utils.py
src/server/cray/cfs/api/controllers/__init__.py
src/server/cray/cfs/api/controllers/security_controller_.py
src/server/cray/cfs/api/models/__init__.py
src/server/cray/cfs/api/models/base_model_.py
src/server/cray/cfs/api/openapi/openapi.yaml
src/server/.gitignore
src/server/Dockerfile
src/server/.dockerignore
src/server/setup.py
src/server/tox.ini
src/server/git_push.sh
src/server/.travis.yml
src/server/cray/cfs/api/encoder.py
src/server/cray/cfs/api/test/__init__.py
src/server/.openapi-generator/VERSION"

FAIL=0

function scan_file {
    echo -n "Scanning $1... "
    # skip empty files
    if [ -s $1 ]; then
        grep -q "Copyright" $1
        if [ $? -ne 0 ]; then
            echo "missing copyright headers"
            return 1
        fi
    fi
    echo "OK"
    return 0
}

function list_include_item {
  local list="$1"
  local item="$2"
  if [[ $list =~ (^|[[:space:]])"$item"($|[[:space:]]) ]] ; then
    # yes, list include item
    result=0
  else
    result=1
  fi
  return $result
}

# Scan extentions
for CE in ${CODE_EXTENSIONS}
do
    for F in `git ls-files "*.${CE}"`
    do
        if ! list_include_item "$WHITELIST_FILES" "$F"; then
            scan_file ${F}
            if [ $? -ne 0 ]; then
                FAIL=1
            fi
        fi
    done
done

# Do the listed extra files
for F in ${EXTRA_FILES}
do
    scan_file ${F}
done

if [ ${FAIL} -eq 0 ]; then
    echo "All scanned code passed"
else
    echo "Some code is missing copyright, see list above"
fi

exit ${FAIL}