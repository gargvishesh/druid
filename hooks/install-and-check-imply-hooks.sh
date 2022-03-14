#!/bin/bash -eu

# Copyright (c) Imply Data, Inc. All rights reserved.
#
# This software is the confidential and proprietary information
# of Imply Data, Inc. You shall not disclose such Confidential
# Information and shall use it only in accordance with the terms
# of the license agreement you entered into with Imply.

DRUID_ROOT=$1

# This method does 2 things.
# - If the given file is not installed, installs it.
# - If the given file exists, checks the integrity of the file.
function exist_and_legit() {
  FILE_NAME=$1
  INSTALLED_FILE=${DRUID_ROOT}/.git/hooks/${FILE_NAME}
  FILE_IN_REPO=${DRUID_ROOT}/hooks/${FILE_NAME}

  if ! [[ -e "${FILE_IN_REPO}" ]]
    then
      echo "Cannot find ${FILE_IN_REPO}!"
      exit 1
  fi
  if ! [[ -e "${INSTALLED_FILE}" ]]
    then
      echo "Cannot find ${INSTALLED_FILE}. Installing the file."
      cp ${FILE_IN_REPO} ${INSTALLED_FILE}
  fi

  INSTALLED_FILE_CHECKSUM=`shasum -a512 ${INSTALLED_FILE} | cut -d ' ' -f1`
  FILE_IN_REPO_CHECKSUM=`shasum -a512 ${FILE_IN_REPO} | cut -d ' ' -f1`

  if [ ${INSTALLED_FILE_CHECKSUM} != ${FILE_IN_REPO_CHECKSUM} ]
  then
    echo "Corrupted file found: ${INSTALLED_FILE}"
    echo "If you haven't modified this file, perhaps you have your own git hooks installed that conflict to Imply Git hooks. See https://implydata.atlassian.net/wiki/spaces/ENG/pages/997621788/Druid+Development+Getting+Started#Installing-Git-hooks for how to install git hooks."
    echo "If you do, revert any change you made in ${INSTALLED_FILE}."
    exit 1
  fi
}

if [ $# != 1 ]
  then
    echo 'usage: program {$DRUID_ROOT}'
    exit 1
fi

exist_and_legit run-all-in-dir.py

# check pre-commit and pre-commits/validate_env.py

exist_and_legit pre-commit
mkdir -p ${DRUID_ROOT}/.git/hooks/pre-commits
exist_and_legit pre-commits/validate_env.py

# check pre-push and pre-pushes/validate_push_dest.py

exist_and_legit pre-push
mkdir -p ${DRUID_ROOT}/.git/hooks/pre-pushes
exist_and_legit pre-pushes/validate_push_dest.py