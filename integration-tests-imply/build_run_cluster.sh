#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

echo $DRUID_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH

# this is a copy of other integration test build/run script
# (instead of running it directly in case we need to do anything special)
# still, some of those scripts are unchill, so change to that directory before doing stuff
IMPLYTESTDIR=$(cd $(dirname $0) && pwd)

export DIR=$IMPLYTESTDIR/../integration-tests
export HADOOP_DOCKER_DIR=$DIR/../examples/quickstart/tutorial/hadoop/docker
export DOCKERDIR=$DIR/docker
export SHARED_DIR=${HOME}/shared

# so docker IP addr will be known during docker build
echo ${DOCKER_IP:=127.0.0.1} > $DOCKERDIR/docker_ip

pushd .
if !($DRUID_INTEGRATION_TEST_SKIP_BUILD_DOCKER); then
  # copy standard integration test resources
  # change to integration-test directory t
  cd $DIR
  bash ./script/copy_resources.sh

  cd $IMPLYTESTDIR
  # copy imply test resources (needs to be in imply-integration-tests root for correct mvn command to run)
  bash ./script/copy_resources.sh
  cp ./docker/druid.sh $SHARED_DIR/docker/druid.sh
  cd $DIR
  bash ./script/docker_build_containers.sh
fi
popd

if !($DRUID_INTEGRATION_TEST_SKIP_RUN_DOCKER); then
  bash ${IMPLYTESTDIR}/stop_cluster.sh
  if [ "$DRUID_INTEGRATION_TEST_GROUP" = "query" ]
  then
    docker-compose -f $IMPLYTESTDIR/docker/docker-compose.query.yml up -d
  elif [ "$DRUID_INTEGRATION_TEST_GROUP" = "keycloak-security" ]
  then
    docker-compose -f $IMPLYTESTDIR/docker/docker-compose.keycloak-security-setup.yml up -d

    echo "sleeping for 2 minutes waiting for Keycloak container to be created"
    sleep 120
    echo "Waiting for keycloak apis to be ready"
    counter=0
    until [ $counter -eq 12 ] || curl localhost:8080; do
        sleep 10
        ((counter++))
    done
    [ $counter -lt 11 ]

    docker exec -e KEYCLOAK_NOT_BEFORE_API_VERSION=$KEYCLOAK_NOT_BEFORE_API_VERSION imply-keycloak /bin/bash -c '/tmp/setup.sh'

    docker-compose -f $IMPLYTESTDIR/docker/docker-compose.keycloak-security-cluster.yml up -d
  elif [ "$DRUID_INTEGRATION_TEST_GROUP" = "ldap-security" ]
  then
    docker-compose -f $IMPLYTESTDIR/docker/docker-compose.ldap-security.yml up -d
  elif [ "$DRUID_INTEGRATION_TEST_GROUP" = "async-download" ]
    then
      docker-compose -f $IMPLYTESTDIR/docker/docker-compose.async-download.yml up -d
  elif [ "$DRUID_INTEGRATION_TEST_GROUP" = "virtual-segments" ]
  then
    docker-compose -f $IMPLYTESTDIR/docker/docker-compose.virtual-segments.yml up -d
  else
    bash ${DIR}/script/docker_run_cluster.sh
  fi
fi

if ($DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER); then
  bash ./script/copy_hadoop_resources.sh
fi
