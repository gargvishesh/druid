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

. $(dirname "$0")/../integration-tests/script/docker_compose_args.sh

DOCKERDIR=$(dirname "$0")/../integration-tests/docker

# Skip stopping docker if flag set (For use during development)
if [ -n "$DRUID_INTEGRATION_TEST_SKIP_RUN_DOCKER" ] && [ "$DRUID_INTEGRATION_TEST_SKIP_RUN_DOCKER" == true ]
then
  exit 0
fi


# stop hadoop container if it exists (can't use docker-compose down because it shares network)
HADOOP_CONTAINER="$(docker ps -aq -f name=druid-it-hadoop)"
if [ ! -z "$HADOOP_CONTAINER" ]
then
  docker stop druid-it-hadoop
  docker rm druid-it-hadoop
fi

# stop ingest service container if it exists
INGEST_CONTAINER="$(docker ps -aq -f name=imply-ingest-service)"
if [ ! -z "$INGEST_CONTAINER" ]
then
  docker stop imply-ingest-service
  docker rm imply-ingest-service
fi

# cleanup if keycloak-security group
if [ "$DRUID_INTEGRATION_TEST_GROUP" = "keycloak-security" ]
then
  docker-compose -f docker/docker-compose.keycloak-security-cluster.yml down
  docker-compose -f docker/docker-compose.keycloak-security-setup.yml down
  if [ ! -z "$(docker volume ls -q -f name=docker_keycloak_db_data)" ]
  then
    docker volume rm docker_keycloak_db_data
  fi
fi

# cleanup if virtual-segments group
if [ "$DRUID_INTEGRATION_TEST_GROUP" = "virtual-segments" ]
then
  docker-compose -f docker/docker-compose.virtual-segments.yml down
fi

# bring down using the same compose args we started with
if [ -z "$DRUID_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH" ]
then
  docker-compose $(getComposeArgs) down
else
  OVERRIDE_ENV=$DRUID_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH docker-compose $(getComposeArgs) down
fi

if [ ! -z "$(docker network ls -q -f name=druid-it-net)" ]
then
  docker network rm druid-it-net
fi
