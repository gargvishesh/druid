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

# picks appropriate docker-compose argments to use when bringing up and down integration test clusters
# for a given test group
getComposeArgs()
{
  if [ "$DRUID_INTEGRATION_TEST_GROUP" = "keycloak-security" ]
  then
    echo "-f $IMPLYTESTDIR/docker/docker-compose.keycloak-security-cluster.yml"
  elif [ "$DRUID_INTEGRATION_TEST_GROUP" = "ldap-security" ]
  then
    echo "-f $IMPLYTESTDIR/docker/docker-compose.ldap-security.yml"
  elif [ "$DRUID_INTEGRATION_TEST_GROUP" = "virtual-segments" ]
  then
    echo "-f $IMPLYTESTDIR/docker/docker-compose.virtual-segments.yml"
  else
    echo "fall-back-to-asf-script"
  fi
}
