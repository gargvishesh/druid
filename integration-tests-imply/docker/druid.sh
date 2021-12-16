#!/bin/bash

#
# Copyright (c) Imply Data, Inc. All rights reserved.
#
# This software is the confidential and proprietary information
# of Imply Data, Inc. You shall not disclose such Confidential
# Information and shall use it only in accordance with the terms
# of the license agreement you entered into with Imply.
#

set -e

# this file is a copy of the other one, but modified to also propagate config values prefixed with imply
# e.g. imply_foo_bar -> imply.foo.bar
# and will replace the other druid.sh when building the docker container

getConfPath()
{
    cluster_conf_base=/tmp/conf/druid/cluster
    case "$1" in
    _common) echo $cluster_conf_base/_common ;;
    historical) echo $cluster_conf_base/data/historical ;;
    historical-for-query-retry-test) echo $cluster_conf_base/data/historical ;;
    middleManager) echo $cluster_conf_base/data/middleManager ;;
    indexer) echo $cluster_conf_base/data/indexer ;;
    coordinator) echo $cluster_conf_base/master/coordinator ;;
    broker) echo $cluster_conf_base/query/broker ;;
    router) echo $cluster_conf_base/query/router ;;
    overlord) echo $cluster_conf_base/master/overlord ;;
    *) echo $cluster_conf_base/misc/$1 ;;
    esac
}

# Delete the old key (if existing) and append new key=value
setKey()
{
    service="$1"
    key="$2"
    value="$3"
    service_conf=$(getConfPath $service)/runtime.properties
    # Delete from all
    sed -ri "/$key=/d" $COMMON_CONF_DIR/common.runtime.properties
    [ -f $service_conf ] && sed -ri "/$key=/d" $service_conf
    [ -f $service_conf ] && echo "$key=$value" >>$service_conf
    [ -f $service_conf ] || echo "$key=$value" >>$COMMON_CONF_DIR/common.runtime.properties

    echo "Setting $key=$value in $service_conf"
}

setupConfig()
{
  echo "$(date -Is) configuring service $DRUID_SERVICE"

  # We put all the config in /tmp/conf to allow for a
  # read-only root filesystem
  mkdir -p /tmp/conf/druid

  COMMON_CONF_DIR=$(getConfPath _common)
  SERVICE_CONF_DIR=$(getConfPath ${DRUID_SERVICE})

  mkdir -p $COMMON_CONF_DIR
  mkdir -p $SERVICE_CONF_DIR
  touch $COMMON_CONF_DIR/common.runtime.properties
  touch $SERVICE_CONF_DIR/runtime.properties

  setKey $DRUID_SERVICE druid.host $(resolveip -s $HOSTNAME)
  setKey $DRUID_SERVICE druid.worker.ip $(resolveip -s $HOSTNAME)

  # Write out all the environment variables starting with druid_ to druid service config file
  # This will replace _ with . in the key
  env | grep ^druid_ | while read evar;
  do
      # Can't use IFS='=' to parse since var might have = in it (e.g. password)
      val=$(echo "$evar" | sed -e 's?[^=]*=??')
      var=$(echo "$evar" | sed -e 's?^\([^=]*\)=.*?\1?g' -e 's?_?.?g')
      setKey $DRUID_SERVICE "$var" "$val"
  done

  # Write out all the environment variables starting with imply_ to druid service config file
  # This will replace _ with . in the key
  env | grep ^imply_ | while read evar;
  do
      # Can't use IFS='=' to parse since var might have = in it (e.g. password)
      val=$(echo "$evar" | sed -e 's?[^=]*=??')
      var=$(echo "$evar" | sed -e 's?^\([^=]*\)=.*?\1?g' -e 's?_?.?g')
      setKey $DRUID_SERVICE "$var" "$val"
  done
}

setupData()
{
  # The "query" and "security" test groups require data to be setup before running the tests.
  # In particular, they requires segments to be download from a pre-existing s3 bucket.
  # This is done by using the loadSpec put into metadatastore and s3 credientials set below.
  if [ "$DRUID_INTEGRATION_TEST_GROUP" = "query" ] || [ "$DRUID_INTEGRATION_TEST_GROUP" = "query-retry" ] || [ "$DRUID_INTEGRATION_TEST_GROUP" = "high-availability" ] || [ "$DRUID_INTEGRATION_TEST_GROUP" = "security" ] || [ "$DRUID_INTEGRATION_TEST_GROUP" = "ldap-security" ] || [ "$DRUID_INTEGRATION_TEST_GROUP" = "keycloak-security" ]; then
    # touch is needed because OverlayFS's copy-up operation breaks POSIX standards. See https://github.com/docker/for-linux/issues/72.
    find /var/lib/mysql -type f -exec touch {} \; && service mysql start \
      && cat /test-data/${DRUID_INTEGRATION_TEST_GROUP}-sample-data.sql | mysql -u root druid && /etc/init.d/mysql stop
    # below s3 credentials needed to access the pre-existing s3 bucket
    setKey $DRUID_SERVICE druid.s3.accessKey AKIAT2GGLKKJQCMG64V4
    setKey $DRUID_SERVICE druid.s3.secretKey HwcqHFaxC7bXMO7K6NdCwAdvq0tcPtHJP3snZ2tR
    # The region of the sample data s3 blobs needed for these test groups
    export AWS_REGION=us-east-1
  fi

  if [ "$DRUID_INTEGRATION_TEST_GROUP" = "query-retry" ]; then
    setKey $DRUID_SERVICE druid.extensions.loadList [\"druid-s3-extensions\",\"druid-integration-tests\"]
  elif [ "$DRUID_INTEGRATION_TEST_GROUP" = "virtual-segments" ] && [ "$DRUID_SERVICE" = "historical" ]; then
    setKey $DRUID_SERVICE druid.extensions.loadList [\"imply-virtual-segments\",\"druid-s3-extensions\"]
    export AWS_REGION=us-east-1
  elif [ "$DRUID_INTEGRATION_TEST_GROUP" = "async-download" ]; then
    setKey $DRUID_SERVICE druid.extensions.loadList [\"imply-sql-async\"]
  elif [ "$DRUID_INTEGRATION_TEST_GROUP" = "imply-s3" ]; then
    setKey $DRUID_SERVICE druid.extensions.loadList [\"imply-sql-async\",\"druid-s3-extensions\"]
  else
    if [ "$DRUID_INTEGRATION_TEST_GROUP" = "keycloak-security" ]; then
      setKey $DRUID_SERVICE druid.extensions.loadList [\"druid-s3-extensions\",\"imply-keycloak\"]
      ########################################
      setKey $DRUID_SERVICE "druid.keycloak.auth-server-url" "http://imply-keycloak:8080/auth"
      setKey $DRUID_SERVICE "druid.keycloak.bearer-only" "true"
      setKey $DRUID_SERVICE "druid.keycloak.ssl-required" "NONE"
      setKey $DRUID_SERVICE "druid.keycloak.verify-token-audience" "true"
      setKey $DRUID_SERVICE "druid.keycloak.use-resource-role-mappings" "true"
      setKey $DRUID_SERVICE "druid.escalator.keycloak.auth-server-url" "http://imply-keycloak:8080/auth"
      setKey $DRUID_SERVICE "druid.escalator.keycloak.ssl-required" "NONE"
      setKey $DRUID_SERVICE "druid.escalator.keycloak.credentials" "{\"secret\" : \"secret\"}"
      setKey $DRUID_SERVICE "druid.escalator.keycloak.verify-token-audience" "true"
    else
      setKey $DRUID_SERVICE druid.extensions.loadList [\"druid-s3-extensions\"]
      export AWS_REGION=us-east-1
    fi
  fi

  # The SqlInputSource tests in the "input-source" test group require data to be setup in MySQL before running the tests.
  if [ "$DRUID_INTEGRATION_TEST_GROUP" = "input-source" ] ; then
    # touch is needed because OverlayFS's copy-up operation breaks POSIX standards. See https://github.com/docker/for-linux/issues/72.
    find /var/lib/mysql -type f -exec touch {} \; && service mysql start \
        && echo "GRANT ALL ON sqlinputsource.* TO 'druid'@'%'; CREATE database sqlinputsource DEFAULT CHARACTER SET utf8mb4;" | mysql -u root druid \
        && cat /test-data/sql-input-source-sample-data.sql | mysql -u root druid \
        && /etc/init.d/mysql stop
  fi
}

