#!/usr/bin/env bash
#
# Copyright (c) Imply Data, Inc. All rights reserved.
#
# This software is the confidential and proprietary information
# of Imply Data, Inc. You shall not disclose such Confidential
# Information and shall use it only in accordance with the terms
# of the license agreement you entered into with Imply.
#

set -e

echo "copying extra imply dependencies"

java -cp "$SHARED_DIR/docker/lib/*" \
   -Ddruid.extensions.directory=$SHARED_DIR/docker/extensions \
   org.apache.druid.cli.Main tools pull-deps \
   -c io.imply:imply-keycloak:$DRUID_VERSION \
   -c org.apache.druid.extensions:imply-druid-security:$DRUID_VERSION \
   -c io.imply:imply-virtual-segments:$DRUID_VERSION \
   -c io.imply:imply-sql-async:$DRUID_VERSION \
   -c io.imply:imply-view-manager:$DRUID_VERSION \
   -c io.imply:indexed-table-loader:$DRUID_VERSION
