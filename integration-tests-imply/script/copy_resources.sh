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
mvn -B dependency:copy-dependencies -DoutputDirectory=$SHARED_DIR/docker/lib

# this copies everything over again, so remove extensions that were moved
rm $SHARED_DIR/docker/lib/druid-s3-extensions-*
rm $SHARED_DIR/docker/lib/druid-azure-extensions-*
rm $SHARED_DIR/docker/lib/druid-google-extensions-*
rm $SHARED_DIR/docker/lib/druid-hdfs-storage-*
rm $SHARED_DIR/docker/lib/druid-kinesis-indexing-service-*

# move ingest service to extension
mkdir -p $SHARED_DIR/docker/extensions/ingest-service
mv $SHARED_DIR/docker/lib/ingest-service-* $SHARED_DIR/docker/extensions/ingest-service

# move imply keycloak to extension
mkdir -p $SHARED_DIR/docker/extensions/imply-keycloak
mv $SHARED_DIR/docker/lib/imply-keycloak-* $SHARED_DIR/docker/extensions/imply-keycloak
mv $SHARED_DIR/docker/lib/jboss-jaxrs-* $SHARED_DIR/docker/extensions/imply-keycloak
