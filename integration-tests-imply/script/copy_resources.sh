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

echo "copying extra imply dependencies"
mvn -B dependency:copy-dependencies -DoutputDirectory=$SHARED_DIR/docker/lib

# this copies everything over again, so remove extensions that were moved
rm $SHARED_DIR/docker/lib/druid-s3-extensions-*
rm $SHARED_DIR/docker/lib/druid-azure-extensions-*
rm $SHARED_DIR/docker/lib/druid-google-extensions-*
rm $SHARED_DIR/docker/lib/druid-hdfs-storage-*
rm $SHARED_DIR/docker/lib/druid-kinesis-indexing-service-*

