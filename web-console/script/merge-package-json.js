#!/usr/bin/env node

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const fs = require('fs-extra');
const path = require('path');

function mergePackageVersions(version) {
  const packageJsons = [
    path.resolve(__dirname, '../package.json'),
    path.resolve(__dirname, '../package.dist.json'),
  ];
  const json = {
    ...packageJsons.reduce((merged, fileName) => ({ ...merged, ...fs.readJSONSync(fileName) }), {}),
    version,
  };
  fs.writeJSONSync(path.resolve(__dirname, '../dist/package.json'), json, { spaces: 2 });
}

if (process.argv.length !== 3) {
  console.error('Expected to receive the next version number as the only argument');
  process.exitCode = 1;
} else {
  mergePackageVersions(process.argv[2]);
}
