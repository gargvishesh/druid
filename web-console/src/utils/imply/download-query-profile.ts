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

import * as JSONBig from 'json-bigint-native';

import { Api } from '../../singletons';
import { downloadFile } from '../general';

interface QueryProfile {
  profileVersion: number;
  queryStatus?: any;
  queryDetail?: any;
  serverStatus?: any;
}

export async function downloadQueryProfile(queryId: string) {
  const profile: QueryProfile = {
    profileVersion: 1,
  };

  try {
    profile.queryStatus = (
      await Api.instance.get(`/druid/v2/sql/async/${Api.encodePath(queryId)}/status`)
    ).data;
  } catch {}

  try {
    profile.queryDetail = (
      await Api.instance.get(`/druid/v2/sql/async/${Api.encodePath(queryId)}`)
    ).data;
  } catch {}

  try {
    profile.serverStatus = (await Api.instance.get(`/status`)).data;
  } catch {}

  downloadFile(JSONBig.stringify(profile, undefined, 2), 'json', `query_profile_${queryId}.json`);
}
