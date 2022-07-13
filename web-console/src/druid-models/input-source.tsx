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

import { nonEmptyArray } from '../utils';

export interface InputSource {
  type: string;
  baseDir?: string;
  filter?: any;
  uris?: string[];
  prefixes?: string[];
  objects?: { bucket: string; path: string }[];
  fetchTimeout?: number;

  // druid
  dataSource?: string;
  interval?: string;
  dimensions?: string[];
  metrics?: string[];
  maxInputSegmentBytesPerTask?: number;

  // inline
  data?: string;

  // hdfs
  paths?: string | string[];

  // http
  httpAuthenticationUsername?: any;
  httpAuthenticationPassword?: any;
}

export type InputSourceDesc =
  | {
      type: 'inline';
      data: string;
    }
  | {
      type: 'local';
      filter?: any;
      baseDir?: string;
      files?: string[];
    }
  | {
      type: 'druid';
      dataSource: string;
      interval: string;
      filter?: any;
      dimensions?: string[]; // ToDo: these are not in the docs https://druid.apache.org/docs/latest/ingestion/native-batch-input-sources.html
      metrics?: string[];
      maxInputSegmentBytesPerTask?: number;
    }
  | {
      type: 'http';
      uris: string[];
      httpAuthenticationUsername?: any;
      httpAuthenticationPassword?: any;
    }
  | {
      type: 's3';
      uris?: string[];
      prefixes?: string[];
      objects?: { bucket: string; path: string }[];
      properties?: {
        accessKeyId?: any;
        secretAccessKey?: any;
        assumeRoleArn?: any;
        assumeRoleExternalId?: any;
      };
    }
  | {
      type: 'google' | 'azure';
      uris?: string[];
      prefixes?: string[];
      objects?: { bucket: string; path: string }[];
    }
  | {
      type: 'hdfs';
      paths?: string | string[];
    }
  | {
      type: 'sql';
      database: any;
      foldCase?: boolean;
      sqls: string[];
    }
  | {
      type: 'combining';
      delegates: InputSource[];
    };

export function issueWithInputSource(inputSource: InputSource | undefined): string | undefined {
  if (!inputSource) return 'does not exist';
  if (!inputSource.type) return 'missing a type';
  switch (inputSource.type) {
    case 'local':
      if (!inputSource.baseDir) return `must have a 'baseDir'`;
      if (!inputSource.filter) return `must have a 'filter'`;
      return;

    case 'http':
      if (!nonEmptyArray(inputSource.uris)) {
        return 'must have at least one uri';
      }
      return;

    case 'druid':
      if (!inputSource.dataSource) return `must have a 'dataSource'`;
      if (!inputSource.interval) return `must have an 'interval'`;
      return;

    case 'inline':
      if (!inputSource.data) return `must have 'data'`;
      return;

    case 's3':
    case 'azure':
    case 'google':
      if (
        !nonEmptyArray(inputSource.uris) &&
        !nonEmptyArray(inputSource.prefixes) &&
        !nonEmptyArray(inputSource.objects)
      ) {
        return 'must have at least one uri or prefix or object';
      }
      return;

    case 'hdfs':
      if (!inputSource.paths) {
        return 'must have paths';
      }
      return;

    default:
      return;
  }
}
