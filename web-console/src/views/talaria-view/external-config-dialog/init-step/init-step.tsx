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

import { Card } from '@blueprintjs/core';
import classNames from 'classnames';
import React from 'react';

import { getIngestionImage, getIngestionTitle, InputSource } from '../../../../druid-models';
import { UrlBaser } from '../../../../singletons';

import './init-step.scss';

export interface InitStepProps {
  onSet(inputSource: InputSource): void;
}

export const InitStep = React.memo(function InitStep(props: InitStepProps) {
  const { onSet } = props;

  function renderIngestionCard(type: string): JSX.Element | undefined {
    return (
      <Card
        className={classNames({ disabled: false })}
        interactive
        elevation={1}
        onClick={() => {
          onSet(
            type === 'example'
              ? {
                  type: 'http',
                  uris: ['https://static.imply.io/data/kttm/kttm-2019-08-25.json.gz'],
                }
              : { type },
          );
        }}
      >
        <img
          src={UrlBaser.base(`/assets/${getIngestionImage(type as any)}.png`)}
          alt={`Ingestion tile for ${type}`}
        />
        <p>
          {getIngestionTitle(type === 'example' ? 'example' : (`index_parallel:${type}` as any))}
        </p>
      </Card>
    );
  }

  return (
    <div className="init-step">
      {renderIngestionCard('s3')}
      {renderIngestionCard('azure')}
      {renderIngestionCard('google')}
      {renderIngestionCard('hdfs')}
      {renderIngestionCard('http')}
      {renderIngestionCard('local')}
      {renderIngestionCard('inline')}
      {renderIngestionCard('example')}
    </div>
  );
});
