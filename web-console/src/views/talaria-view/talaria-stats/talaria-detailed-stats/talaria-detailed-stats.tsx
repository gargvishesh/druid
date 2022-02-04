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

import React, { useState } from 'react';

import { BracedText } from '../../../../components';
import { SimpleCounter } from '../../../../talaria-models';
import { formatBytes, formatInteger, pluralIfNeeded } from '../../../../utils';

import './talaria-detailed-stats.scss';

const formatRows = formatInteger;
const formatSize = (bytes: number) => `(${formatBytes(bytes)})`;
const formatFrames = formatInteger;

interface TalariaDetailedStatsProps {
  title: string;
  labelPrefix: string;
  entries: SimpleCounter[];
  inProgress: boolean;
}

export function TalariaDetailedStats(props: TalariaDetailedStatsProps) {
  const { title, labelPrefix, entries, inProgress } = props;
  const [expands, setExpands] = useState(0);
  const numberToShow = 10 * Math.pow(2, expands);
  const numHidden = entries.length - numberToShow * 2;

  let topEntries: SimpleCounter[];
  let bottomEntries: SimpleCounter[] | undefined;
  let shownEntries: SimpleCounter[];
  if (numHidden <= 0) {
    topEntries = entries;
    shownEntries = entries;
  } else {
    topEntries = entries.slice(0, numberToShow);
    bottomEntries = entries.slice(entries.length - numberToShow);
    shownEntries = topEntries.concat(bottomEntries);
  }

  const rowBraces = shownEntries.map(e => formatRows(e.rows));
  const byteBraces = shownEntries.map(e => formatSize(e.bytes));

  function renderEntry(e: SimpleCounter) {
    return (
      <div
        className="entry"
        key={e.index}
        title={`${inProgress ? 'In progress...\n' : ''}Frames: ${formatFrames(e.frames)}`}
      >
        <div className="label">{`${labelPrefix}${e.index + 1}${inProgress ? '*' : ''}`}</div>
        <BracedText text={formatRows(e.rows)} braces={rowBraces} /> &nbsp;{' '}
        {e.bytes > 0 && <BracedText text={formatSize(e.bytes)} braces={byteBraces} />}
      </div>
    );
  }

  return (
    <div className="talaria-detailed-stats">
      <div className="counter-title">{title}</div>
      {topEntries.map(renderEntry)}
      {bottomEntries && (
        <>
          <div className="show-more" onClick={() => setExpands(expands + 1)}>{`${pluralIfNeeded(
            numHidden,
            'entry',
            'entries',
          )} hidden (click to show)`}</div>
          {bottomEntries.map(renderEntry)}
        </>
      )}
    </div>
  );
}
