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

import { Intent, Label, ProgressBar } from '@blueprintjs/core';
import React from 'react';

import {
  currentStageIndex,
  overallProgress,
  StageDefinition,
  stageProgress,
} from '../../../talaria-models';

import './stage-progress.scss';

export interface StageProgressProps {
  reattach?: boolean;
  stages: StageDefinition[] | undefined;
  onCancel?(): void;
  onToggleLiveReports?(): void;
  showLiveReports?: boolean;
}

export const StageProgress = React.memo(function StageProgress(props: StageProgressProps) {
  const { reattach, stages, onCancel, onToggleLiveReports, showLiveReports } = props;

  const idx = currentStageIndex(stages);
  return (
    <div className="stage-progress">
      <Label>
        {reattach ? 'Reattaching...' : 'Running query...'}
        {onCancel && !reattach && (
          <>
            {' '}
            <span className="cancel" onClick={onCancel}>
              (cancel)
            </span>
          </>
        )}
      </Label>
      {!reattach && (
        <ProgressBar
          className="overall"
          key={stages ? 'actual' : 'pending'}
          intent={stages ? Intent.PRIMARY : undefined}
          value={stages ? overallProgress(stages) : undefined}
        />
      )}
      {stages && idx >= 0 && (
        <>
          <Label>{`Current stage (${idx + 1} of ${stages.length})`}</Label>
          <ProgressBar
            className="stage"
            stripes={false}
            value={stageProgress(stages[idx], stages)}
          />
          {onToggleLiveReports && (
            <Label className="toggle-live-reports" onClick={onToggleLiveReports}>
              {showLiveReports ? 'Hide live reports' : 'Show live reports'}
            </Label>
          )}
        </>
      )}
    </div>
  );
});
