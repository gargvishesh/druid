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

import { Classes, Dialog } from '@blueprintjs/core';
import { QueryResult } from 'druid-query-toolkit';
import React from 'react';

import { Loader } from '../../../components';
import { useQueryManager } from '../../../hooks';
import { QueryOutput2 } from '../query-output2/query-output2';
import { getTalariaSummary } from '../talaria-utils';

import './talaria-results-dialog.scss';

const FN = () => {};

export interface TalariaResultsDialogProps {
  taskId: string;
  onClose: () => void;
}

export const TalariaResultsDialog = React.memo(function TalariaResultsDialog(
  props: TalariaResultsDialogProps,
) {
  const { taskId, onClose } = props;

  const [queryResultState] = useQueryManager<string, QueryResult>({
    processQuery: async (taskId: string) => {
      const report = await getTalariaSummary(taskId);
      if (!report) throw new Error(`could not get report`);

      if (report.result) {
        return report.result;
      } else {
        throw new Error(`No results on report.`);
      }
    },
    initQuery: taskId,
  });

  return (
    <Dialog className="talaria-results-dialog" isOpen onClose={onClose} title="Talaria results">
      <div className={Classes.DIALOG_BODY}>
        {queryResultState.isLoading() && <Loader />}
        {queryResultState.isError() && <div>{queryResultState.getErrorMessage()}</div>}
        {queryResultState.data && (
          <QueryOutput2 queryResult={queryResultState.data} onQueryAction={FN} runeMode={false} />
        )}
      </div>
    </Dialog>
  );
});
