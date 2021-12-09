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

import {
  Button,
  ButtonGroup,
  Classes,
  Dialog,
  FormGroup,
  Intent,
  ProgressBar,
} from '@blueprintjs/core';
import { CancelToken } from 'axios';
import { QueryResult, QueryRunner, SqlQuery } from 'druid-query-toolkit';
import React, { useState } from 'react';

import { AutoForm, Field } from '../../../components';
import { useQueryManager } from '../../../hooks';
import { Api } from '../../../singletons';
import { TalariaQuery, TalariaSummary } from '../../../talaria-models';
import { downloadHref, DruidError, IntermediateQueryState } from '../../../utils';
import { getTalariaSummary, getTaskIdFromQueryResults, killTaskOnCancel } from '../talaria-utils';

import './export-dialog.scss';

type ResultFormat = 'arrayLines';

function resultFormatToExtension(resultFormat: ResultFormat): string {
  switch (resultFormat) {
    case 'arrayLines':
      return 'json';
  }
}

function addExtensionIfNeeded(filename: string, extension: string): string {
  const extensionWithDot = '.' + extension;
  if (filename.endsWith(extensionWithDot)) return filename;
  return filename + extensionWithDot;
}

interface AsyncDownloadParams {
  local?: boolean;
  filename?: string;
  resultFormat?: ResultFormat;
}

export const ASYNC_DOWNLOAD_FIELDS: Field<AsyncDownloadParams>[] = [
  {
    name: 'filename',
    type: 'string',
    defined: a => Boolean(a.local),
    required: true,
    info: <p>The filename of the file that will be downloaded</p>,
  },
  {
    name: 'resultFormat',
    label: 'Result format',
    type: 'string',
    suggestions: ['arrayLines'],
    required: true,
    info: <p>The format of the file that will be downloaded</p>,
  },
];

const queryRunner = new QueryRunner();

interface ExportDialogProps {
  talariaQuery: TalariaQuery;
  onClose: () => void;
}

export const ExportDialog = React.memo(function ExportDialog(props: ExportDialogProps) {
  const { talariaQuery, onClose } = props;

  const [asyncDownloadParams, setAsyncDownloadParams] = useState<AsyncDownloadParams>(() => {
    let filename = 'query';
    const parsedQuery = talariaQuery.getParsedQuery();
    if (parsedQuery instanceof SqlQuery) {
      filename = parsedQuery.getFirstTableName() || filename;
    }

    return {
      local: true,
      filename,
      resultFormat: 'arrayLines',
    };
  });

  const [downloadState, downloadQueryManager] = useQueryManager<
    AsyncDownloadParams,
    TalariaSummary,
    TalariaSummary
  >({
    processQuery: async (_asyncDownloadParams, cancelToken) => {
      const downloadQuery = talariaQuery.changeUnlimited(true).removeInsert();

      const { query, context, prefixLines } = downloadQuery.getEffectiveQueryAndContext();

      let result: QueryResult;
      try {
        result = await queryRunner.runQuery({
          query,
          extraQueryContext: {
            ...context,
            talariaDestination: asyncDownloadParams.local ? undefined : 'external',
          },
          cancelToken,
        });
      } catch (e) {
        throw new DruidError(e, prefixLines);
      }

      const taskId = getTaskIdFromQueryResults(result);
      if (!taskId) {
        throw new Error('Unexpected result, could not determine taskId');
      }

      killTaskOnCancel(taskId, cancelToken);

      return new IntermediateQueryState(TalariaSummary.init(taskId));
    },
    backgroundStatusCheck: async (
      currentSummary: TalariaSummary,
      asyncDownloadParams,
      cancelToken: CancelToken,
    ) => {
      let updatedSummary = currentSummary;

      if (updatedSummary.isWaitingForTask()) {
        const newSummary = await getTalariaSummary(currentSummary.taskId, cancelToken);
        updatedSummary = updatedSummary.updateWith(newSummary);
      }

      if (updatedSummary.isWaitingForTask()) {
        return new IntermediateQueryState(updatedSummary);
      } else {
        if (updatedSummary.error) {
          throw new Error(updatedSummary.getErrorMessage() || 'unexpected issue');
        }

        if (asyncDownloadParams.local) {
          downloadHref({
            href: `/druid/indexer/v1/task/${Api.encodePath(updatedSummary.taskId)}/reports`,
            filename: addExtensionIfNeeded(
              asyncDownloadParams.filename!,
              resultFormatToExtension(asyncDownloadParams.resultFormat!),
            ),
          });
        }

        return updatedSummary;
      }
    },
  });

  function handlePrepare() {
    downloadQueryManager.runQuery(asyncDownloadParams);
  }

  let content: JSX.Element | undefined;
  if (downloadState.isInit()) {
    content = (
      <>
        <FormGroup>
          <ButtonGroup fill>
            <Button
              text="Download from browser"
              active={asyncDownloadParams.local}
              onClick={() => setAsyncDownloadParams({ ...asyncDownloadParams, local: true })}
            />
            <Button
              text="Export to external file"
              active={!asyncDownloadParams.local}
              onClick={() => setAsyncDownloadParams({ ...asyncDownloadParams, local: false })}
            />
          </ButtonGroup>
        </FormGroup>
        <AutoForm
          fields={ASYNC_DOWNLOAD_FIELDS}
          model={asyncDownloadParams}
          onChange={setAsyncDownloadParams}
        />
      </>
    );
  } else if (downloadState.isLoading()) {
    content = (
      <div>
        <ProgressBar />
        <p>Preparing results...</p>
        <p>TaskID: {downloadState.intermediate?.taskId}</p>
      </div>
    );
  } else if (downloadState.isError()) {
    content = <div>Query error: {downloadState.getErrorMessage()}</div>;
  } else if (downloadState.data) {
    content = <div>The query has completed and a the file has been generated.</div>;
  }

  return (
    <Dialog
      className="export-dialog"
      onClose={onClose}
      isOpen
      title="Export data"
      canOutsideClickClose={false}
    >
      <div className={Classes.DIALOG_BODY}>{content}</div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text={downloadState.isLoading() ? 'Cancel' : 'Close'} onClick={onClose} />
          {downloadState.isInit() && (
            <Button text="Export" intent={Intent.PRIMARY} onClick={handlePrepare} />
          )}
        </div>
      </div>
    </Dialog>
  );
});
