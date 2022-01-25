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
import { SqlQuery } from 'druid-query-toolkit';
import React, { useState } from 'react';

import { AutoForm, Field } from '../../../components';
import { useQueryManager } from '../../../hooks';
import { QueryExecution, TalariaQuery } from '../../../talaria-models';
import { IntermediateQueryState } from '../../../utils';
import { downloadResults, submitAsyncQuery, talariaBackgroundStatusCheck } from '../talaria-utils';

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
    QueryExecution,
    QueryExecution
  >({
    processQuery: async (_asyncDownloadParams, cancelToken) => {
      const downloadQuery = talariaQuery
        .changeUnlimited(true)
        .removeInsert()
        .changeEngine('talaria');

      const { query, context, isSql, prefixLines } = downloadQuery.getEffectiveQueryAndContext();
      if (!isSql) throw new Error('must be SQL for export');

      return await submitAsyncQuery({
        query,
        context: {
          ...context,
          talariaDestination: asyncDownloadParams.local ? undefined : 'external',
        },
        skipResults: true,
        prefixLines,
        cancelToken,
      });
    },
    backgroundStatusCheck: async (
      currentSummary: QueryExecution,
      asyncDownloadParams,
      cancelToken: CancelToken,
    ) => {
      const res = await talariaBackgroundStatusCheck(
        currentSummary,
        asyncDownloadParams,
        cancelToken,
      );
      if (res instanceof IntermediateQueryState) return res;

      if (asyncDownloadParams.local) {
        downloadResults(
          res.id,
          addExtensionIfNeeded(
            asyncDownloadParams.filename!,
            resultFormatToExtension(asyncDownloadParams.resultFormat!),
          ),
        );
      }

      return res;
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
        <p>Query ID: {downloadState.intermediate?.id}</p>
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
