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

import { Button, Classes, Dialog, Intent } from '@blueprintjs/core';
import React, { useState } from 'react';

import { useQueryManager } from '../../../hooks';
import { IngestQueryPattern, TalariaQuery } from '../../../talaria-models';
import { queryDruidSql } from '../../../utils';
import { DestinationForm, DestinationInfo } from '../destination-form/destination-form';

import './destination-dialog.scss';

interface DestinationDialogProps {
  ingestQueryPattern: IngestQueryPattern;
  changeIngestQueryPattern(ingestQueryPattern: IngestQueryPattern): void;
  onClose(): void;
}

export const DestinationDialog = React.memo(function DestinationDialog(
  props: DestinationDialogProps,
) {
  const { onClose } = props;

  const [existingTableState] = useQueryManager<string, string[]>({
    initQuery: '',
    processQuery: async (_: string, _cancelToken) => {
      // Check if datasource already exists
      const tables = await queryDruidSql({
        query: `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME NOT LIKE '${TalariaQuery.TMP_PREFIX}%' ORDER BY TABLE_NAME ASC`,
        resultFormat: 'array',
      });

      return tables.map(t => t[0]);
    },
  });

  const [info, setInfo] = useState<DestinationInfo>({ mode: 'new', table: '' });

  return (
    <Dialog
      className="destination-dialog"
      onClose={onClose}
      isOpen
      title="Destination"
      canOutsideClickClose={false}
    >
      {existingTableState.data && (
        <DestinationForm
          className={Classes.DIALOG_BODY}
          existingTables={existingTableState.data}
          destinationInfo={info}
          changeDestinationInfo={setInfo}
        />
      )}
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button text="Save" intent={Intent.PRIMARY} />
        </div>
      </div>
    </Dialog>
  );
});
