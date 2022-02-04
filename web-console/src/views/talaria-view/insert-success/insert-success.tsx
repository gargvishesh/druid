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

import { SqlTableRef } from 'druid-query-toolkit';
import React from 'react';

import { getTotalCountForStage, QueryExecution } from '../../../talaria-models';
import { formatDuration, pluralIfNeeded } from '../../../utils';

import './insert-success.scss';

export interface InsertSuccessProps {
  insertQueryExecution: QueryExecution;
  onStats(): void;
  onQueryChange(queryString: string): void;
}

export const InsertSuccess = React.memo(function InsertSuccess(props: InsertSuccessProps) {
  const { insertQueryExecution, onStats, onQueryChange } = props;

  const datasource = insertQueryExecution.getInsertDatasource();
  if (!datasource || !insertQueryExecution.stages) return null;

  const lastStage = insertQueryExecution.stages[insertQueryExecution.stages.length - 1];
  const rows = getTotalCountForStage(lastStage, 'input', 'rows');
  const table = SqlTableRef.create(datasource);

  const duration = insertQueryExecution.duration;
  return (
    <div className="insert-success">
      <p>{`${pluralIfNeeded(rows, 'row')} inserted into ${datasource}.`}</p>
      <p>
        {duration ? `Insert query took ${formatDuration(duration)}. ` : `Insert query completed. `}
        <span className="action" onClick={onStats}>
          Show stats
        </span>
      </p>
      <p>
        Run{' '}
        <span
          className="action"
          onClick={() => onQueryChange(`SELECT * FROM ${table}`)}
        >{`SELECT * FROM ${table}`}</span>
      </p>
    </div>
  );
});
