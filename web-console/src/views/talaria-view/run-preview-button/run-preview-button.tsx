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
  Icon,
  Intent,
  Menu,
  MenuDivider,
  MenuItem,
  Position,
  useHotkeys,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import React, { useCallback, useMemo, useState } from 'react';

import { MenuCheckbox } from '../../../components';
import { EditContextDialog } from '../../../dialogs/edit-context-dialog/edit-context-dialog';
import { getLink } from '../../../links';
import { TalariaQuery } from '../../../talaria-models';
import { deepDelete, deepSet, pluralIfNeeded, QueryWithContext } from '../../../utils';
import {
  getUseApproximateCountDistinct,
  getUseCache,
  QueryContext,
  setUseApproximateCountDistinct,
  setUseCache,
} from '../../../utils/query-context';
import { ExplainDialog } from '../../query-view/explain-dialog/explain-dialog';

import './run-preview-button.scss';

export function getParallelism(context: QueryContext): number | undefined {
  const { talariaNumTasks } = context;
  return typeof talariaNumTasks === 'number' ? talariaNumTasks : undefined;
}

export function changeParallelism(
  context: QueryContext,
  parallelism: number | undefined,
): QueryContext {
  let newContext = context;
  if (typeof parallelism === 'number') {
    newContext = deepSet(newContext, 'talariaNumTasks', parallelism);
  } else {
    newContext = deepDelete(newContext, 'talariaNumTasks');
  }
  return newContext;
}

export interface RunPreviewButtonProps {
  query: TalariaQuery;
  onQueryChange(query: TalariaQuery): void;
  loading: boolean;
  small?: boolean;
  onRun: ((actuallyIngest: boolean) => void) | undefined;
  onExport: (() => void) | undefined;
}

export const RunPreviewButton = React.memo(function RunPreviewButton(props: RunPreviewButtonProps) {
  const { query, onQueryChange, onRun, loading, small, onExport } = props;
  const [editContextDialogOpen, setEditContextDialogOpen] = useState(false);
  const [explainDialogQuery, setExplainDialogQuery] = useState<QueryWithContext | undefined>();

  const runeMode = query.isJsonLike();
  const insertMode = query.isInsertQuery();
  const queryContext = query.queryContext;
  const numContextKeys = Object.keys(queryContext).length;

  const useCache = getUseCache(queryContext);
  const useApproximateCountDistinct = getUseApproximateCountDistinct(queryContext);

  const handleRun = useCallback(() => {
    if (!onRun) return;
    onRun(true);
  }, [onRun]);

  const handleExplain = useCallback(() => {
    const queryAndContext = query.getEffectiveQueryAndContext();
    if (typeof queryAndContext.query !== 'string') return;
    setExplainDialogQuery({
      queryString: queryAndContext.query,
      queryContext: queryAndContext.context,
      wrapQueryLimit: undefined,
    });
  }, [query]);

  const hotkeys = useMemo(() => {
    if (small) return [];
    return [
      {
        allowInInput: true,
        global: true,
        group: 'Query',
        combo: 'mod + enter',
        label: 'Runs the current query',
        onKeyDown: handleRun,
      },
      {
        allowInInput: true,
        global: true,
        group: 'Query',
        combo: 'mod + e',
        label: 'Explain the current query',
        onKeyDown: handleExplain,
      },
    ];
  }, [small, handleRun, handleExplain]);

  useHotkeys(hotkeys);

  const parallelism = getParallelism(queryContext);
  function renderParallelismMenuItem(p: number | undefined) {
    return (
      <MenuItem
        key={String(p)}
        icon={p === parallelism ? IconNames.TICK : IconNames.BLANK}
        text={p ? String(p) : 'auto'}
        onClick={() => onQueryChange(query.changeQueryContext(changeParallelism(queryContext, p)))}
      />
    );
  }

  return (
    <ButtonGroup className="run-preview-button">
      <Button
        className={runeMode ? 'rune-button' : undefined}
        disabled={loading}
        icon={IconNames.CARET_RIGHT}
        onClick={onRun ? () => onRun(false) : undefined}
        text={insertMode ? 'Preview' : 'Run'}
        intent={onRun && !small ? Intent.PRIMARY : undefined}
        small={small}
        minimal={small}
      />
      {!runeMode && insertMode && (
        <Button
          disabled={loading}
          icon={IconNames.CLOUD_UPLOAD}
          onClick={onRun ? () => onRun(true) : undefined}
          text="Insert"
          intent={!small ? Intent.PRIMARY : undefined}
          small={small}
          minimal={small}
        />
      )}
      <Popover2
        position={Position.BOTTOM_LEFT}
        content={
          <Menu>
            <MenuItem icon={IconNames.DOWNLOAD} text="Export" onClick={onExport} />
            {!runeMode && (
              <MenuItem icon={IconNames.CLEAN} text="Explain SQL query" onClick={handleExplain} />
            )}
            {onQueryChange && (
              <>
                <MenuDivider />
                <MenuItem
                  icon={IconNames.PROPERTIES}
                  text="Edit context"
                  onClick={() => setEditContextDialogOpen(true)}
                  label={numContextKeys ? pluralIfNeeded(numContextKeys, 'key') : undefined}
                />
                <MenuItem
                  icon={IconNames.TWO_COLUMNS}
                  text="Parallelism"
                  label={String(getParallelism(queryContext) || 'auto')}
                >
                  <Menu>
                    {[undefined, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10].map(renderParallelismMenuItem)}
                    <MenuItem
                      icon={Number(parallelism) > 10 ? IconNames.TICK : IconNames.BLANK}
                      text="More"
                    >
                      {renderParallelismMenuItem(11)}
                      {Number(parallelism) > 11 && renderParallelismMenuItem(parallelism)}
                    </MenuItem>
                  </Menu>
                </MenuItem>
                <MenuCheckbox
                  checked={useApproximateCountDistinct}
                  text="Use approximate COUNT(DISTINCT)"
                  onChange={() => {
                    onQueryChange(
                      query.changeQueryContext(
                        setUseApproximateCountDistinct(queryContext, !useApproximateCountDistinct),
                      ),
                    );
                  }}
                />
                <MenuCheckbox
                  checked={useCache}
                  text="Use cache"
                  onChange={() => {
                    onQueryChange(query.changeQueryContext(setUseCache(queryContext, !useCache)));
                  }}
                />
                <MenuCheckbox
                  checked={!query.unlimited}
                  intent={query.unlimited ? Intent.WARNING : undefined}
                  text="Limit inline results"
                  labelElement={
                    query.unlimited ? <Icon icon={IconNames.WARNING_SIGN} /> : undefined
                  }
                  onChange={() => {
                    onQueryChange(query.toggleUnlimited());
                  }}
                />
                <MenuDivider />
              </>
            )}
            <MenuItem
              icon={IconNames.HELP}
              text="DruidSQL documentation"
              href={getLink('DOCS_SQL')}
              target="_blank"
            />
          </Menu>
        }
      >
        <Button
          className={runeMode ? 'rune-button' : undefined}
          icon={IconNames.MORE}
          intent={onRun && !small ? (query.unlimited ? Intent.WARNING : Intent.PRIMARY) : undefined}
          small={small}
          minimal={small}
        />
      </Popover2>
      {editContextDialogOpen && (
        <EditContextDialog
          queryContext={queryContext}
          onQueryContextChange={newContext => {
            if (!onQueryChange) return;
            onQueryChange(query.changeQueryContext(newContext));
          }}
          onClose={() => {
            setEditContextDialogOpen(false);
          }}
        />
      )}
      {explainDialogQuery && (
        <ExplainDialog
          queryWithContext={explainDialogQuery}
          mandatoryQueryContext={{}}
          setQueryString={queryString => onQueryChange(query.changeQueryString(queryString))}
          onClose={() => setExplainDialogQuery(undefined)}
        />
      )}
    </ButtonGroup>
  );
});
