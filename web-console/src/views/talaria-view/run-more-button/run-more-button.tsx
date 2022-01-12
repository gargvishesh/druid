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
import { DruidEngine, TalariaQuery } from '../../../talaria-models';
import { deepDelete, deepSet, pluralIfNeeded, QueryWithContext } from '../../../utils';
import {
  getUseApproximateCountDistinct,
  getUseApproximateTopN,
  getUseCache,
  QueryContext,
  setUseApproximateCountDistinct,
  setUseApproximateTopN,
  setUseCache,
} from '../../../utils/query-context';
import { ExplainDialog } from '../../query-view/explain-dialog/explain-dialog';

import './run-more-button.scss';

function getParallelism(context: QueryContext): number | undefined {
  const { talariaNumTasks } = context;
  return typeof talariaNumTasks === 'number' ? talariaNumTasks : undefined;
}

function changeParallelism(context: QueryContext, parallelism: number | undefined): QueryContext {
  return typeof parallelism === 'number'
    ? deepSet(context, 'talariaNumTasks', parallelism)
    : deepDelete(context, 'talariaNumTasks');
}

function getTalariaFinalizeAggregations(context: QueryContext): boolean {
  const { talariaFinalizeAggregations } = context;
  return typeof talariaFinalizeAggregations === 'boolean' ? talariaFinalizeAggregations : true;
}

function setTalariaFinalizeAggregations(
  context: QueryContext,
  talariaFinalizeAggregations: boolean,
): QueryContext {
  if (talariaFinalizeAggregations) {
    return deepDelete(context, 'talariaFinalizeAggregations');
  } else {
    return deepSet(context, 'talariaFinalizeAggregations', false);
  }
}

export interface RunMoreButtonProps {
  query: TalariaQuery;
  onQueryChange(query: TalariaQuery): void;
  loading: boolean;
  small?: boolean;
  onRun(preview: boolean): void;
  onExport: (() => void) | undefined;
}

export const RunMoreButton = React.memo(function RunMoreButton(props: RunMoreButtonProps) {
  const { query, onQueryChange, onRun, loading, small, onExport } = props;
  const [editContextDialogOpen, setEditContextDialogOpen] = useState(false);
  const [explainDialogQuery, setExplainDialogQuery] = useState<QueryWithContext | undefined>();

  const emptyQuery = query.isEmptyQuery();
  const runeMode = query.isJsonLike();
  const insertMode = query.isInsertQuery();
  const queryContext = query.queryContext;
  const numContextKeys = Object.keys(queryContext).length;

  const talariaFinalizeAggregations = getTalariaFinalizeAggregations(queryContext);
  const useApproximateCountDistinct = getUseApproximateCountDistinct(queryContext);
  const useApproximateTopN = getUseApproximateTopN(queryContext);
  const useCache = getUseCache(queryContext);

  const handleRun = useCallback(() => {
    if (!onRun) return;
    onRun(false);
  }, [onRun]);

  const handlePreview = useCallback(() => {
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
        label: 'Run the current query',
        onKeyDown: handleRun,
      },
      {
        allowInInput: true,
        global: true,
        group: 'Query',
        combo: 'mod + shift + enter',
        label: 'Preview the current query',
        onKeyDown: handlePreview,
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
  }, [small, handleRun, handlePreview, handleExplain]);

  useHotkeys(hotkeys);

  const parallelism = getParallelism(queryContext);
  function renderParallelismMenuItem(p: number | undefined) {
    return (
      <MenuItem
        key={String(p)}
        icon={p === parallelism ? IconNames.TICK : IconNames.BLANK}
        text={typeof p === 'undefined' ? 'auto' : String(p)}
        onClick={() => onQueryChange(query.changeQueryContext(changeParallelism(queryContext, p)))}
      />
    );
  }

  const queryEngine = query.engine;
  function renderQueryEngineMenuItem(e: DruidEngine | undefined) {
    return (
      <MenuItem
        key={String(e)}
        icon={e === queryEngine ? IconNames.TICK : IconNames.BLANK}
        text={typeof e === 'undefined' ? 'auto' : e}
        onClick={() => onQueryChange(query.changeEngine(e))}
      />
    );
  }

  return (
    <div className="run-more-button">
      <ButtonGroup>
        <Button
          className={runeMode ? 'rune-button' : undefined}
          disabled={loading}
          icon={IconNames.CARET_RIGHT}
          onClick={() => onRun(false)}
          text="Run"
          intent={!emptyQuery && !small ? Intent.PRIMARY : undefined}
          small={small}
          minimal={small}
        />
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
                  <MenuItem
                    icon={IconNames.MANY_TO_MANY}
                    text="Query engine"
                    label={query.engine || `${query.getEffectiveEngine()} (auto)`}
                  >
                    <Menu>
                      {([undefined, 'broker', 'async', 'talaria'] as (
                        | DruidEngine
                        | undefined
                      )[]).map(renderQueryEngineMenuItem)}
                    </Menu>
                  </MenuItem>
                  <MenuDivider />
                  <MenuItem
                    icon={IconNames.PROPERTIES}
                    text="Edit context"
                    onClick={() => setEditContextDialogOpen(true)}
                    label={numContextKeys ? pluralIfNeeded(numContextKeys, 'key') : undefined}
                  />
                  <MenuItem
                    icon={IconNames.TWO_COLUMNS}
                    text="Talaria parallelism"
                    label={String(parallelism || 'auto')}
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
                    checked={talariaFinalizeAggregations}
                    text="Finalize aggregations"
                    onChange={() => {
                      onQueryChange(
                        query.changeQueryContext(
                          setTalariaFinalizeAggregations(
                            queryContext,
                            !talariaFinalizeAggregations,
                          ),
                        ),
                      );
                    }}
                  />
                  <MenuCheckbox
                    checked={useApproximateCountDistinct}
                    text="Use approximate COUNT(DISTINCT)"
                    onChange={() => {
                      onQueryChange(
                        query.changeQueryContext(
                          setUseApproximateCountDistinct(
                            queryContext,
                            !useApproximateCountDistinct,
                          ),
                        ),
                      );
                    }}
                  />
                  <MenuCheckbox
                    checked={useApproximateTopN}
                    text="Use approximate TopN"
                    onChange={() => {
                      onQueryChange(
                        query.changeQueryContext(
                          setUseApproximateTopN(queryContext, !useApproximateTopN),
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
                </>
              )}
            </Menu>
          }
        >
          <Button
            className={runeMode ? 'rune-button' : undefined}
            icon={IconNames.MORE}
            intent={
              !emptyQuery && !small
                ? query.unlimited
                  ? Intent.WARNING
                  : Intent.PRIMARY
                : undefined
            }
            small={small}
            minimal={small}
          />
        </Popover2>
      </ButtonGroup>
      {insertMode && (
        <Button
          disabled={loading}
          icon={IconNames.EYE_OPEN}
          onClick={() => onRun(true)}
          text="Preview"
          small={small}
        />
      )}
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
    </div>
  );
});
