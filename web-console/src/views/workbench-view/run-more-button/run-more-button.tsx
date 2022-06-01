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
import { DruidEngine, WorkbenchQuery } from '../../../workbench-models';
import { ExplainDialog } from '../../query-view/explain-dialog/explain-dialog';
import { NumericInputDialog } from '../numeric-input-dialog/numeric-input-dialog';

import './run-more-button.scss';

const PARALLELISM_OPTIONS = [1, 2, 4, 6, 8, 10, 16, 32, 64];

function getParallelism(context: QueryContext): number {
  const { talariaNumTasks } = context;
  return typeof talariaNumTasks === 'number' ? talariaNumTasks : 1;
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
  query: WorkbenchQuery;
  onQueryChange(query: WorkbenchQuery): void;
  loading: boolean;
  small?: boolean;
  onRun(preview: boolean): void;
  extraEngines: DruidEngine[];
}

export const RunMoreButton = React.memo(function RunMoreButton(props: RunMoreButtonProps) {
  const { query, onQueryChange, onRun, loading, small, extraEngines } = props;
  const [editContextDialogOpen, setEditContextDialogOpen] = useState(false);
  const [customParallelismDialogOpen, setCustomParallelismDialogOpen] = useState(false);
  const [explainDialogQuery, setExplainDialogQuery] = useState<QueryWithContext | undefined>();

  const emptyQuery = query.isEmptyQuery();
  const ingestMode = query.isIngestQuery();
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
    const { engine, query: apiQuery } = query.getApiQuery();
    if (typeof apiQuery.query !== 'string') return;
    const queryContext = apiQuery.context || {};
    if (engine === 'sql-task') {
      // Special handling: instead of using the sql/task API we want this query to go via the normal (sync) SQL API with the `talaria` engine selector
      queryContext.talaria = true;
    }
    setExplainDialogQuery({
      queryString: apiQuery.query,
      queryContext,
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
  const queryEngine = query.engine;
  function renderQueryEngineMenuItem(e: DruidEngine | undefined) {
    return (
      <MenuItem
        key={String(e)}
        icon={e === queryEngine ? IconNames.TICK : IconNames.BLANK}
        text={typeof e === 'undefined' ? 'auto' : e}
        onClick={() => onQueryChange(query.changeEngine(e))}
        shouldDismissPopover={false}
      />
    );
  }

  const availableEngines = ([undefined, 'native', 'sql'] as (DruidEngine | undefined)[]).concat(
    extraEngines,
  );

  const effectiveEngine = query.getEffectiveEngine();
  return (
    <div className="run-more-button">
      <Button
        className={effectiveEngine === 'native' ? 'rune-button' : undefined}
        disabled={loading}
        icon={IconNames.CARET_RIGHT}
        onClick={() => onRun(false)}
        text="Run"
        intent={!emptyQuery && !small ? Intent.PRIMARY : undefined}
        small={small}
        minimal={small}
      />
      {ingestMode && (
        <Button
          disabled={loading}
          icon={IconNames.EYE_OPEN}
          onClick={() => onRun(true)}
          text="Preview"
          small={small}
        />
      )}
      {onQueryChange && (
        <Popover2
          position={Position.BOTTOM_LEFT}
          content={
            <Menu>
              <MenuDivider title="Select engine" />
              {availableEngines.map(renderQueryEngineMenuItem)}
              <MenuDivider />
              <MenuItem
                icon={IconNames.PROPERTIES}
                text="Edit context"
                onClick={() => setEditContextDialogOpen(true)}
                label={numContextKeys ? pluralIfNeeded(numContextKeys, 'key') : undefined}
              />
              {effectiveEngine === 'sql-task' ? (
                <>
                  <MenuItem
                    icon={IconNames.TWO_COLUMNS}
                    text="Task parallelism"
                    label={String(parallelism)}
                  >
                    <Menu>
                      {PARALLELISM_OPTIONS.map(p => (
                        <MenuItem
                          key={String(p)}
                          icon={p === parallelism ? IconNames.TICK : IconNames.BLANK}
                          text={String(p)}
                          onClick={() =>
                            onQueryChange(
                              query.changeQueryContext(changeParallelism(queryContext, p)),
                            )
                          }
                          shouldDismissPopover={false}
                        />
                      ))}
                      <MenuItem
                        icon={
                          PARALLELISM_OPTIONS.includes(parallelism)
                            ? IconNames.BLANK
                            : IconNames.TICK
                        }
                        text="Custom"
                        onClick={() => setCustomParallelismDialogOpen(true)}
                      />
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
                </>
              ) : (
                <>
                  <MenuCheckbox
                    checked={useCache}
                    text="Use cache"
                    onChange={() => {
                      onQueryChange(query.changeQueryContext(setUseCache(queryContext, !useCache)));
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
                </>
              )}
              {effectiveEngine !== 'native' && (
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
              )}
              <MenuCheckbox
                checked={!query.unlimited}
                intent={query.unlimited ? Intent.WARNING : undefined}
                text="Limit inline results"
                labelElement={query.unlimited ? <Icon icon={IconNames.WARNING_SIGN} /> : undefined}
                onChange={() => {
                  onQueryChange(query.toggleUnlimited());
                }}
              />
            </Menu>
          }
        >
          <Button small={small} rightIcon={IconNames.CARET_DOWN}>
            {`Engine: ${queryEngine || `auto (${effectiveEngine})`}`}
          </Button>
        </Popover2>
      )}
      <Popover2
        position={Position.BOTTOM_LEFT}
        content={
          <Menu>
            {effectiveEngine !== 'native' && (
              <MenuItem icon={IconNames.CLEAN} text="Explain SQL query" onClick={handleExplain} />
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
        <Button small={small} rightIcon={IconNames.MORE} />
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
      {customParallelismDialogOpen && (
        <NumericInputDialog
          title="Custom parallelism value"
          initValue={parallelism}
          onSave={p => {
            onQueryChange(query.changeQueryContext(changeParallelism(queryContext, p)));
          }}
          onClose={() => setCustomParallelismDialogOpen(false)}
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
