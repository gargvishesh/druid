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
import { EditContextDialog } from '../../../dialogs';
import { getLink } from '../../../links';
import { deepDelete, deepSet, formatInteger, pluralIfNeeded } from '../../../utils';
import {
  changeUseApproximateCountDistinct,
  changeUseApproximateTopN,
  changeUseCache,
  getUseApproximateCountDistinct,
  getUseApproximateTopN,
  getUseCache,
  QueryContext,
} from '../../../utils/query-context';
import { DruidEngine, WorkbenchQuery } from '../../../workbench-models';
import { NumericInputDialog } from '../numeric-input-dialog/numeric-input-dialog';

import './run-panel.scss';

const MAX_NUM_TASK_OPTIONS = [2, 3, 4, 5, 7, 9, 11, 17, 33, 65];

// msqMaxNumTasks

function getMaxNumTasks(context: QueryContext): number {
  const { msqMaxNumTasks } = context;
  return Math.max(typeof msqMaxNumTasks === 'number' ? msqMaxNumTasks : 0, 2);
}

function changeMaxNumTasks(context: QueryContext, numTasks: number | undefined): QueryContext {
  return typeof numTasks === 'number'
    ? deepSet(context, 'msqMaxNumTasks', numTasks)
    : deepDelete(context, 'msqMaxNumTasks');
}

// msqFinalizeAggregations

function getFinalizeAggregations(context: QueryContext): boolean {
  const { msqFinalizeAggregations } = context;
  return typeof msqFinalizeAggregations === 'boolean' ? msqFinalizeAggregations : true;
}

function changeFinalizeAggregations(
  context: QueryContext,
  msqFinalizeAggregations: boolean,
): QueryContext {
  if (msqFinalizeAggregations) {
    return deepDelete(context, 'msqFinalizeAggregations');
  } else {
    return deepSet(context, 'msqFinalizeAggregations', false);
  }
}

// msqDurableShuffleStorage

function getDurableShuffleStorage(context: QueryContext): boolean {
  const { msqDurableShuffleStorage } = context;
  return Boolean(msqDurableShuffleStorage);
}

function changeDurableShuffleStorage(
  context: QueryContext,
  msqDurableShuffleStorage: boolean,
): QueryContext {
  if (msqDurableShuffleStorage) {
    return deepSet(context, 'msqDurableShuffleStorage', true);
  } else {
    return deepDelete(context, 'msqDurableShuffleStorage');
  }
}

// maxParseExceptions

function getMaxParseExceptions(context: QueryContext): number {
  const { maxParseExceptions } = context;
  return Number(maxParseExceptions) || 0;
}

function changeMaxParseExceptions(context: QueryContext, maxParseExceptions: number): QueryContext {
  if (maxParseExceptions !== 0) {
    return deepSet(context, 'maxParseExceptions', maxParseExceptions);
  } else {
    return deepDelete(context, 'maxParseExceptions');
  }
}

export interface RunPanelProps {
  query: WorkbenchQuery;
  onQueryChange(query: WorkbenchQuery): void;
  loading: boolean;
  small?: boolean;
  onRun(preview: boolean): void;
  onExplain?(): void;
  onHistory?(): void;
  queryEngines: DruidEngine[];
}

export const RunPanel = React.memo(function RunPanel(props: RunPanelProps) {
  const { query, onQueryChange, onRun, onExplain, onHistory, loading, small, queryEngines } = props;
  const [editContextDialogOpen, setEditContextDialogOpen] = useState(false);
  const [customNumTasksDialogOpen, setCustomNumTasksDialogOpen] = useState(false);

  const emptyQuery = query.isEmptyQuery();
  const ingestMode = query.isIngestQuery();
  const queryContext = query.queryContext;
  const numContextKeys = Object.keys(queryContext).length;

  const maxParseExceptions = getMaxParseExceptions(queryContext);
  const msqFinalizeAggregations = getFinalizeAggregations(queryContext);
  const msqDurableShuffleStorage = getDurableShuffleStorage(queryContext);
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

  const hotkeys = useMemo(() => {
    if (small) return [];
    const keys = [
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
    ];
    if (onExplain) {
      keys.push({
        allowInInput: true,
        global: true,
        group: 'Query',
        combo: 'mod + e',
        label: 'Explain the current query',
        onKeyDown: onExplain,
      });
    }
    return keys;
  }, [small, handleRun, handlePreview, onExplain]);

  useHotkeys(hotkeys);

  const maxNumTasks = getMaxNumTasks(queryContext);
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

  const availableEngines = ([undefined] as (DruidEngine | undefined)[]).concat(queryEngines);

  const effectiveEngine = query.getEffectiveEngine();
  return (
    <div className="run-panel">
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
          minimal={small}
        />
      )}
      {!small && onQueryChange && (
        <ButtonGroup>
          <Popover2
            position={Position.BOTTOM_LEFT}
            content={
              <Menu>
                {queryEngines.length > 1 && (
                  <>
                    <MenuDivider title="Select engine" />
                    {availableEngines.map(renderQueryEngineMenuItem)}
                    <MenuDivider />
                  </>
                )}
                <MenuItem
                  icon={IconNames.PROPERTIES}
                  text="Edit context"
                  onClick={() => setEditContextDialogOpen(true)}
                  label={numContextKeys ? pluralIfNeeded(numContextKeys, 'key') : undefined}
                />
                {effectiveEngine === 'sql-task' ? (
                  <>
                    <MenuItem
                      icon={IconNames.ERROR}
                      text="Max parse exceptions"
                      label={String(maxParseExceptions)}
                    >
                      {[0, 1, 5, 10, 1000, 10000, -1].map(v => (
                        <MenuItem
                          key={String(v)}
                          icon={v === maxParseExceptions ? IconNames.TICK : IconNames.BLANK}
                          text={v === -1 ? '∞ (-1)' : String(v)}
                          onClick={() => {
                            onQueryChange(
                              query.changeQueryContext(changeMaxParseExceptions(queryContext, v)),
                            );
                          }}
                          shouldDismissPopover={false}
                        />
                      ))}
                    </MenuItem>
                    <MenuCheckbox
                      checked={msqDurableShuffleStorage}
                      text="Durable shuffle storage"
                      onChange={() => {
                        onQueryChange(
                          query.changeQueryContext(
                            changeDurableShuffleStorage(queryContext, !msqDurableShuffleStorage),
                          ),
                        );
                      }}
                    />
                    <MenuCheckbox
                      checked={msqFinalizeAggregations}
                      text="Finalize aggregations"
                      onChange={() => {
                        onQueryChange(
                          query.changeQueryContext(
                            changeFinalizeAggregations(queryContext, !msqFinalizeAggregations),
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
                        onQueryChange(
                          query.changeQueryContext(changeUseCache(queryContext, !useCache)),
                        );
                      }}
                    />
                    <MenuCheckbox
                      checked={useApproximateTopN}
                      text="Use approximate TopN"
                      onChange={() => {
                        onQueryChange(
                          query.changeQueryContext(
                            changeUseApproximateTopN(queryContext, !useApproximateTopN),
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
                          changeUseApproximateCountDistinct(
                            queryContext,
                            !useApproximateCountDistinct,
                          ),
                        ),
                      );
                    }}
                  />
                )}
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
              </Menu>
            }
          >
            <Button
              text={`Engine: ${queryEngine || `auto (${effectiveEngine})`}`}
              rightIcon={IconNames.CARET_DOWN}
              intent={query.unlimited ? Intent.WARNING : undefined}
            />
          </Popover2>
          {effectiveEngine === 'sql-task' && (
            <Popover2
              position={Position.BOTTOM_LEFT}
              content={
                <Menu>
                  <MenuDivider title="Number of tasks to launch" />
                  {MAX_NUM_TASK_OPTIONS.map(m => (
                    <MenuItem
                      key={String(m)}
                      icon={m === maxNumTasks ? IconNames.TICK : IconNames.BLANK}
                      text={formatInteger(m)}
                      label={`(1 controller + ${m === 2 ? '1 worker' : `max ${m - 1} workers`})`}
                      onClick={() =>
                        onQueryChange(query.changeQueryContext(changeMaxNumTasks(queryContext, m)))
                      }
                    />
                  ))}
                  <MenuItem
                    icon={
                      MAX_NUM_TASK_OPTIONS.includes(maxNumTasks) ? IconNames.BLANK : IconNames.TICK
                    }
                    text="Custom"
                    onClick={() => setCustomNumTasksDialogOpen(true)}
                  />
                </Menu>
              }
            >
              <Button text={`Max tasks: ${maxNumTasks}`} rightIcon={IconNames.CARET_DOWN} />
            </Popover2>
          )}
        </ButtonGroup>
      )}
      <Popover2
        position={Position.BOTTOM_LEFT}
        content={
          <Menu>
            {onExplain && (
              <MenuItem icon={IconNames.CLEAN} text="Explain SQL query" onClick={onExplain} />
            )}
            {onHistory && (
              <MenuItem icon={IconNames.HISTORY} text="Query history" onClick={onHistory} />
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
        <Button small={small} minimal={small} rightIcon={IconNames.MORE} />
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
      {customNumTasksDialogOpen && (
        <NumericInputDialog
          title="Custom task number"
          message={
            <>
              <p>Specify the total number of tasks that should be launched.</p>
              <p>
                There must be at least 2 tasks to account for the controller and at least one
                worker.
              </p>
            </>
          }
          minValue={2}
          initValue={maxNumTasks}
          onSave={p => {
            onQueryChange(query.changeQueryContext(changeMaxNumTasks(queryContext, p)));
          }}
          onClose={() => setCustomNumTasksDialogOpen(false)}
        />
      )}
    </div>
  );
});
