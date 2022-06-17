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

import { useEffect, useRef, useState } from 'react';

import { QueryManager, QueryManagerOptions, QueryState } from '../utils';

import { usePermanentCallback } from './use-permanent-callback';

const NULL_QUERY_MANAGER = new Proxy<QueryManager<any, any, any, any>>({} as any, {
  get(_target, p, _receiver) {
    throw new Error(`QueryManager.${String(p)} used before initialization.`);
  },
});

export interface UseQueryManagerOptions<Q, R, I, E extends Error>
  extends Omit<QueryManagerOptions<Q, R, I, E>, 'onStateChange'> {
  query?: Q | undefined;
  initQuery?: Q;
}

export function useQueryManager<Q, R, I = never, E extends Error = Error>(
  options: UseQueryManagerOptions<Q, R, I, E>,
): [QueryState<R, E, I>, QueryManager<Q, R, I, E>] {
  const { query, initQuery, initState, processQuery, backgroundStatusCheck } = options;

  const concreteProcessQuery = usePermanentCallback(processQuery);
  const concreteBackgroundStatusCheck = usePermanentCallback(
    backgroundStatusCheck || ((() => {}) as any),
  );

  const [resultState, setResultState] = useState<QueryState<R, E, I>>(initState || QueryState.INIT);

  const queryManager = useRef<QueryManager<Q, R, I, E>>(NULL_QUERY_MANAGER);

  useEffect(() => {
    // Initialize queryManager on mount to ensure that useQueryManager
    // will be compatible with future React versions that may mount/unmount/remount
    // the same component multiple times while.
    //
    // See https://reactjs.org/docs/strict-mode.html#ensuring-reusable-state
    // and https://github.com/reactwg/react-18/discussions/18
    queryManager.current = new QueryManager<Q, R, I, E>({
      ...options,
      initState,
      processQuery: concreteProcessQuery,
      backgroundStatusCheck: backgroundStatusCheck ? concreteBackgroundStatusCheck : undefined,
      onStateChange: setResultState,
    });

    if (typeof initQuery !== 'undefined') {
      queryManager.current.runQuery(initQuery);
    }
    return () => {
      queryManager.current.terminate();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (typeof query !== 'undefined') {
      queryManager.current.runQuery(query);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query]);

  return [resultState, queryManager.current];
}
