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

import { shallow } from 'enzyme';
import React from 'react';

import { Execution } from '../../../talaria-models';

import { TalariaQueryError } from './talaria-query-error';

describe('TalariaQueryError', () => {
  it('matches snapshot', () => {
    const queryError = shallow(
      <TalariaQueryError
        execution={
          new Execution({
            engine: 'sql-task',
            id: 'talaria1_kttm',
            error: {
              taskId: 'talaria1_kttm_naobfgjd_2021-12-03T23:16:15.269Z',
              host: 'ip-10-3-228-20.ec2.internal:8100',
              stageNumber: 0,
              error: {
                errorCode: 'TooManyBuckets',
                maxBuckets: 10000,
                errorMessage:
                  'Too many partition buckets (max = 10,000); try breaking your query up into smaller queries or using a wider segmentGranularity',
              },
              exceptionStackTrace:
                'io.imply.druid.talaria.frame.cluster.statistics.TooManyBucketsException: Too many buckets; maximum is [10000]\n\tat io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsCollectorImpl.getOrCreateBucketHolder(ClusterByStatisticsCollectorImpl.java:324)\n\tat io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsCollectorImpl.add(ClusterByStatisticsCollectorImpl.java:105)\n\tat io.imply.druid.talaria.indexing.TalariaClusterByStatisticsCollectorWorker.runIncrementally(TalariaClusterByStatisticsCollectorWorker.java:87)\n\tat io.imply.druid.talaria.frame.processor.FrameProcessors$1FrameProcessorWithBaggage.runIncrementally(FrameProcessors.java:136)\n\tat io.imply.druid.talaria.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.runProcessorNow(FrameProcessorExecutor.java:197)\n\tat io.imply.druid.talaria.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.run(FrameProcessorExecutor.java:112)\n\tat io.imply.druid.talaria.indexing.TalariaWorkerTask$2$2.run(TalariaWorkerTask.java:731)\n\tat java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)\n\tat java.util.concurrent.FutureTask.run(FutureTask.java:266)\n\tat org.apache.druid.query.PrioritizedListenableFutureTask.run(PrioritizedExecutorService.java:251)\n\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)\n\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)\n\tat java.lang.Thread.run(Thread.java:748)\n',
            },
          })
        }
      />,
    );

    expect(queryError).toMatchSnapshot();
  });
});
