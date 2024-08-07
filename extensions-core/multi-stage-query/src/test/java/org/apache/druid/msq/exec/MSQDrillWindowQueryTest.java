/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.msq.exec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.msq.exec.MSQDrillWindowQueryTest.DrillWindowQueryMSQComponentSupplier;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.msq.test.CalciteMSQTestsHelper;
import org.apache.druid.msq.test.ExtractResultsFactory;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.test.MSQTestOverlordServiceClient;
import org.apache.druid.msq.test.MSQTestTaskActionClient;
import org.apache.druid.msq.test.VerifyMSQSupportedNativeQueriesPredicate;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.sql.calcite.DrillWindowQueryTest;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.run.SqlEngine;

@SqlTestFrameworkConfig.ComponentSupplier(DrillWindowQueryMSQComponentSupplier.class)
public class MSQDrillWindowQueryTest extends DrillWindowQueryTest
{
  public static class DrillWindowQueryMSQComponentSupplier extends DrillComponentSupplier
  {
    public DrillWindowQueryMSQComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    @Override
    public void configureGuice(DruidInjectorBuilder builder)
    {
      super.configureGuice(builder);
      builder.addModules(CalciteMSQTestsHelper.fetchModules(tempDirProducer::newTempFolder, TestGroupByBuffers.createDefault()).toArray(new Module[0]));
    }

    @Override
    public SqlEngine createEngine(
        QueryLifecycleFactory qlf,
        ObjectMapper queryJsonMapper,
        Injector injector
    )
    {
      final WorkerMemoryParameters workerMemoryParameters = MSQTestBase.makeTestWorkerMemoryParameters();
      final MSQTestOverlordServiceClient indexingServiceClient = new MSQTestOverlordServiceClient(
          queryJsonMapper,
          injector,
          new MSQTestTaskActionClient(queryJsonMapper, injector),
          workerMemoryParameters,
          ImmutableList.of()
      );
      return new MSQTaskSqlEngine(indexingServiceClient, queryJsonMapper);
    }
  }

  @Override
  protected QueryTestBuilder testBuilder()
  {
    return new QueryTestBuilder(new CalciteTestConfig(true))
        .addCustomRunner(new ExtractResultsFactory(() -> (MSQTestOverlordServiceClient) ((MSQTaskSqlEngine) queryFramework().engine()).overlordClient()))
        .skipVectorize(true)
        .verifyNativeQueries(new VerifyMSQSupportedNativeQueriesPredicate());
  }
}
