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

package org.apache.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * This class is just used to pass the strategy type via the "type" parameter for deserilization to appropriate
 * {@link org.apache.druid.indexing.common.task.CompactionRunner} subtype at the overlod.
 */
public class ClientCompactionRunnerInfo
{
  private final CompactionEngine type;

  @JsonCreator
  public ClientCompactionRunnerInfo(@JsonProperty("type") CompactionEngine type)
  {
    this.type = type;
  }

  @JsonProperty
  public CompactionEngine getType()
  {
    return type;
  }

  @Override
  public String toString()
  {
    return "ClientCompactionRunnerInfo{" +
           "type=" + type +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClientCompactionRunnerInfo that = (ClientCompactionRunnerInfo) o;
    return type == that.type;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(type);
  }

  public static CompactionConfigValidationResult validateCompactionConfig(
      DataSourceCompactionConfig newConfig,
      CompactionEngine defaultCompactionEngine
  )
  {
    CompactionEngine compactionEngine = newConfig.getEngine() == null ? defaultCompactionEngine : newConfig.getEngine();
    if (compactionEngine == CompactionEngine.NATIVE) {
      return new CompactionConfigValidationResult(true, null);
    } else {
      return compactionConfigSupportedByMSQEngine(newConfig);
    }
  }

  /**
   * Checks if the provided compaction config is supported by MSQ. The following configs aren't supported:
   * <ul>
   * <li>partitionsSpec of type HashedParititionsSpec.</li>
   * <li>maxTotalRows in DynamicPartitionsSpec.</li>
   * <li>rollup set to false in granularitySpec when metricsSpec is specified. Null is treated as true.</li>
   * <li>queryGranularity set to ALL in granularitySpec.</li>
   * <li>Each metric has output column name same as the input name.</li>
   * </ul>
   */
  private static CompactionConfigValidationResult compactionConfigSupportedByMSQEngine(DataSourceCompactionConfig newConfig)
  {
    List<CompactionConfigValidationResult> validationResults = new ArrayList<>();
    if (newConfig.getTuningConfig() != null) {
      validationResults.add(validatePartitionsSpecForMSQ(newConfig.getTuningConfig().getPartitionsSpec()));
    }
    if (newConfig.getGranularitySpec() != null) {
      validationResults.add(validateRollupForMSQ(
          newConfig.getMetricsSpec(),
          newConfig.getGranularitySpec().isRollup()
      ));
    }
    validationResults.add(validateMaxNumTasksForMSQ(newConfig.getTaskContext()));
    return validationResults.stream()
                            .filter(result -> !result.isValid())
                            .findFirst()
                            .orElse(new CompactionConfigValidationResult(true, null));
  }

  /**
   * Validate that partitionSpec is either 'dynamic` or 'range', and if 'dynamic', ensure 'maxTotalRows' is null.
   */
  public static CompactionConfigValidationResult validatePartitionsSpecForMSQ(PartitionsSpec partitionsSpec)
  {
    if (!(partitionsSpec instanceof DimensionRangePartitionsSpec
          || partitionsSpec instanceof DynamicPartitionsSpec)) {
      return new CompactionConfigValidationResult(
          false,
          "Invalid partitionsSpec type[%s] for MSQ engine. Type must be either 'dynamic' or 'range'.",
          partitionsSpec.getClass().getSimpleName()

      );
    }
    if (partitionsSpec instanceof DynamicPartitionsSpec
        && ((DynamicPartitionsSpec) partitionsSpec).getMaxTotalRows() != null) {
      return new CompactionConfigValidationResult(
          false,
          "maxTotalRows[%d] in DynamicPartitionsSpec not supported for MSQ engine.",
          ((DynamicPartitionsSpec) partitionsSpec).getMaxTotalRows()
      );
    }
    return new CompactionConfigValidationResult(true, null);
  }

  /**
   * Validate rollup is set to false in granularitySpec when metricsSpec is specified.
   */
  public static CompactionConfigValidationResult validateRollupForMSQ(
      AggregatorFactory[] metricsSpec,
      @Nullable Boolean isRollup
  )
  {
    if (metricsSpec != null && isRollup != null && !isRollup) {
      return new CompactionConfigValidationResult(
          false,
          "rollup in granularitySpec must be set to True if metricsSpec is specifed for MSQ engine."
      );
    }
    return new CompactionConfigValidationResult(true, null);
  }

  /**
   * Validate maxNumTasks >= 2 in context.
   */
  public static CompactionConfigValidationResult validateMaxNumTasksForMSQ(Map<String, Object> context)
  {
    if (context != null) {
      int maxNumTasks = QueryContext.of(context)
                                    .getInt(ClientMSQContext.CTX_MAX_NUM_TASKS, ClientMSQContext.DEFAULT_MAX_NUM_TASKS);
      if (maxNumTasks < 2) {
        return new CompactionConfigValidationResult(false,
                                                    "MSQ context maxNumTasks [%,d] cannot be less than 2, "
                                                    + "since at least 1 controller and 1 worker is necessary.",
                                                    maxNumTasks
        );
      }
    }
    return new CompactionConfigValidationResult(true, null);
  }
}
