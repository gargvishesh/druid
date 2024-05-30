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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryContext;

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

  /**
   * Checks if the provided compaction config is supported by the runner
   * @param newConfig The updated compaction config
   * @param engineSource String indicating the source of compaction engine.
   * @return Pair of support boolean and reason string. The reason string is empty if support boolean is True.
   */
  public static NonnullPair<Boolean, String> supportsCompactionConfig(DataSourceCompactionConfig newConfig, String engineSource)
  {
    CompactionEngine compactionEngine = newConfig.getEngine();
    if (compactionEngine == CompactionEngine.MSQ) {
      if (newConfig.getTuningConfig() != null) {
        PartitionsSpec partitionsSpec = newConfig.getTuningConfig().getPartitionsSpec();
        if (!(partitionsSpec instanceof DimensionRangePartitionsSpec
              || partitionsSpec instanceof DynamicPartitionsSpec)) {
          return new NonnullPair<>(false, StringUtils.format(
              "Invalid partition spec type[%s] for MSQ compaction engine[%s]."
              + " Type must be either DynamicPartitionsSpec or DynamicRangePartitionsSpec.",
              partitionsSpec.getClass(),
              engineSource
          )
          );
        }
        if (partitionsSpec instanceof DynamicPartitionsSpec
            && ((DynamicPartitionsSpec) partitionsSpec).getMaxTotalRows() != null) {
          return new NonnullPair<>(false, StringUtils.format(
              "maxTotalRows[%d] in DynamicPartitionsSpec not supported for MSQ compaction engine[%s].",
              ((DynamicPartitionsSpec) partitionsSpec).getMaxTotalRows(), engineSource
          ));
        }
      }

      if (newConfig.getMetricsSpec() != null
          && newConfig.getGranularitySpec() != null
          && !newConfig.getGranularitySpec()
                       .isRollup()) {
        return new NonnullPair<>(false, StringUtils.format(
            "rollup in granularitySpec must be set to True if metricsSpec is specifed "
            + "for MSQ compaction engine[%s].", engineSource));
      }

      QueryContext queryContext = QueryContext.of(newConfig.getTaskContext());

      if (!queryContext.getBoolean(MSQContext.CTX_FINALIZE_AGGREGATIONS, true)) {
        return new NonnullPair<>(false, StringUtils.format(
            "Config[%s] cannot be set to false for auto-compaction with MSQ engine[%s].",
            MSQContext.CTX_FINALIZE_AGGREGATIONS,
            engineSource
        ));
      }

      if (queryContext.getString(MSQContext.CTX_TASK_ASSIGNMENT_STRATEGY, MSQContext.TASK_ASSIGNMENT_STRATEGY_MAX)
                      .equals(MSQContext.TASK_ASSIGNMENT_STRATEGY_AUTO)) {
        return new NonnullPair<>(false, StringUtils.format(
            "Config[%s] cannot be set to value[%s] for auto-compaction with MSQ engine[%s].",
            MSQContext.CTX_TASK_ASSIGNMENT_STRATEGY,
            MSQContext.TASK_ASSIGNMENT_STRATEGY_AUTO,
            engineSource
        ));
      }
    }
    return new NonnullPair<>(true, "");
  }

  /**
   * This class copies over MSQ context parameters from the MSQ extension. This is required to validate the submitted
   * compaction config at the coordinator. The values used here should be kept in sync with those in
   * {@link org.apache.druid.msq.util.MultiStageQueryContext}
   */
  public static class MSQContext
  {
    public static final String CTX_FINALIZE_AGGREGATIONS = "finalizeAggregations";
    public static final String CTX_TASK_ASSIGNMENT_STRATEGY = "taskAssignment";
    public static final String TASK_ASSIGNMENT_STRATEGY_MAX = "MAX";
    public static final String TASK_ASSIGNMENT_STRATEGY_AUTO = "AUTO";
  }
}
