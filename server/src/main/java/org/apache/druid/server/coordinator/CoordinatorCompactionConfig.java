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
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class CoordinatorCompactionConfig
{
  public static final String CONFIG_KEY = "coordinator.compaction.config";

  private static final double DEFAULT_COMPACTION_TASK_RATIO = 0.1;
  private static final int DEFAILT_MAX_COMPACTION_TASK_SLOTS = Integer.MAX_VALUE;
  private static final boolean DEFAULT_USE_AUTO_SCALE_SLOTS = false;
  private static final DataSourceCompactionConfig.Engine DEFAULT_COMPACTION_ENGINE = DataSourceCompactionConfig.Engine.NATIVE;

  private final List<DataSourceCompactionConfig> compactionConfigs;
  private final double compactionTaskSlotRatio;
  private final int maxCompactionTaskSlots;
  private final boolean useAutoScaleSlots;
  private final DataSourceCompactionConfig.Engine engine;

  public static CoordinatorCompactionConfig from(
      CoordinatorCompactionConfig baseConfig,
      List<DataSourceCompactionConfig> compactionConfigs
  )
  {
    return new CoordinatorCompactionConfig(
        compactionConfigs,
        baseConfig.compactionTaskSlotRatio,
        baseConfig.maxCompactionTaskSlots,
        baseConfig.useAutoScaleSlots,
        baseConfig.engine
    );
  }

  public static CoordinatorCompactionConfig from(
      CoordinatorCompactionConfig baseConfig,
      @Nullable Double compactionTaskSlotRatio,
      @Nullable Integer maxCompactionTaskSlots,
      @Nullable Boolean useAutoScaleSlots,
      @Nullable DataSourceCompactionConfig.Engine engine
  )
  {
    return new CoordinatorCompactionConfig(
        baseConfig.compactionConfigs,
        compactionTaskSlotRatio == null ? baseConfig.compactionTaskSlotRatio : compactionTaskSlotRatio,
        maxCompactionTaskSlots == null ? baseConfig.maxCompactionTaskSlots : maxCompactionTaskSlots,
        useAutoScaleSlots == null ? baseConfig.useAutoScaleSlots : useAutoScaleSlots,
        engine == null ? baseConfig.engine : engine
    );
  }

  public static CoordinatorCompactionConfig from(List<DataSourceCompactionConfig> compactionConfigs)
  {
    return new CoordinatorCompactionConfig(compactionConfigs, null, null, null, null);
  }

  public static CoordinatorCompactionConfig empty()
  {
    return new CoordinatorCompactionConfig(ImmutableList.of(), null, null, null, null);
  }

  @JsonCreator
  public CoordinatorCompactionConfig(
      @JsonProperty("compactionConfigs") List<DataSourceCompactionConfig> compactionConfigs,
      @JsonProperty("compactionTaskSlotRatio") @Nullable Double compactionTaskSlotRatio,
      @JsonProperty("maxCompactionTaskSlots") @Nullable Integer maxCompactionTaskSlots,
      @JsonProperty("useAutoScaleSlots") @Nullable Boolean useAutoScaleSlots,
      @JsonProperty("engine") @Nullable final DataSourceCompactionConfig.Engine engine
  )
  {
    this.compactionConfigs = compactionConfigs;
    this.compactionTaskSlotRatio = compactionTaskSlotRatio == null ? DEFAULT_COMPACTION_TASK_RATIO :
                                   compactionTaskSlotRatio;
    this.maxCompactionTaskSlots = maxCompactionTaskSlots == null ? DEFAILT_MAX_COMPACTION_TASK_SLOTS :
                                  maxCompactionTaskSlots;
    this.useAutoScaleSlots = useAutoScaleSlots == null ? DEFAULT_USE_AUTO_SCALE_SLOTS : useAutoScaleSlots;
    this.engine = engine == null ? DEFAULT_COMPACTION_ENGINE : engine;
  }

  @JsonProperty
  public List<DataSourceCompactionConfig> getCompactionConfigs()
  {
    return compactionConfigs;
  }

  @JsonProperty
  public double getCompactionTaskSlotRatio()
  {
    return compactionTaskSlotRatio;
  }

  @JsonProperty
  public int getMaxCompactionTaskSlots()
  {
    return maxCompactionTaskSlots;
  }

  @JsonProperty
  public boolean isUseAutoScaleSlots()
  {
    return useAutoScaleSlots;
  }

  @JsonProperty
  public DataSourceCompactionConfig.Engine getEngine()
  {
    return engine;
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
    CoordinatorCompactionConfig that = (CoordinatorCompactionConfig) o;
    return Double.compare(that.compactionTaskSlotRatio, compactionTaskSlotRatio) == 0 &&
           maxCompactionTaskSlots == that.maxCompactionTaskSlots &&
           useAutoScaleSlots == that.useAutoScaleSlots &&
           engine == that.engine &&
           Objects.equals(compactionConfigs, that.compactionConfigs);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(compactionConfigs, compactionTaskSlotRatio, maxCompactionTaskSlots, useAutoScaleSlots, engine);
  }

  @Override
  public String toString()
  {
    return "CoordinatorCompactionConfig{" +
           "compactionConfigs=" + compactionConfigs +
           ", compactionTaskSlotRatio=" + compactionTaskSlotRatio +
           ", maxCompactionTaskSlots=" + maxCompactionTaskSlots +
           ", useAutoScaleSlots=" + useAutoScaleSlots +
           ", engine=" + engine +
           '}';
  }
}
