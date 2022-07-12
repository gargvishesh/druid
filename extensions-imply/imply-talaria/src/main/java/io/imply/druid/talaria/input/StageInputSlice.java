/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.imply.druid.talaria.kernel.ReadablePartitions;

import java.util.Objects;

/**
 * Input slice representing some partitions of a stage.
 *
 * Sliced from {@link StageInputSpec} by {@link StageInputSpecSlicer}.
 */
@JsonTypeName("stage")
public class StageInputSlice implements InputSlice
{
  private final int stage;
  private final ReadablePartitions partitions;

  @JsonCreator
  public StageInputSlice(
      @JsonProperty("stage") int stageNumber,
      @JsonProperty("partitions") ReadablePartitions partitions
  )
  {
    this.stage = stageNumber;
    this.partitions = Preconditions.checkNotNull(partitions, "partitions");
  }

  @JsonProperty("stage")
  public int getStageNumber()
  {
    return stage;
  }

  @JsonProperty("partitions")
  public ReadablePartitions getPartitions()
  {
    return partitions;
  }

  @Override
  public int numFiles()
  {
    return 0;
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
    StageInputSlice that = (StageInputSlice) o;
    return stage == that.stage && Objects.equals(partitions, that.partitions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stage, partitions);
  }

  @Override
  public String toString()
  {
    return "StageInputSpec{" +
           "stage=" + stage +
           ", partitions=" + partitions +
           '}';
  }
}
