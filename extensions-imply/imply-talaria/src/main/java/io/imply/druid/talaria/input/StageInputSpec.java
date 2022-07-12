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

import java.util.Objects;

/**
 * Input spec representing another stage in the same query.
 */
@JsonTypeName("stage")
public class StageInputSpec implements InputSpec
{
  private final int stage;

  @JsonCreator
  public StageInputSpec(@JsonProperty("stage") int stageNumber)
  {
    this.stage = stageNumber;
  }

  @JsonProperty("stage")
  public int getStageNumber()
  {
    return stage;
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
    StageInputSpec that = (StageInputSpec) o;
    return stage == that.stage;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stage);
  }

  @Override
  public String toString()
  {
    return "StageInputSpec{" +
           "stage=" + stage +
           '}';
  }
}
