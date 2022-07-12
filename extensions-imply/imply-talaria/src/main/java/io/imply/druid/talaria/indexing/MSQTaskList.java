/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;

public class MSQTaskList
{
  private final List<String> taskIds;

  @JsonCreator
  public MSQTaskList(@JsonProperty("taskIds") List<String> taskIds)
  {
    this.taskIds = Preconditions.checkNotNull(taskIds, "taskIds");
  }

  @JsonProperty
  public List<String> getTaskIds()
  {
    return taskIds;
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
    MSQTaskList that = (MSQTaskList) o;
    return Objects.equals(taskIds, that.taskIds);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(taskIds);
  }

  @Override
  public String toString()
  {
    return "MSQTaskList{" +
           "taskIds=" + taskIds +
           '}';
  }
}
