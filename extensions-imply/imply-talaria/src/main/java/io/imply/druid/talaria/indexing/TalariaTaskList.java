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

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class TalariaTaskList
{
  @Nullable
  private final List<String> taskIds;

  @JsonCreator
  public TalariaTaskList(@Nullable @JsonProperty("taskIds") List<String> taskIds)
  {
    this.taskIds = taskIds;
  }

  @Nullable
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
    TalariaTaskList that = (TalariaTaskList) o;
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
    return "TalariaTaskList{" +
           "taskIds=" + taskIds +
           '}';
  }
}
