/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.imply.druid.talaria.util.TalariaContext;

import java.util.Objects;

@JsonTypeName(TaskStartTimeoutFault.CODE)
public class TaskStartTimeoutFault extends BaseTalariaFault
{
  static final String CODE = "TaskStartTimeout";

  private final int numTasks;

  @JsonCreator
  public TaskStartTimeoutFault(@JsonProperty("numTasks") int numTasks)
  {
    super(
        CODE,
        "Unable to launch all the worker tasks in time. There might be insufficient available slots to start all the worker tasks simultaneously."
        + " Try lowering '%s' in your query context to lower than [%d] tasks, or increasing capacity.",
        TalariaContext.CTX_MAX_NUM_TASKS,
        numTasks
    );
    this.numTasks = numTasks;
  }

  @JsonProperty
  public int getNumTasks()
  {
    return numTasks;
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
    if (!super.equals(o)) {
      return false;
    }
    TaskStartTimeoutFault that = (TaskStartTimeoutFault) o;
    return numTasks == that.numTasks;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), numTasks);
  }
}
