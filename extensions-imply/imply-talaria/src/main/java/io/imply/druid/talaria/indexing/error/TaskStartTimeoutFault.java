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
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.imply.druid.talaria.util.TalariaContext;

@JsonTypeName(TaskStartTimeoutFault.CODE)
public class TaskStartTimeoutFault extends BaseTalariaFault
{
  static final String CODE = "TaskStartTimeout";

  @JsonCreator
  public TaskStartTimeoutFault(int numTasks)
  {
    super(
        CODE,
        "Unable to launch all the worker tasks in time. There might be insufficient available slots to start all the worker tasks simultaneously."
        + " Try splitting up the query into smaller chunks with lesser %s[%d] tasks. Another option is to increase capacity.",
        TalariaContext.CTX_MAX_NUM_CONCURRENT_SUB_TASKS,
        numTasks
    );
  }
}
