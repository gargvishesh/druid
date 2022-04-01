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

@JsonTypeName(WorkerRpcFailedFault.CODE)
public class WorkerRpcFailedFault extends BaseTalariaFault
{
  static final String CODE = "WorkerRpcFailed";

  private final String workerTaskId;

  @JsonCreator
  public WorkerRpcFailedFault(@JsonProperty("workerTaskId") final String workerTaskId)
  {
    super(CODE, "RPC call to task failed unrecoverably: [%s]", workerTaskId);
    this.workerTaskId = workerTaskId;
  }

  @JsonProperty
  public String getWorkerTaskId()
  {
    return workerTaskId;
  }
}
