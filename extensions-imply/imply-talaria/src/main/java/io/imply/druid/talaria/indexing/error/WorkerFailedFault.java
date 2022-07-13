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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nullable;
import java.util.Objects;

@JsonTypeName(WorkerFailedFault.CODE)
public class WorkerFailedFault extends BaseTalariaFault
{
  public static final String CODE = "WorkerFailed";

  private final String workerTaskId;

  @Nullable
  private final String errorMsg;

  @JsonCreator
  public WorkerFailedFault(
      @JsonProperty("workerTaskId") final String workerTaskId,
      @JsonProperty("errorMsg") @Nullable final String errorMsg
  )
  {
    super(CODE, "Worker task failed: [%s]%s", workerTaskId, errorMsg != null ? " (" + errorMsg + ")" : "");
    this.workerTaskId = workerTaskId;
    this.errorMsg = errorMsg;
  }

  @JsonProperty
  public String getWorkerTaskId()
  {
    return workerTaskId;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getErrorMsg()
  {
    return errorMsg;
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
    WorkerFailedFault that = (WorkerFailedFault) o;
    return Objects.equals(workerTaskId, that.workerTaskId) && Objects.equals(errorMsg, that.errorMsg);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), workerTaskId, errorMsg);
  }
}
