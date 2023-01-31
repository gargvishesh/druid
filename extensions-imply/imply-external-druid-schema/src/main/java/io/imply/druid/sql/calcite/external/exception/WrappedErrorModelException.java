/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.external.exception;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.ws.rs.core.Response;

public class WrappedErrorModelException extends RuntimeException
{

  @JsonProperty
  private ErrorModel error;

  public ErrorModel getImplyError()
  {
    return error;
  }

  public Response.Status getHttpStatus()
  {
    return Response.Status.BAD_REQUEST;
  }
}
