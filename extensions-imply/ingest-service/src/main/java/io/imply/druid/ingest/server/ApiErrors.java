/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import javax.ws.rs.core.Response;
import java.util.Objects;

public class ApiErrors
{
  private static final Logger LOG = new Logger(ApiErrors.class);

  private ApiErrors()
  {
    // no instantiation
  }

  /**
   * Blame the user, log a warning
   */
  public static Response badRequest(String error, Object... formatArgs)
  {
    final String message = StringUtils.nonStrictFormat(error, formatArgs);
    LOG.warn(message);
    return Response.status(Response.Status.BAD_REQUEST).entity(new ErrorResponse(message)).build();
  }

  /**
   * Blame the user with attached exception, log a warning
   */
  public static Response badRequest(Exception e, String error, Object... formatArgs)
  {
    final String message = StringUtils.nonStrictFormat(error, formatArgs);
    LOG.warn(e, message);
    return Response.status(Response.Status.BAD_REQUEST).entity(new ErrorResponse(message)).build();
  }

  /**
   * Oops, something went wrong...
   */
  public static Response serverError(Exception e, String error, Object... formatArgs)
  {
    final String message = StringUtils.nonStrictFormat(error, formatArgs);
    LOG.error(e, message);
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(message)).build();
  }

  // todo: make this be whatever the API guidelines call for, but will work for now
  public static class ErrorResponse
  {
    private final String error;

    @JsonCreator
    public ErrorResponse(
        @JsonProperty("error") String error
    )
    {
      this.error = error;
    }

    @JsonProperty("error")
    public String getError()
    {
      return error;
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
      ErrorResponse errorResponse = (ErrorResponse) o;
      return Objects.equals(error, errorResponse.error);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(error);
    }

    @Override
    public String toString()
    {
      return "ErrorResponse{" +
             "error='" + error + '\'' +
             '}';
    }
  }
}
