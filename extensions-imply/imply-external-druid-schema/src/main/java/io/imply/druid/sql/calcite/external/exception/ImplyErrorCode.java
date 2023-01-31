/*
 * Copyright (c) 2021 Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information of Imply Data, Inc.
 */

package io.imply.druid.sql.calcite.external.exception;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * A high-level error code.
 * <p>
 * These error codes are intended to be very high-level: they cover things like "NotFound", and **NOT** something
 * like, "FileNotFound". The intention is that a client does not need to know about service- or domain-specific errors
 * to handle an error. But see {@link ErrorModel} and {@link InnerError} for how to provide more specific errors.
 * <p>
 * See <a href="https://implydata.atlassian.net/l/cp/ijtwNKB1">Imply NextGen Error code and error message</a>.
 */
public enum ImplyErrorCode
{
  ALREADY_EXISTS("AlreadyExists"), // Resource cannot be created because it already exists.
  BAD_ARGUMENT("BadArgument"), // An argument was invalid, or a required argument was absent.
  CLIENT_CLOSED_REQUEST("ClientClosedRequest"), // Client closed the connection / request
  DOWN_FOR_MAINTENANCE("DownForMaintenance"), // Polaris is down for maintenance.
  INTERNAL_ERROR("InternalError"), // An internal error occurred. May be transient.
  NOT_FOUND("NotFound"), // The requested resource wasn't found.
  OPERATION_DISABLED("OperationDisabled"), // The operation is disabled.
  OPERATION_NO_LONGER_SUPPORTED("OperationNoLongerSupported"), // The operation is no longer supported.
  REQUEST_TIMEOUT("RequestTimeout"), // Timeout while parsing request.
  RESOURCE_EXHAUSTED("ResourceExhausted"), // Resource limits exceeded.
  RESOURCE_IN_USE("ResourceInUse"), // Resource has an exclusive lock or is otherwise busy.
  UNAUTHORIZED("Unauthorized"); // Insufficient permission to perform an operation.

  private final String value;

  ImplyErrorCode(final String value)
  {
    this.value = value;
  }

  public String value()
  {
    return value;
  }

  @Override
  @JsonValue
  public String toString()
  {
    return value;
  }

  @JsonCreator
  public static ImplyErrorCode fromValue(final String value)
  {
    for (ImplyErrorCode code : ImplyErrorCode.values()) {
      if (code.value.equals(value)) {
        return code;
      }
    }
    throw new IllegalArgumentException("Unexpected value '" + value + "'");
  }
}
