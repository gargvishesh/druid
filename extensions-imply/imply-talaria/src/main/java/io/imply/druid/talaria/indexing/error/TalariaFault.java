/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;

/**
 * Error code for Talaria queries.
 *
 * See {@link TalariaErrorReport#getFaultFromException} for a mapping of exception type to error code.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "errorCode")
public interface TalariaFault
{
  String getErrorCode();

  @Nullable
  String getErrorMessage();

  default String getCodeWithMessage()
  {
    if (getErrorMessage() != null) {
      return getErrorCode() + ": " + getErrorMessage();
    } else {
      return getErrorCode();
    }
  }
}
