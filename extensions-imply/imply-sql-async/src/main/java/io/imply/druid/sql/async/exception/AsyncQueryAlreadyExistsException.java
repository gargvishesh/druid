/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.exception;

import org.apache.druid.java.util.common.StringUtils;

public class AsyncQueryAlreadyExistsException extends Exception
{
  public AsyncQueryAlreadyExistsException(final String asyncResultId)
  {
    super(StringUtils.format("Async query [%s] already exists", asyncResultId));
  }
}