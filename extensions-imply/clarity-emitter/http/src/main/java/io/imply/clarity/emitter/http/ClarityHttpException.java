/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.http;

import org.apache.druid.java.util.common.StringUtils;

public class ClarityHttpException extends Exception
{
  private final boolean quiet;

  private ClarityHttpException(String message, boolean quiet)
  {
    super(message);
    this.quiet = quiet;
  }

  public static ClarityHttpException info(String message, Object... params)
  {
    return new ClarityHttpException(StringUtils.nonStrictFormat(message, params), true);
  }

  public static ClarityHttpException warn(String message, Object... params)
  {
    return new ClarityHttpException(StringUtils.nonStrictFormat(message, params), false);
  }

  public boolean isQuiet()
  {
    return quiet;
  }
}
