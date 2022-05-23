/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

/**
 * Helper class for defining parameters used by the Talaria's "warning framework"
 */
public class TalariaWarnings
{
  /**
   * Query context key for limiting the maximum number of parse exceptions that a Talaria job can generate
   */
  public static final String CTX_MAX_PARSE_EXCEPTIONS_ALLOWED = "maxParseExceptions";

  /**
   * Default number of parse exceptions permissible for a Talaria job
   */
  public static final Long DEFAULT_MAX_PARSE_EXCEPTIONS_ALLOWED = -1L;
}
