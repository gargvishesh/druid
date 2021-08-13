/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

public class SqlAsyncUtil
{
  // Nothing bad will happen if IDs matching this pattern are used as-is in znodes, filenames, etc.
  private static final Pattern SAFE_ID_PATTERN = Pattern.compile("^[a-zA-Z0-9._\\-]{1,128}$");

  private SqlAsyncUtil()
  {
    // No instantiation.
  }

  public static String safeId(final String id)
  {
    if (SAFE_ID_PATTERN.matcher(id).matches()) {
      return id;
    } else {
      return Hashing.sha256().hashString(id, StandardCharsets.UTF_8).toString();
    }
  }
}
