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
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.common.utils.IdUtils;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

public class SqlAsyncUtil
{
  // Nothing bad will happen if IDs matching this pattern are used as-is in znodes, filenames, etc.
  private static final Pattern SAFE_ID_PATTERN = Pattern.compile("^[a-zA-Z0-9._\\-]{1,128}$");
  static final char BROKER_ID_AND_SQL_QUERY_ID_SEPARATOR_CHAR = '_';
  static final String BROKER_ID_AND_SQL_QUERY_ID_SEPARATOR_STRING = String.valueOf(BROKER_ID_AND_SQL_QUERY_ID_SEPARATOR_CHAR);

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

  public static String createAsyncResultId(String brokerId, String sqlQueryId)
  {
    assert !brokerId.contains(BROKER_ID_AND_SQL_QUERY_ID_SEPARATOR_STRING);
    // Note that the created asyncResultId must start with brokerId follow by BROKER_ID_AND_SQL_QUERY_ID_SEPARATOR
    return IdUtils.getRandomIdWithPrefix(brokerId + BROKER_ID_AND_SQL_QUERY_ID_SEPARATOR_STRING + sqlQueryId);
  }

  public static String getBrokerIdFromAsyncResultId(String asyncResultId)
  {
    assert asyncResultId.contains(BROKER_ID_AND_SQL_QUERY_ID_SEPARATOR_STRING);
    return StringUtils.substringBefore(asyncResultId, BROKER_ID_AND_SQL_QUERY_ID_SEPARATOR_STRING);
  }
}
