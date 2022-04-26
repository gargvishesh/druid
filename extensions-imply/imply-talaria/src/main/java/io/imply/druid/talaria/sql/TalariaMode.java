/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.sql;

import com.google.common.collect.ImmutableMap;
import io.imply.druid.talaria.indexing.error.WarningsHelper;
import org.apache.druid.java.util.common.ISE;

import java.util.HashMap;
import java.util.Map;

public class TalariaMode
{
  public static final String LENIENT_MODE = "lenient";
  public static final String STRICT_MODE = "strict";

  private static final Map<String, Map<String, Object>> modeToDefaultQueryContextMap = new HashMap<>();

  static {
    Map<String, Object> lenientModeDefaultQueryContextMap =
        ImmutableMap.<String, Object>builder()
                    .put(WarningsHelper.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, -1)
                    .build();

    Map<String, Object> strictModeDefaultQueryContextMap =
        ImmutableMap.<String, Object>builder()
                    .put(WarningsHelper.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 0)
                    .build();

    modeToDefaultQueryContextMap.put(LENIENT_MODE, lenientModeDefaultQueryContextMap);
    modeToDefaultQueryContextMap.put(STRICT_MODE, strictModeDefaultQueryContextMap);
  }

  public static Map<String, Object> backfillQueryContext(String mode, Map<String, Object> originalQueryContext)
  {
    if (!modeToDefaultQueryContextMap.containsKey(mode)) {
      throw new ISE("%s is not a valid talaria mode", mode);
    }
    final Map<String, Object> newContext = new HashMap<>(modeToDefaultQueryContextMap.get(mode));
    newContext.putAll(originalQueryContext);
    return newContext;
  }
}
