/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.talaria.indexing.error.TalariaWarnings;
import io.imply.druid.talaria.util.TalariaContext;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryContext;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public enum TalariaMode
{

  NON_STRICT_MODE("nonStrict", ImmutableMap.of(TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, -1)),
  STRICT_MODE("strict", ImmutableMap.of(TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 0));

  private final String value;
  private final Map<String, Object> defaultQueryContext;

  private static final Logger log = new Logger(TalariaMode.class);

  public static final String CTX_TALARIA_MODE = "mode";
  public static final List<String> CTX_TALARIA_MODE_LEGACY_ALIASES = ImmutableList.of("msqMode");
  public static final String CTX_TALARIA_MODE_DEFAULT = "strict";

  TalariaMode(final String value, final Map<String, Object> defaultQueryContext)
  {
    this.value = value;
    this.defaultQueryContext = new HashMap<>(defaultQueryContext);
  }

  public static String getTalariaMode(QueryContext queryContext)
  {
    return (String) TalariaContext.getValueFromPropertyMap(
        queryContext.getMergedParams(),
        CTX_TALARIA_MODE,
        CTX_TALARIA_MODE_LEGACY_ALIASES,
        "strict"
    );
  }

  @Nullable
  public static TalariaMode fromString(String str)
  {
    for (TalariaMode talariaMode : TalariaMode.values()) {
      if (talariaMode.value.equalsIgnoreCase(str)) {
        return talariaMode;
      }
    }
    return null;
  }


  public static void populateDefaultQueryContext(final String modeStr, final QueryContext originalQueryContext)
  {
    TalariaMode mode = TalariaMode.fromString(modeStr);
    if (mode == null) {
      throw new ISE(
          "%s is an unknown talaria mode. Acceptable modes: %s",
          modeStr,
          Arrays.stream(TalariaMode.values()).map(m -> m.value).collect(Collectors.toList())
      );
    }
    Map<String, Object> defaultQueryContext = mode.defaultQueryContext;
    log.debug("Populating default query context with %s for the %s talaria mode", defaultQueryContext, mode);
    originalQueryContext.addDefaultParams(defaultQueryContext);
  }
}
