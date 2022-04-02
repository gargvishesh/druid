/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Context keys that trigger different behaviors during an INSERT.
 */
public class TalariaInsertContextKeys
{
  /**
   * Controls sort order within segments. Normally, this is the same as the overall order of the query (from the
   * CLUSTERED BY clause) but it can be overridden.
   */
  public static final String CTX_SORT_ORDER = "talariaSegmentSortOrder";

  private static final Pattern LOOKS_LIKE_JSON_ARRAY = Pattern.compile("^\\s*\\[.*", Pattern.DOTALL);

  private TalariaInsertContextKeys()
  {
    // No instantiation.
  }

  /**
   * Decodes {@link #CTX_SORT_ORDER} from either a JSON or CSV string.
   */
  public static List<String> decodeSortOrder(@Nullable final String sortOrderString)
  {
    if (sortOrderString == null) {
      return Collections.emptyList();
    } else if (LOOKS_LIKE_JSON_ARRAY.matcher(sortOrderString).matches()) {
      try {
        // Not caching this ObjectMapper in a static, because we expect to use it infrequently (once per INSERT
        // query that uses this feature) and there is no need to keep it around longer than that.
        return new ObjectMapper().readValue(sortOrderString, new TypeReference<List<String>>() {});
      }
      catch (JsonProcessingException e) {
        throw new IAE("Invalid JSON provided for [%s]", CTX_SORT_ORDER);
      }
    } else {
      final RFC4180Parser csvParser = new RFC4180ParserBuilder().withSeparator(',').build();

      try {
        return Arrays.stream(csvParser.parseLine(sortOrderString))
                     .filter(s -> s != null && !s.isEmpty())
                     .map(String::trim)
                     .collect(Collectors.toList());
      }
      catch (IOException e) {
        throw new IAE("Invalid CSV provided for [%s]", CTX_SORT_ORDER);
      }
    }
  }
}
