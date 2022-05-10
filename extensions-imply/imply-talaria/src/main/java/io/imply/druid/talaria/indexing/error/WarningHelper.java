/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WarningHelper
{
  public static final String CTX_MAX_PARSE_EXCEPTIONS_ALLOWED = "maxParseExceptions";
  public static final Long DEFAULT_MAX_PARSE_EXCEPTIONS_ALLOWED = -1L;

  public static class FaultsExceededChecker
  {
    final Map<String, Long> maxFaultsAllowedCount;
    final Map<String, Long> currentFaultsEncounteredCount = new HashMap<>();

    public FaultsExceededChecker(final Map<String, Long> maxFaultsAllowedCount)
    {
      maxFaultsAllowedCount.forEach((ignored, count) ->
                                        Preconditions.checkArgument(count >= 0 || count == -1, "invalid count")
      );
      this.maxFaultsAllowedCount = maxFaultsAllowedCount;
    }

    public boolean addFaultsAndCheckIfExceeded(final List<String> talariaFaults)
    {
      boolean ret = false;
      for (final String talariaFault : talariaFaults) {
        Long limit = maxFaultsAllowedCount.getOrDefault(talariaFault, -1L);
        currentFaultsEncounteredCount.compute(
            talariaFault,
            (ignored, currentCount) -> currentCount == null ? 1L : currentCount + 1L
        );
        if (limit != -1) {
          if (currentFaultsEncounteredCount.get(talariaFault) > limit) {
            ret = true;
          }
        }
      }
      return ret;
    }
  }
}
