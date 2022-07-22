/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.metrics;

import com.google.common.collect.ImmutableList;
import io.imply.clarity.emitter.BaseClarityEmitterConfig;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class ClarityMetricsUtils
{
  private static final String CUSTOM_QUERY_DIMENSION_PREFIX = "context:";

  private static final String PIVOT_PARAMS_KEY = "pivotParams";
  private static final String USER_PARAMS_KEY = "userParams";

  private static final List<String> IMPLY_QUERY_DIMENSIONS = ImmutableList.of(
      "implyView",
      "implyViewTitle",
      "implyFeature",
      "implyDataCube",
      "druidFeature",
      "implyUser",
      "implyUserEmail"
  );

  // dimensions that should be masked if anonymization is enabled
  private static final List<String> IMPLY_SENSITIVE_QUERY_DIMENSIONS = ImmutableList.of(
      "implyUser",
      "implyUserEmail"
  );

  private ClarityMetricsUtils()
  {
    // No instantiations.
  }

  public static void addContextDimensions(
      final BaseClarityEmitterConfig config,
      final BiConsumer<String, String> dimensionSetter,
      final Function<String, Object> dimensionGetter,
      @Nullable QueryContext context
  )
  {
    if (context == null) {
      return;
    }

    final Map<String, Object> userParams = (Map<String, Object>) context.get(USER_PARAMS_KEY);
    final Map<String, Object> pivotParams = Optional.ofNullable(userParams)
                                       .map(param -> (Map<String, Object>) param.get(PIVOT_PARAMS_KEY))
                                       .orElse(null);

    for (final String implyDim : IMPLY_QUERY_DIMENSIONS) {
      final Object value;
      if (pivotParams != null && pivotParams.containsKey(implyDim)) {
        value = pivotParams.get(implyDim);
      } else {
        value = context.get(implyDim);
      }
      if (value != null) {
        dimensionSetter.accept(implyDim, String.valueOf(value));
      }
    }

    for (final String customDimIn : config.getCustomQueryDimensions()) {
      final String customDimOut = CUSTOM_QUERY_DIMENSION_PREFIX + customDimIn;
      final Object value = context.get(customDimIn);

      // Don't overwrite an existing dimension with a custom one.
      final Object existingValue = dimensionGetter.apply(customDimOut);

      if (value != null && existingValue == null) {
        dimensionSetter.accept(customDimOut, String.valueOf(value));
      }
    }

    final Object lane = context.get(QueryContexts.LANE_KEY);

    if (lane != null) {
      dimensionSetter.accept("lane", String.valueOf(lane));
    }

    final Object priority = context.get(QueryContexts.PRIORITY_KEY);
    if (priority != null) {
      dimensionSetter.accept("priority", String.valueOf(priority));
    }

    if (config.isAnonymous()) {
      for (final String sensitiveDim : IMPLY_SENSITIVE_QUERY_DIMENSIONS) {
        final Object value = dimensionGetter.apply(sensitiveDim);
        if (value != null) {
          String anonymizedValue = DigestUtils.sha256Hex(value + "-" + config.getClusterName());
          dimensionSetter.accept(sensitiveDim, anonymizedValue);
        }
      }
    }
  }
}
