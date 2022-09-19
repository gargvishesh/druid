/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.postaggregators;

import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import io.imply.druid.timeseries.TimeSeries;

public class PostAggregatorsUtil
{
  private PostAggregatorsUtil()
  {
  }

  public static TimeSeries<?> asTimeSeries(Object computedField)
  {
    if (computedField instanceof SimpleTimeSeriesContainer) {
      computedField = ((SimpleTimeSeriesContainer) computedField).getSimpleTimeSeries();
    }

    TimeSeries<?> timeSeries = ((TimeSeries<?>) computedField);

    return timeSeries;
  }
}
