/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.aggregation.postprocessors;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.imply.druid.timeseries.SimpleTimeSeries;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "interpolation", value = InterpolatorTimeSeriesFn.class),
    @JsonSubTypes.Type(name = "timeWeightedAverage", value = TimeWeightedAvgTimeSeriesFn.class)
})
public interface TimeSeriesFn
{
  SimpleTimeSeries compute(SimpleTimeSeries input, int maxEntries);

  String cacheString();
}
