/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries;

import org.apache.druid.java.util.common.NonnullPair;

import java.util.LinkedHashMap;
import java.util.Map;

public class SerdeUtils
{
  public static NonnullPair<TimeSeries.EdgePoint, TimeSeries.EdgePoint> getBounds(Object serialized)
  {
    LinkedHashMap<String, Object> object = (LinkedHashMap<String, Object>) serialized;
    Map<String, Map<String, Number>> boundsMap = (Map<String, Map<String, Number>>) object.get("bounds");
    Number startTimestamp = boundsMap.get("start").get("timestamp");
    Number startData = boundsMap.get("start").get("data");
    TimeSeries.EdgePoint start = new TimeSeries.EdgePoint(startTimestamp == null ? -1 : startTimestamp.longValue(),
                                                          startData == null ? -1 : startData.doubleValue());
    Number endTimestamp = boundsMap.get("end").get("timestamp");
    Number endData = boundsMap.get("end").get("data");
    TimeSeries.EdgePoint end = new TimeSeries.EdgePoint(endTimestamp == null ? -1 : endTimestamp.longValue(),
                                                        endData == null ? -1 : endData.doubleValue());
    return new NonnullPair<>(start, end);
  }
}
