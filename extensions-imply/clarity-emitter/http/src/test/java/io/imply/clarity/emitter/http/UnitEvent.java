/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.http;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.core.Event;
import org.joda.time.DateTime;

import java.util.Map;

public class UnitEvent implements Event
{
  private final String feed;
  private final Number value;
  private final String targetURLKey;
  private final DateTime createdTime;
  private final String nodeType;
  private final String druidVersion;
  private final String clusterName;
  private final String implyVersion;
  private final Map<String, Object> userDims;

  public UnitEvent(
      String feed,
      Number value,
      String targetURLKey,
      String nodeType,
      String druidVersion,
      String clusterName,
      String implyVersion,
      Map<String, Object> userDims
  )
  {
    this.feed = feed;
    this.value = value;
    this.targetURLKey = targetURLKey;
    this.nodeType = nodeType;
    this.druidVersion = druidVersion;
    this.clusterName = clusterName;
    this.implyVersion = implyVersion;
    this.userDims = userDims;

    createdTime = DateTimes.nowUtc();
  }

  @Override
  @JsonValue
  public Map<String, Object> toMap()
  {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.put("feed", feed);
    builder.put("implyNodeType", nodeType);
    builder.put("implyDruidVersion", druidVersion);
    builder.put("implyCluster", clusterName);
    builder.put("implyVersion", implyVersion);
    builder.put("metrics", ImmutableMap.of("value", value));
    builder.putAll(
        Maps.filterEntries(
            userDims,
            new Predicate<Map.Entry<String, Object>>()
            {
              @Override
              public boolean apply(Map.Entry<String, Object> input)
              {
                return input.getKey() != null && input.getValue() != null;
              }
            }
        )
    );

    return builder.build();
  }

  public DateTime getCreatedTime()
  {
    return createdTime;
  }

  @Override
  public String getFeed()
  {
    return targetURLKey;
  }

  public boolean isSafeToBuffer()
  {
    return true;
  }
}
