/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity;

import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.core.EventMap;

import java.util.Map;

public class TestEvent implements Event
{
  private final String feed;
  private final String identity;
  private final String remoteAddress;
  private final Map<String, Object> metricsMap;

  public TestEvent(
      String identity,
      String remoteAddress,
      String feed,
      Map<String, Object> metricsMap
  )
  {
    this.identity = identity;
    this.remoteAddress = remoteAddress;
    this.feed = feed;
    this.metricsMap = metricsMap;
  }

  @Override
  @JsonValue
  public EventMap toMap()
  {
    return EventMap.builder()
                   .put("feed", feed)
                   .put("identity", identity)
                   .put("remoteAddress", remoteAddress)
                   .put("metrics", metricsMap)
                   .build();
  }

  @Override
  public String getFeed()
  {
    return feed;
  }
}