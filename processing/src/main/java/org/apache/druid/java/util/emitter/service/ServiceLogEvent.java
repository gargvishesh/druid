/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.apache.druid.java.util.emitter.service;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.core.EventMap;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

/**
 * Event that is meant to be structured and be emitted elsewhere.
 */
@PublicApi
public class ServiceLogEvent implements Event
{
  private static final String FEED = "service-logs";
  private final DateTime createdTime;
  private final ImmutableMap<String, String> serviceDims;
  private final Map<String, Object> data;
  private final String feed;

  private ServiceLogEvent(
      DateTime createdTime,
      ImmutableMap<String, String> serviceDimensions,
      Map<String, Object> data,
      String feed
  )
  {
    this.createdTime = createdTime;
    this.serviceDims = serviceDimensions;
    this.data = Maps.filterEntries(data, map -> map.getKey() != null);
    this.feed = feed;
  }

  @Override
  @JsonValue
  public EventMap toMap()
  {
    return EventMap
        .builder()
        .put("feed", getFeed())
        .put("timestamp", createdTime.toString())
        .putAll(serviceDims)
        .put("data", data)
        .build();
  }

  @Override
  public String getFeed()
  {
    return FEED;
  }

  public String getService()
  {
    return serviceDims.get("service");
  }

  public String getHost()
  {
    return serviceDims.get("host");
  }

  public Map<String, Object> getData()
  {
    return ImmutableMap.copyOf(data);
  }

  public static class Builder
  {
    private final Map<String, Object> userDims = new TreeMap<>();
    private String feed = FEED;

    public ServiceLogEvent.Builder setFeed(String feed)
    {
      this.feed = feed;
      return this;
    }

    public ServiceLogEvent.Builder setDimensions(Map<String, Object> dimensions)
    {
      userDims.putAll(dimensions);
      return this;
    }

    public ServiceLogEvent.Builder setDimension(String dim, String[] values)
    {
      userDims.put(dim, Arrays.asList(values));
      return this;
    }

    public ServiceLogEvent.Builder setDimensionIfNotNull(String dim, Object value)
    {
      if (value != null) {
        userDims.put(dim, value);
      }
      return this;
    }

    public ServiceLogEvent.Builder setDimension(String dim, Object value)
    {
      userDims.put(dim, value);
      return this;
    }

    public Object getDimension(String dim)
    {
      return userDims.get(dim);
    }

    public ServiceEventBuilder<ServiceLogEvent> build(
        final DateTime createdTime
    )
    {

      return new ServiceEventBuilder<ServiceLogEvent>()
      {
        @Override
        public ServiceLogEvent build(ImmutableMap<String, String> serviceDimensions)
        {
          return new ServiceLogEvent(
              createdTime,
              serviceDimensions,
              userDims,
              feed
          );
        }
      };
    }
  }
}
