/*
 * Copyright (c) 2019 Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 *  of Imply Data, Inc.
 */

package io.imply.druid.cloudwatch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class CloudWatchLogEvent
{
  private final String id;
  private final long timestamp;
  private final String message;

  @JsonCreator
  public CloudWatchLogEvent(
      @JsonProperty("id") final String id,
      @JsonProperty("timestamp") final long timestamp,
      @JsonProperty("message") final String message
  )
  {
    this.id = id;
    this.timestamp = timestamp;
    this.message = message;
  }

  @JsonProperty
  public String getId()
  {
    return id;
  }

  @JsonProperty
  public long getTimestamp()
  {
    return timestamp;
  }

  @JsonProperty
  public String getMessage()
  {
    return message;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CloudWatchLogEvent that = (CloudWatchLogEvent) o;
    return timestamp == that.timestamp &&
           Objects.equals(id, that.id) &&
           Objects.equals(message, that.message);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(id, timestamp, message);
  }

  @Override
  public String toString()
  {
    return "CloudWatchLogEvent{" +
           "id='" + id + '\'' +
           ", timestamp=" + timestamp +
           ", message='" + message + '\'' +
           '}';
  }
}
