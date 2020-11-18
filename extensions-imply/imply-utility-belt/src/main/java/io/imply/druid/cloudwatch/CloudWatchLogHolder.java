/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.cloudwatch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

public class CloudWatchLogHolder
{
  private final String messageType;
  private final String owner;
  private final String logGroup;
  private final String logStream;
  private final List<String> subscriptionFilters;
  private final List<CloudWatchLogEvent> logEvents;

  @JsonCreator
  public CloudWatchLogHolder(
      @JsonProperty("messageType") final String messageType,
      @JsonProperty("owner") final String owner,
      @JsonProperty("logGroup") final String logGroup,
      @JsonProperty("logStream") final String logStream,
      @JsonProperty("subscriptionFilters") final List<String> subscriptionFilters,
      @JsonProperty("logEvents") final List<CloudWatchLogEvent> logEvents
  )
  {
    this.messageType = messageType;
    this.owner = owner;
    this.logGroup = logGroup;
    this.logStream = logStream;
    this.subscriptionFilters = subscriptionFilters;
    this.logEvents = logEvents;
  }

  @JsonProperty
  public String getMessageType()
  {
    return messageType;
  }

  @JsonProperty
  public String getOwner()
  {
    return owner;
  }

  @JsonProperty
  public String getLogGroup()
  {
    return logGroup;
  }

  @JsonProperty
  public String getLogStream()
  {
    return logStream;
  }

  @JsonProperty
  public List<String> getSubscriptionFilters()
  {
    return subscriptionFilters;
  }

  @JsonProperty
  public List<CloudWatchLogEvent> getLogEvents()
  {
    return logEvents;
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
    final CloudWatchLogHolder that = (CloudWatchLogHolder) o;
    return Objects.equals(messageType, that.messageType) &&
           Objects.equals(owner, that.owner) &&
           Objects.equals(logGroup, that.logGroup) &&
           Objects.equals(logStream, that.logStream) &&
           Objects.equals(subscriptionFilters, that.subscriptionFilters) &&
           Objects.equals(logEvents, that.logEvents);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(messageType, owner, logGroup, logStream, subscriptionFilters, logEvents);
  }

  @Override
  public String toString()
  {
    return "CloudWatchLogHolder{" +
           "messageType='" + messageType + '\'' +
           ", owner='" + owner + '\'' +
           ", logGroup='" + logGroup + '\'' +
           ", logStream='" + logStream + '\'' +
           ", subscriptionFilters=" + subscriptionFilters +
           ", logEvents=" + logEvents +
           '}';
  }
}
