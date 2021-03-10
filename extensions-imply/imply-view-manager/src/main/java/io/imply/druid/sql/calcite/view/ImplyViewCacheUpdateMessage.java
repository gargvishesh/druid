/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

public class ImplyViewCacheUpdateMessage
{
  private final Map<String, ImplyViewDefinition> views;

  @JsonCreator
  public ImplyViewCacheUpdateMessage(
      @JsonProperty("views") Map<String, ImplyViewDefinition> views
  )
  {
    this.views = views;
  }

  @JsonProperty
  public Map<String, ImplyViewDefinition> getViews()
  {
    return views;
  }

  @Override
  public String toString()
  {
    return "ImplyViewCacheUpdateMessage{" +
           "views=" + views +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ImplyViewCacheUpdateMessage that = (ImplyViewCacheUpdateMessage) o;
    return Objects.equals(getViews(), that.getViews());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getViews());
  }
}
