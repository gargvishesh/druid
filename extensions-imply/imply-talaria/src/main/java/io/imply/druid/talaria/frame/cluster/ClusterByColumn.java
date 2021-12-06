/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Objects;

/**
 * Represents a component of a cluster-by key definition.
 */
public class ClusterByColumn
{
  private final String columnName;
  private final boolean descending;

  @JsonCreator
  public ClusterByColumn(
      @JsonProperty("columnName") String columnName,
      @JsonProperty("descending") boolean descending
  )
  {
    if (columnName == null || columnName.isEmpty()) {
      throw new IAE("Cannot have null or empty column name");
    }

    this.columnName = columnName;
    this.descending = descending;
  }

  @JsonProperty
  public String columnName()
  {
    return columnName;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean descending()
  {
    return descending;
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
    ClusterByColumn that = (ClusterByColumn) o;
    return descending == that.descending && Objects.equals(columnName, that.columnName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columnName, descending);
  }

  @Override
  public String toString()
  {
    return StringUtils.format("%s%s", columnName, descending ? " DESC" : "");
  }
}
