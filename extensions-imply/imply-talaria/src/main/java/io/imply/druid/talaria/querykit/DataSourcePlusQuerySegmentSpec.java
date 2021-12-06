/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.spec.QuerySegmentSpec;

import java.util.Objects;

public class DataSourcePlusQuerySegmentSpec
{
  private final DataSource dataSource;
  private final QuerySegmentSpec querySegmentSpec;

  @JsonCreator
  public DataSourcePlusQuerySegmentSpec(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("querySegmentSpec") QuerySegmentSpec querySegmentSpec
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.querySegmentSpec = Preconditions.checkNotNull(querySegmentSpec, "querySegmentSpec");
  }

  @JsonProperty
  public DataSource getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public QuerySegmentSpec getQuerySegmentSpec()
  {
    return querySegmentSpec;
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
    DataSourcePlusQuerySegmentSpec that = (DataSourcePlusQuerySegmentSpec) o;
    return Objects.equals(dataSource, that.dataSource) && Objects.equals(querySegmentSpec, that.querySegmentSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, querySegmentSpec);
  }

  @Override
  public String toString()
  {
    return "DataSourcePlusQuerySegmentSpec{" +
           "dataSource=" + dataSource +
           ", querySegmentSpec=" + querySegmentSpec +
           '}';
  }
}
