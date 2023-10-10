/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.union;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;

public class UnionSubQueryOverrides
{
  private final DataSource dataSource;
  private final QuerySegmentSpec querySegmentSpec;
  private final DimFilter dimFilter;

  public UnionSubQueryOverrides(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter dimFilter
  ) {
    this.dataSource = dataSource;
    this.querySegmentSpec = querySegmentSpec;
    this.dimFilter = dimFilter;
  }

  @JsonProperty("dataSource")
  public DataSource getDataSource()
  {
    return dataSource;
  }

  @JsonProperty("intervals")
  public QuerySegmentSpec getQuerySegmentSpec()
  {
    return querySegmentSpec;
  }

  @JsonProperty("filter")
  public DimFilter getDimFilter()
  {
    return dimFilter;
  }

  public DataSource maybeFilteredDataSource()
  {
    if (dimFilter == null) {
      return dataSource;
    } else {
      throw new UOE("Cannot yet specify filters on union sub queries.");
    }
  }
}
