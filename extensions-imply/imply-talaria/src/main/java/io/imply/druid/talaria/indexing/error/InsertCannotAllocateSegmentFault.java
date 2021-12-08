/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.joda.time.Interval;

@JsonTypeName(InsertCannotAllocateSegmentFault.CODE)
public class InsertCannotAllocateSegmentFault extends BaseTalariaFault
{
  static final String CODE = "InsertCannotAllocateSegment";

  private final String dataSource;
  private final Interval interval;

  @JsonCreator
  public InsertCannotAllocateSegmentFault(
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("interval") final Interval interval
  )
  {
    super(CODE, "Cannot allocate segment for dataSource [%s], interval [%s]", dataSource, interval);
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.interval = Preconditions.checkNotNull(interval, "interval");
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }
}
