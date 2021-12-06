/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Interval;

public class InsertTimeOutOfBoundsFault extends BaseTalariaFault
{
  static final String CODE = "InsertTimeOutOfBounds";

  private final Interval interval;

  public InsertTimeOutOfBoundsFault(@JsonProperty("interval") Interval interval)
  {
    super(CODE, "Query generated time chunk [%s] out of bounds specified by replaceExistingTimeChunks", interval);
    this.interval = interval;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }
}
